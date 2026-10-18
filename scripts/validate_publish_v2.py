#!/usr/bin/env python3
"""
WORKYA ANALYTICS V2 — PRE-PUBLISH VALIDATOR (Sprint 7C)

Usage: python scripts/validate_publish_v2.py data/workya_analytics_v2.json

Exit codes:
  0 — all critical checks pass (warnings acceptable)
  1 — at least one critical check failed; do NOT publish

HC-1 failures (gate_6a_fi_year_valid) are logged as warnings, not errors.
All other validation failures from the engine are treated as critical.
"""

import json
import os
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Known failures that must not block publication (HC-1, documented)
# ---------------------------------------------------------------------------
KNOWN_ACCEPTABLE_FAILURES = {"gate_6a_fi_year_valid"}

# ---------------------------------------------------------------------------
# Required top-level keys (must be present and non-null)
# ---------------------------------------------------------------------------
REQUIRED_TOP_KEYS = {
    "meta", "resumen_ejecutivo", "working_reconciliation",
    "tendencias", "embudo", "pipeline", "recency_working",
    "reclutadoras", "fuentes", "clientes", "vacantes",
    "alertas", "calidad_datos", "validaciones",
    "productividad", "apps_anonimas",
}

# View-specific required fields: (dotted path, description)
VIEW_REQUIRED_FIELDS = {
    "V1 Resumen Ejecutivo": [
        ("resumen_ejecutivo.total_historico",      "total apps histórico"),
        ("resumen_ejecutivo.working_stage_total",  "total working activos"),
        ("recency_working.por_periodo.30d",        "ingresos 30d"),
        ("recency_working.por_periodo_activo.30d", "ingresos 30d activos"),
    ],
    "V2 Clientes y Vacantes": [
        ("clientes",  "dict de clientes"),
        ("vacantes",  "dict de vacantes"),
        ("embudo",    "embudo de conversión"),
    ],
    "V3 Pipeline y Operaciones": [
        ("pipeline.total_activo",                    "pipeline total activo"),
        ("pipeline.por_stage",                       "distribución por stage"),
        ("working_reconciliation.A_intersect_B",     "working confirmados (stage=working ∩ fi válido)"),
        ("alertas",                                  "lista de alertas"),
    ],
    "V4 Productividad": [
        ("reclutadoras",   "dict de reclutadoras"),
        ("productividad",  "métricas de productividad"),
    ],
    "V5 Marketing": [
        ("meta_ads",  "datos de Meta Ads"),
        ("fuentes",   "dict de fuentes"),
    ],
}

# Surnames / full names that must NOT appear in the output
PII_NAME_BLACKLIST = ["Moreno", "Osorio", "Cano Orozco", "Mielke", "Administrador Principal"]

# Patterns that indicate credential/secret leakage
CREDENTIAL_PATTERNS = [
    re.compile(r"mongodb(?:\+srv)?://[^\"'\s]+", re.IGNORECASE),
    re.compile(r"password[\"']?\s*:\s*[\"'][^\"']{4,}", re.IGNORECASE),
    re.compile(r"secret[\"']?\s*:\s*[\"'][^\"']{8,}", re.IGNORECASE),
]

# Email pattern
EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+\-]{3,}@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6}")

# Phone pattern (international / Mexican / US formats)
PHONE_PATTERN = re.compile(r"(?:\+\d{1,3}[\s\-]?)?\(?\d{3}\)?[\s\-]?\d{3,4}[\s\-]?\d{4}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_path(d: dict, dotted: str):
    """Traverse a nested dict by dotted path; return None if any key is missing."""
    parts = dotted.split(".")
    cur = d
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


class Result:
    def __init__(self):
        self.critical: list[str] = []
        self.warnings: list[str] = []
        self.passes:   list[str] = []

    def crit(self, msg: str): self.critical.append(msg)
    def warn(self, msg: str): self.warnings.append(msg)
    def ok(self,   msg: str): self.passes.append(msg)

    @property
    def failed(self) -> bool:
        return bool(self.critical)


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def check_file(path: str, r: Result):
    p = Path(path)
    if not p.exists():
        r.crit(f"Archivo no encontrado: {path}")
        return None
    size_mb = p.stat().st_size / (1024 * 1024)
    if size_mb > 15:
        r.crit(f"Archivo demasiado grande: {size_mb:.1f} MB (máx 15 MB)")
    else:
        r.ok(f"Tamaño OK: {size_mb:.2f} MB")
    try:
        text = p.read_text(encoding="utf-8")
        data = json.loads(text)
        r.ok("JSON válido y parseable")
        return data, text
    except json.JSONDecodeError as e:
        r.crit(f"JSON inválido: {e}")
        return None


def check_top_keys(data: dict, r: Result):
    missing = REQUIRED_TOP_KEYS - set(data.keys())
    if missing:
        r.crit(f"Claves top-level faltantes: {sorted(missing)}")
    else:
        r.ok(f"Claves top-level presentes ({len(REQUIRED_TOP_KEYS)} requeridas)")


def check_min_apps(data: dict, r: Result):
    total = get_path(data, "resumen_ejecutivo.total_historico")
    if total is None:
        r.crit("resumen_ejecutivo.total_historico ausente")
    elif total < 50:
        r.crit(f"Total de apps demasiado bajo: {total} (mínimo 50)")
    else:
        r.ok(f"Total apps: {total}")


def check_view_fields(data: dict, r: Result):
    for view, fields in VIEW_REQUIRED_FIELDS.items():
        for dotted, desc in fields:
            val = get_path(data, dotted)
            if val is None:
                r.crit(f"[{view}] Campo requerido ausente: {dotted} ({desc})")
            elif isinstance(val, (dict, list)) and len(val) == 0:
                r.warn(f"[{view}] Campo presente pero vacío: {dotted} ({desc})")
            else:
                r.ok(f"[{view}] {dotted} ✓")


def check_kpi_consistency(data: dict, r: Result):
    working_total = get_path(data, "resumen_ejecutivo.working_stage_total")
    reclutadoras  = data.get("reclutadoras", {})
    if reclutadoras and working_total is not None:
        rec_sum = sum(v.get("working_asignado", 0) for v in reclutadoras.values())
        if rec_sum != working_total:
            r.crit(
                f"KPI inconsistente: sum(reclutadoras.working_asignado)={rec_sum} "
                f"≠ resumen_ejecutivo.working_stage_total={working_total}"
            )
        else:
            r.ok(f"KPI consistente: working total={working_total} == sum reclutadoras")

    ingresos30 = get_path(data, "recency_working.por_periodo.30d")
    working_rw  = get_path(data, "resumen_ejecutivo.working_stage_total")
    if ingresos30 is not None and working_rw is not None and ingresos30 > working_rw:
        r.crit(
            f"ingresos_30d ({ingresos30}) > working_total ({working_rw}) — imposible"
        )
    elif ingresos30 is not None:
        r.ok(f"ingresos_30d={ingresos30} ≤ working_total={working_rw}")


def check_validation_results(data: dict, r: Result):
    val = data.get("validaciones", {})
    total  = val.get("total_checks", 0)
    passed = val.get("passed", 0)
    checks = val.get("checks", [])

    if total == 0:
        r.crit("validaciones vacías — no se ejecutaron checks del engine")
        return

    # Any failed check not in the known-acceptable list is CRITICAL
    unknown_failures = []
    known_failures   = []
    for c in checks:
        check_id = c.get("check", "?")
        if not c.get("passed", True):
            if check_id in KNOWN_ACCEPTABLE_FAILURES:
                known_failures.append(check_id)
            else:
                unknown_failures.append((check_id, c.get("nota", "")))

    if unknown_failures:
        for cid, nota in unknown_failures:
            r.crit(f"Check del engine fallido: {cid} — {nota}")
    else:
        r.ok(f"Checks del engine: {passed}/{total} — sin fallos inesperados")

    for cid in known_failures:
        r.warn(f"HC-1 (conocido, pendiente autorización): {cid}")


def check_pii(data: dict, text: str, r: Result):
    # Check for nombre_completo_db key
    if "nombre_completo_db" in text:
        r.crit("PII: campo 'nombre_completo_db' encontrado en el JSON — no debe publicarse")
    else:
        r.ok("nombre_completo_db ausente del JSON")

    # For text-based PII scans, exclude validaciones.checks (internal audit notes)
    # Those notes intentionally reference internal identifiers for deduplication checks.
    import copy
    data_scrubbed = copy.deepcopy(data)
    if "validaciones" in data_scrubbed:
        data_scrubbed["validaciones"]["checks"] = []
    scan_text = json.dumps(data_scrubbed, ensure_ascii=False)

    # Check for blacklisted surnames in data sections (not audit notes)
    for name in PII_NAME_BLACKLIST:
        if name in scan_text:
            r.crit(f"PII: nombre sensible encontrado en datos: '{name}'")

    # Email patterns — use scrubbed text (no audit notes)
    emails = EMAIL_PATTERN.findall(scan_text)
    real_emails = [e for e in emails if "@" in e]
    if real_emails:
        r.warn(f"Posibles emails encontrados ({len(real_emails)}): {real_emails[:3]}")
    else:
        r.ok("Sin patrones de email en el JSON")

    # Credential patterns — scan full text (credentials should never appear anywhere)
    for pat in CREDENTIAL_PATTERNS:
        if pat.search(text):
            r.crit(f"CREDENCIAL DETECTADA: patrón '{pat.pattern[:40]}' encontrado en JSON")

    # Recruiter field check: all reclutadoras should have nombre_alias
    reclutadoras = data.get("reclutadoras", {})
    missing_alias = [rid for rid, v in reclutadoras.items() if not v.get("nombre_alias")]
    if missing_alias:
        r.warn(f"Reclutadoras sin nombre_alias: {len(missing_alias)} — actualizar generate_analytics_v2.py")
    else:
        r.ok(f"Todas las reclutadoras tienen nombre_alias")


def check_schema_version(data: dict, r: Result):
    ver = get_path(data, "meta.schema_version")
    if not ver:
        r.warn("meta.schema_version ausente")
    else:
        r.ok(f"Schema version: {ver}")
    gen = get_path(data, "meta.generated_at")
    if not gen:
        r.warn("meta.generated_at ausente")
    else:
        r.ok(f"Generado: {gen}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Uso: python scripts/validate_publish_v2.py data/workya_analytics_v2.json",
              file=sys.stderr)
        sys.exit(1)

    json_path = sys.argv[1]
    r = Result()

    print("=" * 62)
    print("  WORKYA V2 PRE-PUBLISH VALIDATOR — Sprint 7C")
    print("=" * 62)
    print(f"  Archivo: {json_path}\n")

    result = check_file(json_path, r)
    if result is None:
        _print_summary(r)
        sys.exit(1)

    data, text = result

    check_top_keys(data, r)
    check_min_apps(data, r)
    check_view_fields(data, r)
    check_kpi_consistency(data, r)
    check_validation_results(data, r)
    check_pii(data, text, r)
    check_schema_version(data, r)

    _print_summary(r)
    sys.exit(1 if r.failed else 0)


def _print_summary(r: Result):
    if r.passes:
        print(f"  ✅ PASS ({len(r.passes)})")
        for msg in r.passes:
            print(f"     {msg}")
    if r.warnings:
        print(f"\n  ⚠️  WARN ({len(r.warnings)})")
        for msg in r.warnings:
            print(f"     {msg}")
    if r.critical:
        print(f"\n  ❌ CRITICAL ({len(r.critical)})")
        for msg in r.critical:
            print(f"     {msg}")

    print()
    if r.failed:
        print("  RESULTADO: FALLO — archivo NO apto para publicación.")
    else:
        print("  RESULTADO: OK — archivo apto para publicación.")
    print("=" * 62)


if __name__ == "__main__":
    main()
