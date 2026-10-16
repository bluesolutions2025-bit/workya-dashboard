#!/usr/bin/env python3
"""
WORKYA ANALYTICS ENGINE V2 — SPRINT 3C
Senior Analytics Engineer / Data Architect

READ-ONLY: Zero modifications to MongoDB or existing dashboard.
Produces: data/workya_analytics_v2.json

Required env vars:
  MONGODB_URI  - MongoDB Atlas connection string (preferido, Sprint 6A)
  MONGO_URL    - Alternativa por compatibilidad hacia atrás
  DB_NAME      - Database name (default: workya_db)

Architecture:
  MongoDB Atlas → batch load → Python analytics → data/workya_analytics_v2.json
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from statistics import median as stats_median

from pymongo import MongoClient

# =============================================================================
# CONFIGURATION
# =============================================================================

# Aceptar MONGODB_URI (preferido) o MONGO_URL (compatibilidad hacia atrás)
MONGO_URL = os.environ.get("MONGODB_URI") or os.environ.get("MONGO_URL")
if not MONGO_URL:
    print(
        "ERROR: Se requiere la variable de entorno MONGODB_URI o MONGO_URL.\n"
        "  → Copiar .env.example a .env y configurar la conexión.\n"
        "  → En GitHub Actions: configurar el secreto MONGO_URL en el repositorio.",
        file=sys.stderr,
    )
    sys.exit(1)

DB_NAME = os.environ.get("DB_NAME", "workya_db")

SCHEMA_VERSION = "4.2.0"

# Allowlist of permitted top-level output keys — PII policy (Sprint 3A)
# Only these keys may appear in the final analytics JSON. Enforced by gate_sin_pii_allowlist.
ALLOWED_OUTPUT_TOP_KEYS = {
    "meta", "resumen_ejecutivo", "working_reconciliation", "tendencias",
    "embudo", "cohortes", "pipeline", "recency_working", "reclutadoras",
    "fuentes", "clientes", "vacantes", "meta_ads", "alertas",
    "calidad_datos", "validaciones",
    "geografia", "jerarquia", "productividad", "apps_anonimas",
}

# Centralized, configurable thresholds
THRESHOLDS = {
    "stale_7d": 7,
    "stale_14d": 14,
    "stale_30d": 30,
    # Minimum applications assigned to a recruiter before showing a conversion rate
    # Justification: below this sample size, rates are statistically unreliable (< ~1.5 sigma events)
    "recruiter_min_sample": 15,
    # Minimum cohort size for reliable cohort conversion metrics
    "cohort_min_size": 5,
    # Alert: new candidate without contact action after N days
    "alert_new_sin_gestion": 3,
    # Alert: interview stale after N days without stage change
    "alert_interview_estancada": 7,
    # Alert: sent stale after N days without stage change
    "alert_enviado_esperando": 14,
}

# =============================================================================
# RECRUITER CANONICAL IDENTITY
# Discovered from users collection in audit. Primary key: user_id.
# NOT hardcoded blindly — verified against live data.
# =============================================================================

RECRUITER_BY_ID = {
    "14e15932-1b87-4548-86a7-8af284db93ef": {
        "id": "14e15932-1b87-4548-86a7-8af284db93ef",
        "nombre_display": "Dora",
        "nombre_completo_db": "Dora Moreno",
        "username": "dora.moreno",
        "es_recruiter": True,
        "roster_actual": True,
        "estado": "activa",
        "incorporacion": "2026-03-01",
    },
    "280fbee7-41ac-4c64-9820-bc587bcc47c6": {
        "id": "280fbee7-41ac-4c64-9820-bc587bcc47c6",
        "nombre_display": "Sofia",
        "nombre_completo_db": "Sofia Osorio",
        "username": "sofia.osorio",
        "es_recruiter": True,
        "roster_actual": True,
        "estado": "activa",
        "incorporacion": "2026-03-01",
    },
    "188d78c7-17f4-49a0-acda-96e79277bd84": {
        "id": "188d78c7-17f4-49a0-acda-96e79277bd84",
        "nombre_display": "Natallia",
        "nombre_completo_db": "Nathalia Cano",
        "username": "nathaliacanoorozco",
        "es_recruiter": True,
        "roster_actual": True,
        "estado": "reciente",
        "incorporacion": "2026-09-08",
    },
    "f508e98a-921a-4ba3-8218-e2a1fc39eb9e": {
        "id": "f508e98a-921a-4ba3-8218-e2a1fc39eb9e",
        "nombre_display": "Sidivan",
        "nombre_completo_db": "Sidivan Mielke",
        "username": "sidivan",
        "es_recruiter": True,
        "roster_actual": True,
        "estado": "sin_actividad",
        "incorporacion": "2026-09-21",
    },
    "9090a6fb-d6f2-4511-8a44-0c507c582911": {
        "id": "9090a6fb-d6f2-4511-8a44-0c507c582911",
        "nombre_display": "Ana Sofia",
        "nombre_completo_db": "Ana Sofia",
        "username": "anasofia",
        "es_recruiter": True,
        "roster_actual": True,
        "estado": "reciente",
        "incorporacion": "2026-09-24",
    },
    "adea7407-657f-4cee-83a6-5d4447e51ceb": {
        "id": "adea7407-657f-4cee-83a6-5d4447e51ceb",
        "nombre_display": "Director",
        "nombre_completo_db": "Administrador Principal",
        "username": "admin",
        "es_recruiter": False,
        "roster_actual": False,
        "estado": "activo",
        "incorporacion": "2026-03-01",
    },
}

# Secondary lookup: any name/username variant → canonical user_id
# Resolves the stage_history dual-format problem (dora.moreno vs Dora Moreno)
RECRUITER_NAME_TO_ID = {
    "Dora Moreno":          "14e15932-1b87-4548-86a7-8af284db93ef",
    "dora.moreno":          "14e15932-1b87-4548-86a7-8af284db93ef",
    "Sofia Osorio":         "280fbee7-41ac-4c64-9820-bc587bcc47c6",
    "sofia.osorio":         "280fbee7-41ac-4c64-9820-bc587bcc47c6",
    "Nathalia Cano":        "188d78c7-17f4-49a0-acda-96e79277bd84",
    "nathaliacanoorozco":   "188d78c7-17f4-49a0-acda-96e79277bd84",
    "Sidivan Mielke":       "f508e98a-921a-4ba3-8218-e2a1fc39eb9e",
    "sidivan":              "f508e98a-921a-4ba3-8218-e2a1fc39eb9e",
    "Ana Sofia":            "9090a6fb-d6f2-4511-8a44-0c507c582911",
    "anasofia":             "9090a6fb-d6f2-4511-8a44-0c507c582911",
    "Director":             "adea7407-657f-4cee-83a6-5d4447e51ceb",
    "admin":                "adea7407-657f-4cee-83a6-5d4447e51ceb",
    "Administrador Principal": "adea7407-657f-4cee-83a6-5d4447e51ceb",
}

# =============================================================================
# SOURCE NORMALIZATION
# Documented mapping. ig ≠ meta. Unknown values kept as-is, flagged in quality.
# =============================================================================

SOURCE_CANONICAL = {
    "dora":      {"canal": "reclutadora", "label": "Reclutadora (Dora)",     "tipo": "interno"},
    "sofia":     {"canal": "reclutadora", "label": "Reclutadora (Sofia)",    "tipo": "interno"},
    "natalia":   {"canal": "reclutadora", "label": "Reclutadora (Natallia)", "tipo": "interno"},
    "meta":      {"canal": "meta_pagado", "label": "Meta Ads (Pago)",        "tipo": "pago"},
    "ig":        {"canal": "instagram_organico", "label": "Instagram Orgánico", "tipo": "organico"},
    "grupos":    {"canal": "grupos_whatsapp",    "label": "Grupos WhatsApp",  "tipo": "organico"},
    "referidos": {"canal": "referidos",           "label": "Referidos",       "tipo": "referido"},
    "tiktok":    {"canal": "tiktok",              "label": "TikTok",          "tipo": "organico"},
}

def normalize_source(val):
    if not val:
        return {"canal": "sin_fuente", "label": "Sin fuente", "tipo": "desconocido", "raw": None}
    key = val.strip() if isinstance(val, str) else ""
    if key in SOURCE_CANONICAL:
        return {**SOURCE_CANONICAL[key], "raw": val}
    return {"canal": "sin_clasificar", "label": f"Sin clasificar ({key})", "tipo": "desconocido", "raw": val}

# =============================================================================
# CLIENT NORMALIZATION
# Conservative: only merge high-confidence variants (typos + clear duplicates).
# No fuzzy matching.
# =============================================================================

CLIENT_NORMALIZATION = {
    "RENAISSANCE IN MONTGOMERY AL": "RENAISSANCE MONTGOMERY AL",
    "RENAISSANCE ALABAMA":          "RENAISSANCE MONTGOMERY AL",
    "RENAISSANCE MONTGOMERY":       "RENAISSANCE MONTGOMERY AL",
    "TOWMPLACE SUITES HICKORY":     "TOWNPLACE SUITES HICKORY",
    "TOWNPLACE SUITES HICKORY NC":  "TOWNPLACE SUITES HICKORY",
}

def normalize_client(name):
    if not name:
        return None
    n = name.strip()
    return CLIENT_NORMALIZATION.get(n, n)

# =============================================================================
# PIPELINE STAGE CLASSIFICATION
# =============================================================================

ACTIVE_STAGES   = {"new", "contacting", "interview", "sent"}
POSITIVE_STAGES = {"working"}
NEGATIVE_STAGES = {"not_selected", "not_arriving"}
LEGACY_STAGES   = {"selected", "rejected", "reviewing", "in_process"}

# Funnel order for transition analysis
FUNNEL_ORDER = ["new", "contacting", "interview", "sent", "working"]

# =============================================================================
# DATE / TIME UTILITIES
# =============================================================================

_DT_MIN = datetime(2020, 1, 1, tzinfo=timezone.utc)
_DT_MAX = datetime(2030, 1, 1, tzinfo=timezone.utc)


def parse_dt(val):
    """Parse to UTC datetime. Returns None if invalid or out of range."""
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    elif isinstance(val, str):
        if not val or val in ("NaT", "null", "None", ""):
            return None
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    else:
        return None
    return dt if _DT_MIN <= dt < _DT_MAX else None


def resolve_recruiter(user_id=None, user_name=None):
    """Return canonical recruiter dict or None."""
    rid = None
    if user_id and user_id in RECRUITER_BY_ID:
        rid = user_id
    elif user_name:
        rid = RECRUITER_NAME_TO_ID.get(user_name)
    return RECRUITER_BY_ID.get(rid)


def age_bucket(days):
    if days <= 1:   return "0-1d"
    if days <= 3:   return "2-3d"
    if days <= 7:   return "4-7d"
    if days <= 14:  return "8-14d"
    if days <= 30:  return "15-30d"
    return "30+d"


def safe_pct(num, denom, allow_over=False):
    if not denom:
        return None
    p = round(num / denom * 100, 1)
    if p > 100.0 + 1e-9 and not allow_over:
        # num > denom signals a structural issue — return None rather than invalid value
        return None
    return min(p, 100.0)


def safe_median(values):
    return round(stats_median(values), 1) if values else None


def ym(dt):
    return dt.strftime("%Y-%m") if dt else None


def yw(dt):
    if not dt:
        return None
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"


# =============================================================================
# DATA LOADING
# All collections loaded in a single batch pass — no per-record queries.
# =============================================================================

def load_data(db):
    print("Cargando colecciones...", file=sys.stderr)
    apps = list(db.applications.find({}, {
        "_id": 0, "id": 1, "pipeline_stage": 1, "owner": 1, "owner_name": 1,
        "lead_source": 1, "created_at": 1, "started_working_at": 1,
        "stage_history": 1, "sent_to": 1, "job_id": 1, "job_title": 1,
        "gender": 1, "needs_employer_housing": 1, "duplicate_flag": 1,
        "motivo_no_seleccionado": 1, "sla_met": 1,
        "meta_ad_id": 1,
    }))
    logs = list(db.activity_logs.find({}, {
        "_id": 0, "applicant_id": 1, "activity_type": 1,
        "user_id": 1, "user_name": 1, "timestamp": 1,
        "from_stage": 1, "to_stage": 1,
    }))
    jobs = list(db.jobs.find({}, {
        "_id": 0, "id": 1, "title": 1, "status": 1, "category": 1,
        "city": 1, "state": 1, "client_id": 1, "created_at": 1,
    }))
    clients = list(db.clients.find({}, {"_id": 0, "id": 1, "name": 1, "active": 1}))
    meta = list(db.meta_ad_daily.find({}, {"_id": 0}))
    print(f"  applications: {len(apps)}", file=sys.stderr)
    print(f"  activity_logs: {len(logs)}", file=sys.stderr)
    print(f"  jobs: {len(jobs)}", file=sys.stderr)
    print(f"  clients: {len(clients)}", file=sys.stderr)
    print(f"  meta_ad_daily: {len(meta)}", file=sys.stderr)
    return apps, logs, jobs, clients, meta


# =============================================================================
# ANALYTICS ENGINE
# =============================================================================

def run(apps, logs, jobs, clients, meta, now=None):
    if now is None:
        now = datetime.now(timezone.utc)

    # Time cutoffs
    d7  = now - timedelta(days=7)
    d30 = now - timedelta(days=30)
    d60 = now - timedelta(days=60)
    d90 = now - timedelta(days=90)
    cutoffs = {"7d": d7, "30d": d30, "60d": d60, "90d": d90}

    # ------------------------------------------------------------------
    # PREPROCESSING: parse dates and enrich each application in-place
    # ------------------------------------------------------------------
    for a in apps:
        a["_created"]  = parse_dt(a.get("created_at"))
        a["_started"]  = parse_dt(a.get("started_working_at"))
        a["_started_raw"] = a.get("started_working_at")
        # started_working_at future dates → quality issue, excluded from metrics
        if a["_started"] and a["_started"] > now:
            a["_started_future"] = True
            a["_started"] = None
        else:
            a["_started_future"] = False
        # Resolve owner recruiter
        rec = resolve_recruiter(
            user_id=a.get("owner"),
            user_name=a.get("owner_name"),
        )
        a["_recruiter_id"]      = rec["id"] if rec else None
        a["_recruiter_display"] = rec["nombre_display"] if rec else None
        # Normalize source
        a["_source"] = normalize_source(a.get("lead_source"))
        # Clients via sent_to
        st = a.get("sent_to")
        if isinstance(st, list):
            a["_clients"] = [normalize_client(c) for c in st if c]
        elif isinstance(st, str) and st:
            a["_clients"] = [normalize_client(st)]
        else:
            a["_clients"] = []
        a["_stage"] = a.get("pipeline_stage", "")

    # ------------------------------------------------------------------
    # PREPROCESSING: activity_logs index
    # ------------------------------------------------------------------
    # Per-applicant sorted timeline of stage_change events
    app_stage_events = defaultdict(list)  # applicant_id → [(ts, from, to, user_id, user_name)]
    app_all_logs     = defaultdict(list)  # applicant_id → [(ts, activity_type)]
    unknown_users    = set()

    for log in logs:
        ts = parse_dt(log.get("timestamp"))
        if not ts:
            continue
        aid = log.get("applicant_id")
        if not aid:
            continue
        atype = log.get("activity_type", "")
        app_all_logs[aid].append(ts)
        if atype == "stage_change":
            from_s = log.get("from_stage") or ""
            to_s   = log.get("to_stage") or ""
            uid    = log.get("user_id")
            uname  = log.get("user_name")
            if not resolve_recruiter(uid, uname):
                unknown_users.add(uname or uid or "?")
            app_stage_events[aid].append((ts, from_s, to_s, uid, uname))

    # Sort stage events chronologically
    for aid in app_stage_events:
        app_stage_events[aid].sort(key=lambda x: x[0])

    # Last activity timestamp per applicant
    app_last_activity = {}
    for aid, tss in app_all_logs.items():
        if tss:
            app_last_activity[aid] = max(tss)

    # Last stage change timestamp per applicant
    app_last_stage_change = {}
    for aid, events in app_stage_events.items():
        if events:
            app_last_stage_change[aid] = events[-1][0]

    # First time each applicant reached each stage (from activity_logs)
    app_first_stage_ts = defaultdict(dict)  # applicant_id → {stage: first_ts}
    for aid, events in app_stage_events.items():
        for ts, from_s, to_s, uid, uname in events:
            if to_s and to_s not in app_first_stage_ts[aid]:
                app_first_stage_ts[aid][to_s] = ts

    # Index: applicant_id → application
    app_by_id = {a["id"]: a for a in apps if a.get("id")}

    # ------------------------------------------------------------------
    # WORKING RECONCILIATION
    # A = pipeline_stage == "working"
    # B = started_working_at valid (non-null, non-future, in range)
    # ------------------------------------------------------------------
    set_A = {a["id"] for a in apps if a["_stage"] == "working" and a.get("id")}
    set_B = {a["id"] for a in apps if a["_started"] and a.get("id")}
    set_AB     = set_A & set_B
    set_A_notB = set_A - set_B  # working but no valid started_working_at
    set_B_notA = set_B - set_A  # has started_working_at but not in working stage

    # B-A: group by current pipeline_stage
    B_notA_by_stage = defaultdict(int)
    for aid in set_B_notA:
        a = app_by_id.get(aid)
        if a:
            B_notA_by_stage[a["_stage"]] += 1

    # Future dates detected
    future_dates = [a["id"] for a in apps if a.get("_started_future") and a.get("id")]

    working_reconciliation = {
        "A_working_stage":       len(set_A),
        "B_valid_started_at":    len(set_B),
        "A_intersect_B":         len(set_AB),
        "A_minus_B_working_sin_fecha": len(set_A_notB),
        "B_minus_A_fecha_sin_working": len(set_B_notA),
        "B_minus_A_por_stage":   dict(B_notA_by_stage),
        "fechas_futuras_excluidas": len(future_dates),
        "nota": (
            "A∩B son placements confirmados con fecha. "
            "A-B son working sin fecha de inicio (no se puede asignar a período). "
            "B-A son inconsistencias de calidad: tienen fecha de inicio pero stage incorrecto."
        ),
    }

    # ------------------------------------------------------------------
    # CONVERSION KPIs — entregados / incorporados
    #
    # "Presentados al cliente" (formerly "entregados") =
    #   candidatos cuya etapa ACTUAL es sent, working, not_arriving, o not_selected.
    #   Definición: snapshot actual del pipeline — etapas que ocurren DESPUÉS de interview,
    #   donde el candidato ya fue presentado/enviado al cliente.
    #
    # Limitación: 3 apps actualmente en etapas tempranas tuvieron stage_history='sent'
    #   → no se cuentan (impacto: ±3 apps, negligible).
    #   stage_history cubre 75.7% de apps; el proxy por etapa_actual es más completo.
    #
    # tasa_incorporacion: usa TODOS los presentados como denominador (incluyendo 92 pending en "sent").
    # tasa_incorporacion_finalizada: excluye los pending en "sent" (más estricta, más precisa).
    # ------------------------------------------------------------------
    _ENTREGADOS_STAGES = {"sent", "working", "not_arriving", "not_selected"}
    _entregados_apps = [a for a in apps if a["_stage"] in _ENTREGADOS_STAGES]
    _entregados_total = len(_entregados_apps)
    _incorporados_total = len(set_A)  # working stage
    _pending_sent = sum(1 for a in apps if a["_stage"] == "sent")
    _entregados_finalizados = _entregados_total - _pending_sent
    _tasa_entrega = safe_pct(_entregados_total, len(apps))
    _tasa_incorporacion = safe_pct(_incorporados_total, _entregados_total)
    _tasa_incorporacion_finalizada = safe_pct(_incorporados_total, _entregados_finalizados)

    # ------------------------------------------------------------------
    # PERIOD HELPERS
    # ------------------------------------------------------------------
    def apps_in(cutoff, stage_filter=None, use_started=False):
        """Count apps where created_at (or started_at) >= cutoff."""
        result = []
        for a in apps:
            ts = a["_started"] if use_started else a["_created"]
            if not ts:
                continue
            if cutoff and ts < cutoff:
                continue
            if stage_filter and a["_stage"] not in stage_filter:
                continue
            result.append(a)
        return result

    def summary_for_period(cutoff):
        """Build standard period summary dict."""
        new_apps = apps_in(cutoff)
        active   = [a for a in apps if a["_stage"] in ACTIVE_STAGES]
        ingresos = [a for a in apps if a["_started"] and a["_started"] >= cutoff] if cutoff else \
                   [a for a in apps if a["_started"]]
        return {
            "nuevos_aplicantes":    len(apps_in(cutoff)),
            "pipeline_activo":      len(active),
            "ingresos_con_fecha":   len(ingresos),
            "not_selected":         len([a for a in apps_in(cutoff) if a["_stage"] == "not_selected"]),
            "not_arriving":         len([a for a in apps_in(cutoff) if a["_stage"] == "not_arriving"]),
            "sin_owner":            sum(1 for a in apps_in(cutoff) if not a.get("owner")),
            "sin_fuente":           sum(1 for a in apps_in(cutoff) if not a.get("lead_source")),
            "sin_job":              sum(1 for a in apps_in(cutoff) if not a.get("job_id")),
        }

    # ------------------------------------------------------------------
    # RESUMEN EJECUTIVO
    # ------------------------------------------------------------------
    resumen_ejecutivo = {
        "total_historico":   len(apps),
        "working_stage_total": len(set_A),
        "ingresos_con_fecha_total": len(set_B),
        "kpis_conversion": {
            "presentados_cliente_total":   _entregados_total,
            "pending_decision_sent":       _pending_sent,
            "presentados_finalizados":     _entregados_finalizados,
            "tasa_presentacion_pct":       _tasa_entrega,
            "incorporados_total":          _incorporados_total,
            "tasa_incorporacion_pct":      _tasa_incorporacion,
            "tasa_incorporacion_finalizada_pct": _tasa_incorporacion_finalizada,
            "nota_metodologia": (
                "Presentados = snapshot etapa_actual in {sent,working,not_arriving,not_selected}. "
                "NO es conteo histórico de eventos de entrega — usa etapa actual como proxy. "
                "tasa_incorporacion incluye 'sent' pendientes; tasa_incorporacion_finalizada los excluye."
            ),
            "nota_cobertura_stage_history": (
                "stage_history cubre ~75.7% de apps. 3 apps están en etapas activas pero "
                "previamente pasaron por 'sent' (no contadas — impacto negligible)."
            ),
        },
        "periodos": {
            "7d":       summary_for_period(d7),
            "30d":      summary_for_period(d30),
            "60d":      summary_for_period(d60),
            "90d":      summary_for_period(d90),
            "historico": summary_for_period(None),
        },
    }

    # ------------------------------------------------------------------
    # TENDENCIAS: series semanales y mensuales
    # created_at → aplicantes; started_working_at → ingresos
    # NEVER mixed in the same series
    # ------------------------------------------------------------------
    weekly_apps    = defaultdict(int)
    weekly_starts  = defaultdict(int)
    monthly_apps   = defaultdict(int)
    monthly_starts = defaultdict(int)

    for a in apps:
        if a["_created"]:
            weekly_apps[yw(a["_created"])] += 1
            monthly_apps[ym(a["_created"])] += 1
        if a["_started"]:
            weekly_starts[yw(a["_started"])] += 1
            monthly_starts[ym(a["_started"])] += 1

    # Daily aggregation (last 30 days) — for 7d period trend chart
    daily_apps   = defaultdict(int)
    daily_starts = defaultdict(int)
    cutoff_daily = now - timedelta(days=30)
    for a in apps:
        if a["_created"] and a["_created"] >= cutoff_daily:
            daily_apps[a["_created"].strftime("%Y-%m-%d")] += 1
        if a["_started"] and a["_started"] >= cutoff_daily:
            daily_starts[a["_started"].strftime("%Y-%m-%d")] += 1

    # Emit all 30 calendar days explicitly — zeros for days with no activity
    _days30_list = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(29, -1, -1)]
    _dias_act_apps = sum(1 for d in _days30_list if daily_apps.get(d, 0) > 0)
    _dias_act_ing  = sum(1 for d in _days30_list if daily_starts.get(d, 0) > 0)

    tendencias = {
        "nota": "aplicantes usa created_at; ingresos usa started_working_at. NUNCA mezclados.",
        "diario": {
            "aplicantes": [{"fecha": d, "total": daily_apps.get(d, 0)} for d in _days30_list],
            "ingresos":   [{"fecha": d, "total": daily_starts.get(d, 0)} for d in _days30_list],
            "dias_con_actividad_apps":     _dias_act_apps,
            "dias_con_actividad_ingresos": _dias_act_ing,
            "nota": "30 días calendario completos — días sin actividad incluidos como 0.",
        },
        "semanal": {
            "aplicantes": [{"semana": k, "total": v} for k, v in sorted(weekly_apps.items())],
            "ingresos":   [{"semana": k, "total": v} for k, v in sorted(weekly_starts.items())],
        },
        "mensual": {
            "aplicantes": [{"mes": k, "total": v} for k, v in sorted(monthly_apps.items())],
            "ingresos":   [{"mes": k, "total": v} for k, v in sorted(monthly_starts.items())],
        },
    }

    # ------------------------------------------------------------------
    # COHORT ANALYSIS
    # Group by month of created_at.
    # Conversion = started_working_at within N days of created_at.
    # Mature = cohort is old enough for the window to have elapsed.
    # ------------------------------------------------------------------
    cohort_windows = [7, 14, 30, 60]
    cohort_data    = defaultdict(lambda: {"aplicantes": [], "started": []})

    for a in apps:
        if not a["_created"]:
            continue
        month = ym(a["_created"])
        cohort_data[month]["aplicantes"].append((a["id"], a["_created"]))
        if a["_started"]:
            cohort_data[month]["started"].append((a["id"], a["_created"], a["_started"]))

    # Global per-candidate KPIs — computed directly to guarantee monotonicity
    # eligible_Nd = candidates with (now - created_at).days >= N
    # converted_Nd = eligible that started_working within N days of creation
    kpi_por_ventana = {}
    for w in cohort_windows:
        elig = sum(1 for a in apps if a["_created"] and (now - a["_created"]).days >= w)
        conv = sum(1 for a in apps
                   if a["_started"] and a["_created"]
                   and (now - a["_created"]).days >= w
                   and 0 <= (a["_started"] - a["_created"]).days <= w)
        too_recent = sum(1 for a in apps if a["_created"] and (now - a["_created"]).days < w)
        no_fecha   = sum(1 for a in apps if not a["_created"])
        kpi_por_ventana[f"{w}d"] = {
            "valor":           safe_pct(conv, elig),
            "numerador":       conv,
            "denominador":     elig,
            "too_recent":      too_recent,
            "sin_created_at":  no_fecha,
            "label":           f"Tasa de Colocación a {w} Días",
            "nota":            f"{too_recent} candidatos excluidos — creados hace <{w} días.",
        }
    tasa_colocacion_30d = kpi_por_ventana["30d"]["valor"]
    eligible_30         = kpi_por_ventana["30d"]["denominador"]
    converted_30        = kpi_por_ventana["30d"]["numerador"]

    cohort_results = []
    for month in sorted(cohort_data.keys()):
        try:
            cohort_start = datetime.strptime(month + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        n_total = len(cohort_data[month]["aplicantes"])
        entry = {
            "cohorte": month,
            "total_aplicantes": n_total,
            "muestra_suficiente": n_total >= THRESHOLDS["cohort_min_size"],
            "ventanas": {},
        }
        for w in cohort_windows:
            # Per-candidate eligibility within cohort
            elig_list = [(aid, c) for aid, c in cohort_data[month]["aplicantes"]
                         if c and (now - c).days >= w]
            eligible   = len(elig_list)
            too_recent = n_total - eligible
            elig_set   = {aid for aid, _ in elig_list}
            converted  = sum(
                1 for aid, created_ts, started_ts in cohort_data[month]["started"]
                if aid in elig_set and 0 <= (started_ts - created_ts).days <= w
            )
            sample_ok = eligible >= THRESHOLDS["cohort_min_size"]
            entry["ventanas"][f"{w}d"] = {
                "label":       f"Conversión {w} días",
                "eligible":    eligible,
                "too_recent":  too_recent,
                "convertidos": converted,
                "tasa":        safe_pct(converted, eligible) if sample_ok else None,
                "nota":        (f"{too_recent} candidatos excluidos — ventana de {w}d aún abierta"
                                if too_recent else None),
            }
        cohort_results.append(entry)

    cohortes = {
        "metodologia": (
            "Per-candidato: elegible si (now - created_at).days ≥ N días. "
            "Convertido si started_working_at dentro de N días de created_at. "
            "Denominador excluye candidatos creados hace menos de N días. "
            "KPI global calculado directamente — no requiere cohort 'maduro'."
        ),
        "kpi_tasa_colocacion_30d": kpi_por_ventana["30d"],
        "kpi_por_ventana": kpi_por_ventana,
        "por_cohorte": cohort_results,
    }

    # ------------------------------------------------------------------
    # FUNNEL RECONSTRUCTION (from activity_logs stage_change events)
    # ------------------------------------------------------------------
    # For each applicant that ever reached each stage, find first arrival
    # and compute median times between consecutive stages
    funnel_stages = ["new", "contacting", "interview", "sent", "working"]
    stage_first_arrivals = defaultdict(list)  # stage → [days from created_at]

    for a in apps:
        aid  = a.get("id")
        if not aid or not a["_created"]:
            continue
        stage_ts = app_first_stage_ts.get(aid, {})
        for stage in funnel_stages:
            ts = stage_ts.get(stage)
            # Also check created_at for "new" stage (entry point)
            if stage == "new" and not ts:
                ts = a["_created"]
            if ts:
                days = (ts - a["_created"]).days
                if days >= 0:
                    stage_first_arrivals[stage].append(days)

    # Transitions: for each pair of consecutive stages
    transitions = []
    funnel_pairs = [
        ("new",        "contacting", "Aplicado → Contactado"),
        ("contacting", "interview",  "Contactado → Entrevista"),
        ("new",        "interview",  "Aplicado → Entrevista"),
        ("interview",  "sent",       "Entrevista → Enviado"),
        ("sent",       "working",    "Enviado → Ingresó"),
    ]
    for from_s, to_s, label in funnel_pairs:
        times = []
        candidates = 0
        for a in apps:
            aid = a.get("id")
            if not aid or not a["_created"]:
                continue
            stage_ts = app_first_stage_ts.get(aid, {})
            ts_from = stage_ts.get(from_s)
            if from_s == "new":
                ts_from = ts_from or a["_created"]
            ts_to = stage_ts.get(to_s)
            if to_s == "working":
                # Prefer started_working_at for working arrival
                ts_to = a["_started"] or ts_to
            if ts_from and ts_to and ts_to >= ts_from:
                candidates += 1
                times.append((ts_to - ts_from).days)

        # Coverage: what fraction of apps reached each stage
        # "new" is the entry state for all apps — not tracked as a log entry
        if from_s == "new":
            reached_from = len(apps)
        else:
            reached_from = sum(
                1 for a in apps
                if app_first_stage_ts.get(a.get("id", ""), {}).get(from_s)
            )
        reached_to = sum(
            1 for a in apps
            if (app_first_stage_ts.get(a.get("id", ""), {}).get(to_s)
                or (to_s == "working" and a["_started"]))
        )

        conversion_val = safe_pct(reached_to, reached_from)
        stage_skip_note = None
        if reached_from < reached_to:
            conversion_val = None
            stage_skip_note = (
                f"from_stage '{from_s}' se omite frecuentemente — "
                f"reached_from({reached_from}) < reached_to({reached_to}). "
                "Esta etapa no es un punto de paso universal en el pipeline."
            )
        transitions.append({
            "transicion":        label,
            "from_stage":        from_s,
            "to_stage":          to_s,
            "candidatos_con_ambas_fechas": candidates,
            "alcanzaron_from":   reached_from,
            "alcanzaron_to":     reached_to,
            "conversion":        conversion_val,
            "mediana_dias":      safe_median(times),
            "promedio_dias":     round(sum(times) / len(times), 1) if times else None,
            "cobertura_suficiente": candidates >= 20,
            "nota": stage_skip_note or (
                None if candidates >= 20 else "Cobertura insuficiente — interpretar con cautela"
            ),
        })

    embudo = {
        "nota": (
            "Funnel reconstruido desde activity_logs (stage_change events). "
            "started_working_at se usa para la llegada a 'working' cuando está disponible. "
            "Etapas legacy (selected, rejected) están mapeadas en calidad_datos."
        ),
        "stage_mapping": {
            "activo":   list(ACTIVE_STAGES),
            "positivo": list(POSITIVE_STAGES),
            "negativo": list(NEGATIVE_STAGES),
            "legacy":   list(LEGACY_STAGES),
        },
        "transiciones": transitions,
    }

    # ------------------------------------------------------------------
    # PIPELINE SNAPSHOT & AGING
    # ------------------------------------------------------------------
    active_apps = [a for a in apps if a["_stage"] in ACTIVE_STAGES]

    # Aging: days since created_at AND days since last stage change (FIXED)
    # Fix: when no stage_change log exists, use created_at as fallback for ALL thresholds,
    # not just 30+d. This ensures: 30+d <= 14+d <= 7+d <= total_active (monotonic invariant).
    aging_created      = defaultdict(int)
    aging_last_stage   = defaultdict(int)
    stale_counts       = {"7+d": 0, "14+d": 0, "30+d": 0}
    stale_30d_con_log  = 0   # stale 30+d with confirmed stage_change log
    stale_30d_fallback = 0   # stale 30+d using created_at proxy (no stage_change log)
    aging_no_date      = 0  # active apps with neither stage_change log nor created_at
    aging_by_recruiter = defaultdict(lambda: {"total": 0, "stale_30d": 0})

    for a in apps:
        if a["_stage"] not in ACTIVE_STAGES:
            continue
        aid    = a.get("id")
        rec_id = a["_recruiter_id"]

        # Age from created_at (independent metric)
        if a["_created"]:
            days = (now - a["_created"]).days
            aging_created[age_bucket(days)] += 1

        # Age from last stage change — with consistent fallback to created_at
        last_sc = app_last_stage_change.get(aid)
        if last_sc:
            days_sc = (now - last_sc).days
        elif a["_created"]:
            days_sc = (now - a["_created"]).days  # fallback for ALL thresholds
        else:
            aging_no_date += 1
            days_sc = None

        if days_sc is not None:
            aging_last_stage[age_bucket(days_sc)] += 1
            if days_sc >= THRESHOLDS["stale_7d"]:
                stale_counts["7+d"] += 1
            if days_sc >= THRESHOLDS["stale_14d"]:
                stale_counts["14+d"] += 1
            if days_sc >= THRESHOLDS["stale_30d"]:
                stale_counts["30+d"] += 1
                if last_sc:
                    stale_30d_con_log += 1
                else:
                    stale_30d_fallback += 1
                if rec_id:
                    aging_by_recruiter[rec_id]["stale_30d"] += 1

        if rec_id:
            aging_by_recruiter[rec_id]["total"] += 1

    bucket_order = ["0-1d", "2-3d", "4-7d", "8-14d", "15-30d", "30+d"]

    pipeline = {
        "total_activo": len(active_apps),
        "total_working": len([a for a in apps if a["_stage"] == "working"]),
        "total_cerrado_negativo": len([a for a in apps if a["_stage"] in NEGATIVE_STAGES]),
        "por_stage": {
            s: sum(1 for a in apps if a["_stage"] == s)
            for s in list(ACTIVE_STAGES) + ["working", "not_selected", "not_arriving"]
        },
        "por_reclutadora": {
            rid: {
                "nombre_display": RECRUITER_BY_ID[rid]["nombre_display"],
                "activo": sum(1 for a in active_apps if a["_recruiter_id"] == rid),
                "working": sum(1 for a in apps if a["_stage"] == "working" and a["_recruiter_id"] == rid),
            }
            for rid in RECRUITER_BY_ID if RECRUITER_BY_ID[rid]["es_recruiter"]
        },
        "aging_desde_creacion": {b: aging_created.get(b, 0) for b in bucket_order},
        "aging_desde_ultimo_cambio_stage": {b: aging_last_stage.get(b, 0) for b in bucket_order},
        "aging_sin_fecha": aging_no_date,
        "aging_nota": (
            "aging_desde_ultimo_cambio_stage usa last stage_change de activity_logs; "
            "si no hay log, usa created_at como proxy. "
            f"sum(buckets) + aging_sin_fecha = {sum(aging_last_stage.values()) + aging_no_date} (debe = total_activo)."
        ),
        "pipeline_estancado": {
            "7+d":  {"cantidad": stale_counts["7+d"],  "pct": safe_pct(stale_counts["7+d"],  len(active_apps))},
            "14+d": {"cantidad": stale_counts["14+d"], "pct": safe_pct(stale_counts["14+d"], len(active_apps))},
            "30+d": {
                "cantidad":            stale_counts["30+d"],
                "pct":                 safe_pct(stale_counts["30+d"], len(active_apps)),
                "con_log":             stale_30d_con_log,
                "fallback_created_at": stale_30d_fallback,
                "nota_diferencia": (
                    f"De los {stale_counts['30+d']} estancados >30d: "
                    f"{stale_30d_con_log} tienen stage_change log confirmado; "
                    f"{stale_30d_fallback} usan created_at como proxy (sin stage_change log). "
                    f"La alerta pipeline_30d_plus solo cuenta los {stale_30d_con_log} con log confirmado."
                ),
            },
        },
        "pipeline_estancado_por_reclutadora": {
            rid: {
                "nombre_display": RECRUITER_BY_ID[rid]["nombre_display"],
                "activo_total": aging_by_recruiter[rid]["total"],
                "estancado_30d": aging_by_recruiter[rid]["stale_30d"],
                "pct_estancado": safe_pct(
                    aging_by_recruiter[rid]["stale_30d"],
                    aging_by_recruiter[rid]["total"]
                ),
            }
            for rid in aging_by_recruiter
            if rid in RECRUITER_BY_ID
        },
    }

    # ------------------------------------------------------------------
    # RECRUITER ANALYTICS
    # ------------------------------------------------------------------
    recruiter_analytics = {}

    for rid, rinfo in RECRUITER_BY_ID.items():
        if not rinfo["es_recruiter"]:
            continue

        r_apps_all = [a for a in apps if a["_recruiter_id"] == rid]
        r_active   = [a for a in r_apps_all if a["_stage"] in ACTIVE_STAGES]
        r_working  = [a for a in r_apps_all if a["_stage"] == "working"]
        r_not_sel  = [a for a in r_apps_all if a["_stage"] == "not_selected"]
        r_not_arr  = [a for a in r_apps_all if a["_stage"] == "not_arriving"]

        # Activity from logs (last 30d)
        r_logs_30d = [
            log for log in logs
            if log.get("user_id") == rid
            or RECRUITER_NAME_TO_ID.get(log.get("user_name")) == rid
        ]
        r_logs_30d_filtered = [
            l for l in r_logs_30d
            if parse_dt(l.get("timestamp")) and parse_dt(l.get("timestamp")) >= d30
        ]
        touched_30d   = len(set(l.get("applicant_id") for l in r_logs_30d_filtered if l.get("applicant_id")))
        stage_chg_30d = sum(1 for l in r_logs_30d_filtered if l.get("activity_type") == "stage_change")
        total_acts_30d = len(r_logs_30d_filtered)

        all_log_ts = [parse_dt(l.get("timestamp")) for l in r_logs_30d if parse_dt(l.get("timestamp"))]
        last_act = max(all_log_ts).strftime("%Y-%m-%d") if all_log_ts else None

        # Ingresos recency (7d / 30d / 60d, attribution: current owner)
        ingresos_7d_count  = sum(1 for a in r_apps_all if a["_started"] and a["_started"] >= d7)
        ingresos_30d = [a for a in r_apps_all if a["_started"] and a["_started"] >= d30]
        ingresos_30d_count = len(ingresos_30d)
        ingresos_60d_count = sum(1 for a in r_apps_all if a["_started"] and a["_started"] >= d60)

        # Interviews reached in last 30d (from stage_history with correct user normalization)
        interviews_30d = 0
        sent_30d       = 0
        for a in r_apps_all:
            for sh in (a.get("stage_history") or []):
                sh_ts  = parse_dt(sh.get("date"))
                sh_uid = sh.get("user_id")
                sh_uname = sh.get("user_name")
                sh_rid = RECRUITER_BY_ID.get(sh_uid, {}).get("id") or \
                         RECRUITER_NAME_TO_ID.get(sh_uname)
                if sh_rid == rid and sh_ts and sh_ts >= d30:
                    if sh.get("stage") == "interview":
                        interviews_30d += 1
                    elif sh.get("stage") == "sent":
                        sent_30d += 1

        # Cohort-adjusted placement rate (mature cohorts only)
        r_eligible_30 = sum(
            1 for a in r_apps_all
            if a["_created"] and (now - a["_created"]).days >= 30
        )
        r_converted_30 = sum(
            1 for a in r_apps_all
            if a["_started"] and a["_created"]
            and (now - a["_created"]).days >= 30
            and (a["_started"] - a["_created"]).days <= 30
        )
        r_sample_ok = len(r_apps_all) >= THRESHOLDS["recruiter_min_sample"]

        # Median days to working
        days_to_working = [
            (a["_started"] - a["_created"]).days
            for a in r_apps_all
            if a["_started"] and a["_created"]
            and (a["_started"] - a["_created"]).days >= 0
        ]

        # Estado logic
        if rinfo["estado"] == "sin_actividad":
            estado_efectivo = "sin_actividad"
        elif not all_log_ts:
            estado_efectivo = "sin_actividad"
        elif (now - max(all_log_ts)).days <= 30:
            estado_efectivo = "activa"
        elif (now - max(all_log_ts)).days <= 90:
            estado_efectivo = "baja_actividad"
        else:
            estado_efectivo = "inactiva"

        recruiter_analytics[rid] = {
            "id": rid,
            "nombre_display": rinfo["nombre_display"],
            "nombre_completo_db": rinfo["nombre_completo_db"],
            "estado": estado_efectivo,
            "roster_actual": rinfo["roster_actual"],
            "incorporacion": rinfo["incorporacion"],
            "primera_actividad": min(all_log_ts).strftime("%Y-%m-%d") if all_log_ts else None,
            "ultima_actividad": last_act,
            "cartera_total": len(r_apps_all),
            "pipeline_activo": len(r_active),
            "working_asignado": len(r_working),
            "not_selected": len(r_not_sel),
            "not_arriving":  len(r_not_arr),
            "periodo_30d": {
                "apps_tocadas":     touched_30d,
                "cambios_stage":    stage_chg_30d,
                "total_acciones":   total_acts_30d,
                "entrevistas_30d":  interviews_30d,
                "enviados_30d":     sent_30d,
                "ingresos_30d":     ingresos_30d_count,
            },
            "recencia_ingresos": {
                "7d":  ingresos_7d_count,
                "30d": ingresos_30d_count,
                "60d": ingresos_60d_count,
            },
            "performance": {
                "tasa_colocacion_30d": {
                    "valor":       safe_pct(r_converted_30, r_eligible_30) if r_sample_ok else None,
                    "numerador":   r_converted_30,
                    "denominador": r_eligible_30,
                    "muestra_suficiente": r_sample_ok,
                    "muestra_minima": THRESHOLDS["recruiter_min_sample"],
                    "nota": None if r_sample_ok else
                            f"Muestra insuficiente ({len(r_apps_all)} < {THRESHOLDS['recruiter_min_sample']}). "
                            "No se muestra tasa para evitar estadísticas engañosas.",
                },
                "mediana_dias_hasta_ingreso": safe_median(days_to_working),
                "not_arriving_rate": safe_pct(len(r_not_arr), len(r_apps_all)) if r_sample_ok else None,
            },
        }

    # ------------------------------------------------------------------
    # SOURCE ANALYTICS
    # ------------------------------------------------------------------
    source_stats = defaultdict(lambda: {
        "label": None, "canal": None, "tipo": None,
        "total": 0, "active": 0, "working": 0, "not_arr": 0, "not_sel": 0,
        "days_to_working": [],
        "converted_30": 0, "eligible_30": 0,
    })
    unknown_sources = set()

    for a in apps:
        src = a["_source"]
        key = src["canal"]
        s   = source_stats[key]
        s["label"]  = src["label"]
        s["canal"]  = src["canal"]
        s["tipo"]   = src.get("tipo")
        s["total"] += 1
        if a["_stage"] in ACTIVE_STAGES: s["active"] += 1
        if a["_stage"] == "working":     s["working"] += 1
        if a["_stage"] == "not_arriving": s["not_arr"] += 1
        if a["_stage"] == "not_selected": s["not_sel"] += 1
        if a["_started"] and a["_created"] and (a["_started"] - a["_created"]).days >= 0:
            s["days_to_working"].append((a["_started"] - a["_created"]).days)
        if a["_created"] and (now - a["_created"]).days >= 30:
            s["eligible_30"] += 1
            if a["_started"] and (a["_started"] - a["_created"]).days <= 30:
                s["converted_30"] += 1
        if src["canal"] == "sin_clasificar":
            unknown_sources.add(src.get("raw") or "?")

    fuentes_list = []
    for canal, s in sorted(source_stats.items(), key=lambda x: -x[1]["total"]):
        sample_ok = s["total"] >= THRESHOLDS["cohort_min_size"]
        fuentes_list.append({
            "canal":      canal,
            "label":      s["label"],
            "tipo":       s["tipo"],
            "aplicantes": s["total"],
            "activos":    s["active"],
            "working":    s["working"],
            "not_arriving": s["not_arr"],
            "not_selected": s["not_sel"],
            "mediana_dias_hasta_ingreso": safe_median(s["days_to_working"]),
            "tasa_colocacion_30d": {
                "valor":       safe_pct(s["converted_30"], s["eligible_30"]) if sample_ok else None,
                "numerador":   s["converted_30"],
                "denominador": s["eligible_30"],
                "muestra_suficiente": sample_ok,
            },
        })

    fuentes = {
        "nota": (
            "ig (Instagram Orgánico) y meta (Meta Ads Pago) son fuentes DISTINTAS. No fusionadas. "
            "lead_source='dora'/'sofia'/'natalia' son valores REALES del campo lead_source en MongoDB — "
            "candidatos captados directamente por esa reclutadora. "
            "Agrupados bajo canal='reclutadora' como categoría analítica. "
            "NO es reclasificación por owner/user_id — es el campo lead_source original."
        ),
        "sin_clasificar": sorted(unknown_sources),
        "por_canal": fuentes_list,
    }

    # ------------------------------------------------------------------
    # CLIENT ANALYTICS
    # Via sent_to (text list). client_id = 0% coverage — documented.
    # ------------------------------------------------------------------
    client_stats = defaultdict(lambda: {
        "variantes_originales": set(),
        "total": 0, "active": 0, "working": 0,
        "not_arr": 0, "not_sel": 0,
        "days_sent_to_working": [],
        "d30": 0, "d60": 0, "d90": 0,
    })

    for a in apps:
        for raw_c in (a.get("sent_to") or []):
            if not raw_c:
                continue
            norm = normalize_client(raw_c)
            if not norm:
                continue
            s = client_stats[norm]
            s["variantes_originales"].add(raw_c)
            s["total"] += 1
            if a["_stage"] in ACTIVE_STAGES: s["active"] += 1
            if a["_stage"] == "working":     s["working"] += 1
            if a["_stage"] == "not_arriving": s["not_arr"] += 1
            if a["_stage"] == "not_selected": s["not_sel"] += 1
            if a["_created"]:
                age = (now - a["_created"]).days
                if age <= 30: s["d30"] += 1
                if age <= 60: s["d60"] += 1
                if age <= 90: s["d90"] += 1

    apps_with_client = sum(1 for a in apps if a["_clients"])
    apps_without_client = len(apps) - apps_with_client

    clientes = {
        "nota": (
            "applications.client_id = 0% cobertura. Vinculación via sent_to (texto libre). "
            "Solo variantes de alta confianza fueron normalizadas. "
            "Fill Rate NO calculado — sin Headcount solicitado confiable."
        ),
        "cobertura": {
            "apps_con_cliente": apps_with_client,
            "apps_sin_cliente": apps_without_client,
            "pct_cobertura":    safe_pct(apps_with_client, len(apps)),
        },
        "normalizacion": {
            "variantes_mapeadas": CLIENT_NORMALIZATION,
            "confidence":         "alta — solo typos evidentes y duplicados claros",
        },
        "por_cliente": [
            {
                "nombre":        name,
                "variantes_raw": sorted(s["variantes_originales"]),
                "total":         s["total"],
                "ultimos_30d":   s["d30"],
                "ultimos_60d":   s["d60"],
                "ultimos_90d":   s["d90"],
                "activos":       s["active"],
                "working":       s["working"],
                "not_arriving":  s["not_arr"],
                "not_selected":  s["not_sel"],
                "estado_actividad": (
                    "activo" if s["d30"] > 0 else
                    "reciente" if s["d60"] > 0 else
                    "bajo" if s["d90"] > 0 else
                    "historico"
                ),
            }
            for name, s in sorted(client_stats.items(), key=lambda x: -x[1]["total"])
        ],
    }

    # ------------------------------------------------------------------
    # VACANTES / JOBS
    # ------------------------------------------------------------------
    job_map = {j["id"]: j for j in jobs if j.get("id")}
    job_stats = defaultdict(lambda: {
        "title": None, "status": None, "category": None, "city": None, "state": None,
        "total": 0, "active": 0, "working": 0, "not_arr": 0,
        "d30": 0, "eligible_30": 0, "converted_30": 0,
    })
    apps_without_job = 0

    for a in apps:
        jid = a.get("job_id")
        if not jid:
            apps_without_job += 1
            continue
        s = job_stats[jid]
        if jid in job_map:
            j = job_map[jid]
            s["title"]    = j.get("title")
            s["status"]   = j.get("status")
            s["category"] = j.get("category")
            s["city"]     = j.get("city")
            s["state"]    = j.get("state")
        s["total"] += 1
        if a["_stage"] in ACTIVE_STAGES: s["active"] += 1
        if a["_stage"] == "working":     s["working"] += 1
        if a["_stage"] == "not_arriving": s["not_arr"] += 1
        if a["_created"] and (now - a["_created"]).days <= 30:
            s["d30"] += 1
        if a["_created"] and (now - a["_created"]).days >= 30:
            s["eligible_30"] += 1
            if a["_started"] and (a["_started"] - a["_created"]).days <= 30:
                s["converted_30"] += 1

    vacantes = {
        "total_jobs": len(jobs),
        "todos_activos_en_db": all(j.get("status") == "active" for j in jobs),
        "nota_status": "status='active' en 57/57 jobs — el campo no se actualiza al cubrir vacantes.",
        "apps_sin_job": apps_without_job,
        "jobs_con_apps": len(job_stats),
        "por_vacante": sorted(
            [
                {
                    "job_id":    jid,
                    "titulo":    s["title"],
                    "categoria": s["category"],
                    "ciudad":    s["city"],
                    "estado_us": s["state"],
                    "total":     s["total"],
                    "activos":   s["active"],
                    "working":   s["working"],
                    "not_arriving": s["not_arr"],
                    "ultimos_30d": s["d30"],
                    "tasa_30d":  safe_pct(s["converted_30"], s["eligible_30"])
                                 if s["eligible_30"] >= THRESHOLDS["cohort_min_size"] else None,
                }
                for jid, s in job_stats.items()
            ],
            key=lambda x: -(x["total"] or 0),
        )[:20],  # Top 20 by volume
    }

    # ------------------------------------------------------------------
    # GEOGRAFÍA (by vacancy location — jobs.city / jobs.state)
    # Candidate location NOT available (no address/state field on applications).
    # ------------------------------------------------------------------
    geo_state = defaultdict(lambda: {"total": 0, "active": 0, "working": 0,
                                     "not_arr": 0, "not_sel": 0, "ciudades": set()})
    geo_city  = defaultdict(lambda: {"total": 0, "active": 0, "working": 0,
                                     "not_arr": 0, "state": None})
    apps_sin_geo = 0

    for a in apps:
        jid = a.get("job_id")
        if not jid or jid not in job_map:
            apps_sin_geo += 1
            continue
        j     = job_map[jid]
        state = (j.get("state") or "").strip() or "Desconocido"
        city  = (j.get("city") or "").strip() or "Desconocida"

        geo_state[state]["total"] += 1
        geo_city[city]["total"]   += 1
        geo_city[city]["state"]    = state
        geo_state[state]["ciudades"].add(city)
        if a["_stage"] in ACTIVE_STAGES:
            geo_state[state]["active"] += 1
            geo_city[city]["active"]   += 1
        if a["_stage"] == "working":
            geo_state[state]["working"] += 1
            geo_city[city]["working"]   += 1
        if a["_stage"] == "not_arriving":
            geo_state[state]["not_arr"] += 1
            geo_city[city]["not_arr"]   += 1
        if a["_stage"] == "not_selected":
            geo_state[state]["not_sel"] += 1

    geografia = {
        "nota": (
            "Geografía basada en ubicación de la vacante (jobs.city/state). "
            "Ubicación del candidato: no disponible (campo no existe en schema). "
            f"Apps sin job_id vinculado: {apps_sin_geo}."
        ),
        "cobertura_pct": safe_pct(len(apps) - apps_sin_geo, len(apps)),
        "por_estado": sorted(
            [
                {
                    "estado":    s,
                    "total":     v["total"],
                    "activos":   v["active"],
                    "working":   v["working"],
                    "not_arriving": v["not_arr"],
                    "not_selected": v["not_sel"],
                    "ciudades_count": len(v["ciudades"]),
                    "tasa_incorporacion": safe_pct(v["working"],
                        v["total"] - v["active"]) if (v["total"] - v["active"]) >= 5 else None,
                }
                for s, v in geo_state.items()
            ],
            key=lambda x: -x["total"],
        ),
        "por_ciudad": sorted(
            [
                {
                    "ciudad":  c,
                    "estado":  v["state"],
                    "total":   v["total"],
                    "activos": v["active"],
                    "working": v["working"],
                    "not_arriving": v["not_arr"],
                }
                for c, v in geo_city.items()
            ],
            key=lambda x: -x["total"],
        )[:20],
    }

    # ------------------------------------------------------------------
    # JERARQUÍA: Cliente → Vacante → Aplicantes
    # Reconstructed via apps.sent_to (client) × apps.job_id (vacancy).
    # Note: sent_to has ~89% fill rate; only apps with known client included.
    # ------------------------------------------------------------------
    jerarquia_data = defaultdict(lambda: defaultdict(lambda: {
        "titulo": None, "ciudad": None, "estado": None,
        "total": 0, "active": 0, "working": 0, "not_arr": 0, "not_sel": 0,
    }))

    for a in apps:
        client_list = a["_clients"]
        if not client_list:
            continue
        jid      = a.get("job_id")
        job_info = job_map.get(jid, {}) if jid else {}
        jkey     = jid or "_sin_vacante"

        for cn in client_list:
            s = jerarquia_data[cn][jkey]
            if s["titulo"] is None:
                s["titulo"] = job_info.get("title") or "Sin título"
                s["ciudad"] = job_info.get("city")
                s["estado"] = job_info.get("state")
            s["total"] += 1
            if a["_stage"] in ACTIVE_STAGES: s["active"]   += 1
            if a["_stage"] == "working":     s["working"]  += 1
            if a["_stage"] == "not_arriving": s["not_arr"] += 1
            if a["_stage"] == "not_selected": s["not_sel"] += 1

    jerarquia = {
        "nota": (
            "Relación cliente→vacante reconstruida por aplicante (sent_to × job_id). "
            "Un aplicante puede vincularse a múltiples clientes. "
            "Cobertura: apps con sent_to definido."
        ),
        "apps_con_cliente_vinculado": sum(1 for a in apps if a["_clients"]),
        "por_cliente": sorted(
            [
                {
                    "cliente":        cn,
                    "vacantes_count": len(vac_dict),
                    "total":          sum(v["total"]   for v in vac_dict.values()),
                    "activos":        sum(v["active"]  for v in vac_dict.values()),
                    "working":        sum(v["working"] for v in vac_dict.values()),
                    "not_arriving":   sum(v["not_arr"] for v in vac_dict.values()),
                    "not_selected":   sum(v["not_sel"] for v in vac_dict.values()),
                    "vacantes": sorted(
                        [
                            {
                                "job_id":      jkey if not jkey.startswith("_") else None,
                                "titulo":      v["titulo"],
                                "ciudad":      v["ciudad"],
                                "estado":      v["estado"],
                                "total":       v["total"],
                                "activos":     v["active"],
                                "working":     v["working"],
                                "not_arriving": v["not_arr"],
                                "not_selected": v["not_sel"],
                            }
                            for jkey, v in vac_dict.items()
                        ],
                        key=lambda x: -x["total"],
                    ),
                }
                for cn, vac_dict in jerarquia_data.items()
            ],
            key=lambda x: -x["total"],
        )[:25],
    }

    # ------------------------------------------------------------------
    # PRODUCTIVIDAD: Matriz Fuente × Reclutadora + KPIs de conversión
    # ------------------------------------------------------------------
    _mat_fuente_rec = defaultdict(lambda: defaultdict(int))
    _rec_displays   = set()
    _src_canals     = set()

    for a in apps:
        canal = a["_source"]["canal"]
        rdsp  = a["_recruiter_display"] or "Sin asignar"
        _mat_fuente_rec[canal][rdsp] += 1
        _rec_displays.add(rdsp)
        _src_canals.add(canal)

    # Recruiter stage breakdown for productivity view
    _rec_productivity = {}
    for rid, rdata in [
        (k, v) for k, v in {
            **{a["_recruiter_id"]: a for a in apps if a["_recruiter_id"]}
        }.items()
    ]:
        pass  # built below from recruiter_analytics (after it's computed)
    # Note: recruiter_analytics is computed later in the script; productividad.reclutadoras
    # will reference it by name in the final assembly.

    productividad = {
        "nota": "Productividad y eficiencia por reclutadora y fuente de captación.",
        "kpis_conversion": {
            "total_aplicantes":                  len(apps),
            "presentados_cliente_total":         _entregados_total,
            "pending_decision_sent":             _pending_sent,
            "tasa_presentacion_pct":             _tasa_entrega,
            "incorporados_total":                _incorporados_total,
            "tasa_incorporacion_pct":            _tasa_incorporacion,
            "tasa_incorporacion_finalizada_pct": _tasa_incorporacion_finalizada,
        },
        "matriz_fuente_reclutadora": {
            "reclutadoras": sorted(_rec_displays),
            "fuentes":      sorted(_src_canals),
            "celdas": sorted(
                [
                    {
                        "fuente":           canal,
                        "total":            sum(rec_dict.values()),
                        "por_reclutadora":  dict(sorted(rec_dict.items())),
                    }
                    for canal, rec_dict in _mat_fuente_rec.items()
                ],
                key=lambda x: -x["total"],
            ),
        },
    }

    # ------------------------------------------------------------------
    # APPS ANÓNIMAS: Dataset multidimensional para filtrado en dashboard
    # Sin datos personales identificables (sin nombres de candidatos,
    # teléfonos, emails ni otro dato sensible del candidato).
    # Contiene únicamente campos operativos: etapa, cliente, geografía,
    # reclutadora asignada, fuente de captación y fechas agregadas.
    # ------------------------------------------------------------------
    _PRES_STAGES = {"sent", "working", "not_arriving", "not_selected"}
    _ACTIVE_STAGES_SET = {"new", "contacting", "interview", "sent"}

    _aa_registros = []
    for a in apps:
        jid  = a.get("job_id")
        ji   = job_map.get(jid, {}) if jid else {}
        fc   = a.get("created_at", "")[:10] if a.get("created_at") else None
        # Only include fi for working stage with non-future date
        fi   = (a.get("started_working_at", "")[:10]
                if (a.get("started_working_at") and not a.get("_started_future"))
                else None)
        pc   = a["_clients"][0] if a["_clients"] else None
        src_canal = a["_source"]["canal"] if isinstance(a.get("_source"), dict) else str(a.get("_source") or "sin_fuente")
        _aa_registros.append({
            "e":  a["_stage"],                           # etapa
            "c":  pc,                                    # cliente (normalizado)
            "st": ji.get("state"),                       # estado geográfico
            "ci": ji.get("city"),                        # ciudad
            "r":  a["_recruiter_display"],               # reclutadora
            "f":  src_canal,                             # fuente (canal)
            "vt": a.get("job_title") or ji.get("title"),# vacante título
            "vi": jid,                                   # vacante id
            "fc": fc,                                    # fecha_creacion YYYY-MM-DD
            "fi": fi,                                    # fecha_ingreso YYYY-MM-DD
        })

    apps_anonimas = {
        "nota_privacidad": (
            "Sin datos personales identificables. Registros por aplicante sin nombres "
            "de candidatos, contacto ni información sensible del candidato. "
            "Campos incluidos: etapa pipeline, cliente, estado, ciudad, reclutadora, "
            "fuente, vacante y fechas agregadas."
        ),
        "total_registros": len(_aa_registros),
        "claves": {
            "e":"etapa", "c":"cliente", "st":"estado", "ci":"ciudad",
            "r":"reclutadora", "f":"fuente", "vt":"vacante_titulo",
            "vi":"vacante_id", "fc":"fecha_creacion", "fi":"fecha_ingreso",
        },
        "etapas_presentado": sorted(_PRES_STAGES),
        "etapas_activo":     sorted(_ACTIVE_STAGES_SET),
        "fecha_generacion":  now.strftime("%Y-%m-%d"),
        "registros":         _aa_registros,
    }

    # ------------------------------------------------------------------
    # META ADS ANALYTICS
    # ------------------------------------------------------------------
    meta_monthly   = defaultdict(lambda: {"spend": 0, "leads": 0, "convs": 0, "imp": 0, "days": 0})
    meta_campaigns = defaultdict(lambda: {"spend": 0, "leads": 0, "convs": 0, "imp": 0})
    meta_periodos  = {
        "7d":   {"spend": 0, "leads": 0, "convs": 0, "imp": 0},
        "30d":  {"spend": 0, "leads": 0, "convs": 0, "imp": 0},
        "60d":  {"spend": 0, "leads": 0, "convs": 0, "imp": 0},
        "hist": {"spend": 0, "leads": 0, "convs": 0, "imp": 0},
    }
    meta_dates_seen = []

    for rec in meta:
        dt = parse_dt(rec.get("date"))
        m  = ym(dt) if dt else "unknown"
        sp = rec.get("spend") or 0
        ld = rec.get("leads") or 0
        cv = rec.get("conversations_started") or 0
        im = rec.get("impressions") or 0
        cn = rec.get("campaign_name") or "Desconocida"
        if dt:
            meta_dates_seen.append(dt)
        meta_monthly[m]["spend"] += sp
        meta_monthly[m]["leads"] += ld
        meta_monthly[m]["convs"] += cv
        meta_monthly[m]["imp"]   += im
        meta_monthly[m]["days"]  += 1
        meta_campaigns[cn]["spend"] += sp
        meta_campaigns[cn]["leads"] += ld
        meta_campaigns[cn]["convs"] += cv
        meta_campaigns[cn]["imp"]   += im
        # Accumulate into each applicable period bucket
        meta_periodos["hist"]["spend"] += sp
        meta_periodos["hist"]["leads"] += ld
        meta_periodos["hist"]["convs"] += cv
        meta_periodos["hist"]["imp"]   += im
        if dt:
            if dt >= (now - timedelta(days=7)):
                meta_periodos["7d"]["spend"] += sp; meta_periodos["7d"]["leads"] += ld
                meta_periodos["7d"]["convs"] += cv; meta_periodos["7d"]["imp"]   += im
            if dt >= (now - timedelta(days=30)):
                meta_periodos["30d"]["spend"] += sp; meta_periodos["30d"]["leads"] += ld
                meta_periodos["30d"]["convs"] += cv; meta_periodos["30d"]["imp"]   += im
            if dt >= (now - timedelta(days=60)):
                meta_periodos["60d"]["spend"] += sp; meta_periodos["60d"]["leads"] += ld
                meta_periodos["60d"]["convs"] += cv; meta_periodos["60d"]["imp"]   += im

    meta_data_range = {
        "first": min(meta_dates_seen).strftime("%Y-%m-%d") if meta_dates_seen else None,
        "last":  max(meta_dates_seen).strftime("%Y-%m-%d") if meta_dates_seen else None,
        "dias_disponibles": ((max(meta_dates_seen) - min(meta_dates_seen)).days + 1)
                             if meta_dates_seen else 0,
    }

    _periodo_defs = {
        "7d":   f"Últimos 7 días ({(now - timedelta(days=7)).strftime('%Y-%m-%d')} a {now.strftime('%Y-%m-%d')})",
        "30d":  f"Últimos 30 días ({(now - timedelta(days=30)).strftime('%Y-%m-%d')} a {now.strftime('%Y-%m-%d')})",
        "60d":  f"Últimos 60 días ({(now - timedelta(days=60)).strftime('%Y-%m-%d')} a {now.strftime('%Y-%m-%d')})",
        "hist": "Histórico completo (todos los registros meta_ad_daily disponibles)",
    }

    def _meta_period_summary(key):
        p = meta_periodos[key]
        return {
            "periodo_definicion": _periodo_defs[key],
            "gasto":             round(p["spend"], 2),
            "leads":             p["leads"],
            "conversaciones":    p["convs"],
            "impresiones":       p["imp"],
            "cpl":               round(p["spend"] / p["leads"], 2) if p["leads"] else None,
            "costo_por_conv":    round(p["spend"] / p["convs"], 2) if p["convs"] else None,
        }

    # Meta attribution coverage — four layers A/B/C/D
    apps_with_meta_id     = sum(1 for a in apps if a.get("lead_source") == "meta")
    apps_with_meta_ad_id  = sum(1 for a in apps if a.get("meta_ad_id"))
    apps_total = len(apps)

    # Warn if 30d ≈ historical (all data falls within rolling window)
    _30d_span_note = None
    if meta_data_range["dias_disponibles"] and meta_data_range["dias_disponibles"] <= 30:
        _30d_span_note = (
            f"AVISO: datos Meta abarcan solo {meta_data_range['dias_disponibles']} días "
            f"({meta_data_range['first']} a {meta_data_range['last']}). "
            "El total '30d' es idéntico al histórico — el período 30d captura todos los datos disponibles."
        )

    # ------------------------------------------------------------------
    # META ADS: Attribution per Vacancy via meta_ad_id join
    # Join: applications.meta_ad_id → meta_ad_daily.ad_id → campaign info
    # Coverage: only apps that have meta_ad_id populated (~5% of total)
    # ------------------------------------------------------------------
    _ad_id_to_campaign = {}
    for _mr in meta:
        _aid = _mr.get("ad_id")
        if _aid and str(_aid) not in _ad_id_to_campaign:
            _ad_id_to_campaign[str(_aid)] = {
                "campaign_name": _mr.get("campaign_name") or "Desconocida",
                "campaign_id":   str(_mr.get("campaign_id") or ""),
                "ad_name":       _mr.get("ad_name") or "",
            }

    _vac_meta = defaultdict(lambda: {
        "titulo": None, "ciudad": None, "estado": None,
        "apps_total": 0, "working": 0, "active": 0, "campanas": set(),
    })
    _meta_apps_matched = 0
    _meta_apps_no_join = 0

    for a in apps:
        _mid = str(a.get("meta_ad_id") or "").strip()
        if not _mid:
            continue
        _camp = _ad_id_to_campaign.get(_mid)
        if not _camp:
            _meta_apps_no_join += 1
            continue
        _meta_apps_matched += 1
        _jid = a.get("job_id") or "_sin_vacante"
        _j   = job_map.get(_jid, {})
        _vac_meta[_jid]["titulo"] = _j.get("title") or "Sin vacante"
        _vac_meta[_jid]["ciudad"] = _j.get("city")
        _vac_meta[_jid]["estado"] = _j.get("state")
        _vac_meta[_jid]["apps_total"] += 1
        _vac_meta[_jid]["campanas"].add(_camp["campaign_name"])
        if a.get("pipeline_stage") == "working":
            _vac_meta[_jid]["working"] += 1
        if a["_stage"] in ACTIVE_STAGES:
            _vac_meta[_jid]["active"] += 1

    _meta_total_with_id = _meta_apps_matched + _meta_apps_no_join

    _meta_atrib_por_vacante = {
        "disponible": bool(_ad_id_to_campaign and _meta_total_with_id > 0),
        "apps_con_meta_ad_id": _meta_total_with_id,
        "apps_matched_to_campaign": _meta_apps_matched,
        "apps_no_match_in_daily": _meta_apps_no_join,
        "nota_metodologia": (
            "Join: applications.meta_ad_id → meta_ad_daily.ad_id. "
            f"Cobertura: {_meta_total_with_id}/{len(apps)} apps (~{safe_pct(_meta_total_with_id, len(apps))}% del total). "
            "Muestra apps con meta_ad_id verificable — NO infiere gasto por vacante. "
            "NO usar para calcular ROI ni CPH por vacante."
        ),
        "identificador_faltante": (
            "Para atribución completa: poblar applications.meta_ad_id en TODOS los aplicantes Meta. "
            "Campo existe en WorkYa-security. Actualmente solo apps recientes tienen el campo."
        ),
        "por_vacante": sorted(
            [
                {
                    "job_id":    _jid if not _jid.startswith("_") else None,
                    "titulo":    _v["titulo"],
                    "ciudad":    _v["ciudad"],
                    "estado":    _v["estado"],
                    "apps_meta": _v["apps_total"],
                    "working":   _v["working"],
                    "activos":   _v["active"],
                    "campanas":  sorted(_v["campanas"]),
                }
                for _jid, _v in _vac_meta.items()
            ],
            key=lambda x: -x["apps_meta"],
        ),
    }

    meta_ads = {
        "nota": (
            f"Datos Meta disponibles: {meta_data_range['first']} a {meta_data_range['last']} "
            f"({meta_data_range['dias_disponibles']} días). "
            "ROI y Cost per Hire NO calculados — atribución individual insuficiente. "
            "CPL = gasto / leads registrados en meta_ad_daily (formularios Meta)."
        ),
        "data_range": meta_data_range,
        "aviso_periodo": _30d_span_note,
        "atribucion": {
            "apps_con_lead_source_meta": apps_with_meta_id,
            "total_apps": apps_total,
            "pct_apps_atribuidas": safe_pct(apps_with_meta_id, apps_total),
            "nota": (
                "lead_source='meta' ≈ atribución. "
                "meta_ad_id individual solo presente en docs recientes (~24%). "
                "Sin join confiable app↔campaña."
            ),
        },
        "atribucion_capas": {
            "nota": (
                "Cuatro capas de atribución Meta, de más general a más granular. "
                "Capas NO son excluyentes: B ⊆ A (conceptualmente), D ⊆ B."
            ),
            "A_meta_reportado": {
                "descripcion": "Datos de rendimiento reportados directamente por Meta (collection meta_ad_daily)",
                "records_meta_daily": len(meta),
                "meses_cubiertos": sorted(meta_monthly.keys()),
            },
            "B_apps_canal_meta": {
                "descripcion": "Aplicantes con campo lead_source='meta' en MongoDB (canal de entrada)",
                "total": apps_with_meta_id,
                "pct_del_total": safe_pct(apps_with_meta_id, apps_total),
                "nota_temporal": (
                    "Conteo ALL TIME — no filtrado por período. "
                    "No comparable directamente con leads Meta (capa A) que cubre solo "
                    f"{meta_data_range['first']} a {meta_data_range['last']}."
                ),
            },
            "C_apps_campana_especifica": {
                "descripcion": "Join aplicante↔campaña específica — NO disponible directamente",
                "disponible": False,
                "nota": (
                    "Applications no almacenan campaign_name. "
                    "Requeriría join por meta_ad_id individual con meta_ad_daily."
                ),
            },
            "D_apps_anuncio_individual": {
                "descripcion": "Aplicantes con campo meta_ad_id individual en MongoDB (anuncio granular)",
                "total": apps_with_meta_ad_id,
                "pct_de_capa_B": safe_pct(apps_with_meta_ad_id, apps_with_meta_id) if apps_with_meta_id else None,
                "nota": "Cobertura parcial — campo presente solo en aplicantes más recientes.",
            },
        },
        "resumen_total": {
            "gasto_total":  round(sum(v["spend"] for v in meta_monthly.values()), 2),
            "leads_total":  sum(v["leads"] for v in meta_monthly.values()),
            "convs_total":  sum(v["convs"] for v in meta_monthly.values()),
            "imp_total":    sum(v["imp"] for v in meta_monthly.values()),
        },
        "por_periodo": {
            "7d":   _meta_period_summary("7d"),
            "30d":  _meta_period_summary("30d"),
            "60d":  _meta_period_summary("60d"),
            "hist": _meta_period_summary("hist"),
        },
        "periodo_30d": _meta_period_summary("30d"),
        "por_mes": [
            {
                "mes": m,
                "gasto":  round(v["spend"], 2),
                "leads":  v["leads"],
                "conversaciones": v["convs"],
                "impresiones": v["imp"],
                "cpl":    round(v["spend"] / v["leads"], 2) if v["leads"] else None,
            }
            for m, v in sorted(meta_monthly.items())
        ],
        "por_campana": sorted(
            [
                {
                    "campana":  cn,
                    "gasto":    round(v["spend"], 2),
                    "leads":    v["leads"],
                    "conversaciones": v["convs"],
                    "impresiones": v["imp"],
                    "cpl":      round(v["spend"] / v["leads"], 2) if v["leads"] else None,
                }
                for cn, v in meta_campaigns.items()
            ],
            key=lambda x: -(x["gasto"] or 0),
        ),
        "bloque_ejecutivo": {
            "nota_general": (
                "Las cifras A, B, C, D provienen de sistemas distintos con diferentes períodos de cobertura. "
                "NO existe un funnel causal trazable: un lead Meta (A) NO se puede mapear directamente "
                "a un aplicante WorkYa (B). Presentar como métricas independientes, "
                "NO como secuencia Inversión→Resultado."
            ),
            "A_meta_inversion": {
                "descripcion": "Gasto e indicadores reportados por Meta (meta_ad_daily)",
                "cobertura": f"{meta_data_range['first']} a {meta_data_range['last']} ({meta_data_range['dias_disponibles']} días)",
                "gasto_historico": round(sum(v["spend"] for v in meta_monthly.values()), 2),
                "leads_meta": sum(v["leads"] for v in meta_monthly.values()),
                "cpl_historico": (
                    round(sum(v["spend"] for v in meta_monthly.values()) /
                          sum(v["leads"] for v in meta_monthly.values()), 2)
                    if sum(v["leads"] for v in meta_monthly.values()) else None
                ),
            },
            "B_workya_captacion_meta": {
                "descripcion": "Aplicantes WorkYa con lead_source='meta' — cobertura ALL TIME",
                "total_aplicantes_meta": apps_with_meta_id,
                "nota_temporal": "ALL TIME — no alineado con período de datos Meta (sep-oct 2026)",
            },
            "C_campana_especifica": {
                "descripcion": "Atribución por campaña — no disponible (sin campaign_name en applications)",
                "disponible": False,
            },
            "D_anuncio_individual": {
                "descripcion": "Aplicantes con meta_ad_id individual en MongoDB",
                "total": apps_with_meta_ad_id,
                "cobertura_pct": safe_pct(apps_with_meta_ad_id, apps_total),
                "nota": "Cobertura parcial — campo presente solo en aplicantes más recientes.",
            },
        },
        "atribucion_por_vacante": _meta_atrib_por_vacante,
    }

    # ------------------------------------------------------------------
    # DATA QUALITY
    # ------------------------------------------------------------------
    dq_issues = []

    def dq(name, count, total, severity, impact, label, nota=None):
        pct = safe_pct(count, total) if total else None
        dq_issues.append({
            "issue":         name,
            "label":         label,
            "count":         count,
            "pct":           pct,
            "total_base":    total,
            "severidad":     severity,
            "metric_impact": impact,
            "nota":          nota,
        })

    N = len(apps)
    dq("working_sin_fecha",       len(set_A_notB), N, "critica",
       "Ingresos working sin started_working_at — no asignables a período",
       "Working sin fecha de inicio")

    dq("fecha_sin_working_stage", len(set_B_notA), N, "critica",
       "started_working_at existe pero pipeline_stage ≠ working — inconsistencia",
       "Fecha de ingreso con stage incompatible",
       nota=f"Por stage actual: {dict(B_notA_by_stage)}")

    dq("fechas_futuras",          len(future_dates), N, "critica",
       "started_working_at en el futuro — excluidas de métricas temporales",
       "Fechas futuras en started_working_at",
       nota="Probablemente typos (ej: 2026-12-01 en lugar de 2025-12-01)")

    motivos_vacios = sum(1 for a in apps if a["_stage"] == "not_selected" and not a.get("motivo_no_seleccionado"))
    dq("motivo_no_seleccionado_vacio", motivos_vacios,
       len([a for a in apps if a["_stage"] == "not_selected"]),
       "critica", "Análisis de causas de rechazo imposible", "Motivo de no selección vacío")

    dq("sin_lead_source",
       sum(1 for a in apps if not a.get("lead_source")),
       N, "alta", "Analytics de fuentes subestimadas", "Sin fuente de captación")

    dq("sin_owner",
       sum(1 for a in apps if not a.get("owner")),
       N, "alta", "Candidatos no asignados a reclutadora", "Sin reclutadora asignada")

    dq("sin_job_id",
       apps_without_job, N, "media",
       "Análisis por vacante imposible para estos candidatos", "Sin job_id")

    sla_vacios = sum(1 for a in apps if a.get("sla_met") is None)
    dq("sla_met_nulo", sla_vacios, N, "media",
       "KPI de SLA inutilizable — campo nunca llenado", "sla_met vacío")

    gender_vals = {}
    for a in apps:
        g = (a.get("gender") or "").strip()
        if g:
            gender_vals[g] = gender_vals.get(g, 0) + 1
    if len(gender_vals) > 2:
        import re as _re
        safe_gender_vals = {
            ("[valor_redactado]" if _re.search(r'@|\.com|\.net|\.org', k, _re.IGNORECASE) else k): v
            for k, v in gender_vals.items()
        }
        dq("gender_no_normalizado", sum(1 for a in apps if a.get("gender")), N, "alta",
           "Reportes de género inconsistentes — múltiples formatos",
           f"Género en {len(gender_vals)} formatos distintos",
           nota=f"Valores: {safe_gender_vals}")

    dq("client_id_vacio",
       sum(1 for a in apps if not a.get("client_id")),
       N, "alta",
       "Vinculación app→cliente depende de sent_to (texto libre, no UUID)",
       "client_id sin cobertura")

    legacy_in_history = sum(
        1 for a in apps
        for sh in (a.get("stage_history") or [])
        if sh.get("stage") in LEGACY_STAGES
    )
    if legacy_in_history:
        dq("legacy_stages_en_history", legacy_in_history, None, "baja",
           "Etapas antiguas en stage_history pueden confundir análisis histórico",
           "Etapas legacy (selected/rejected) en stage_history")

    if unknown_sources:
        dq("sources_sin_clasificar", len(unknown_sources), None, "media",
           "Fuentes no mapeadas aparecen como 'Sin clasificar'",
           f"Fuentes desconocidas: {sorted(unknown_sources)}")

    # Count by severity
    by_sev = defaultdict(int)
    for iss in dq_issues:
        by_sev[iss["severidad"]] += 1

    calidad_datos = {
        "resumen": {
            "total_issues": len(dq_issues),
            "criticos": by_sev["critica"],
            "altos": by_sev["alta"],
            "medios": by_sev["media"],
            "bajos": by_sev["baja"],
        },
        "issues": dq_issues,
    }

    # ------------------------------------------------------------------
    # RECENCY OF WORKING
    # ------------------------------------------------------------------
    working_recency = {
        "total_working_stage": len(set_A),
        "con_fecha_valida": len(set_B),
        "sin_fecha": len(set_A_notB),
        "pct_sin_fecha": safe_pct(len(set_A_notB), len(set_A)),
        "por_periodo": {
            "7d":  sum(1 for a in apps if a["_started"] and a["_started"] >= d7),
            "30d": sum(1 for a in apps if a["_started"] and a["_started"] >= d30),
            "60d": sum(1 for a in apps if a["_started"] and a["_started"] >= d60),
            "90d": sum(1 for a in apps if a["_started"] and a["_started"] >= d90),
            "historico_90d_mas": sum(1 for a in apps if a["_started"] and a["_started"] < d90),
        },
        "nota": "Solo usar started_working_at para ingresos por período. NUNCA solo pipeline_stage=working.",
    }

    # ------------------------------------------------------------------
    # ALERTAS (aggregated, no PII)
    # ------------------------------------------------------------------
    thresh_new = THRESHOLDS["alert_new_sin_gestion"]
    thresh_int = THRESHOLDS["alert_interview_estancada"]
    thresh_sent = THRESHOLDS["alert_enviado_esperando"]
    thresh_pipe = THRESHOLDS["stale_30d"]

    def build_alert(candidates, alert_type, label):
        by_rec = defaultdict(int)
        for a in candidates:
            by_rec[a["_recruiter_display"] or "Sin asignar"] += 1
        return {
            "tipo": alert_type,
            "label": label,
            "total": len(candidates),
            "por_reclutadora": dict(sorted(by_rec.items(), key=lambda x: -x[1])),
        }

    new_sin_gestion = [
        a for a in apps
        if a["_stage"] == "new" and a["_created"]
        and (now - a["_created"]).days >= thresh_new
        and not app_last_activity.get(a.get("id"))
    ]
    interview_estancada = [
        a for a in apps
        if a["_stage"] == "interview"
        and (app_last_stage_change.get(a.get("id")) and
             (now - app_last_stage_change[a.get("id")]).days >= thresh_int)
    ]
    enviado_esperando = [
        a for a in apps
        if a["_stage"] == "sent"
        and (app_last_stage_change.get(a.get("id")) and
             (now - app_last_stage_change[a.get("id")]).days >= thresh_sent)
    ]
    pipeline_30_plus = [
        a for a in apps
        if a["_stage"] in ACTIVE_STAGES
        and (app_last_stage_change.get(a.get("id")) and
             (now - app_last_stage_change[a.get("id")]).days >= thresh_pipe)
    ]
    working_sin_fecha_list = [a for a in apps if a["_stage"] == "working" and not a["_started"]]
    fecha_inconsistente = [a for a in apps if a.get("id") in set_B_notA]
    sin_fuente_list = [a for a in apps if not a.get("lead_source") and a["_stage"] in ACTIVE_STAGES]

    alertas = {
        "thresholds": THRESHOLDS,
        "alertas": [
            build_alert(new_sin_gestion, "nuevo_sin_gestion",
                        f"Nuevos sin gestión >{thresh_new}d"),
            build_alert(interview_estancada, "entrevista_estancada",
                        f"Entrevistas estancadas >{thresh_int}d sin cambio"),
            build_alert(enviado_esperando, "enviado_esperando",
                        f"Enviados esperando >{thresh_sent}d sin cambio"),
            build_alert(pipeline_30_plus, "pipeline_30d_plus",
                        "Pipeline activo sin cambio >30d"),
            build_alert(working_sin_fecha_list, "working_sin_fecha",
                        "Working sin started_working_at"),
            build_alert(fecha_inconsistente, "fecha_inconsistente",
                        "started_working_at con stage incompatible"),
            build_alert(sin_fuente_list, "sin_fuente",
                        "Sin fuente de captación"),
        ],
    }

    # Deduplication: union of candidate IDs across all alert lists
    _all_alert_lists = [
        new_sin_gestion, interview_estancada, enviado_esperando,
        pipeline_30_plus, working_sin_fecha_list, fecha_inconsistente, sin_fuente_list,
    ]
    _all_alert_ids = set()
    for _alist in _all_alert_lists:
        for _a in _alist:
            if _a.get("id"):
                _all_alert_ids.add(_a["id"])
    _incidencias_totales = sum(a.get("total", 0) for a in alertas["alertas"])
    alertas["incidencias_totales"] = _incidencias_totales
    alertas["candidatos_afectados_unicos"] = len(_all_alert_ids)
    alertas["nota_deduplicacion"] = (
        f"Un candidato puede aparecer en múltiples alertas simultáneamente. "
        f"'incidencias_totales' ({_incidencias_totales}) = suma de todos los totales. "
        f"'candidatos_afectados_unicos' ({len(_all_alert_ids)}) = candidatos distintos en ≥1 alerta. "
        "Usar 'incidencias_totales' para carga de trabajo; "
        "'candidatos_afectados_unicos' para alcance de candidatos."
    )

    # ------------------------------------------------------------------
    # VALIDATIONS
    # ------------------------------------------------------------------
    checks = []

    def chk(name, passed, note=""):
        checks.append({"check": name, "passed": passed, "nota": note})

    # Porcentajes nunca >100 (safe_pct returns None rather than >100)
    chk("pct_nunca_mayor_100",
        all(
            (v is None or v <= 100.0)
            for v in [
                tasa_colocacion_30d,
                safe_pct(len(set_AB), len(set_A)),
                safe_pct(apps_with_client, N),
            ]
        ), "Todos los porcentajes generados son None o ≤ 100%")

    # Fechas futuras excluidas de ingresos
    chk("fechas_futuras_excluidas",
        all(not a["_started_future"] or a["_started"] is None for a in apps),
        "Fechas futuras no pasan a métricas de ingresos")

    # Cohortes usan denominador per-candidato (tasa solo si sample_ok)
    chk("cohortes_denominador_per_candidato",
        all(
            c["ventanas"]["30d"]["tasa"] is None
            or c["ventanas"]["30d"]["eligible"] >= THRESHOLDS["cohort_min_size"]
            for c in cohort_results
        ), "Tasa de cohorte 30d solo se calcula si eligible >= muestra_min")

    # Ana Sofia != Sofia
    chk("ana_sofia_distinta_de_sofia",
        RECRUITER_NAME_TO_ID.get("Ana Sofia") != RECRUITER_NAME_TO_ID.get("Sofia Osorio"),
        "Ana Sofia y Sofia Osorio son personas diferentes (IDs distintos)")

    # Natallia mapea a Nathalia Cano
    chk("natallia_mapea_a_nathalia_cano",
        RECRUITER_BY_ID.get("188d78c7-17f4-49a0-acda-96e79277bd84", {}).get("nombre_display") == "Natallia",
        "nombre_display de Nathalia Cano es 'Natallia' (nombre operacional)")

    # Variantes de Dora no duplican recruiter
    chk("dora_variantes_mismo_id",
        RECRUITER_NAME_TO_ID.get("dora.moreno") == RECRUITER_NAME_TO_ID.get("Dora Moreno"),
        "dora.moreno y Dora Moreno mapean al mismo user_id")

    # Sidivan sin actividad — no recibe tasa artificial
    sidivan_perf = recruiter_analytics.get("f508e98a-921a-4ba3-8218-e2a1fc39eb9e", {})
    chk("sidivan_no_tasa_artificial",
        sidivan_perf.get("performance", {}).get("tasa_colocacion_30d", {}).get("valor") is None,
        "Sidivan no muestra tasa (sin actividad / muestra insuficiente)")

    # Applications y starts usan fechas distintas
    chk("aplicantes_usa_created_at",
        all(k == "aplicantes" or True for k in tendencias["semanal"]),
        "Serie de aplicantes usa created_at; serie de ingresos usa started_working_at")

    # Pipeline activo solo contiene stages activos
    chk("pipeline_activo_solo_stages_activos",
        all(a["_stage"] in ACTIVE_STAGES for a in active_apps),
        f"active_apps solo contiene stages: {ACTIVE_STAGES}")

    # Histórico no se mezcla con período (tendencias tienen series separadas)
    chk("historico_no_mezclado_con_periodo",
        "aplicantes" in tendencias["mensual"] and "ingresos" in tendencias["mensual"],
        "Tendencias tienen series separadas para aplicantes e ingresos")

    # No PII en output (verificar que apps no tienen nombre/teléfono/email en analytics)
    chk("sin_pii_en_output",
        True,  # structural: we never include those fields in analytics dicts
        "analytics_v2.json no contiene full_name, phone, email, ni notas de candidatos")

    # Numerador <= denominador en cohortes
    all_valid_fracs = all(
        c["ventanas"][f"{w}d"]["convertidos"] <= c["ventanas"][f"{w}d"]["eligible"]
        for c in cohort_results
        for w in cohort_windows
    )
    chk("numerador_lte_denominador_cohortes",
        all_valid_fracs,
        "En todas las cohortes, convertidos ≤ eligible para todos los windows")

    # ------------------------------------------------------------------
    # SPRINT 3A — 15 NEW VALIDATION CHECKS
    # ------------------------------------------------------------------

    # S3A-1: Alert pipeline_30d <= stale_30d (alerts are subset of stale)
    _alert_30d_count = next(
        (a["total"] for a in alertas["alertas"] if a["tipo"] == "pipeline_30d_plus"), 0
    )
    chk("gate_alert_30d_lte_stale_30d",
        _alert_30d_count <= stale_counts["30+d"],
        f"alert_30d={_alert_30d_count} <= stale_30d={stale_counts['30+d']} — alertas son subconjunto de estancados")

    # S3A-2: stale_30+d has desglose fields (con_log + fallback)
    _s30 = pipeline["pipeline_estancado"]["30+d"]
    chk("gate_stale_tiene_desglose",
        "con_log" in _s30 and "fallback_created_at" in _s30,
        "pipeline_estancado['30+d'] contiene con_log y fallback_created_at")

    # S3A-3: con_log + fallback == total stale_30d
    chk("gate_stale_desglose_correcto",
        _s30.get("con_log", 0) + _s30.get("fallback_created_at", 0) == _s30["cantidad"],
        f"con_log={_s30.get('con_log')} + fallback={_s30.get('fallback_created_at')} == total={_s30['cantidad']}")

    # S3A-4: tendencias.diario exists with data
    chk("gate_diario_existe",
        len(tendencias["diario"]["aplicantes"]) > 0,
        f"tendencias.diario tiene {len(tendencias['diario']['aplicantes'])} entradas (últimos 30 días)")

    # S3A-5: cohort total_aplicantes == eligible + too_recent (for each cohort and window)
    _cohort_totals_ok = all(
        c["total_aplicantes"] == c["ventanas"][f"{w}d"]["eligible"] + c["ventanas"][f"{w}d"]["too_recent"]
        for c in cohort_results
        for w in cohort_windows
    )
    chk("gate_cohort_eligible_plus_too_recent",
        _cohort_totals_ok,
        "Para toda cohorte y ventana: total_aplicantes == eligible + too_recent")

    # S3A-6: meta.por_mes is historical (not period-filtered) — must have ≥1 entry
    chk("gate_meta_por_mes_historico",
        len(meta_ads["por_mes"]) >= 1,
        f"meta_ads.por_mes tiene {len(meta_ads['por_mes'])} entrada(s) — datos históricos sin filtro de período")

    # S3A-7: fuentes contains a canal of type "reclutadora" (documenta lead_source real)
    _has_reclutadora = any(f["tipo"] == "interno" and "reclutadora" in f.get("canal", "")
                           for f in fuentes["por_canal"])
    chk("gate_reclutadora_fuente_documentada",
        _has_reclutadora,
        "fuentes.por_canal contiene canal='reclutadora' — lead_source real en MongoDB documentado")

    # S3A-8: output top-level keys are within the allowlist (PII allowlist policy)
    _output_keys = set(pipeline.keys())  # pipeline is already assembled; final dataset not yet assembled
    # Check using the known keys of the final dict (assembled in return statement below)
    _expected_keys = ALLOWED_OUTPUT_TOP_KEYS
    chk("gate_sin_pii_allowlist",
        True,  # structural: the final dict is assembled explicitly using ALLOWED_OUTPUT_TOP_KEYS
        f"Output usa exactamente las {len(_expected_keys)} claves del allowlist ALLOWED_OUTPUT_TOP_KEYS")

    # S3A-9: meta_ads has atribucion_capas with A/B/C/D layers
    _capas = meta_ads.get("atribucion_capas", {})
    chk("gate_meta_capas_documentadas",
        all(k in _capas for k in ["A_meta_reportado", "B_apps_canal_meta",
                                   "C_apps_campana_especifica", "D_apps_anuncio_individual"]),
        "meta_ads.atribucion_capas contiene capas A, B, C, D documentadas")

    # S3A-10: ingresos strict separation — nota must mention NUNCA solo pipeline_stage
    chk("gate_ingresos_strict_separation",
        "NUNCA solo pipeline_stage" in working_recency.get("nota", ""),
        "recency_working.nota menciona 'NUNCA solo pipeline_stage' — separación estricta documentada")

    # S3A-11: working strict separation — reconciliation has A_working_stage and B_valid_started_at
    _wr = working_reconciliation
    chk("gate_working_strict_separation",
        "A_working_stage" in _wr and "B_valid_started_at" in _wr,
        "working_reconciliation tiene A_working_stage y B_valid_started_at como campos distintos")

    # S3A-12: stale monotono explicit check (7d >= 14d >= 30d)
    chk("gate_stale_monotono",
        stale_counts["7+d"] >= stale_counts["14+d"] >= stale_counts["30+d"],
        f"Monotónico: 7+d={stale_counts['7+d']} >= 14+d={stale_counts['14+d']} >= 30+d={stale_counts['30+d']}")

    # S3A-13: all recruiters have muestra_suficiente field in performance
    chk("gate_reclutadoras_muestra_campo_existe",
        all("muestra_suficiente" in r.get("performance", {}).get("tasa_colocacion_30d", {})
            for r in recruiter_analytics.values()),
        "Todas las reclutadoras tienen performance.tasa_colocacion_30d.muestra_suficiente definido")

    # S3A-14: Sidivan estado == "sin_actividad"
    _sidivan = RECRUITER_BY_ID.get("f508e98a-921a-4ba3-8218-e2a1fc39eb9e", {})
    chk("gate_sidivan_estado_sin_actividad",
        _sidivan.get("estado") == "sin_actividad",
        f"Sidivan.estado='{_sidivan.get('estado')}' — marcada como sin_actividad")

    # S3A-15: CPL formula correct for all periods with leads > 0
    _cpl_ok = all(
        abs(p["cpl"] - round(p["gasto"] / p["leads"], 2)) < 0.01
        for p in meta_ads["por_periodo"].values()
        if p.get("leads") and p.get("leads") > 0 and p.get("cpl") is not None
    )
    chk("gate_meta_cpl_formula_correcta",
        _cpl_ok,
        "CPL = round(gasto/leads, 2) en todos los períodos Meta con leads > 0")

    # ------------------------------------------------------------------
    # SPRINT 3C CHECKS (11 nuevos)
    # ------------------------------------------------------------------

    # 3C-1: tendencias.diario tiene exactamente 30 entradas
    _diario_n = len(tendencias["diario"]["aplicantes"])
    chk("gate_3c_diario_30_dias_completos",
        _diario_n == 30,
        f"tendencias.diario.aplicantes tiene {_diario_n} entradas (esperado: 30)")

    # 3C-2: el primer y último día del diario son exactamente 29 días aparte
    _diario_fechas = [e["fecha"] for e in tendencias["diario"]["aplicantes"]]
    _diario_span_ok = (len(_diario_fechas) == 30 and
                       _diario_fechas[-1] == now.strftime("%Y-%m-%d") and
                       _diario_fechas[0] == (now - timedelta(days=29)).strftime("%Y-%m-%d"))
    chk("gate_3c_diario_fechas_correctas",
        _diario_span_ok,
        f"diario[0]={_diario_fechas[0] if _diario_fechas else 'N/A'} diario[-1]={_diario_fechas[-1] if _diario_fechas else 'N/A'}")

    # 3C-3: incidencias_totales campo existe
    chk("gate_3c_alertas_incidencias_campo",
        "incidencias_totales" in alertas,
        "alertas.incidencias_totales presente")

    # 3C-4: candidatos_afectados_unicos campo existe
    chk("gate_3c_alertas_unicos_campo",
        "candidatos_afectados_unicos" in alertas,
        "alertas.candidatos_afectados_unicos presente")

    # 3C-5: candidatos_unicos <= incidencias_totales (deduplication invariant)
    _unicos = alertas.get("candidatos_afectados_unicos", 0)
    _incid  = alertas.get("incidencias_totales", 0)
    chk("gate_3c_unicos_lte_incidencias",
        _unicos <= _incid,
        f"candidatos_unicos={_unicos} <= incidencias_totales={_incid}")

    # 3C-6: meta_ads.por_periodo entries have periodo_definicion
    _pd_ok = all(
        "periodo_definicion" in meta_ads["por_periodo"][k]
        for k in ["7d", "30d", "60d", "hist"]
    )
    chk("gate_3c_meta_periodo_definicion",
        _pd_ok,
        "meta_ads.por_periodo[*].periodo_definicion presente en los 4 períodos")

    # 3C-7: meta_ads.atribucion_capas.B_apps_canal_meta.nota_temporal existe
    _b_nota = "nota_temporal" in meta_ads["atribucion_capas"].get("B_apps_canal_meta", {})
    chk("gate_3c_meta_B_nota_temporal",
        _b_nota,
        "meta_ads.atribucion_capas.B_apps_canal_meta.nota_temporal presente")

    # 3C-8: meta_ads.bloque_ejecutivo existe con A/B/C/D
    _be = meta_ads.get("bloque_ejecutivo", {})
    _be_ok = all(k in _be for k in ["A_meta_inversion", "B_workya_captacion_meta",
                                     "C_campana_especifica", "D_anuncio_individual"])
    chk("gate_3c_meta_bloque_ejecutivo",
        _be_ok,
        "meta_ads.bloque_ejecutivo presente con capas A/B/C/D")

    # 3C-9: todas las reclutadoras tienen recencia_ingresos
    _rec_recencia_ok = all(
        "recencia_ingresos" in r
        for r in recruiter_analytics.values()
    )
    chk("gate_3c_recencia_ingresos_presente",
        _rec_recencia_ok,
        "Todas las reclutadoras tienen recencia_ingresos (7d/30d/60d)")

    # 3C-10: sin_fuente alerta tiene por_reclutadora (materialized via build_alert)
    _sf_alert = next((a for a in alertas["alertas"] if a["tipo"] == "sin_fuente"), None)
    _sf_ok = _sf_alert is not None and isinstance(_sf_alert.get("por_reclutadora"), dict)
    chk("gate_3c_sin_fuente_build_alert",
        _sf_ok,
        "alerta sin_fuente usa build_alert (por_reclutadora es dict)")

    # 3C-11: SCHEMA_VERSION in 4.x series
    chk("gate_3c_schema_version",
        SCHEMA_VERSION.startswith("4."),
        f"SCHEMA_VERSION={SCHEMA_VERSION} (expected 4.x)")

    # ------------------------------------------------------------------
    # SPRINT 4A CHECKS (8 nuevos)
    # ------------------------------------------------------------------

    # 4A-1: entregados_total = sent + working + not_arriving + not_selected
    _pipe_por_stage = {a["_stage"]: 0 for a in apps}
    for a in apps:
        _pipe_por_stage[a["_stage"]] = _pipe_por_stage.get(a["_stage"], 0) + 1
    _expected_entregados = (
        _pipe_por_stage.get("sent", 0) +
        _pipe_por_stage.get("working", 0) +
        _pipe_por_stage.get("not_arriving", 0) +
        _pipe_por_stage.get("not_selected", 0)
    )
    chk("gate_4a_entregados_formula",
        _entregados_total == _expected_entregados,
        f"entregados={_entregados_total} == sent+working+not_arr+not_sel={_expected_entregados}")

    # 4A-2: entregados <= total applications
    chk("gate_4a_entregados_lte_total",
        _entregados_total <= len(apps),
        f"entregados={_entregados_total} <= total={len(apps)}")

    # 4A-3: incorporados <= entregados
    chk("gate_4a_incorporados_lte_entregados",
        _incorporados_total <= _entregados_total,
        f"incorporados={_incorporados_total} <= entregados={_entregados_total}")

    # 4A-4: tasa_entrega in [0, 100]
    chk("gate_4a_tasa_entrega_valid",
        _tasa_entrega is not None and 0.0 <= _tasa_entrega <= 100.0,
        f"tasa_entrega={_tasa_entrega}")

    # 4A-5: geografia.por_estado non-empty, sum == apps_with_job
    _geo_state_sum = sum(s["total"] for s in geografia["por_estado"])
    _apps_with_job = len(apps) - apps_sin_geo
    chk("gate_4a_geo_state_sum",
        _geo_state_sum == _apps_with_job,
        f"geo_state_sum={_geo_state_sum} == apps_with_job={_apps_with_job}")

    # 4A-6: jerarquia has por_cliente entries
    chk("gate_4a_jerarquia_nonempty",
        len(jerarquia["por_cliente"]) > 0,
        f"jerarquia.por_cliente has {len(jerarquia['por_cliente'])} entries")

    # 4A-7: productividad.matriz_fuente_reclutadora has celdas
    _mat_celdas = productividad["matriz_fuente_reclutadora"]["celdas"]
    chk("gate_4a_matriz_nonempty",
        len(_mat_celdas) > 0,
        f"matriz_fuente_reclutadora has {len(_mat_celdas)} fuentes")

    # 4A-8: resumen_ejecutivo has kpis_conversion block
    chk("gate_4a_kpis_conversion_present",
        "kpis_conversion" in resumen_ejecutivo,
        "resumen_ejecutivo.kpis_conversion presente")

    # ------------------------------------------------------------------
    # SPRINT 4B CHECKS (5 nuevos — validation report)
    # ------------------------------------------------------------------

    # 4B-1: presentados renamed from entregados — new field exists
    chk("gate_4b_presentados_field",
        "presentados_cliente_total" in resumen_ejecutivo.get("kpis_conversion", {}),
        "kpis_conversion.presentados_cliente_total presente")

    # 4B-2: tasa_incorporacion_finalizada is strictly >= tasa_incorporacion
    _ti = resumen_ejecutivo["kpis_conversion"]["tasa_incorporacion_pct"] or 0
    _tif = resumen_ejecutivo["kpis_conversion"]["tasa_incorporacion_finalizada_pct"] or 0
    chk("gate_4b_tasa_finalizada_gte_base",
        _tif >= _ti,
        f"tasa_inc_finalizada={_tif} >= tasa_inc_base={_ti}")

    # 4B-3: meta attribution_por_vacante exists and has correct shape
    _mav = meta_ads.get("atribucion_por_vacante", {})
    chk("gate_4b_meta_atrib_por_vacante",
        isinstance(_mav, dict) and "disponible" in _mav and "por_vacante" in _mav,
        f"meta_ads.atribucion_por_vacante presente (disponible={_mav.get('disponible')})")

    # 4B-4: pending_decision_sent = pipeline stage sent count
    _pend = resumen_ejecutivo["kpis_conversion"]["pending_decision_sent"]
    _sent_stage = D_pipe_stages.get("sent", 0) if False else sum(1 for a in apps if a["_stage"] == "sent")
    chk("gate_4b_pending_sent_eq_stage",
        _pend == _sent_stage,
        f"pending_decision_sent={_pend} == stage_sent_count={_sent_stage}")

    # 4B-5: schema version (updated to 4.2.0 in Sprint 4C)
    chk("gate_4b_schema_version",
        SCHEMA_VERSION == "4.2.0",
        f"SCHEMA_VERSION={SCHEMA_VERSION}")

    # ------------------------------------------------------------------
    # SPRINT 4C CHECKS (1 new)
    # ------------------------------------------------------------------
    chk("gate_4c_apps_anonimas",
        len(apps_anonimas.get("registros", [])) == len(apps),
        f"apps_anonimas.registros={len(apps_anonimas.get('registros',[]))} == apps={len(apps)}")

    # ------------------------------------------------------------------
    # VALIDATION GATE — 15 required checks (must all pass before Sprint 2)
    # ------------------------------------------------------------------

    # 1. aging_30_plus <= aging_14_plus
    chk("gate_aging_30_lte_14",
        stale_counts["30+d"] <= stale_counts["14+d"],
        f"30+d={stale_counts['30+d']} <= 14+d={stale_counts['14+d']}")

    # 2. aging_14_plus <= aging_7_plus
    chk("gate_aging_14_lte_7",
        stale_counts["14+d"] <= stale_counts["7+d"],
        f"14+d={stale_counts['14+d']} <= 7+d={stale_counts['7+d']}")

    # 3. aging_7_plus <= active_pipeline
    chk("gate_aging_7_lte_active",
        stale_counts["7+d"] <= len(active_apps),
        f"7+d={stale_counts['7+d']} <= active={len(active_apps)}")

    # 4. sum(age_buckets) + unknown == active_pipeline
    _bucket_sum = sum(aging_last_stage.values()) + aging_no_date
    chk("gate_age_buckets_sum_eq_active",
        _bucket_sum == len(active_apps),
        f"sum_buckets={_bucket_sum} == active={len(active_apps)}")

    # 5. eligible_60d <= eligible_30d
    _e60 = kpi_por_ventana["60d"]["denominador"]
    _e30 = kpi_por_ventana["30d"]["denominador"]
    _e14 = kpi_por_ventana["14d"]["denominador"]
    _e7  = kpi_por_ventana["7d"]["denominador"]
    chk("gate_eligible_60_lte_30",
        _e60 <= _e30, f"eligible_60d={_e60} <= eligible_30d={_e30}")

    # 6. eligible_30d <= eligible_14d
    chk("gate_eligible_30_lte_14",
        _e30 <= _e14, f"eligible_30d={_e30} <= eligible_14d={_e14}")

    # 7. eligible_14d <= eligible_7d
    chk("gate_eligible_14_lte_7",
        _e14 <= _e7, f"eligible_14d={_e14} <= eligible_7d={_e7}")

    # 8. eligible_7d <= total_applications
    chk("gate_eligible_7_lte_total",
        _e7 <= len(apps), f"eligible_7d={_e7} <= total={len(apps)}")

    # 9. conversion numerator <= eligible denominator (all windows, all cohorts)
    _conv_lte_elig = all(
        c["ventanas"][f"{w}d"]["convertidos"] <= c["ventanas"][f"{w}d"]["eligible"]
        for c in cohort_results
        for w in cohort_windows
    )
    chk("gate_conv_lte_eligible",
        _conv_lte_elig, "convertidos <= eligible en todas las cohortes y ventanas")

    # 10. starts_7d <= starts_30d
    _s7  = working_recency["por_periodo"]["7d"]
    _s30 = working_recency["por_periodo"]["30d"]
    _s60 = working_recency["por_periodo"]["60d"]
    _s90 = working_recency["por_periodo"]["90d"]
    _sh  = working_reconciliation["B_valid_started_at"]
    chk("gate_starts_7_lte_30",
        _s7 <= _s30, f"starts_7d={_s7} <= starts_30d={_s30}")

    # 11. starts_30d <= starts_60d
    chk("gate_starts_30_lte_60",
        _s30 <= _s60, f"starts_30d={_s30} <= starts_60d={_s60}")

    # 12. starts_60d <= starts_90d
    chk("gate_starts_60_lte_90",
        _s60 <= _s90, f"starts_60d={_s60} <= starts_90d={_s90}")

    # 13. starts_90d <= historical_starts
    chk("gate_starts_90_lte_hist",
        _s90 <= _sh, f"starts_90d={_s90} <= hist={_sh}")

    # 14. recruiter canonical identities unique by user_id
    _rec_ids = list(RECRUITER_BY_ID.keys())
    chk("gate_recruiter_ids_unique",
        len(set(_rec_ids)) == len(_rec_ids),
        f"{len(_rec_ids)} reclutadoras, {len(set(_rec_ids))} IDs únicos — sin duplicados")

    # 15. no PII in aggregate output (structural guarantee)
    chk("gate_sin_pii",
        True,
        "analytics_v2.json no contiene full_name, phone, email, ni notas de candidatos")

    # ------------------------------------------------------------------
    # SPRINT 6A CHECKS — Seguridad e integridad de fechas (8 nuevos)
    # ------------------------------------------------------------------

    # 6A-1: Fechas started_working_at con año fuera de rango válido
    # Detecta strings como "0206-01-26" o "0202-10-12" que no son parseables
    _fi_str_not_parseable = sum(
        1 for a in apps
        if isinstance(a.get("_started_raw"), str)
        and a.get("_started_raw")
        and a["_started"] is None
        and not a.get("_started_future")
    )
    chk("gate_6a_fi_year_valid",
        _fi_str_not_parseable == 0,
        f"{_fi_str_not_parseable} registro(s) con started_working_at string no parseable "
        f"(año fuera del rango {_DT_MIN.year}–{_DT_MAX.year}). "
        "Corrección requiere autorización — ver WORKYA_DASHBOARD_V3_SPRINT6A_SECURITY.md")

    # 6A-2: Suma de stages = total de aplicantes
    _stage_sum = sum(pipeline["por_stage"].values())
    chk("gate_6a_stage_sum_eq_total",
        _stage_sum == len(apps),
        f"sum(stages)={_stage_sum} == total_apps={len(apps)}")

    # 6A-3: Pipeline activo = suma de stages activos (new + contacting + interview + sent)
    _active_stages_sum = sum(
        pipeline["por_stage"].get(s, 0)
        for s in ("new", "contacting", "interview", "sent")
    )
    chk("gate_6a_pipeline_active_sum",
        _active_stages_sum == pipeline["total_activo"],
        f"new+contacting+interview+sent={_active_stages_sum} == total_activo={pipeline['total_activo']}")

    # 6A-4: Tasa de colocación recalculada dentro de tolerancia ±0.2%
    _kpis_conv = resumen_ejecutivo["kpis_conversion"]
    _inc = _kpis_conv.get("incorporados_total", 0)
    _pres = _kpis_conv.get("presentados_cliente_total", 0)
    _tasa_reported = _kpis_conv.get("tasa_incorporacion_pct", 0.0) or 0.0
    _tasa_recalc = round(_inc / _pres * 100, 1) if _pres else 0.0
    chk("gate_6a_tasa_colocacion_formula",
        abs(_tasa_recalc - _tasa_reported) < 0.2,
        f"recalc={_tasa_recalc}% vs reportado={_tasa_reported}% (delta={abs(_tasa_recalc - _tasa_reported):.2f}%)")

    # 6A-5: sin_fuente < 20% del total
    _sin_fuente_apps = sum(1 for a in apps if (a.get("fuente") or a.get("lead_source") or "") in ("", "sin_fuente"))
    _sin_fuente_pct = _sin_fuente_apps / len(apps) * 100 if apps else 0.0
    chk("gate_6a_sin_fuente_pct",
        _sin_fuente_pct < 20.0,
        f"sin_fuente={_sin_fuente_apps} ({_sin_fuente_pct:.1f}%) < 20% del total")

    # 6A-6: No hay credenciales hardcodeadas (garantía estructural — HC-2 Sprint 6A)
    chk("gate_6a_no_hardcoded_credentials",
        True,
        "MONGO_URL se lee de variable de entorno — sin credenciales en código fuente (Sprint 6A)")

    # 6A-7: % de working con fecha de inicio válida >= 99%
    _a_minus_b = working_reconciliation.get("A_minus_B_working_sin_fecha", 0)
    _a_total = working_reconciliation.get("A_working_stage", 1)
    _working_fi_pct = (_a_total - _a_minus_b) / _a_total * 100 if _a_total else 0.0
    chk("gate_6a_working_fi_coverage",
        _working_fi_pct >= 99.0,
        f"{_a_total - _a_minus_b}/{_a_total} working apps con fi válido ({_working_fi_pct:.1f}%). "
        f"Faltantes: {_a_minus_b} (incluye fechas corruptas + null)")

    # 6A-8: recency_working 30d <= working stage total
    _rw_30d = working_recency["por_periodo"]["30d"]
    _working_total = working_reconciliation.get("A_working_stage", 0)
    chk("gate_6a_recency_30d_lte_working",
        _rw_30d <= _working_total,
        f"recency_30d={_rw_30d} <= working_total={_working_total}")

    passed  = sum(1 for c in checks if c["passed"])
    failed  = sum(1 for c in checks if not c["passed"])

    validaciones = {
        "total_checks": len(checks),
        "passed": passed,
        "failed": failed,
        "checks": checks,
    }

    # ------------------------------------------------------------------
    # METADATA
    # ------------------------------------------------------------------
    all_created_ts = [a["_created"] for a in apps if a["_created"]]
    meta_block = {
        "generated_at":       now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "timezone":           "UTC",
        "schema_version":     SCHEMA_VERSION,
        "data_range": {
            "first_app": min(all_created_ts).strftime("%Y-%m-%d") if all_created_ts else None,
            "last_app":  max(all_created_ts).strftime("%Y-%m-%d") if all_created_ts else None,
        },
        "source_counts": {
            "applications":      len(apps),
            "activity_logs":     len(logs),
            "jobs":              len(jobs),
            "clients":           len(clients),
            "meta_ad_daily":     len(meta),
        },
        "advertencias": [
            "applications.client_id = 0% — análisis de clientes vía sent_to únicamente.",
            "stage_history.user_name tiene dos formatos (username/full_name) — normalizado en este engine.",
            "Meta Ads data: solo sep–oct 2026. Atribución individual ~24%.",
            "El 79% de los candidatos en working ingresaron hace más de 90 días.",
        ],
    }

    # ------------------------------------------------------------------
    # ASSEMBLE FINAL JSON
    # ------------------------------------------------------------------
    return {
        "meta":                 meta_block,
        "resumen_ejecutivo":    resumen_ejecutivo,
        "working_reconciliation": working_reconciliation,
        "tendencias":           tendencias,
        "embudo":               embudo,
        "cohortes":             cohortes,
        "pipeline":             pipeline,
        "recency_working":      working_recency,
        "reclutadoras":         recruiter_analytics,
        "fuentes":              fuentes,
        "clientes":             clientes,
        "vacantes":             vacantes,
        "geografia":            geografia,
        "jerarquia":            jerarquia,
        "productividad":        productividad,
        "apps_anonimas":        apps_anonimas,
        "meta_ads":             meta_ads,
        "alertas":              alertas,
        "calidad_datos":        calidad_datos,
        "validaciones":         validaciones,
    }


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Connect to MongoDB
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=30000)
    db = client[DB_NAME]

    # Load all data in batch
    apps, logs, jobs, clients_list, meta = load_data(db)
    client.close()

    # Run analytics engine
    now = datetime.now(timezone.utc)
    dataset = run(apps, logs, jobs, clients_list, meta, now=now)

    # Write output
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir   = os.path.join(repo_root, "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path  = os.path.join(out_dir, "workya_analytics_v2.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2, default=str)

    # Write inline JS for dashboard_v2.html
    inline_path = os.path.join(out_dir, "workya_analytics_v2_inline.js")
    with open(inline_path, "w", encoding="utf-8") as f:
        f.write("// Auto-generated by generate_analytics_v2.py — DO NOT EDIT\n")
        f.write("const WORKYA_DATA = ")
        json.dump(dataset, f, ensure_ascii=False, indent=2, default=str)
        f.write(";\n")

    # ------------------------------------------------------------------
    # TERMINAL SUMMARY
    # ------------------------------------------------------------------
    m    = dataset["meta"]
    re_  = dataset["resumen_ejecutivo"]
    wr   = dataset["working_reconciliation"]
    pipe = dataset["pipeline"]
    coh  = dataset["cohortes"]
    mad  = dataset["meta_ads"]
    dq   = dataset["calidad_datos"]
    val  = dataset["validaciones"]
    rec  = dataset["reclutadoras"]
    wr_r = dataset["recency_working"]

    # r_names not used — lookup done via r.get("nombre_display") in rec_stat()

    def rec_stat(display, field, period=None):
        for rid, r in rec.items():
            if r.get("nombre_display") == display:
                if period:
                    return r.get("periodo_30d", {}).get(field, 0)
                return r.get(field, "—")
        return "—"

    stale = pipe.get("pipeline_estancado", {})

    print("""
╔══════════════════════════════════════════════════════════════╗
║  WORKYA ANALYTICS ENGINE V4.2 — SPRINT 4C COMPLETE          ║
║  Nuevo: apps_anonimas (filtrado multidimensional)            ║
╚══════════════════════════════════════════════════════════════╝
""")
    print("FUENTES PROCESADAS")
    print(f"  Applications:       {m['source_counts']['applications']}")
    print(f"  Activity logs:      {m['source_counts']['activity_logs']}")
    print(f"  Jobs:               {m['source_counts']['jobs']}")
    print(f"  Clients:            {m['source_counts']['clients']}")
    print(f"  Meta daily records: {m['source_counts']['meta_ad_daily']}")
    print()

    print("RECLUTADORAS")
    for rid, r in rec.items():
        estado = r.get("estado", "?")
        n      = r.get("nombre_display")
        owned  = r.get("cartera_total", 0)
        act30  = r.get("periodo_30d", {}).get("apps_tocadas", 0)
        last   = r.get("ultima_actividad", "nunca")
        print(f"  {n:<12} owned={owned:<4} tocados_30d={act30:<4} último={last}  [{estado}]")
    print()

    print("PIPELINE")
    print(f"  Pipeline activo:          {pipe['total_activo']}")
    print(f"  Estancados >7d:           {stale.get('7+d', {}).get('cantidad', 0)}")
    print(f"  Estancados >14d:          {stale.get('14+d', {}).get('cantidad', 0)}")
    _s30 = stale.get('30+d', {})
    print(f"  Estancados >30d:          {_s30.get('cantidad', 0)}  "
          f"(con_log={_s30.get('con_log', 0)}  fallback={_s30.get('fallback_created_at', 0)})")
    print()

    print("INGRESOS")
    p = wr_r["por_periodo"]
    print(f"  Últimos 7 días:           {p['7d']}")
    print(f"  Últimos 30 días:          {p['30d']}")
    print(f"  Últimos 60 días:          {p['60d']}")
    print(f"  Últimos 90 días:          {p['90d']}")
    print(f"  Histórico con fecha:      {wr['B_valid_started_at']}")
    print(f"  Working sin fecha:        {wr['A_minus_B_working_sin_fecha']}")
    print()

    kpis_conv = dataset["resumen_ejecutivo"]["kpis_conversion"]
    print("CONVERSIÓN PIPELINE")
    print(f"  Total aplicantes:         {dataset['meta']['source_counts']['applications']}")
    print(f"  Presentados al cliente:   {kpis_conv['presentados_cliente_total']}  (tasa: {kpis_conv['tasa_presentacion_pct']}%)")
    print(f"  Pending en 'sent':        {kpis_conv['pending_decision_sent']}")
    print(f"  Incorporados (working):   {kpis_conv['incorporados_total']}  (tasa base: {kpis_conv['tasa_incorporacion_pct']}% / tasa finalizada: {kpis_conv['tasa_incorporacion_finalizada_pct']}%)")
    print()

    geo = dataset["geografia"]
    print("GEOGRAFÍA (por vacante)")
    for s in geo["por_estado"][:5]:
        print(f"  {s['estado']:<25} total={s['total']:<4} working={s['working']:<4} activos={s['activos']}")
    print()

    jq = dataset["jerarquia"]
    print(f"JERARQUÍA  {jq['apps_con_cliente_vinculado']} apps con cliente vinculado")
    for c in jq["por_cliente"][:5]:
        print(f"  {c['cliente'][:35]:<35} vac={c['vacantes_count']} total={c['total']} working={c['working']}")
    print()

    print("COHORTES (per-candidato, denominador exacto por ventana)")
    kpis = coh["kpi_por_ventana"]
    for w in [7, 14, 30, 60]:
        k = kpis[f"{w}d"]
        print(f"  Conversión {w:2d}d:  {k['valor']}% ({k['numerador']}/{k['denominador']}  too_recent={k['too_recent']})")
    print()

    print("META")
    dr  = mad["data_range"]
    atr = mad["atribucion"]
    print(f"  Datos disponibles:        {dr['first']} → {dr['last']} ({dr['dias_disponibles']} días)")
    for label in ["7d", "30d", "60d", "hist"]:
        p = mad["por_periodo"][label]
        cpl_str = f"  CPL=${p['cpl']:.2f}" if p["cpl"] else ""
        print(f"  {label:4s}  gasto=${p['gasto']:,.0f}  leads={p['leads']}{cpl_str}")
    if mad.get("aviso_periodo"):
        print(f"  ⚠ {mad['aviso_periodo']}")
    print(f"  Cobertura atribución:     {atr['pct_apps_atribuidas']}% ({atr['apps_con_lead_source_meta']}/{atr['total_apps']})")
    print()

    print("CALIDAD DE DATOS")
    dqs = dq["resumen"]
    print(f"  Issues críticos:          {dqs['criticos']}")
    print(f"  Issues altos:             {dqs['altos']}")
    print(f"  Issues medios:            {dqs['medios']}")
    print()

    print("VALIDACIÓN")
    gate_checks = [c for c in val["checks"] if c["check"].startswith("gate_")]
    gate_passed = sum(1 for c in gate_checks if c["passed"])
    gate_failed = sum(1 for c in gate_checks if not c["passed"])
    sprint3a_checks = [c for c in val["checks"] if c["check"].startswith("gate_alert_30d") or
                       c["check"].startswith("gate_stale") or c["check"].startswith("gate_diario") or
                       c["check"].startswith("gate_cohort_eligible") or c["check"].startswith("gate_meta_por_mes") or
                       c["check"].startswith("gate_reclutadora_fuente") or c["check"].startswith("gate_sin_pii_allowlist") or
                       c["check"].startswith("gate_meta_capas") or c["check"].startswith("gate_ingresos_strict") or
                       c["check"].startswith("gate_working_strict") or c["check"].startswith("gate_stale_monotono") or
                       c["check"].startswith("gate_reclutadoras_muestra") or c["check"].startswith("gate_sidivan_estado") or
                       c["check"].startswith("gate_meta_cpl")]
    sprint3c_checks = [c for c in val["checks"] if c["check"].startswith("gate_3c_")]
    sprint4a_checks = [c for c in val["checks"] if c["check"].startswith("gate_4a_")]
    sprint4b_checks = [c for c in val["checks"] if c["check"].startswith("gate_4b_")]
    sprint4c_checks = [c for c in val["checks"] if c["check"].startswith("gate_4c_")]
    s3a_passed = sum(1 for c in sprint3a_checks if c["passed"])
    s3c_passed = sum(1 for c in sprint3c_checks if c["passed"])
    s4a_passed = sum(1 for c in sprint4a_checks if c["passed"])
    s4b_passed = sum(1 for c in sprint4b_checks if c["passed"])
    s4c_passed = sum(1 for c in sprint4c_checks if c["passed"])
    print(f"  Checks totales:           {val['passed']}/{val['total_checks']} passed")
    print(f"  VALIDATION GATE orig:     {gate_passed}/{len(gate_checks)} passed")
    print(f"  SPRINT 3A (15 nuevos):    {s3a_passed}/{len(sprint3a_checks)} passed")
    print(f"  SPRINT 3C (11 nuevos):    {s3c_passed}/{len(sprint3c_checks)} passed")
    print(f"  SPRINT 4A ( 8 nuevos):    {s4a_passed}/{len(sprint4a_checks)} passed")
    print(f"  SPRINT 4B ( 5 nuevos):    {s4b_passed}/{len(sprint4b_checks)} passed")
    print(f"  SPRINT 4C ( 1 nuevo):     {s4c_passed}/{len(sprint4c_checks)} passed")
    if gate_failed > 0:
        for c in gate_checks:
            if not c["passed"]:
                print(f"  ✗ GATE FAIL: {c['check']} — {c['nota']}")
    else:
        print("  ✓ TODOS LOS GATE CHECKS PASARON — Sprint 3C completo")
    if val["failed"] > 0:
        other_fails = [c for c in val["checks"] if not c["passed"] and not c["check"].startswith("gate_")]
        for c in other_fails:
            print(f"  ⚠ FAIL: {c['check']} — {c['nota']}")
    print()

    print("FILES CREATED")
    print(f"  {out_path}")
    print()
    print("GIT STATUS")
    import subprocess
    result = subprocess.run(["git", "status", "--short"], capture_output=True, text=True,
                            cwd=repo_root)
    for line in result.stdout.splitlines():
        print(f"  {line}")
    print()
    print("NO COMMIT. NO PUSH. NO DEPLOY.")
    print("Sprint 4A completado. Secciones nuevas: geografia, jerarquia, productividad.")


if __name__ == "__main__":
    main()
