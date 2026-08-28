#!/usr/bin/env python3
"""
Fetches all applications from MongoDB Atlas and regenerates the static dashboard HTML.

Required env vars:
  MONGO_URL       - MongoDB Atlas connection string
  DB_NAME         - Database name (default: workya_db)
  DASHBOARD_PATH  - Path to index.html (default: index.html)

Pipeline stage mapping:
  working       -> WD  (Trabajando)
  sent          -> ED  (Enviados, pendiente confirmación)
  not_selected  -> NSD (No Seleccionados)
  not_arriving  -> NID (No Ingresó)
  interview     -> EP  (En Proceso — Entrevista)
  new           -> EP  (En Proceso — Nuevo)
  contacting    -> EP  (En Proceso — Contactando)
"""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

MONGO_URL = os.environ["MONGO_URL"]
DB_NAME = os.environ.get("DB_NAME", "workya_db")
DASHBOARD_PATH = os.environ.get("DASHBOARD_PATH", "index.html")

LEAD_SOURCE_LABELS = {
    "tiktok": "TikTok",
    "meta": "Meta",
    "grupos": "Grupos",
    "eventos": "Eventos",
    "referidos": "Referidos",
    "dora": "Dora",
    "sofia": "Sofía",
}

STAGE_WORKING = "working"
STAGE_SENT = "sent"
STAGE_NOT_SELECTED = "not_selected"
STAGE_NOT_ARRIVING = "not_arriving"
STAGE_INTERVIEW = "interview"
STAGE_NEW = "new"
STAGE_CONTACTING = "contacting"

STAGE_LABELS = {
    "interview": "Entrevista",
    "new": "Nuevo",
    "contacting": "Contactando",
}

# ── Analytics constants ────────────────────────────────────────────────────

_STAGE_COL = {
    "working": "trabajando",
    "sent": "enviado",
    "not_selected": "no_sel",
    "not_arriving": "no_ing",
    "interview": "entrevista",
    "new": "nuevos",
    "contacting": "contactado",
}

_MONTH_ES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
             "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
_MONTH_EN = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

_STAGE_LABEL_NTD = {
    "working": "Trabajando",
    "sent": "Enviado",
    "not_selected": "No Seleccionado",
    "not_arriving": "No ingresó",
    "interview": "Entrevista",
    "new": "Nuevo",
    "contacting": "Contactando",
}

# ── Primitive formatters ───────────────────────────────────────────────────

_DT_MIN = datetime(2020, 1, 1, tzinfo=timezone.utc)
_DT_MAX = datetime(2030, 1, 1, tzinfo=timezone.utc)
# Ingresos (started_working_at) capped at today to exclude typos like 2026 vs 2025
_DT_INGRESOS_MAX = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)


def _parse_dt(val):
    """Parse a MongoDB date value to a timezone-aware datetime (or None).
    Returns None for dates outside 2020-2030 (bad data guard)."""
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    elif isinstance(val, str):
        if not val or val == "NaT":
            return None
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    else:
        return None
    return dt if _DT_MIN <= dt < _DT_MAX else None


def _fmt_fi(dt):
    """Format start date as DD/MM/YYYY (used in WD.fi)."""
    if dt is None:
        return ""
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            return dt[:10]
    return dt.strftime("%d/%m/%Y")


def _fmt_sort(dt):
    """Format date as YYYY-MM-DD (used in WD.fs for sorting)."""
    if dt is None:
        return ""
    if isinstance(dt, str):
        return dt[:10]
    return dt.strftime("%Y-%m-%d")


def _fmt_long(dt):
    """Format date as 'YYYY-MM-DD 00:00:00' (used in ED.fe / NSD.fa / NID.fe)."""
    if dt is None:
        return "NaT"
    if isinstance(dt, str):
        if not dt or dt == "NaT":
            return "NaT"
        return dt[:10] + " 00:00:00"
    return dt.strftime("%Y-%m-%d 00:00:00")


def _sanitize(text):
    """Collapse whitespace/newlines so JS string literals stay on one line."""
    if not text:
        return text
    return " ".join(text.split())


# ── App field helpers ──────────────────────────────────────────────────────

def _get_notes(app):
    notes = _sanitize((app.get("notes") or "").strip())
    if not notes:
        notes = _sanitize((app.get("deactivation_reason") or "").strip())
    return notes or "Sin observación"


def _get_last_send_date(app):
    history = app.get("sent_to_history") or []
    if history:
        raw = history[-1].get("date")
        return _fmt_long(raw)
    return "NaT"


def _get_last_client(app):
    """Return the last client a candidate was sent to."""
    history = app.get("sent_to_history") or []
    if history:
        return history[-1].get("agency") or "N/A"
    sent = app.get("sent_to") or []
    return sent[-1] if sent else "N/A"


def _get_all_clients(app):
    """Return all clients as a comma-separated string (for ED / NID)."""
    sent = app.get("sent_to") or []
    return ", ".join(sent) if sent else "N/A"


# ── Pipeline row builders ──────────────────────────────────────────────────

def _build_wd(app):
    return {
        "n": app.get("full_name", ""),
        "c": _get_last_client(app),
        "fi": _fmt_fi(app.get("started_working_at")),
        "fs": _fmt_sort(app.get("started_working_at")),
        "p": app.get("job_title", ""),
        "r": (app.get("owner_name") or "Sin asignar"),
        "pa": app.get("country_origin", ""),
        "f": LEAD_SOURCE_LABELS.get(app.get("lead_source") or "", "Sin fuente"),
    }


def _build_ed(app):
    return {
        "n": app.get("full_name", ""),
        "c": _get_all_clients(app),
        "fe": _get_last_send_date(app),
        "p": app.get("job_title", ""),
        "r": (app.get("owner_name") or "Sin asignar"),
        "o": _get_notes(app),
    }


def _build_nsd(app):
    return {
        "n": app.get("full_name", ""),
        "pa": app.get("country_origin", ""),
        "p": app.get("job_title", ""),
        "r": (app.get("owner_name") or "Sin asignar"),
        "fa": _fmt_long(app.get("created_at")),
        "o": _get_notes(app),
    }


def _build_nid(app):
    return {
        "n": app.get("full_name", ""),
        "c": _get_all_clients(app),
        "fe": _get_last_send_date(app),
        "p": app.get("job_title", ""),
        "r": (app.get("owner_name") or "Sin asignar"),
        "o": _get_notes(app),
    }


def _build_ep(app):
    stage = app.get("pipeline_stage", "")
    return {
        "n": app.get("full_name", ""),
        "e": STAGE_LABELS.get(stage, stage),
        "p": app.get("job_title", ""),
        "r": (app.get("owner_name") or "Sin asignar"),
        "pa": app.get("country_origin", ""),
        "f": LEAD_SOURCE_LABELS.get(app.get("lead_source") or "", "Sin fuente"),
        "fa": _fmt_long(app.get("created_at")),
        "o": _get_notes(app),
    }


# ── Analytics builders ─────────────────────────────────────────────────────

def _build_weekly(apps):
    """Weekly breakdown: applications by created_at week + ingresos by started_working_at."""
    bucket = defaultdict(lambda: dict(
        total=0, trabajando=0, enviado=0, no_sel=0,
        no_ing=0, entrevista=0, nuevos=0, contactado=0, ingresos=0,
    ))

    for app in apps:
        dt = _parse_dt(app.get("created_at"))
        if not dt:
            continue
        y, w, _ = dt.isocalendar()
        col = _STAGE_COL.get(app.get("pipeline_stage") or "new", "nuevos")
        b = bucket[(y, w)]
        b["total"] += 1
        b[col] += 1

    for app in apps:
        if (app.get("pipeline_stage") or "") != "working":
            continue
        dt = _parse_dt(app.get("started_working_at"))
        if not dt or dt > _DT_INGRESOS_MAX:
            continue  # skip future dates (likely typos e.g. 2026 entered instead of 2025)
        y, w, _ = dt.isocalendar()
        bucket[(y, w)]["ingresos"] += 1

    cum = 0
    result = []
    for (y, w) in sorted(bucket):
        # Monday of the ISO week
        jan4 = datetime(y, 1, 4, tzinfo=timezone.utc)
        monday = jan4 - timedelta(days=jan4.isoweekday() - 1) + timedelta(weeks=w - 1)
        sunday = monday + timedelta(days=6)
        label = f"{monday.day} {_MONTH_EN[monday.month - 1]}"
        ws = monday.strftime("%Y-%m-%d")
        we = f"{sunday.day:02d}/{sunday.month:02d}"
        d = bucket[(y, w)]
        cum += d["ingresos"]
        result.append({
            "label": label, "ws": ws, "we": we,
            "total": d["total"],
            "trabajando": d["trabajando"], "enviado": d["enviado"],
            "no_sel": d["no_sel"], "no_ing": d["no_ing"],
            "entrevista": d["entrevista"], "nuevos": d["nuevos"],
            "contactado": d["contactado"],
            "ingresos": d["ingresos"], "cum": cum,
        })
    return result


def _build_monthly(apps):
    """Monthly breakdown: applications by created_at month + ingresos by started_working_at."""
    bucket = defaultdict(lambda: dict(
        total=0, trabajando=0, enviado=0, no_sel=0,
        no_ing=0, entrevista=0, nuevos=0, contactado=0, ingresos=0,
    ))

    for app in apps:
        dt = _parse_dt(app.get("created_at"))
        if not dt:
            continue
        key = (dt.year, dt.month)
        col = _STAGE_COL.get(app.get("pipeline_stage") or "new", "nuevos")
        b = bucket[key]
        b["total"] += 1
        b[col] += 1

    for app in apps:
        if (app.get("pipeline_stage") or "") != "working":
            continue
        dt = _parse_dt(app.get("started_working_at"))
        if not dt or dt > _DT_INGRESOS_MAX:
            continue  # skip future dates (likely typos)
        key = (dt.year, dt.month)
        bucket[key]["ingresos"] += 1

    cum = 0
    result = []
    for (y, m) in sorted(bucket):
        d = bucket[(y, m)]
        cum += d["ingresos"]
        label = f"{_MONTH_ES[m - 1]} {str(y)[2:]}"
        result.append({
            "label": label,
            "total": d["total"],
            "trabajando": d["trabajando"], "enviado": d["enviado"],
            "no_sel": d["no_sel"], "no_ing": d["no_ing"],
            "entrevista": d["entrevista"], "nuevos": d["nuevos"],
            "contactado": d["contactado"],
            "ingresos": d["ingresos"], "cum": cum,
        })
    return result


def _build_fuentes(apps):
    """Lead-source analytics with conversion rate."""
    bucket = defaultdict(lambda: dict(
        total=0, trabajando=0, enviado=0, entrevista=0, no_sel=0, no_ing=0,
    ))

    for app in apps:
        src = LEAD_SOURCE_LABELS.get(app.get("lead_source") or "", "Sin fuente")
        stage = app.get("pipeline_stage") or "new"
        b = bucket[src]
        b["total"] += 1
        if stage == "working":
            b["trabajando"] += 1
        elif stage == "sent":
            b["enviado"] += 1
        elif stage == "interview":
            b["entrevista"] += 1
        elif stage == "not_selected":
            b["no_sel"] += 1
        elif stage == "not_arriving":
            b["no_ing"] += 1

    result = []
    for name, d in sorted(bucket.items(), key=lambda x: -x[1]["total"]):
        tasa = round(d["trabajando"] / d["total"] * 100, 1) if d["total"] else 0.0
        result.append({
            "name": name,
            "total": d["total"],
            "trabajando": d["trabajando"],
            "enviado": d["enviado"],
            "entrevista": d["entrevista"],
            "no_sel": d["no_sel"],
            "no_ing": d["no_ing"],
            "tasa": tasa,
        })
    return result


def _build_ntd(apps):
    """Candidates from all stages that have meaningful notes."""
    result = []
    for app in apps:
        notes = _sanitize((app.get("notes") or "").strip())
        if not notes:
            notes = _sanitize((app.get("deactivation_reason") or "").strip())
        if not notes:
            continue

        stage = app.get("pipeline_stage") or "new"
        client = (
            _get_last_client(app)
            if stage in ("working", "sent", "not_arriving")
            else "N/A"
        )
        result.append({
            "n": _sanitize(app.get("full_name") or ""),
            "e": _STAGE_LABEL_NTD.get(stage, stage),
            "c": client,
            "r": _sanitize(app.get("owner_name") or "Sin asignar"),
            "o": notes,
        })
    return result


def _build_gender(apps):
    """Gender breakdown — returns None if field is absent in the data."""
    total = defaultdict(int)
    working = defaultdict(int)
    found = False

    for app in apps:
        # Try common field names
        g = (
            app.get("gender")
            or app.get("genero")
            or app.get("sexo")
            or ""
        )
        if not g:
            continue
        found = True
        label = "Masculino" if str(g).lower() in ("m", "male", "masculino", "hombre") else "Femenino"
        total[label] += 1
        if (app.get("pipeline_stage") or "") == "working":
            working[label] += 1

    if not found:
        return None
    return {"total": dict(total), "working": dict(working)}


def _build_aloj(apps):
    """Housing-needed counts — returns (si, no) or (None, None) if field absent."""
    si = no = 0
    found = False

    for app in apps:
        v = (
            app.get("needs_housing")
            if "needs_housing" in app
            else app.get("alojamiento")
            if "alojamiento" in app
            else app.get("housing")
            if "housing" in app
            else None
        )
        if v is None:
            continue
        found = True
        if v in (True, 1, "si", "yes", "true", "sí"):
            si += 1
        else:
            no += 1

    return (si, no) if found else (None, None)


# ── Main pipeline ──────────────────────────────────────────────────────────

def fetch_arrays():
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=30000)
    db = client[DB_NAME]

    projection = {
        "_id": 0,
        "full_name": 1,
        "job_title": 1,
        "country_origin": 1,
        "owner_name": 1,
        "lead_source": 1,
        "pipeline_stage": 1,
        "sent_to": 1,
        "sent_to_history": 1,
        "started_working_at": 1,
        "created_at": 1,
        "notes": 1,
        "deactivation_reason": 1,
        # Optional demographic fields (ignored if absent)
        "gender": 1,
        "genero": 1,
        "sexo": 1,
        "needs_housing": 1,
        "alojamiento": 1,
        "housing": 1,
    }
    apps = list(db.applications.find({}, projection))
    client.close()

    wd, ed, nsd, nid, ep = [], [], [], [], []
    for app in apps:
        stage = app.get("pipeline_stage") or "new"
        if stage == STAGE_WORKING:
            wd.append(_build_wd(app))
        elif stage == STAGE_SENT:
            ed.append(_build_ed(app))
        elif stage == STAGE_NOT_SELECTED:
            nsd.append(_build_nsd(app))
        elif stage == STAGE_NOT_ARRIVING:
            nid.append(_build_nid(app))
        elif stage in (STAGE_INTERVIEW, STAGE_NEW, STAGE_CONTACTING):
            ep.append(_build_ep(app))

    # Most recent first
    wd.sort(key=lambda x: x.get("fs", ""), reverse=True)
    ed.sort(key=lambda x: x.get("fe", ""), reverse=True)
    nsd.sort(key=lambda x: x.get("fa", ""), reverse=True)
    nid.sort(key=lambda x: x.get("fe", ""), reverse=True)
    ep.sort(key=lambda x: x.get("fa", ""), reverse=True)

    # Analytics
    weekly = _build_weekly(apps)
    monthly = _build_monthly(apps)
    fuentes = _build_fuentes(apps)
    ntd = _build_ntd(apps)
    gender = _build_gender(apps)
    aloj_si, aloj_no = _build_aloj(apps)

    return wd, ed, nsd, nid, ep, weekly, monthly, fuentes, ntd, gender, aloj_si, aloj_no


def update_html(wd, ed, nsd, nid, ep, weekly, monthly, fuentes, ntd, gender, aloj_si, aloj_no):
    with open(DASHBOARD_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    def _replace_array(content, name, data):
        json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        pattern = rf"const {name}=\[.*?\];"
        replacement = f"const {name}={json_str};"
        if not re.search(pattern, content, re.DOTALL):
            print(f"WARNING: pattern for const {name}=[...] not found", file=sys.stderr)
        return re.sub(pattern, lambda m, r=replacement: r, content, flags=re.DOTALL)

    # Pipeline arrays
    for name, data in {"WD": wd, "ED": ed, "NSD": nsd, "NID": nid, "EP": ep}.items():
        content = _replace_array(content, name, data)

    # Analytics arrays
    for name, data in {"WEEKLY": weekly, "MONTHLY": monthly, "FUENTES": fuentes, "NTD": ntd}.items():
        content = _replace_array(content, name, data)

    # Demographic scalars (only replace if data was found in MongoDB)
    if gender is not None:
        gender_str = json.dumps(gender, ensure_ascii=False, separators=(",", ":"))
        pattern = r'const GENDER=\{[^;]*\};'
        if re.search(pattern, content):
            content = re.sub(pattern, lambda m, r=f"const GENDER={gender_str};": r, content)

    if aloj_si is not None:
        content = re.sub(r'const ALOJ_SI=\d+;', f'const ALOJ_SI={aloj_si};', content)
        content = re.sub(r'const ALOJ_NO=\d+;', f'const ALOJ_NO={aloj_no};', content)

    # Timestamp
    now = datetime.now(timezone.utc)
    last_updated = now.strftime("%d/%m/%Y %H:%M")
    pattern = r'const LAST_UPDATED="[^"]*";'
    replacement = f'const LAST_UPDATED="{last_updated}";'
    if not re.search(pattern, content):
        print("WARNING: pattern for LAST_UPDATED not found", file=sys.stderr)
    content = re.sub(pattern, replacement, content)

    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    print(
        f"Dashboard updated — "
        f"WD:{len(wd)}  ED:{len(ed)}  NSD:{len(nsd)}  NID:{len(nid)}  EP:{len(ep)}  "
        f"WEEKLY:{len(weekly)}  MONTHLY:{len(monthly)}  FUENTES:{len(fuentes)}  NTD:{len(ntd)}  "
        f"Updated:{last_updated}"
    )


if __name__ == "__main__":
    wd, ed, nsd, nid, ep, weekly, monthly, fuentes, ntd, gender, aloj_si, aloj_no = fetch_arrays()
    update_html(wd, ed, nsd, nid, ep, weekly, monthly, fuentes, ntd, gender, aloj_si, aloj_no)
