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
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

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


def _get_notes(app):
    notes = (app.get("notes") or "").strip()
    if not notes:
        notes = (app.get("deactivation_reason") or "").strip()
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
    }
    apps = list(db.applications.find({}, projection))
    client.close()

    wd, ed, nsd, nid = [], [], [], []
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

    # Most recent first
    wd.sort(key=lambda x: x.get("fs", ""), reverse=True)
    ed.sort(key=lambda x: x.get("fe", ""), reverse=True)
    nsd.sort(key=lambda x: x.get("fa", ""), reverse=True)
    nid.sort(key=lambda x: x.get("fe", ""), reverse=True)

    return wd, ed, nsd, nid


def update_html(wd, ed, nsd, nid):
    with open(DASHBOARD_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    arrays = {"WD": wd, "ED": ed, "NSD": nsd, "NID": nid}
    for name, data in arrays.items():
        json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        pattern = rf"const {name}=\[.*?\];"
        replacement = f"const {name}={json_str};"
        new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)
        if new_content == content:
            print(f"WARNING: pattern for const {name}=[...] not found", file=sys.stderr)
        content = new_content

    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    print(
        f"Dashboard updated — WD:{len(wd)}  ED:{len(ed)}  NSD:{len(nsd)}  NID:{len(nid)}"
    )


if __name__ == "__main__":
    wd, ed, nsd, nid = fetch_arrays()
    update_html(wd, ed, nsd, nid)
