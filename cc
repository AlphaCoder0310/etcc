#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
cb_recon_web.py  --  LAGRANGE : CB desk web app
------------------------------------------------------------
Minimal, reliable shell over the existing verified modules:
  load_trade_history.py  (EQRMS txt -> eqrms.trade_history)
  recon_cb.py            (matching engine + report HTML + Outlook send)

Two tabs:
  1. Trade Booking Reconciliation  (recon_cb.py)
  2. Delta Check (CBA)             (delta_check_recovered.py, DB leg only;
     the derivation leg needs the Derivation window + Bloomberg and
     stays in the notebook)

Stack: FastAPI + uvicorn (already used by CB Nuke Station) + one
embedded HTML page. No frontend framework, no CDN, no templates.

Buttons
-------
  Refresh    : (1) load any new "Trade History.YYYYMMDD.txt" from
               M:\CB\Snaps\TradeHistory into MariaDB (idempotent upsert),
               (2) re-query the DB, (3) rebuild the report.
               NOTE: exporting the txt out of EQRMS is still manual.
  Open Draft : compose the current report in Outlook for review.
  Send Now   : send the current report immediately.

Run
---
  python cb_recon_web.py            # http://127.0.0.1:59988
  (or use run_cb_recon_web.bat)
"""

import io
import os
import json
import math
import time
import sys
import threading
import contextlib
import datetime as dt
from typing import Optional, Dict, Any, List

import hmac
import hashlib
import secrets
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, RedirectResponse
from pydantic import BaseModel

import load_trade_history as ldr
import recon_cb as rc
em = rc   # single combined module serves both roles

LAGRANGE_BUILD = "2026-08-01.embed"   # shown in header/console; bump on deploy

NUKE_EMBED = os.environ.get("NUKE_EMBED", "1") == "1"
# embedded mode serves Nuke Station at /nuke/ from THIS process; the desk
# reaches it via this host, so default to all interfaces in that mode
HOST = os.environ.get("LAGRANGE_HOST",
                      "0.0.0.0" if NUKE_EMBED else "127.0.0.1")
PORT = 59988

# DB updater script (writes cbanalytics.lp_model_output / nuked_price).
# Override with env var CBA_UPDATER if the path ever moves.
CBA_UPDATER = os.environ.get("CBA_UPDATER", r"M:\CB\BAU\cba_mariadb.py")
CBA_UPDATER_TIMEOUT = 900   # seconds

# Derivation app window (delta check auto-grab). Exact title preferred,
# any window starting with the prefix accepted (survives version bumps).
DERIV_WINDOW_TITLE = "Derivation (2023-12-rev3-db466.1 : Release)"
DERIV_TITLE_PREFIX = "Derivation ("
DERIV_MIN_ROWS = 600   # below this the grab retries with the simple sequence

# CB Nuke Station server (embedded in its own tab; app runs separately)
NUKE_URL = os.environ.get("NUKE_URL", "http://127.0.0.1:59999")
NUKE_AUTOSTART = os.environ.get("NUKE_AUTOSTART", "1") == "1"
NUKE_APP = os.environ.get("NUKE_APP", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "app.py"))
_NUKE_PROC = None


def _nuke_up(timeout=2):
    import urllib.request
    try:
        with urllib.request.urlopen(NUKE_URL, timeout=timeout) as r:
            return 200 <= r.status < 500
    except Exception:
        return False


_NUKE_NOTE = "autostart not attempted yet"


def _maybe_start_nuke():
    """Start app.py as a child ONLY if nothing already serves NUKE_URL
    and the URL is local. Child log -> nuke_station_console.log; its
    browser popup suppressed; terminated when Lagrange exits.
    Records its verdict in _NUKE_NOTE and is safe to call again (the
    Connect button retries it)."""
    global _NUKE_PROC, _NUKE_NOTE
    if not NUKE_AUTOSTART:
        _NUKE_NOTE = "autostart disabled (NUKE_AUTOSTART=0)"
        print("[nuke]", _NUKE_NOTE)
        return
    from urllib.parse import urlparse
    host = urlparse(NUKE_URL).hostname
    if host not in ("127.0.0.1", "localhost"):
        _NUKE_NOTE = "NUKE_URL is remote (%s) - autostart skipped" % host
        print("[nuke]", _NUKE_NOTE)
        return
    if _NUKE_PROC is not None and _NUKE_PROC.poll() is None:
        _NUKE_NOTE = "child running (pid %d)" % _NUKE_PROC.pid
        return
    if _nuke_up():
        _NUKE_NOTE = "already running at %s (not started by Lagrange)" % NUKE_URL
        print("[nuke]", _NUKE_NOTE)
        return
    if not os.path.exists(NUKE_APP):
        _NUKE_NOTE = ("app.py NOT FOUND at %s - put Nuke Station's app.py "
                      "there, or set NUKE_APP to its real path" % NUKE_APP)
        print("[nuke]", _NUKE_NOTE)
        return
    import subprocess
    import atexit
    logpath = os.path.join(os.path.dirname(NUKE_APP),
                           "nuke_station_console.log")
    logf = open(logpath, "a")
    env = dict(os.environ, NUKE_NO_BROWSER="1", LAGRANGE_CHILD="1")
    _NUKE_PROC = subprocess.Popen(
        [sys.executable, NUKE_APP],
        cwd=os.path.dirname(NUKE_APP) or ".",
        stdin=subprocess.PIPE,          # child exits when this pipe closes
        stdout=logf, stderr=subprocess.STDOUT, env=env)
    _NUKE_NOTE = "started app.py (pid %d), log: %s" % (_NUKE_PROC.pid, logpath)
    print("[nuke]", _NUKE_NOTE)

    def _stop():
        if _NUKE_PROC and _NUKE_PROC.poll() is None:
            try:
                _NUKE_PROC.stdin.close()      # primary: pipe EOF
            except Exception:
                pass
            try:
                _NUKE_PROC.terminate()        # belt and braces
                _NUKE_PROC.wait(timeout=5)
            except Exception:
                try:
                    _NUKE_PROC.kill()
                except Exception:
                    pass
    atexit.register(_stop)

_NUKE_MOD = None


_DSCAN = {"ok": False, "note": "not attempted", "path": ""}

def _mount_dscan():
    """Embed delta_scan_web at /dscan/ (same process) when
    the file sits beside this one; harmless if absent."""
    p = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "delta_scan_web.py")
    dsp = os.environ.get("DSCAN_APP", p)
    _DSCAN["path"] = dsp
    if not os.path.exists(dsp):
        _DSCAN["note"] = "delta_scan_web.py not found at " + dsp
        print("[dscan]", _DSCAN["note"])
        return
    try:
        import importlib.util
        sp2 = importlib.util.spec_from_file_location(
            "delta_scan_embedded", dsp)
        m2 = importlib.util.module_from_spec(sp2)
        sys.modules["delta_scan_embedded"] = m2
        sp2.loader.exec_module(m2)
        app.mount("/dscan", m2.app)
        _DSCAN.update({"ok": True, "note": "embedded at /dscan/ "
                       "(same process), build " +
                       str(getattr(m2, "BUILD", "?"))})
        print("[dscan]", _DSCAN["note"])
    except Exception as e:
        import traceback
        _DSCAN["note"] = "embed failed: " + traceback.format_exc(
            limit=4)[-700:]
        print("[dscan]", _DSCAN["note"])


def _embed_nuke():
    """Import app.py and mount its FastAPI app at /nuke on THIS server.
    One process, one port; no child, no 59999."""
    global _NUKE_MOD, _NUKE_NOTE
    if not os.path.exists(NUKE_APP):
        _NUKE_NOTE = ("embed failed: app.py NOT FOUND at %s - put Nuke "
                      "Station's app.py there or set NUKE_APP" % NUKE_APP)
        print("[nuke]", _NUKE_NOTE)
        return
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("nuke_station_embedded",
                                                      NUKE_APP)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["nuke_station_embedded"] = mod
        spec.loader.exec_module(mod)
        app.mount("/nuke", mod.app)
        _NUKE_MOD = mod
        _NUKE_NOTE = "embedded at /nuke/ (same process, port %d)" % PORT
        print("[nuke]", _NUKE_NOTE)
    except Exception as e:
        import traceback
        _NUKE_NOTE = "embed failed: %r" % e
        print("[nuke]", _NUKE_NOTE)
        traceback.print_exc()


import contextlib as _ctxlib


@_ctxlib.asynccontextmanager
async def _lagrange_lifespan(_a):
    import asyncio
    if _NUKE_MOD is not None:
        from starlette.concurrency import run_in_threadpool as _rit
        await _rit(_NUKE_MOD.load_persisted_state)
        started = []
        for _fn in ("rfx_poller", "refdata_poller", "autosave_poller",
                    "div_poller", "snap8_poller", "vol_poller"):
            if hasattr(_NUKE_MOD, _fn):
                asyncio.create_task(getattr(_NUKE_MOD, _fn)())
                started.append(_fn)
        try:
            if getattr(_NUKE_MOD, "RFX_WAKE", None):
                _NUKE_MOD.RFX_WAKE.set()
        except Exception:
            pass
        print("[nuke] embedded pollers started: " + ", ".join(started))
    asyncio.create_task(_blotter_ws_listener())
    yield


app = FastAPI(title="Lagrange", lifespan=_lagrange_lifespan)

# last successfully built report, guarded by a lock (Send uses this)
_LOCK = threading.Lock()
_LAST = {"subject": None, "html": None, "label": None, "built_at": None}
_LAST_DELTA = {"subject": None, "html": None, "built_at": None}

_DESKTOP_ONLY = ["pyautogui", "pyperclip", "win32gui", "win32con",
                 "win32com", "win32com.client",
                 "openpyxl", "xbbg", "tkinter", "tkinter.ttk"]


def _import_delta():
    """Import the delta-check module; stub desktop-only deps if absent.
    The web tab only uses its DB query + HTML builder, so missing
    Bloomberg/GUI libraries must not block the tab."""
    import types
    for m in _DESKTOP_ONLY:
        if m not in sys.modules:
            try:
                __import__(m)
            except Exception:
                stub = types.ModuleType(m)
                if m == "xbbg":
                    stub.blp = None
                if m == "openpyxl":
                    stub.Workbook = stub.load_workbook = None
                if m == "win32com.client":
                    stub.Dispatch = None
                    if "win32com" in sys.modules:
                        sys.modules["win32com"].client = stub
                if m == "tkinter":
                    class _T:  # attribute sponge
                        def __getattr__(self, k): return object
                    stub = _T()
                sys.modules[m] = stub
    if "tkinter" in sys.modules and "tkinter.ttk" not in sys.modules:
        sys.modules["tkinter.ttk"] = types.ModuleType("tkinter.ttk")
    import delta_check_recovered as dcmod
    return dcmod


# ----------------------------------------------------------------------
# report building (mirrors recon_cb_email.main's data path, no argparse)
# ----------------------------------------------------------------------
def run_loader():
    """Run the txt loader; never raise -- return (ok, output_text)."""
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            ldr.main([])
        return True, buf.getvalue()
    except SystemExit as e:          # loader sys.exits on e.g. missing M: dir
        return False, (buf.getvalue() + "\n[loader] %s" % e)
    except Exception as e:
        return False, (buf.getvalue() + "\n[loader] unexpected error: %s" % e)


def build_report(date_from=None, date_to=None, single_date=None):
    """Fetch, reconcile and build the email-grade HTML. Returns dict."""
    use_range = bool(date_from)
    if use_range:
        start_day = rc.parse_date(date_from)
        end_day = rc.parse_date(date_to)
        days = rc.date_range(start_day, end_day)
        day_label = "%s to %s" % (start_day.isoformat(), end_day.isoformat())
    else:
        start_day = end_day = rc.parse_date(single_date)
        days = [start_day]
        day_label = start_day.isoformat()

    conn = rc.connect()
    if use_range:
        hist_all, no_isin_all, zero_qty = rc.fetch_history_range(
            conn, start_day, end_day)
        blot_all, bad_side, neg_qty = rc.fetch_blotter_range(
            conn, start_day, end_day)
    else:
        hist_all, no_isin_all, zero_qty = rc.fetch_history(conn, start_day)
        blot_all, bad_side, neg_qty = rc.fetch_blotter(conn, start_day)
        for row in hist_all + no_isin_all + blot_all:
            row.setdefault("trade_date", start_day)
    ref, ref_notes = em.bond_ref(conn, hist_all + no_isin_all, blot_all)
    prevmap = rc.fetch_prev_accts(
        conn, [r["isin"] for r in hist_all + blot_all], end_day)
    conn.close()

    notes = list(ref_notes)
    if zero_qty:
        notes.append("%d history rows with zero/NULL quantity skipped"
                     % zero_qty)
    if bad_side:
        notes.append("%d blotter rows with unrecognized client_side skipped"
                     % len(bad_side))
    if neg_qty:
        notes.append("%d blotter rows had negative quantity; ABS used - "
                     "verify sign convention" % neg_qty)
    if not hist_all and not no_isin_all:
        notes.append("no CB rows in trade_history for %s - run the loader?"
                     % day_label)

    per_day_counts = None
    if use_range:
        from collections import defaultdict
        by = lambda rows: _group(rows)
        hist_by, no_by, blot_by = by(hist_all), by(no_isin_all), by(blot_all)
        per_day_counts = {}
        for day in days:
            h_copy = em._fresh_copy(hist_by.get(day, []))
            b_copy = em._fresh_copy(blot_by.get(day, []))
            day_res = rc.reconcile(h_copy, b_copy)
            per_day_counts[day] = em.build_counts(day_res, no_by.get(day, []))

    for row in hist_all + blot_all:
        row["matched"] = False
    results = rc.reconcile(hist_all, blot_all)
    rc.annotate_prev_accts(results, prevmap)
    nets = rc.net_check(hist_all, blot_all)

    subject, html = em.build_html(
        day_label, results, nets, no_isin_all, notes,
        hist_all, blot_all, ref,
        per_day_counts=per_day_counts, days=days)

    counts = em.build_counts(results, no_isin_all)
    alerts = sum(v for k, v in counts.items() if k != "MATCHED")
    return dict(subject=subject, html=html, label=day_label,
                counts=counts, alerts=alerts,
                matched=counts.get("MATCHED", 0))


def _group(rows):
    out = {}
    for r in rows:
        out.setdefault(r["trade_date"], []).append(r)
    return out


# ----------------------------------------------------------------------
# API
# ----------------------------------------------------------------------
class RefreshReq(BaseModel):
    mode: str = "today"          # today | date | range
    date: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    skip_loader: bool = False


class SendReq(BaseModel):
    to: str = ""
    cc: str = ""
    send: bool = False           # False = open draft, True = send now


@app.post("/api/refresh")
def api_refresh(req: RefreshReq):
    with _LOCK:
        loader_ok, loader_out = (True, "[loader] skipped by request")
        if not req.skip_loader:
            loader_ok, loader_out = run_loader()
        try:
            if req.mode == "range":
                rep = build_report(date_from=req.date_from,
                                   date_to=req.date_to)
            elif req.mode == "date":
                rep = build_report(single_date=req.date)
            else:
                rep = build_report(single_date=None)   # today
        except SystemExit as e:
            return JSONResponse(status_code=400,
                                content={"ok": False, "error": str(e),
                                         "loader": loader_out})
        except Exception as e:
            return JSONResponse(status_code=500,
                                content={"ok": False, "error": repr(e),
                                         "loader": loader_out})
        _LAST.update(subject=rep["subject"], html=rep["html"],
                     label=rep["label"],
                     built_at=dt.datetime.now().strftime("%H:%M:%S"))
        return {"ok": True, "loader_ok": loader_ok, "loader": loader_out,
                "subject": rep["subject"], "label": rep["label"],
                "alerts": rep["alerts"], "matched": rep["matched"],
                "counts": rep["counts"], "html": rep["html"],
                "built_at": _LAST["built_at"]}


@app.post("/api/send")
def api_send(req: SendReq):
    with _LOCK:
        if not _LAST["html"]:
            return JSONResponse(status_code=400, content={
                "ok": False, "error": "No report built yet - press Refresh first."})
        subject, html = _LAST["subject"], _LAST["html"]
    # Outlook COM in a FastAPI worker thread needs COM initialised
    try:
        import pythoncom
        pythoncom.CoInitialize()
        com_inited = True
    except ImportError:
        com_inited = False
    try:
        ok, msg = em.send_outlook(subject, html, req.to, req.cc, req.send)
    finally:
        if com_inited:
            pythoncom.CoUninitialize()
    return {"ok": ok, "message": msg, "subject": subject}


def _latest_snap_ts():
    """MAX(snap_ts) from cbanalytics.lp_model_output, or None."""
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(snap_ts) "
                            "FROM cbanalytics.lp_model_output")
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            conn.close()
    except Exception:
        return None


def _df_data_as_of(df):
    try:
        m = df["eqrms_snap_ts"].dropna().astype(str).max()
        return m.split(".")[0] if m else None
    except Exception:
        return None


def _run_db_updater():
    """Run cba_mariadb.py as a subprocess. Returns (ok, log_text).
    Subprocess (not import) so its argparse/sys.exit/globals cannot
    affect the server."""
    import subprocess
    if not os.path.exists(CBA_UPDATER):
        return False, "[updater] script not found: %s" % CBA_UPDATER
    try:
        r = subprocess.run(
            [sys.executable, CBA_UPDATER],
            cwd=os.path.dirname(CBA_UPDATER) or ".",
            capture_output=True, text=True,
            timeout=CBA_UPDATER_TIMEOUT)
    except subprocess.TimeoutExpired:
        return False, ("[updater] timed out after %ds" % CBA_UPDATER_TIMEOUT)
    except Exception as e:
        return False, "[updater] failed to launch: %r" % e
    log = (r.stdout or "") + (("\n" + r.stderr) if r.stderr else "")
    tail = "\n".join(log.strip().splitlines()[-25:])
    if r.returncode != 0:
        return False, ("[updater] exit code %d\n%s" % (r.returncode, tail))
    return True, "[updater] ok\n%s" % tail


class DeltaUpdateReq(BaseModel):
    pass


@app.post("/api/delta/update_db")
def api_delta_update_db():
    with _LOCK:
        ok, log = _run_db_updater()
    if not ok:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": "DB update failed",
                                     "log": log})
    return {"ok": True, "log": log}


_LAST_TWCB = {"html": "", "subject": ""}


_BONDCFG_FILE: dict = {}


class BondCfgReq(BaseModel):
    isin: str = ""
    short_name: str = ""
    tol: str = ""
    autopilot: str = ""


@app.get("/api/rfq/bondcfg")
def api_bondcfg_get():
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "rows": list(_BONDCFG_FILE.values())}
    try:
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT isin, short_name, tol, "
                            "autopilot FROM cba_app.rfq_bond_cfg "
                            "ORDER BY short_name")
                return {"ok": True, "rows": [
                    {"isin": a, "short_name": b,
                     "tol": "" if c is None else str(c),
                     "autopilot": int(d or 0)}
                    for a, b, c, d in cur.fetchall()]}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.post("/api/rfq/bondcfg")
def api_bondcfg_set(req: BondCfgReq, request: Request):
    user = ((getattr(request.state, "auth", None) or {})
            .get("user") or "lagrange")
    isin = (req.isin or "").strip().upper()[:20]
    if not isin:
        return JSONResponse(status_code=400, content={
            "ok": False, "error": "isin required"})
    tol = _fnum(req.tol)
    ap = 1 if str(req.autopilot) in ("1", "true", "True") else 0
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        _BONDCFG_FILE[isin] = {"isin": isin,
            "short_name": req.short_name[:64],
            "tol": "" if tol is None else str(tol),
            "autopilot": ap}
        return {"ok": True}
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO cba_app.rfq_bond_cfg "
                            "(isin, short_name, tol, autopilot, "
                            "updated_by, updated_at) VALUES "
                            "(%s,%s,%s,%s,%s,NOW()) ON DUPLICATE KEY UPDATE short_name=VALUES(short_name), tol=VALUES(tol), autopilot=VALUES(autopilot), updated_by=VALUES(updated_by), updated_at=NOW()",
                            (isin, req.short_name[:64], tol, ap, user))
            conn.commit()
            return {"ok": True}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


class TwcbRunReq(BaseModel):
    mark_seen: bool = False


@app.post("/api/twcb/run")
def api_twcb_run(req: TwcbRunReq):
    """Run the TW CB issuance pipeline feeds (TWSE / TPEx / SFB)
    library-style and cache the report for Open Draft / Send
    Now. mark_seen=False views without consuming novelty."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        evs = [
            {"source": "MOPS (TWSE listed)", "date": "2026-08-15",
             "company": "2330 TSMC", "text": "board resolved issuance of unsecured convertible bonds", "link": "",
             "text_en": "board resolved issuance of unsecured convertible bonds", "is_new": True},
            {"source": "SFB effective-registration", "date":
             "2026-08-14", "company": "6488 GlobalWafers",
             "text": "CB shelf registration effective NT$8,000,000,000", "link": "",
             "text_en": "CB shelf registration effective NT$8,000,000,000", "is_new": False},
        ]
        shelf_rows = [
            {"days_left": 5, "date": "2026-05-20",
             "company": "1560", "text": "\u8f49\u63db\u516c\u53f8\u50b5 1,000,000,000",
             "text_en": "convertible bonds 1,000,000,000"},
            {"days_left": 88, "date": "2026-08-11",
             "company": "2464", "text": "\u8f49\u63db\u516c\u53f8\u50b5(\u7121\u64d4\u4fdd) 1,000,000,000",
             "text_en": "convertible bonds (unsecured) 1,000,000,000"},
        ]
        _LAST_TWCB["html"] = "<html><body>FILE-mode TW CB report</body></html>"
        _LAST_TWCB["subject"] = "[TW CB pipeline] 1 new | FILE"
        return {"ok": True, "events": evs, "new": 1,
                "shelf_live": 3, "shelf_rows": shelf_rows,
                "stats": "TWSE raw=120 TPEx raw=88 SFB raw=6",
                "errors": [], "warnings": []}
    try:
        if TW_BF["running"]:
            return JSONResponse(status_code=409, content={
                "ok": False, "error": "backfill in progress - try again in a minute"})
        import importlib
        try:
            twm = importlib.import_module("tw_cb_pipeline_monitor")
        except ImportError:
            return JSONResponse(status_code=500, content={
                "ok": False, "error": "tw_cb_pipeline_monitor.py not found - place it next to cb_recon_web.py "
                "(M:\\CB\\BAU) and restart."})
        session = twm.make_session()
        all_events, errors, warnings = [], [], []
        raw_counts = {}
        import datetime as _dt
        is_weekday = _dt.date.today().weekday() < 5
        feeds = [
            ("TWSE", lambda: twm.feed_mops(session,
                twm.TWSE_MATERIAL_URL, "MOPS (TWSE listed)")),
            ("TPEx", lambda: twm.feed_tpex(session)),
            ("SFB", lambda: twm.feed_sfb(session, warnings)),
        ]
        for name, fn in feeds:
            try:
                evs, raw = fn()
                raw_counts[name] = raw
                all_events.extend(evs)
                if raw == 0 and is_weekday:
                    warnings.append("%s returned 0 raw rows on a weekday (TW holiday, or feed degraded)" % name)
            except Exception as e:
                errors.append("%s: %s" % (name, e))
        shelf, live_now = {}, []
        _has_shelf = all(hasattr(twm, a) for a in
                         ("_load_json", "_save_json", "SHELF_FILE",
                          "update_shelf", "shelf_days_left"))
        if _has_shelf:
            shelf = twm._load_json(twm.SHELF_FILE)
            sfb_events = [e for e in all_events
                          if e["source"] == "SFB effective-registration"]
            twm.update_shelf(shelf, sfb_events, warnings)
            try:
                twm._save_json(twm.SHELF_FILE, shelf)
            except Exception as e:
                errors.append("shelf save failed: %s" % e)
            live_now = [v for v in shelf.values()
                        if (twm.shelf_days_left(v["date"]) or -1) >= 0]
        else:
            warnings.append("desk monitor is an older version (no shelf tracking) - drop in the v2 file to enable it")
        seen = twm.load_seen()
        now_ts = time.time()
        new_events = []
        for ev in all_events:
            k = twm.event_key(ev)
            ev["is_new"] = k not in seen
            if ev["is_new"]:
                new_events.append(ev)
                if req.mark_seen:
                    seen[k] = now_ts
        if req.mark_seen:
            try:
                twm.save_seen(seen)
            except Exception as e:
                errors.append("seen-state save failed: %s" % e)
        stats = " ".join("%s raw=%s" % (k, v)
                         for k, v in raw_counts.items())
        try:
            _LAST_TWCB["html"] = twm.render_html(
                new_events, errors, warnings, stats, shelf)
        except TypeError:
            _LAST_TWCB["html"] = twm.render_html(
                new_events, errors, warnings, stats)
        except Exception:
            _LAST_TWCB["html"] = (
                "<html><body><h3>TW CB pipeline</h3><ul>"
                + "".join("<li>[%s] %s | %s | %s</li>" % (
                    e.get("source", ""), e.get("date", ""),
                    e.get("company", ""), e.get("text", ""))
                    for e in new_events)
                + "</ul></body></html>")
        _LAST_TWCB["subject"] = (
            "[TW CB pipeline] %d new | shelf %d | %d err | %d warn | %s" % (len(new_events), len(live_now),
            len(errors), len(warnings),
            _dt.date.today().isoformat()))
        _fmt = getattr(twm, "fmt_thousands", None) or (lambda s: s)
        _ten = getattr(twm, "translate_en", None)
        _cjk = getattr(twm, "has_cjk", None) or (lambda s: True)
        for ev in all_events:
            raw = ev.get("text") or ""
            ev["text_en"] = (_fmt(_ten(raw))
                             if (_ten and _cjk(raw)) else "")
            ev["text"] = _fmt(raw)
        shelf_rows = []
        if _has_shelf:
            for v in shelf.values():
                dl = twm.shelf_days_left(v.get("date", ""))
                if dl is None or dl < 0:
                    continue
                _t = v.get("text") or ""
                shelf_rows.append({
                    "days_left": dl, "date": v.get("date", ""),
                    "company": v.get("company", ""),
                    "text": _fmt(_t),
                    "text_en": (_fmt(_ten(_t)) if (_ten and
                        _cjk(_t)) else "")})
            shelf_rows.sort(key=lambda x: x["days_left"])
        return {"ok": True, "events": all_events,
                "new": len(new_events),
                "shelf_live": len(live_now),
                "shelf_rows": shelf_rows, "stats": stats,
                "errors": errors, "warnings": warnings}
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


TW_BF = {"running": False, "msg": "", "error": ""}


class TwcbBackfillReq(BaseModel):
    days: int = 92


def _tw_backfill_worker(days: int):
    try:
        import importlib
        twm = importlib.import_module("tw_cb_pipeline_monitor")
        session = twm.make_session()
        warnings: list = []
        evs, raw = twm.backfill_sfb(session, days, warnings)
        shelf = twm._load_json(twm.SHELF_FILE)
        sfb_events = [e for e in evs
                      if e["source"] == "SFB effective-registration"]
        twm.update_shelf(shelf, sfb_events, warnings)
        twm._save_json(twm.SHELF_FILE, shelf)
        seen = twm.load_seen()
        now_ts = time.time()
        for ev in evs:
            seen.setdefault(twm.event_key(ev), now_ts)
        twm.save_seen(seen)
        live = [v for v in shelf.values()
                if (twm.shelf_days_left(v.get("date", "")) or -1) >= 0]
        TW_BF["msg"] = ("backfill done: walked %dd, %d SFB events, shelf now %d live (marked seen so the daily email stays quiet)" % (days, len(evs), len(live)))
    except Exception as e:
        TW_BF["error"] = str(e)
    finally:
        TW_BF["running"] = False


@app.post("/api/twcb/backfill")
def api_twcb_backfill(req: TwcbBackfillReq):
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        TW_BF.update(running=False, error="",
                     msg="backfill done: walked 92d, 14 SFB events, shelf now 9 live (FILE)")
        return {"ok": True, "started": True}
    if TW_BF["running"]:
        return JSONResponse(status_code=409, content={
            "ok": False, "error": "backfill already running"})
    try:
        import importlib
        twm = importlib.import_module("tw_cb_pipeline_monitor")
    except ImportError:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": "tw_cb_pipeline_monitor.py not found next to cb_recon_web.py"})
    if not hasattr(twm, "backfill_sfb"):
        return JSONResponse(status_code=500, content={
            "ok": False, "error": "desk monitor is an older version without backfill - drop in the v2 file"})
    days = max(1, min(int(req.days or 92), 200))
    TW_BF.update(running=True, msg="", error="")
    threading.Thread(target=_tw_backfill_worker,
                     args=(days,), daemon=True).start()
    return {"ok": True, "started": True}


@app.get("/api/twcb/backfill_status")
def api_twcb_backfill_status():
    return {"ok": True, **TW_BF}


class DelReq(BaseModel):
    rfq_id: str = ""


@app.post("/api/rfq/delete")
def api_rfq_delete(req: DelReq, request: Request):
    """Hard-delete a CANCELLED line (and its quote history)."""
    user = ((getattr(request.state, "auth", None) or {})
            .get("user") or "trader")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                _st = ("('CANCELLED','DONE')"
                       if user == "jb33880" else
                       "('CANCELLED')")
                cur.execute("DELETE FROM cba_app.rfq WHERE "
                            "rfq_id=%s AND status IN " + _st,
                            (req.rfq_id,))
                if not cur.rowcount:
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False, "error": ("only CANCELLED"
                        + ("/DONE" if user == "jb33880" else "")
                        + " lines can be deleted")})
                cur.execute("DELETE FROM cba_app.rfq_quote_hist "
                            "WHERE rfq_id=%s", (req.rfq_id,))
            conn.commit()
            return {"ok": True}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.get("/api/rfq/live")
def api_rfq_live_lane():
    """Zero-build live lane: M-LIVE fields for active rows,
    straight from in-memory nuke state. No DB, no service."""
    out = {}
    try:
        snap = RFQ_SNAP.get("data") or {}
        for r in snap.get("rows", []):
            if r.get("status") not in ("REQUESTED", "QUOTED",
                                       "WORKING", "IMPROVE",
                                       "HIT"):
                continue
            lv = _rfq_live(r.get("sec_id"), r.get("style"))
            _b = lv.get("nqb");  _b = lv.get("bid") if _b is None else _b
            _a = lv.get("nqa");  _a = lv.get("ask") if _a is None else _a
            out[str(r.get("rfq_id"))] = {
                "lb": None if _b is None else round(_b, 2),
                "la": None if _a is None else round(_a, 2),
                "lvs": lv.get("nvs") if lv.get("nvs") is not None else lv.get("und"),
                "lfx": lv.get("nfx") if lv.get("nfx") is not None else lv.get("fx"),
                "ld": lv.get("nd")}
    except Exception:
        pass
    return {"ok": True, "live": out, "ts": time.time()}


class RePx(BaseModel):
    sec_id: str = ""
    style: str = ""
    ovd_spot: str = ""
    ovd_fx: str = ""
    ovd_delta: str = ""
    q_delta: str = ""     # typed QUOTE-band delta wins when present


@app.post("/api/rfq/reprice")
def api_rfq_reprice(req: RePx):
    """Instant draft repricing at the caller's refs \u2014 pure
    in-memory engine, no DB, no service. Feeds the live
    preview while typing OvdSpot / OvdFx."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "bid": None, "ask": None}
    try:
        _d = _fnum(req.q_delta) if _fnum(req.q_delta) is not None \
            else _fnum(req.ovd_delta)
        q = _rfq_live(req.sec_id, (req.style or "outright"),
                      ovd_spot=_fnum(req.ovd_spot),
                      ovd_delta=_d,
                      ovd_fx=_fnum(req.ovd_fx))
        return {"ok": True,
                "bid": q.get("bid"), "ask": q.get("ask"),
                "nd": q.get("nd")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


from fastapi import WebSocket, WebSocketDisconnect


@app.websocket("/ws/rfq")
async def ws_rfq(ws: WebSocket):
    """Live-sync channel: the server sends {type:rev,rev:N} whenever
    the RFQ dataset changes; the browser then fetches the list.
    Carries no RFQ data itself, so it needs no session."""
    import asyncio as _aio
    await ws.accept()
    RFQ_REV["loop"] = _aio.get_running_loop()
    RFQ_PEERS.add(ws)
    try:
        await ws.send_text(json.dumps({"type": "rev",
                                       "rev": RFQ_REV["n"],
                                       "why": "hello"}))
        while True:
            await ws.receive_text()          # client pings keep it alive
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        RFQ_PEERS.discard(ws)


class ExpQ(BaseModel):
    rfq_id: str = ""
    token: str = ""


@app.post("/api/rfq/expire")
def api_rfq_expire(req: ExpQ, request: Request):
    """Trader E button: expire the standing quote exactly like the
    engine timeout does (same routine), attributed to the trader."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or "lagrange")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "rev": 0}
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT rfq_id, row_version, q_rev, bid_px, "
                            "ask_px, status, off_flag FROM cba_app.rfq "
                            "WHERE rfq_id=%s FOR UPDATE", (req.rfq_id,))
                h = cur.fetchone()
                if not h:
                    conn.rollback()
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "rfq not found"})
                r = {"rfq_id": h[0], "row_version": h[1], "q_rev": h[2],
                     "bid_px": h[3], "ask_px": h[4]}
                if h[5] in ("HIT", "DONE", "CANCELLED"):
                    conn.rollback()
                    return JSONResponse(status_code=400, content={
                        "ok": False, "error": "frozen line - cannot "
                        "expire a HIT / DONE / CANCELLED RFQ"})
                _adj = (h[6] or "") not in ("", "expired")
                if h[3] is None and h[4] is None and not _adj:
                    conn.rollback()
                    return JSONResponse(status_code=400, content={
                        "ok": False, "error": "nothing to expire: no "
                        "standing quote and not adjusting"})
                if int(h[1] or 0) != int(req.token or 0):
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False, "error": "Row changed underneath "
                        "you - refreshing; please retry."})
                rev = _rfq_expire_quote(cur, r, user)
                _rfq_log(cur, req.rfq_id, user, "quote expired by trader")
            conn.commit()
            RFQ_SNAP["ts"] = 0.0     # next list pass re-reads the row
            return {"ok": bool(rev), "rev": rev}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


# ---------------- CB CONVERSION bid sheet (reference model) ----------------
CONV_LAG_BD = 5                    # placeholder: TW ECB delivery lag (business days)
CONV_PROFILE = {                   # placeholder market cost profiles (flagged in UI)
    "TW": {"tax_pct": 0.30, "comm_bp": 5, "borrow_pct": 1.25, "fund_pct": 5.00,
           "fx_bp": 3, "fx_slip_bp": 0, "rebate_pct": 0.0, "fee_usd": 300, "lag_cd": 7},
    "HK": {"tax_pct": 0.10, "comm_bp": 5, "borrow_pct": 1.00, "fund_pct": 5.00,
           "fx_bp": 2, "fx_slip_bp": 0, "rebate_pct": 0.0, "fee_usd": 300, "lag_cd": 4},
    "KR": {"tax_pct": 0.18, "comm_bp": 5, "borrow_pct": 1.50, "fund_pct": 5.00,
           "fx_bp": 4, "fx_slip_bp": 0, "rebate_pct": 0.0, "fee_usd": 300, "lag_cd": 4},
    "JP": {"tax_pct": 0.00, "comm_bp": 4, "borrow_pct": 0.75, "fund_pct": 5.00,
           "fx_bp": 2, "fx_slip_bp": 0, "rebate_pct": 0.0, "fee_usd": 300, "lag_cd": 4},
    "*":  {"tax_pct": 0.10, "comm_bp": 5, "borrow_pct": 1.00, "fund_pct": 5.00,
           "fx_bp": 3, "fx_slip_bp": 0, "rebate_pct": 0.0, "fee_usd": 300, "lag_cd": 5},
}
CONV_EDGE = 0.25
CONV_DEEP_ITM = 90.0               # RFQ delta (%) at/above which the bid is real


def _conv_market(ric):
    r = str(ric or "").upper()
    for suf, mk in ((".TW", "TW"), (".TWO", "TW"), (".HK", "HK"),
                    (".KS", "KR"), (".KQ", "KR"), (".T", "JP")):
        if r.endswith(suf):
            return mk
    return "*"


def conv_bid(p):
    """bid = parity - X.  p: spot, fx, cp, fixed_fx, qty (face, bond ccy),
    rfq_type, delta, slip_bp, edge + cost params.  Returns every column so
    X reconciles exactly to its parts.  Mirrors the JS on the page."""
    f = lambda k, d=None: (float(p[k]) if p.get(k) not in (None, "") else d)
    spot, fx, cp, ffx = f("spot"), f("fx"), f("cp"), f("fixed_fx", 1.0)
    out = {"shares": None, "parity": None, "x": None, "bid": None,
           "status": "terms?", "costs": {}}
    if not (spot and fx and cp and ffx):
        return out
    shares = 100.0 * ffx / cp
    parity = shares * spot / fx
    qty = f("qty", 1_000_000.0) or 1_000_000.0
    slip_bp = f("slip_bp", 0.0)      # versus: stock leg agreed -> 0 bp
    lag = f("lag_cd", 7.0)
    c = {
        "tax":     parity * f("tax_pct", 0.0) / 100.0,
        "fees":    parity * f("comm_bp", 0.0) / 1e4,
        "stk_slip": parity * slip_bp / 1e4,
        "fx_sprd": parity * f("fx_bp", 0.0) / 1e4,
        "fx_slip": parity * f("fx_slip_bp", 0.0) / 1e4,
        # carry over the delivery lag: bond funded (paid), short proceeds
        # earn the rebate (earned -> negative), borrow fee paid
        "funding": parity * f("fund_pct", 0.0) / 100.0 * lag / 360.0,
        "proceeds": -parity * f("rebate_pct", 0.0) / 100.0 * lag / 360.0,
        "borrow":  parity * f("borrow_pct", 0.0) / 100.0 * lag / 365.0,
        "other":   f("fee_usd", 0.0) / (qty / 100.0),
    }
    c = {k: round(v, 4) for k, v in c.items()}   # shown parts define X
    edge = f("edge", CONV_EDGE)
    x = round(sum(c.values()) + edge, 4)
    bid = math.floor((parity - x) * 100) / 100.0        # round DOWN to 0.01
    delta = f("delta", 0.0)
    out.update({"shares": round(shares, 4), "parity": round(parity, 2),
                "net_carry": round(-(c["funding"] + c["proceeds"]
                                     + c["borrow"]), 4),
                "costs": c,
                "edge": edge, "x": x, "bid": bid,
                "x_shown": round(parity - bid, 2),
                "status": ("CONVERSION BID" if delta >= CONV_DEEP_ITM
                           else "floor only")})
    return out


_CONV_MASTER = {"ts": 0.0, "by_sid": {}, "by_isin": {}}


_CONV_MASTER_LOCK = threading.Lock()


def _conv_master(force=False):
    """Identity + conversion terms per security from the embedded Nuke
    Station's refdata (cbanalytics + eqrms + prefs). Cached 5 min."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return _CONV_MASTER
    if not force and time.time() - _CONV_MASTER["ts"] < 300:
        return _CONV_MASTER
    mod = globals().get("_NUKE_MOD")
    if not mod:
        return _CONV_MASTER
    if not _CONV_MASTER_LOCK.acquire(blocking=False):
        return _CONV_MASTER                    # another thread is refreshing: use current
    try:
      try:
        ids = [int(x) for x in (mod.STATE.get("ids") or [])]
        rows, _err = mod.build_refdata(ids) if ids else ([], None)
        by_sid, by_isin = {}, {}
        for r in rows:
            sid = int(r.get("secId"))
            rec = {"secId": sid, "company": r.get("company_name", ""),
                   "short_name": (r.get("short_name") or
                                  (mod.STATE.get("rows", {}).get(sid) or
                                   {}).get("short_name", "")),
                   "expiry": (r.get("expiry_date") or "")[:10],
                   "isin": r.get("isin", ""), "ric": r.get("ric", ""),
                   "sec_fx": r.get("sec_fx", ""), "und_fx": r.get("und_fx", ""),
                   "cp": _fnum(r.get("lp_conversion_price") or
                               r.get("conversion_price")),
                   "fixed_fx": _fnum(r.get("conversion_fixed_fx"))}
            by_sid[sid] = rec
            if rec["isin"]:
                by_isin[rec["isin"].upper()] = rec
        _CONV_MASTER.update({"ts": time.time(), "by_sid": by_sid,
                             "by_isin": by_isin})
      except Exception as e:
        print("[conv] master lookup failed:", e)
    finally:
        _CONV_MASTER_LOCK.release()
    return _CONV_MASTER


def _conv_ensure():
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.conv_terms (
              isin VARCHAR(20) PRIMARY KEY, params TEXT,
              updated_at DATETIME, updated_by VARCHAR(50)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.conv_sheet (
              skey VARCHAR(40) PRIMARY KEY, sec_id INT NULL,
              hidden TINYINT DEFAULT 0, added_by VARCHAR(50),
              added_at DATETIME
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        conn.commit()
    finally:
        conn.close()


@app.get("/api/conv/rows")
def api_conv_rows():
    """One line per bond: latest RFQ per ISIN (from the RFQ snapshot) +
    stored terms/params + the reference calculation."""
    rows = ((RFQ_SNAP.get("data") or {}).get("rows") or [])
    latest = {}
    for r in rows:
        k = r.get("isin") or r.get("short_name")
        if not k:
            continue
        if k not in latest or str(r.get("rfq_id")) > str(latest[k].get("rfq_id")):
            latest[k] = r
    stored = {}
    if os.environ.get("LAGRANGE_TEST_LIVE") != "FILE":
        try:
            _conv_ensure()
            conn = rc.connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT isin, params FROM cba_app.conv_terms")
                    for i, pj in cur.fetchall():
                        try:
                            stored[i] = json.loads(pj or "{}")
                        except Exception:
                            pass
            finally:
                conn.close()
        except Exception:
            pass
    manual, hidden = {}, set()
    if os.environ.get("LAGRANGE_TEST_LIVE") != "FILE":
        try:
            conn = rc.connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT skey, sec_id, hidden FROM "
                                "cba_app.conv_sheet")
                    for sk, sid, hid in cur.fetchall():
                        if hid:
                            hidden.add(sk)
                        else:
                            manual[sk] = sid
            finally:
                conn.close()
        except Exception:
            pass
    master = _conv_master()
    for sk, sid in manual.items():          # manual lines (no RFQ yet)
        if sk not in latest:
            latest[sk] = {"isin": sk, "sec_id": sid, "_manual": 1}
    out = []
    for k, r in latest.items():
        if k in hidden:
            continue
        sid = None
        try:
            sid = int(r.get("sec_id")) if r.get("sec_id") not in (None, "") \
                else None
        except Exception:
            sid = None
        ident = (master["by_sid"].get(sid) if sid else None) or \
            master["by_isin"].get(str(k).upper()) or {}
        mk = _conv_market(ident.get("ric") or r.get("ric") or
                          r.get("und_ric") or "")
        prof = dict(CONV_PROFILE.get(mk, CONV_PROFILE["*"]))
        p = {"isin": ident.get("isin") or k, "key": k,
             "rfq_id": r.get("rfq_id"), "manual": bool(r.get("_manual")),
             "secId": ident.get("secId") or sid or "",
             "company": ident.get("company", ""),
             "expiry": ident.get("expiry", ""), "ric": ident.get("ric", ""),
             "sec_fx": ident.get("sec_fx", ""),
             "und_fx": ident.get("und_fx", ""),
             "short_name": r.get("short_name") or ident.get("short_name"),
             "market": mk,
             "spot": _fnum(r.get("eff_vs") or r.get("live_spot")),
             "fx": _fnum(r.get("eff_fx") or r.get("live_und")),
             "qty": _fnum(r.get("qty")),
             "delta": _fnum(r.get("q_delta") or r.get("live_delta")),
             "cp": ident.get("cp"), "fixed_fx": ident.get("fixed_fx"),
             "edge": CONV_EDGE,
             "est": ["profile"] + ([] if ident.get("cp") else ["terms"]),
             "ref": [x for x in ("cp", "fixed_fx") if ident.get(x)]}
        p.update(prof)
        p.update(stored.get(k, {}))
        p["calc"] = conv_bid(p)
        out.append(p)
    if not out:      # empty sheet: show the examples (not persisted)
        for e in CONV_EXAMPLES:
            mk = e.get("market") or _conv_market(
                master["by_sid"].get(e["sec_id"], {}).get("ric") or "")
            p = {"isin": e["isin"], "key": e["isin"], "secId": e["sec_id"],
                 "short_name": e["short_name"], "market": mk, "manual": True,
                 "example": 1, "est": ["profile", "terms"],
                 "edge": CONV_EDGE}
            p.update(CONV_PROFILE.get(mk, CONV_PROFILE["*"]))
            p.update({k: v for k, v in e.items()
                      if k not in ("isin", "sec_id", "short_name")})
            p.update(master["by_sid"].get(e["sec_id"], {}) and
                     {k: master["by_sid"][e["sec_id"]].get(k, "") for k in
                      ("company", "expiry", "ric", "sec_fx", "und_fx")})
            p["calc"] = conv_bid(p)
            out.append(p)
    out.sort(key=lambda x: (x["calc"]["status"] != "CONVERSION BID",
                            x.get("short_name") or ""))
    return {"ok": True, "rows": out, "edge": CONV_EDGE,
            "deep_itm": CONV_DEEP_ITM}


CONV_EXAMPLES = [   # preview lines; terms are assumptions (est)
    dict(isin="XS2853493117", sec_id=28357967, short_name="Wiwynn 29", market="TW",
         spot=2090, fx=31.803, cp=1017.75, fixed_fx=32.576, qty=200000,
         delta=95),
    dict(isin="XS3462009369", sec_id=60426052, short_name="Ase 31", market="TW",
         spot=610, fx=31.757, cp=900, fixed_fx=32.0, qty=3500000, delta=56),
    dict(isin="XS3444080413", sec_id=60617653, short_name="Bizlink 31", market="TW",
         spot=1935, fx=31.757, cp=2600, fixed_fx=32.2, qty=5600000,
         delta=45),
    dict(isin="XS3253754934", sec_id=55995604, short_name="Giga 31", market="TW",
         spot=357, fx=31.75, cp=330, fixed_fx=32.3, qty=2200000, delta=92),
    dict(isin="XS3202703834", sec_id=44497742, short_name="Wistron 30", market="TW",
         spot=186.5, fx=31.443, cp=250, fixed_fx=30.9, qty=1900000,
         delta=67),
    dict(isin="XS3357078263", sec_id=57643239, short_name="MMG 27", market="HK",
         spot=9.69, fx=7.8396, cp=12.5, fixed_fx=7.78, qty=8600000,
         delta=43),
    dict(isin="XS3105284809", sec_id=39476009, short_name="Nissan 31", market="JP",
         spot=302.5, fx=155.285, cp=350, fixed_fx=148.5, qty=10000000,
         delta=55),
]


@app.post("/api/conv/examples")
def api_conv_examples(request: Request):
    """Seed the sheet with the example lines (shared, persisted, est)."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or "lagrange")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "n": len(CONV_EXAMPLES)}
    _conv_ensure()
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            for e in CONV_EXAMPLES:
                cur.execute("INSERT INTO cba_app.conv_sheet (skey, sec_id, "
                            "hidden, added_by, added_at) VALUES (%s,%s,0,%s,"
                            "NOW()) ON DUPLICATE KEY UPDATE hidden=0",
                            (e["isin"], e["sec_id"], user))
                params = dict(CONV_PROFILE.get(e.get("market", "*"),
                                               CONV_PROFILE["*"]))
                params.update({k: v for k, v in e.items()
                               if k not in ("isin", "sec_id")})
                params.update({"est": ["profile", "terms"], "example": 1})
                cur.execute("INSERT INTO cba_app.conv_terms (isin, params, "
                            "updated_at, updated_by) VALUES (%s,%s,NOW(),%s) "
                            "ON DUPLICATE KEY UPDATE params=VALUES(params), "
                            "updated_at=NOW(), updated_by=VALUES(updated_by)",
                            (e["isin"], json.dumps(params), user))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "n": len(CONV_EXAMPLES)}


class ConvKey(BaseModel):
    q: str = ""


@app.post("/api/conv/add")
def api_conv_add(req: ConvKey, request: Request):
    """Add a bond by SECID or ISIN (identity from the Nuke master)."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or "lagrange")
    q = (req.q or "").strip().upper()
    if not q:
        return JSONResponse(status_code=400, content={"ok": False,
                            "error": "give a SECID or ISIN"})
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "key": q}
    master = _conv_master(force=True)
    rec = None
    if q.isdigit():
        rec = master["by_sid"].get(int(q))
    if rec is None:
        rec = master["by_isin"].get(q)
    if rec is None and not q.isdigit() and len(q) != 12:
        return JSONResponse(status_code=404, content={"ok": False,
                            "error": f"{q}: not a SECID in Nuke and not "
                            "an ISIN"})
    key = (rec or {}).get("isin") or q
    sid = (rec or {}).get("secId")
    _conv_ensure()
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO cba_app.conv_sheet (skey, sec_id, "
                        "hidden, added_by, added_at) VALUES (%s,%s,0,%s,NOW())"
                        " ON DUPLICATE KEY UPDATE hidden=0, "
                        "sec_id=COALESCE(VALUES(sec_id), sec_id)",
                        (key, sid, user))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "key": key, "known": bool(rec)}


@app.post("/api/conv/del")
def api_conv_del(req: ConvKey, request: Request):
    """Remove a line: manual lines are deleted, RFQ-sourced lines are
    hidden (the RFQ itself is untouched)."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or "lagrange")
    key = (req.q or "").strip()
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    _conv_ensure()
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO cba_app.conv_sheet (skey, sec_id, "
                        "hidden, added_by, added_at) VALUES (%s,NULL,1,%s,"
                        "NOW()) ON DUPLICATE KEY UPDATE hidden=1, "
                        "added_by=VALUES(added_by), added_at=NOW()",
                        (key, user))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


class ConvSave(BaseModel):
    isin: str = ""
    params: Dict[str, Any] = {}


@app.post("/api/conv/save")
def api_conv_save(req: ConvSave, request: Request):
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or "lagrange")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    _conv_ensure()
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO cba_app.conv_terms (isin, params, "
                        "updated_at, updated_by) VALUES (%s,%s,NOW(),%s) "
                        "ON DUPLICATE KEY UPDATE params=VALUES(params), "
                        "updated_at=NOW(), updated_by=VALUES(updated_by)",
                        (req.isin, json.dumps(req.params), user))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


# ============ IDB QUOTES: broker runs -> quotes -> board -> compare ============
import re as _re
import hashlib as _hl

IDB_SOURCE = "IDB1"
IDB_GAP_PTS = 0.50
IDB_STALE_MIN = 90
IDB_WIDE_PTS = 3.0
IDB_REV = {"n": 0}            # bumped on every IDB write; caches key on it
_IDB_GRID_CACHE = {"key": None, "ts": 0.0, "rows": None}
_IDB_GRID_LOCK = threading.Lock()
_IDB_HIST_CACHE = {"rev": -1, "board": None}
_NUKE_RUN_LOCK = threading.Lock()
_IDB_TIME = _re.compile(r"^(\d{1,2}:\d{2}(?::\d{2})?)\s+")
_IDB_SENDER = _re.compile(r"^[^:\n]{2,40}\((?:IDB|idb)[^)]*\):\s*|^[A-Za-z][A-Za-z .'-]{1,30}:\s+(?=[A-Za-z])")
_IDB_MAT = _re.compile(r"^(?:'?(\d{2})|20(\d{2}))$")
_IDB_NUM = r"\d+(?:\.\d+)?"
_IDB_TRADE_WORDS = ("paid", "lifted", "given", "hit", "traded", "done")
_IDB_SIDE_WORDS = ("bid", "offered", "offer", "ofrd", "ofr", "ask") + _IDB_TRADE_WORDS
_IDB_REF_WORDS = ("r", "ref", "vs", "v")
_IDB_SKIP = ("levels as of", "as of ")


def _idb_parse_line(line, prev_time=None, state=None):
    """One chat line -> dict(kind, ...). kind in quote|region|header|
    skip|review. Implements the spec's shorthand table."""
    state = state if state is not None else {}
    raw = line.rstrip()
    s = raw.strip()
    if not s:
        return {"kind": "skip"}
    if any(k in s.lower() for k in _IDB_SKIP):
        return {"kind": "review", "reason": "informational line", "line": raw}
    # region header: flag emoji or all-caps place with no digits
    _regions = ("HONG KONG", "TAIWAN", "KOREA", "JAPAN", "SINGAPORE",
                "CHINA", "INDIA", "ASIA", "EUROPE", "US", "AUSTRALIA",
                "MALAYSIA", "THAILAND", "INDONESIA", "PHILIPPINES")
    if _re.search(r"[\U0001F1E6-\U0001F1FF]", s) or (
            s.isupper() and _re.sub(r"[^A-Z ]", "", s).strip() in _regions):
        region = _re.sub(r"[^A-Za-z ]", "", s).strip()
        state["region"] = region
        return {"kind": "region", "region": region}
    s = _IDB_SENDER.sub("", s)          # "Name (IDB): 09:20 ..."
    t = None
    m = _IDB_TIME.match(s)
    if m:
        t = m.group(1)
        if len(t) == 5:
            t += ":00"
        s = s[m.end():]
    s = _IDB_SENDER.sub("", s)          # "09:20 Name (IDB): ..."
    comment = ""
    mc = _re.search(r"\(([^)]*)\)\s*$", s)
    if mc:
        comment = mc.group(1).strip()
        s = s[:mc.start()].rstrip()
    toks = s.split()
    # name = tokens up to the maturity token
    name, year, i = [], None, 0
    while i < len(toks):
        mm = _IDB_MAT.match(toks[i])
        if mm and name:
            # a bare 2-digit number is a PRICE, not a maturity, when the
            # next token is a side/trade word, a ref word or a dash
            # ("BIDU/TRIP 88 bid r 309.20", "HUATAI 98 - 100 r 16")
            bare = mm.group(1) is not None and not toks[i].startswith("'")
            nxt = toks[i + 1].lower() if i + 1 < len(toks) else ""
            if bare and (nxt in _IDB_SIDE_WORDS or nxt in _IDB_REF_WORDS
                         or nxt.startswith("-")):
                break                       # price starts here
            year = mm.group(1) or mm.group(2)
            i += 1
            break
        if _re.match(r"^\d", toks[i]) and name:
            break                     # price starts, no maturity given
        name.append(toks[i]); i += 1
    if not name:
        return {"kind": "review", "reason": "no bond name", "line": raw}
    rest = " ".join(toks[i:])
    if not rest:
        if len(name) <= 2 and all(w.isupper() for w in name):
            return {"kind": "header", "name": " ".join(name)}
        return {"kind": "review", "reason": "no price", "line": raw}
    q = {"kind": "quote", "name": " ".join(name).upper(), "year": year,
         "bid": None, "offer": None, "trade": None, "size": None,
         "ref": None, "comment": comment, "flags": [], "line": raw,
         "time": t}
    low = rest.lower()
    # stock ref: r / ref / vs / v + number
    mr = _re.search(r"\b(?:r|ref|vs|v)\s*(" + _IDB_NUM + r")\s*(k|m|mm)?\b", low)
    if mr:
        mult = {"k": 1e3, "m": 1e6, "mm": 1e6}.get(mr.group(2) or "", 1.0)
        q["ref"] = float(mr.group(1)) * mult
        low = low[:mr.start()] + " " + low[mr.end():]
    # size: 3m / 2.5mm
    ms = _re.search(r"\b(" + _IDB_NUM + r")\s*mm?\b", low)
    if ms:
        q["size"] = float(ms.group(1))
        low = low[:ms.start()] + " " + low[ms.end():]
    # two-way: A - B or A-B (B may be a shorthand fraction)
    m2 = _re.search(r"(" + _IDB_NUM + r")\s*-\s*(" + _IDB_NUM + r")", low)
    if m2:
        atxt, btxt = m2.group(1), m2.group(2)
        a, b = float(atxt), float(btxt)
        if "." in atxt and "." not in btxt and len(btxt) <= 2:
            # 97.25-55 -> 97.55 ; 99.75-25 -> 100.25
            handle = int(a)
            frac = b / 100.0 if len(btxt) == 2 else b / 10.0
            b = handle + frac
            if b < a:
                b += 1.0
        q["bid"], q["offer"] = a, b
    else:
        mb = _re.search(r"(" + _IDB_NUM + r")\s*bid\b", low)
        mo = _re.search(r"(" + _IDB_NUM + r")\s*(?:offered|offer|ofrd|ofr|ask)\b", low)
        mt = _re.search(r"(" + _IDB_NUM + r")\s*(?:" + "|".join(_IDB_TRADE_WORDS) + r")\b", low)
        if mb:
            q["bid"] = float(mb.group(1))
        if mo:
            q["offer"] = float(mo.group(1))
        if mt:
            q["trade"] = float(mt.group(1))
        if not (mb or mo or mt):
            mn = _re.search(r"(" + _IDB_NUM + r")", low)
            if not mn:
                return {"kind": "review", "reason": "no price", "line": raw}
            q["offer"] = None
            q["bid"] = None
            q["trade"] = None
            q["px"] = float(mn.group(1))
            q["flags"].append("NO_SIDE")
    if q["ref"] is None:
        q["flags"].append("NO_REF")
    if q["bid"] is not None and q["offer"] is not None:
        if q["offer"] < q["bid"]:
            q["flags"].append("CROSSED")
        elif q["offer"] - q["bid"] > IDB_WIDE_PTS:
            q["flags"].append("WIDE")
    if t is None:
        if prev_time:
            q["time"] = prev_time
        else:
            q["flags"].append("NO_TIME")
    return q


def idb_parse_run(text):
    """Whole paste -> (quotes, others). others = region/header/review."""
    quotes, others, prev_t, st = [], [], None, {}
    for ln in text.splitlines():
        r = _idb_parse_line(ln, prev_t, st)
        if r["kind"] == "quote":
            r["region"] = st.get("region", "")
            quotes.append(r)
            if r.get("time"):
                prev_t = r["time"]
        elif r["kind"] != "skip":
            others.append(r)
    return quotes, others


def idb_fingerprint(trade_date, source, qtime, line):
    norm = _re.sub(r"\s+", "", line).lower()
    return _hl.sha1(f"{trade_date}|{source}|{qtime or ''}|{norm}"
                    .encode("utf-8")).hexdigest()


def idb_key(name, year):
    return _re.sub(r"[^A-Z0-9]", "", (name or "").upper()) + (year or "")


def idb_roll(px, ref, spot, delta, parity):
    """price now = px + delta x parity x (1 - ref/spot). delta as % or
    fraction; parity in points at spot. None when unadjustable."""
    d = _fnum(delta)
    if None in (px, ref, spot, parity, d) or not spot or not ref:
        return None
    if abs(d) > 1.5:
        d = d / 100.0
    return px + d * float(parity) * (1.0 - float(ref) / float(spot))


def idb_compare_one(key, mkt, mark, now_time=None):
    """mkt: broker levels AS PARSED (bid/bid_ref/bid_time, offer/...).
    mark: {my_bid, my_offer, ref, repriced(bool)} = the desk quote
    re-nuked at the broker's reference (Issue 4/5). No rolling: MKT is
    the broker's price exactly as quoted; MY is our quote at that ref."""
    flags = []
    mb, mo = mkt.get("bid"), mkt.get("offer")
    myb, myo = _fnum(mark.get("my_bid")), _fnum(mark.get("my_offer"))
    gap, basis = None, ""
    if myb is None and myo is None:
        flags.append("NOT PRICED YET" if mark.get("bref") is not None and mark.get("run_spot") is None and mark.get("stale") else "NO MARK")
    else:
        if not mark.get("repriced"):
            flags.append("NOT REPRICED (inputs changed)")
        rs = _fnum(mark.get("run_spot"))
        rb, ro = _fnum(mkt.get("bid_ref")), _fnum(mkt.get("offer_ref"))
        same = lambda a, b: (a is None or b is None or abs(a - b) <= 1e-6 * max(1.0, abs(b)))
        # a side is comparable only when it was quoted at the ref my quote was priced at
        bid_ok = mb is not None and same(rs, rb)
        ofr_ok = mo is not None and same(rs, ro)
        if mb is not None and not bid_ok:
            flags.append("BID @%.4g \u2260 REF %.4g (not compared)" % (rb, rs))
        if mo is not None and not ofr_ok:
            flags.append("OFR @%.4g \u2260 REF %.4g (not compared)" % (ro, rs))
        if bid_ok and myo is not None and mb > myo:
            flags.append("MKT BID > MY OFFER")
        if ofr_ok and myb is not None and mo < myb:
            flags.append("MKT OFFER < MY BID")
        if bid_ok and ofr_ok and myb is not None and myo is not None:
            gap, basis = (mb + mo) / 2 - (myb + myo) / 2, "mid"
        elif bid_ok and myb is not None:
            gap, basis = mb - myb, "bid"
        elif ofr_ok and myo is not None:
            gap, basis = mo - myo, "offer"
        if gap is not None and abs(gap) > IDB_GAP_PTS:
            flags.append("GAP %+.2f (%s)" % (gap, basis))
    ref, side, diff = _idb_ref_for(mkt)
    if mark.get("run_spot") is not None:
        ref = mark.get("run_spot")                 # "ref used" = the spot the run used
    if diff:
        flags.append("SIDES @ DIFF TIME (ref from %s)" % side)
    lt = mkt.get("last_time")
    if lt and now_time:
        try:
            h1, m1 = [int(x) for x in str(lt).split(":")[:2]]
            h2, m2 = [int(x) for x in str(now_time).split(":")[:2]]
            if (h2 * 60 + m2) - (h1 * 60 + m1) > IDB_STALE_MIN:
                flags.append("STALE")
        except Exception:
            pass
    urg = 0 if any(f.startswith("MKT") for f in flags) else (
        1 if (myb is not None or myo is not None) else 2)
    return {"key": key, "mkt_bid": mb, "mkt_offer": mo,
            "my_bid": myb, "my_offer": myo, "ref_used": ref, "ref_side": side,
            "gap": None if gap is None else round(gap, 3),
            "flags": " | ".join(flags), "urg": urg,
            "raw_bid": mb, "raw_offer": mo,
            "bid_ref": mkt.get("bid_ref"), "offer_ref": mkt.get("offer_ref"),
            "bid_time": mkt.get("bid_time"), "offer_time": mkt.get("offer_time")}


def _idb_ensure():
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.idb_raw (
              id INT PRIMARY KEY AUTO_INCREMENT, pasted_at DATETIME,
              pasted_by VARCHAR(50), source VARCHAR(20), trade_date DATE,
              text MEDIUMTEXT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.idb_quotes (
              id INT PRIMARY KEY AUTO_INCREMENT, raw_id INT, trade_date DATE,
              source VARCHAR(20), qtime VARCHAR(8), region VARCHAR(40),
              name VARCHAR(60), yr VARCHAR(4), bid DECIMAL(10,4) NULL,
              offer DECIMAL(10,4) NULL, trade DECIMAL(10,4) NULL,
              size DECIMAL(10,3) NULL, ref DECIMAL(14,4) NULL,
              comment VARCHAR(200), flags VARCHAR(80), line VARCHAR(400),
              fp CHAR(40) UNIQUE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.idb_unparsed (
              id INT PRIMARY KEY AUTO_INCREMENT, raw_id INT, trade_date DATE,
              line VARCHAR(400), reason VARCHAR(100))
              ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.idb_alias (
              broker_key VARCHAR(40) PRIMARY KEY, sec_id INT NULL,
              my_short VARCHAR(60), note VARCHAR(120),
              status VARCHAR(10) DEFAULT 'ASSUMED', updated_by VARCHAR(50),
              updated_at DATETIME) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.idb_marks (
              mkey VARCHAR(40) PRIMARY KEY, my_bid DECIMAL(10,4) NULL,
              my_offer DECIMAL(10,4) NULL, spot DECIMAL(14,4) NULL,
              delta DECIMAL(8,3) NULL, parity DECIMAL(10,3) NULL,
              updated_by VARCHAR(50), updated_at DATETIME)
              ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        conn.commit()
    finally:
        conn.close()


class IdbIngest(BaseModel):
    text: str = ""
    source: str = ""
    date: str = ""


@app.post("/api/idb/ingest")
def api_idb_ingest(req: IdbIngest, request: Request):
    user = ((getattr(request.state, "auth", None) or {}).get("user") or "lagrange")
    src_ = (req.source or IDB_SOURCE).strip()[:20]
    tdate = (req.date or dt.date.today().isoformat())[:10]
    quotes, others = idb_parse_run(req.text or "")
    lines = [l for l in (req.text or "").splitlines() if l.strip()]
    if lines and len(quotes) * 2 < len(lines):
        return {"ok": False, "error": "fewer than half the lines read as "
                "quotes (%d of %d) - is this a broker run?" % (len(quotes), len(lines)),
                "parsed": len(quotes), "lines": len(lines)}
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "new": len(quotes), "dup": 0,
                "review": sum(1 for o in others if o["kind"] == "review"),
                "quotes": quotes}
    _idb_ensure()
    conn = rc.connect()
    new = dup = 0
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO cba_app.idb_raw (pasted_at, pasted_by, "
                        "source, trade_date, text) VALUES (NOW(),%s,%s,%s,%s)",
                        (user, src_, tdate, req.text))
            raw_id = cur.lastrowid
            for q in quotes:
                fp = idb_fingerprint(tdate, src_, q.get("time"), q["line"])
                cur.execute("INSERT IGNORE INTO cba_app.idb_quotes (raw_id, "
                            "trade_date, source, qtime, region, name, yr, bid, "
                            "offer, trade, size, ref, comment, flags, line, fp) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (raw_id, tdate, src_, q.get("time"), q.get("region", ""),
                             q["name"], q.get("year"), q["bid"], q["offer"],
                             q["trade"], q["size"], q["ref"], q["comment"][:200],
                             "|".join(q["flags"])[:80], q["line"][:400], fp))
                if cur.rowcount:
                    new += 1
                else:
                    dup += 1
            for o in others:
                if o["kind"] == "review":
                    cur.execute("INSERT INTO cba_app.idb_unparsed (raw_id, "
                                "trade_date, line, reason) VALUES (%s,%s,%s,%s)",
                                (raw_id, tdate, o["line"][:400], o["reason"][:100]))
        conn.commit()
    finally:
        conn.close()
    if new:
        IDB_REV["n"] += 1
    return {"ok": True, "new": new, "dup": dup,
            "bonds": len({idb_key(q["name"], q.get("year")) for q in quotes}),
            "review": sum(1 for o in others if o["kind"] == "review")}


def _idb_aliases():
    out = {}
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return out
    try:
        _idb_ensure()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT broker_key, sec_id, my_short, note, status "
                            "FROM cba_app.idb_alias")
                for k, sid, ms, note, stt in cur.fetchall():
                    out[k] = {"sec_id": sid, "my_short": ms or "", "note": note or "",
                              "status": stt or "ASSUMED"}
        finally:
            conn.close()
    except Exception:
        pass
    return out


def _idb_board(tdate, source=None):
    """Latest bid / offer / trade per broker key (aliases applied)."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {}, {}
    _idb_ensure()
    aliases = _idb_aliases()
    board, spell = {}, {}
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            where = []; params = []
            if tdate:
                where.append("trade_date=%s"); params.append(tdate)
            if source:
                where.append("source=%s"); params.append(source)
            cur.execute("SELECT qtime, name, yr, bid, offer, trade, size, ref, "
                        "flags, source, trade_date FROM cba_app.idb_quotes"
                        + ((" WHERE " + " AND ".join(where)) if where else "")
                        + " ORDER BY trade_date, qtime, id", tuple(params))
            for qt, nm, yr, b, o, tr, sz, ref, fl, src_, tdd in cur.fetchall():
                tdd = str(tdd)[:10]
                bk = idb_key(nm, yr)
                al = aliases.get(bk)
                key = (al or {}).get("my_short") or bk
                spell.setdefault(key, set()).add(f"{nm} {yr or ''}".strip())
                row = board.setdefault(key, {"key": key, "broker_key": bk,
                    "sec_id": (al or {}).get("sec_id"),
                    "alias_status": (al or {}).get("status", ""),
                    "bid": None, "bid_ref": None, "bid_time": None,
                    "offer": None, "offer_ref": None, "offer_time": None,
                    "trade": None, "trade_time": None, "last_time": None,
                    "n": 0, "source": src_})
                row["n"] += 1
                row["last_time"] = qt; row["last_date"] = tdd
                if b is not None:
                    row.update(bid=float(b), bid_ref=(None if ref is None else float(ref)), bid_time=qt, bid_date=tdd)
                if o is not None:
                    row.update(offer=float(o), offer_ref=(None if ref is None else float(ref)), offer_time=qt, offer_date=tdd)
                if tr is not None:
                    row.update(trade=float(tr), trade_time=qt, trade_date=tdd)
    finally:
        conn.close()
    for k, r in board.items():
        r["spellings"] = sorted(spell.get(k, []))
    return board, aliases


def _idb_mark_from_nuke(sec_id):
    """my bid/offer, spot, delta, parity from the embedded Nuke Station."""
    try:
        lv = _rfq_live(int(sec_id), "outright")
        spot = lv.get("und") if lv.get("und") is not None else lv.get("spot")
        mk = {"my_bid": lv.get("nqb"), "my_offer": lv.get("nqa"),
              "spot": spot, "delta": lv.get("nd"), "parity": None, "src": "nuke"}
        m = _conv_master().get("by_sid", {}).get(int(sec_id), {})
        cp, ff, fx = m.get("cp"), m.get("fixed_fx") or 1.0, lv.get("fx")
        if cp and spot:
            mod = globals().get("_NUKE_MOD")
            bt = ((mod.STATE.get("rows", {}).get(int(sec_id)) or {}).get("bond_type")
                  if mod else "") or ""
            if bt.lower().startswith("vanil") or not fx:
                mk["parity"] = spot / cp * 100.0
            else:
                mk["parity"] = (spot / fx) / (cp / ff) * 100.0
        return mk
    except Exception:
        return None


def _idb_manual_marks():
    out = {}
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return out
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT mkey, my_bid, my_offer, spot, delta, parity "
                            "FROM cba_app.idb_marks")
                for k, b, o, s, d, p in cur.fetchall():
                    out[k] = {"my_bid": _fnum(b), "my_offer": _fnum(o), "spot": _fnum(s),
                              "delta": _fnum(d), "parity": _fnum(p), "src": "manual"}
        finally:
            conn.close()
    except Exception:
        pass
    return out


@app.get("/api/idb/board")
def api_idb_board(date: str = "", source: str = ""):
    tdate = (date or dt.date.today().isoformat())[:10]
    board, aliases = _idb_board(tdate, source or None)
    rows = sorted(board.values(), key=lambda r: r["key"])
    return {"ok": True, "date": tdate, "rows": rows,
            "aliases": aliases}


@app.get("/api/idb/compare")
def api_idb_compare(date: str = "", source: str = ""):
    tdate = (date or dt.date.today().isoformat())[:10]
    board, aliases = _idb_board(tdate, source or None)
    manual = _idb_manual_marks()
    now_t = dt.datetime.now().strftime("%H:%M:%S") \
        if tdate == dt.date.today().isoformat() else None
    out = []
    st = (globals().get("_NUKE_MOD").snapshot() if globals().get("_NUKE_MOD") else {})
    for key, mkt in board.items():
        al = aliases.get(mkt["broker_key"]) or {}
        sid = _nk_resolve(al) or mkt.get("sec_id")
        mark = {}
        if sid:
            nkr = IDB_NUKE.get(int(sid)); ref, _s, _d = _idb_ref_for(mkt)
            _idb_ovd_load()
            if nkr and nkr.get("res"):
                qb, qa = _idb_quote_from(nkr["res"], (st.get("rows") or {}).get(int(sid)) or {})
                cur = _idb_inputs(int(sid), st)
                with _IDB_PENDING_LOCK:
                    pend = int(sid) in IDB_PENDING
                stale = pend or (tuple(nkr.get("inputs") or ()) != tuple(float(x) if x is not None else None for x in cur))
                mark = {"my_bid": qb, "my_offer": qa, "ref": nkr.get("ref"), "repriced": not stale,
                        "stale": stale, "run_spot": nkr.get("ref"), "bref": ref}
            else:
                mark = {"my_bid": None, "my_offer": None, "ref": None, "repriced": False,
                        "stale": True, "run_spot": None, "bref": ref}
        mkt = dict(mkt); mkt["sec_id"] = sid
        row = idb_compare_one(key, mkt, mark, now_t)
        row.update({"broker_key": mkt["broker_key"], "sec_id": mkt.get("sec_id"),
                    "spellings": mkt.get("spellings", []),
                    "alias_status": mkt.get("alias_status", ""),
                    "my_short": al.get("my_short", ""),
                    "mark_src": ("idb-run" if mark.get("repriced") else ("stale" if sid else "")),
                    "last_time": mkt.get("last_time")})
        out.append(row)
    out.sort(key=lambda r: (r["urg"], -(abs(r["gap"]) if r["gap"] is not None else -1), r["key"]))
    return {"ok": True, "date": tdate, "rows": out}


@app.get("/api/idb/unparsed")
def api_idb_unparsed(date: str = ""):
    tdate = (date or dt.date.today().isoformat())[:10]
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "rows": []}
    _idb_ensure()
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, line, reason FROM cba_app.idb_unparsed "
                        "WHERE trade_date=%s ORDER BY id", (tdate,))
            rows = [{"id": i, "line": l, "reason": r} for i, l, r in cur.fetchall()]
    finally:
        conn.close()
    return {"ok": True, "rows": rows}


# ---- IDB grid: Nuke Station's sections, fed from Nuke's shared state ----
NK_OVD = ("ovdSpot", "ovdCbFx", "ovdUndFx")
# IDB tab keeps its OWN overrides and its OWN re-nuke results: nothing
# here is written into Nuke Station's shared state (read-only use only).
IDB_OVD: Dict[int, Dict[str, str]] = {}      # sec_id -> {ovdSpot, ovdCbFx, ovdUndFx}
IDB_NUKE: Dict[int, Dict[str, Any]] = {}     # sec_id -> {"ref", "res", "ts", "by"}
_IDB_OVD_LOADED = {"ok": False}


def _idb_ovd_load():
    if _IDB_OVD_LOADED["ok"] or os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        _IDB_OVD_LOADED["ok"] = True
        return
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.idb_ovd (
                  sec_id INT PRIMARY KEY, ovd_spot VARCHAR(30), ovd_cbfx VARCHAR(30),
                  ovd_undfx VARCHAR(30), updated_by VARCHAR(50), updated_at DATETIME)
                  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
                cur.execute("SELECT sec_id, ovd_spot, ovd_cbfx, ovd_undfx FROM cba_app.idb_ovd")
                for sid, a, b, c in cur.fetchall():
                    IDB_OVD[int(sid)] = {"ovdSpot": a or "", "ovdCbFx": b or "", "ovdUndFx": c or ""}
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print("[idb] ovd load failed:", e)
    _IDB_OVD_LOADED["ok"] = True


def _idb_ovd_set(sid, fields, user):
    """IDB-tab override write: memory + cba_app.idb_ovd. Never Nuke."""
    _idb_ovd_load()
    row = IDB_OVD.setdefault(int(sid), {"ovdSpot": "", "ovdCbFx": "", "ovdUndFx": ""})
    for f, v in fields.items():
        if f in NK_OVD:
            row[f] = "" if v is None else str(v)
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return row
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO cba_app.idb_ovd (sec_id, ovd_spot, ovd_cbfx, "
                            "ovd_undfx, updated_by, updated_at) VALUES (%s,%s,%s,%s,%s,NOW()) "
                            "ON DUPLICATE KEY UPDATE ovd_spot=VALUES(ovd_spot), "
                            "ovd_cbfx=VALUES(ovd_cbfx), ovd_undfx=VALUES(ovd_undfx), "
                            "updated_by=VALUES(updated_by), updated_at=NOW()",
                            (int(sid), row["ovdSpot"], row["ovdCbFx"], row["ovdUndFx"], user))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print("[idb] ovd save failed:", e)
    return row


IDB_QUOTE_STEP = 0.05          # same as Nuke Station's QuoteBid/QuoteAsk (fmtBA)


def idb_round_quote(v, step=IDB_QUOTE_STEP):
    """Nuke's rule: nearest step, both sides (Math.round(v/step)*step)."""
    if v is None:
        return None
    import math as _m
    return round(_m.floor(float(v) / step + 0.5) * step, 2)   # exactly Math.round(v/step)*step


def _idb_quote_from(res, row):
    """Desk quote = engine mkt bid/ask + X (same arithmetic as _rfq_live),
    then rounded to 0.05 exactly as Nuke Station shows QuoteBid/QuoteAsk."""
    if not res:
        return None, None
    x2 = _fnum(row.get("x_both")) or 0.0
    mb, ma = _fnum(res.get("ovdMktBid")), _fnum(res.get("ovdMktAsk"))
    qb = None if mb is None else idb_round_quote(mb + (_fnum(row.get("x_bid")) or 0.0) + x2)
    qa = None if ma is None else idb_round_quote(ma + (_fnum(row.get("x_ask")) or 0.0) + x2)
    return qb, qa


def _idb_ref_for(b):
    """Broker reference to re-nuke at: the more recently quoted side's
    @ref (Issue 5B). Returns (ref, side, diff_times)."""
    bt, ot = b.get("bid_time") or "", b.get("offer_time") or ""
    bd, od = b.get("bid_date") or "", b.get("offer_date") or ""
    br, orf = b.get("bid_ref"), b.get("offer_ref")
    diff = bool(bt and ot and (bt != ot or bd != od))
    if br is not None and orf is not None:
        return ((br, "bid", diff) if (bd, bt) >= (od, ot) else (orf, "offer", diff))
    if br is not None:
        return br, "bid", diff
    if orf is not None:
        return orf, "offer", diff
    return None, "", diff


def _nk_num(v):
    return _fnum(str(v).replace(",", "")) if v not in (None, "") else None


def _nk_norm(s):
    return _re.sub(r"[^A-Z0-9]", "", str(s or "").upper())


def _nk_short_index():
    """normalised short_name -> secId. Nuke's per-row short_name (what
    you typed in Nuke Station) wins; refdata prefs fill the rest."""
    idx = {}
    master = _conv_master().get("by_sid", {})
    for sid, rec in master.items():
        n = _nk_norm(rec.get("short_name"))
        if n:
            idx.setdefault(n, sid)
    mod = globals().get("_NUKE_MOD")
    if mod:
        for sid, row in (mod.STATE.get("rows") or {}).items():
            n = _nk_norm(row.get("short_name"))
            if n:
                idx[n] = int(sid)
    return idx


def _nk_resolve(alias):
    """alias {sec_id, my_short} -> secId or None (short_name wins when
    it matches a Nuke bond; else the stored sec_id)."""
    n = _nk_norm((alias or {}).get("my_short"))
    if n:
        sid = _nk_short_index().get(n)
        if sid:
            return sid
    sid = (alias or {}).get("sec_id")
    return int(sid) if sid else None


def _nk_suggest(broker_key, spellings, master, rows):
    """Propose my bond for a broker name: name tokens must appear in
    the Nuke short_name or company name; a broker year must match the
    short_name's year. Returns (secId, short_name, score) or None."""
    m = _re.match(r"^([A-Z]+?)(\d{2})?$", broker_key or "")
    if not m:
        return None
    bname, byear = m.group(1), m.group(2)
    words = set()
    for sp in (spellings or []):
        for w in _re.sub(r"[^A-Z ]", " ", str(sp).upper()).split():
            if len(w) >= 3 and not w.isdigit():
                words.add(w)
    words.add(bname)
    best = None
    cands = {}
    for sid, rec in master.items():
        cands[sid] = (rec.get("short_name") or "", rec.get("company") or "")
    for sid, row in (rows or {}).items():
        sn = row.get("short_name") or ""
        if sn:
            cands[int(sid)] = (sn, cands.get(int(sid), ("", ""))[1])
    for sid, (sn, co) in cands.items():
        snn, con = _nk_norm(sn), _nk_norm(co)
        sy = _re.search(r"(\d{2})\s*$", sn or "")
        sy = sy.group(1) if sy else None
        if byear and sy and byear != sy:
            continue                                  # wrong year: never
        score = 0
        for w in words:
            if w in snn:
                score += 3 + (2 if snn.startswith(w) else 0)
            elif w in con:
                score += 2
        if score == 0:
            continue
        if byear and sy == byear:
            score += 2
        if best is None or score > best[2]:
            best = (int(sid), sn or co, score)
    return best if (best and best[2] >= 3) else None


def _nk_details(sid, st, master, rfx):
    """Nuke's sections for one security as a flat dict."""
    row = (st.get("rows") or {}).get(sid) or {}
    nk = (st.get("nuke") or {}).get(sid) or {}
    idn = master.get(sid, {})
    ric = idn.get("ric") or nk.get("ric") or ""
    fxr = (row.get("und_fx") or idn.get("und_fx") or "").strip()
    srf = rfx.get(ric) or {}
    frf = rfx.get(fxr) or {}
    try:
        lv = _rfq_live(sid, "outright")
    except Exception:
        lv = {}
    nd = lv.get("nd")
    spot = (_nk_num(row.get("ovdSpot")) or _nk_num(nk.get("liveSpot"))
            or _nk_num(srf.get("last")))
    fx = (_nk_num(row.get("ovdUndFx")) or _nk_num(nk.get("liveUndFx"))
          or _nk_num(frf.get("last")))
    cp, ff = idn.get("cp"), idn.get("fixed_fx") or 1.0
    bt = (row.get("bond_type") or "").lower()
    parity = None
    if cp and spot:
        parity = ((spot / cp * 100.0) if (bt.startswith("vanil") or not fx)
                  else (spot / fx) / (cp / ff) * 100.0)
    sl, sc = _nk_num(srf.get("last")), _nk_num(srf.get("close"))
    return {"secId": sid, "company": idn.get("company", ""),
            "short_name": row.get("short_name") or idn.get("short_name", ""),
            "bond_type": row.get("bond_type", ""), "ric": ric,
            "expiry": idn.get("expiry", ""), "isin": idn.get("isin", ""),
            "sec_fx": idn.get("sec_fx", ""), "und_fx": fxr,
            "n_bid": _nk_num(nk.get("nBid")), "n_gamma": _nk_num(nk.get("nGamma")),
            "n_spread": _nk_num(nk.get("nSpread")), "n_spot": _nk_num(nk.get("nSpot")),
            "n_spotfx": _nk_num(nk.get("nSpotFx")), "n_delta": nd,
            "parityPct": None if parity is None else round(parity, 2),
            "x_bid": row.get("x_bid", ""), "or_bid_sprd": row.get("or_bid_sprd", ""),
            "ovd_bid": _nk_num(nk.get("ovdMktBid")), "ovd_ask": _nk_num(nk.get("ovdMktAsk")),
            "or_ask_sprd": row.get("or_ask_sprd", ""), "x_ask": row.get("x_ask", ""),
            "x_both": row.get("x_both", ""),
            "quote_bid": lv.get("nqb"), "quote_ask": lv.get("nqa"),
            "stk_move": (round((sl / sc - 1) * 100, 2) if (sl and sc) else None),
            "ovdSpot": row.get("ovdSpot", ""), "ovdCbFx": row.get("ovdCbFx", ""),
            "ovdUndFx": row.get("ovdUndFx", ""),
            "live_bid": _nk_num(nk.get("liveMktBid")), "live_ask": _nk_num(nk.get("liveMktAsk")),
            "live_spot": _nk_num(nk.get("liveSpot")), "live_cbfx": _nk_num(nk.get("liveCbFx")),
            "live_undfx": _nk_num(nk.get("liveUndFx")),
            "eod_bid": _nk_num(nk.get("eodMktBid")), "eod_ask": _nk_num(nk.get("eodMktAsk")),
            "eod_spot": _nk_num(nk.get("eodSpot")), "eod_cbfx": _nk_num(nk.get("eodCbFx")),
            "eod_undfx": _nk_num(nk.get("eodUndFx")),
            "stk_last": sl, "stk_time": srf.get("last_time", ""),
            "stk_date": srf.get("last_date", ""), "stk_close": sc,
            "stk_closedt": srf.get("close_date", ""),
            "fx_last": _nk_num(frf.get("last")), "fx_time": frf.get("last_time", ""),
            "fx_date": frf.get("last_date", ""), "fx_close": _nk_num(frf.get("close")),
            "fx_closedt": frf.get("close_date", ""),
            "_spot": spot, "_nd": nd, "_parity": parity}


def _idb_grid_rows(tdate):
    """One row per broker name seen today; Nuke's sections fill in
    once the row is mapped to my short_name (or a SECID)."""
    mod = globals().get("_NUKE_MOD")
    if not mod:
        return [], "nuke module not loaded"
    st = mod.snapshot()
    master = _conv_master().get("by_sid", {})
    board, aliases = _idb_board(tdate)
    _want = []
    for _sid, _rec in master.items():
        _want += [_rec.get("ric") or "", (_rec.get("und_fx") or "").strip()]
    if _IDB_RD["data"] and time.time() - _IDB_RD["ts"] < 600:   # never opens a session here
        rfx, _rfx_src = {**(st.get("rfx") or {}), **_IDB_RD["data"]}, "idb-session (cached)"
    else:
        rfx, _rfx_src = (st.get("rfx") or {}), "nuke cache (IDB session opens on Last/Close)"
    _IDB_RD["last_src"] = _rfx_src
    if _IDB_HIST_CACHE["rev"] != IDB_REV["n"] or _IDB_HIST_CACHE["board"] is None:
        _IDB_HIST_CACHE["board"], _ = _idb_board(None)   # once per IDB write
        _IDB_HIST_CACHE["rev"] = IDB_REV["n"]
    hist = _IDB_HIST_CACHE["board"]          # every name ever seen
    for key, b in hist.items():
        if key not in board:
            b = dict(b); b["hist"] = True
            board[key] = b
    for bk, al in aliases.items():        # mapped names with no quotes yet
        key = al.get("my_short") or bk
        if key not in board and bk not in {v["broker_key"] for v in board.values()}:
            board[key] = {"key": key, "broker_key": bk, "sec_id": al.get("sec_id"),
                          "alias_status": al.get("status", ""), "spellings": [bk],
                          "bid": None, "offer": None, "trade": None, "n": 0,
                          "bid_ref": None, "offer_ref": None, "bid_time": "",
                          "offer_time": "", "last_time": None, "hist": True}
    manual = _idb_manual_marks()
    now_t = (dt.datetime.now().strftime("%H:%M:%S")
             if tdate == dt.date.today().isoformat() else None)
    out = []
    for key, b in sorted(board.items()):
        al = aliases.get(b["broker_key"]) or {}
        sid = _nk_resolve(al)
        _h = bool(b.get("hist"))
        _dm = lambda d: ("%s-%s " % (d[5:7], d[8:10])) if (_h and d) else ""
        r = {"idb_key": key, "broker_key": b["broker_key"], "hist": _h,
             "idb_name": ", ".join(b.get("spellings") or [b["broker_key"]]),
             "my_short": al.get("my_short", ""),
             "map": ("OK" if sid else ("NO MATCH" if al.get("my_short") else "")),
             "alias_status": al.get("status", ""),
             "idb_bid": b.get("bid"), "idb_bref": b.get("bid_ref"),
             "idb_btime": _dm(b.get("bid_date", "")) + (b.get("bid_time") or ""),
             "idb_ask": b.get("offer"), "idb_aref": b.get("offer_ref"),
             "idb_atime": _dm(b.get("offer_date", "")) + (b.get("offer_time") or ""),
             "idb_rb": None, "idb_ra": None, "idb_gap": None, "idb_flag": ""}
        if sid and _h:                    # historic: show Nuke details, no compare
            det = _nk_details(sid, st, master, rfx)
            _idb_ovd_load(); ov = IDB_OVD.get(sid, {})
            det["ovdSpot"], det["ovdCbFx"], det["ovdUndFx"] = (
                ov.get("ovdSpot", ""), ov.get("ovdCbFx", ""), ov.get("ovdUndFx", ""))
            r.update({k: v for k, v in det.items() if not k.startswith("_")})
            ld = b.get("last_date") or ""
            r["idb_flag"] = ("HIST " + ld[5:7] + "-" + ld[8:10]) if ld else "NO QUOTE"
        elif sid:
            det = _nk_details(sid, st, master, rfx)
            _idb_ovd_load()
            ov = IDB_OVD.get(sid, {})
            det["ovdSpot"], det["ovdCbFx"], det["ovdUndFx"] = (
                ov.get("ovdSpot", ""), ov.get("ovdCbFx", ""), ov.get("ovdUndFx", ""))
            nkr = IDB_NUKE.get(sid)
            ref, side, _diff = _idb_ref_for(b)
            cur_inputs = _idb_inputs(sid, st, ov)
            with _IDB_PENDING_LOCK:
                pend = sid in IDB_PENDING
            if nkr and nkr.get("res"):
                res = nkr["res"]; row0 = (st.get("rows") or {}).get(sid) or {}
                qb, qa = _idb_quote_from(res, row0)
                det["ovd_bid"], det["ovd_ask"] = _nk_num(res.get("ovdMktBid")), _nk_num(res.get("ovdMktAsk"))
                det["quote_bid"], det["quote_ask"] = qb, qa
                det["n_delta"] = (_nk_num(res.get("nDelta")) or 0) * 100 if res.get("nDelta") is not None else det["n_delta"]
                stale = pend or (tuple(nkr.get("inputs") or ()) != tuple(float(x) if x is not None else None for x in cur_inputs))
                mark = {"my_bid": qb, "my_offer": qa, "ref": nkr.get("ref"), "repriced": not stale,
                        "stale": stale, "run_spot": nkr.get("ref"), "bref": ref}
            else:
                # never priced by the IDB tab yet: blank result, never Nuke's
                for _k in ("ovd_bid", "ovd_ask", "quote_bid", "quote_ask"):
                    det[_k] = None
                mark = {"my_bid": None, "my_offer": None, "ref": None, "repriced": False,
                        "stale": True, "run_spot": None, "bref": ref}
            c = idb_compare_one(key, b, mark, now_t)      # my bid/offer == quotebid/quoteask, always
            r.update({k: v for k, v in det.items() if not k.startswith("_")})
            r.update({"idb_rb": c["my_bid"], "idb_ra": c["my_offer"], "idb_ref": c["ref_used"],
                      "idb_gap": c["gap"], "idb_flag": c["flags"],
                      "idb_nuked": (nkr or {}).get("ts", "")})
        else:
            sug = _nk_suggest(b["broker_key"], b.get("spellings"), master,
                              (st.get("rows") or {}))
            if sug:
                r["suggest"] = {"secId": sug[0], "short_name": sug[1], "score": sug[2]}
            r["idb_flag"] = ("NO MARK" if not al.get("my_short")
                             else "NO MATCH: " + al["my_short"])
            if _h:
                ld = b.get("last_date") or ""
                r["idb_flag"] += (" | HIST " + ld[5:7] + "-" + ld[8:10]) if ld else " | NO QUOTE"
        out.append(r)
    out.sort(key=lambda x: (0 if "MKT" in (x["idb_flag"] or "") else
                            (1 if (x.get("secId") and not x["hist"]) else
                             (2 if not x["hist"] else 3)), x["idb_key"]))
    return out, None


@app.get("/api/idb/grid")
def api_idb_grid(date: str = ""):
    tdate = (date or dt.date.today().isoformat())[:10]
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "date": tdate, "rows": [], "note": "FILE mode"}
    mod = globals().get("_NUKE_MOD")
    nv = (mod.STATE.get("version") if mod else 0)
    key = (tdate, IDB_REV["n"], nv)
    with _IDB_GRID_LOCK:                      # single-flight: N browsers -> 1 build
        c = _IDB_GRID_CACHE
        if c["rows"] is not None and c["key"] == key and time.time() - c["ts"] < 2.0:
            rows, err = c["rows"], None
        else:
            rows, err = _idb_grid_rows(tdate)
            if err is None:
                c.update({"key": key, "ts": time.time(), "rows": rows})
    meta = (mod.STATE.get("nukeMeta") if mod else None) or {}
    return {"ok": err is None, "error": err, "date": tdate, "rows": rows,
            "nuke_ts": meta.get("ts", ""), "nuke_by": meta.get("by", ""),
            "rfx_src": _IDB_RD.get("last_src", "") + ((" · last error: " + _IDB_RD["err"][:80]) if _IDB_RD.get("err") else "")}


def _nk_write(sid, fields, user):
    """Write override fields into Nuke's shared state (+persist+push),
    with the Vanilla mirror rule Nuke applies itself."""
    mod = globals().get("_NUKE_MOD")
    if not mod:
        raise RuntimeError("nuke module not loaded")
    row = mod.STATE["rows"].setdefault(sid, mod._blank_row())
    touched = []
    for f, v in fields.items():
        if f in NK_OVD:
            row[f] = "" if v is None else str(v)
            touched.append(f)
    if hasattr(mod, "_van_mirror") and mod._van_mirror(row) and "ovdCbFx" not in touched:
        touched.append("ovdCbFx")
    if touched:
        mod.db_upsert_state_fields(sid, row, user, touched)
    return touched


def _nk_push():
    mod = globals().get("_NUKE_MOD")
    lp = getattr(mod, "MAIN_LOOP", None) if mod else None
    if lp:
        import asyncio as _aio
        _aio.run_coroutine_threadsafe(mod.broadcast({
            "type": "snapshot", "state": mod.snapshot(),
            "online": len(mod.CLIENTS)}), lp)


class IdbOvd(BaseModel):
    sec_id: str = ""
    field: str = ""
    value: str = ""


@app.post("/api/idb/ovd")
def api_idb_ovd(req: IdbOvd, request: Request):
    """IDB-tab override (isolated: never touches Nuke Station's state)."""
    user = ((getattr(request.state, "auth", None) or {}).get("user") or "lagrange")
    if req.field not in NK_OVD or not str(req.sec_id).isdigit():
        return JSONResponse(status_code=400, content={"ok": False, "error": "bad field / sec_id"})
    _idb_ovd_set(int(req.sec_id), {req.field: req.value.strip()}, user)
    IDB_REV["n"] += 1
    _idb_mark_pending([int(req.sec_id)])
    r = _idb_run([int(req.sec_id)], user)          # immediate when the engine is free
    return {"ok": True, "renuked": bool(r.get("ok")), "queued": bool(r.get("queued"))}


class IdbFill(BaseModel):
    kind: str = ""          # live | eod | last | close | clear
    sec_ids: List[int] = []


# ---- IDB tab: its own Refinitiv session (never Nuke Station's) ----
_IDB_RD = {"sess": None, "ts": 0.0, "data": {}, "err": "", "hold_until": 0.0, "fails": 0}
_IDB_RD_LOCK = threading.Lock()
IDB_RFX_TTL = 30


def _idb_rd_session():
    """A separate, named desktop session for the IDB tab. Opening it does
    not touch Nuke's default session; a failure here only affects IDB."""
    import refinitiv.data as rd
    with _IDB_RD_LOCK:
        s = _IDB_RD["sess"]
        if s is not None:
            return s
        cfg = None
        for base in (os.getcwd(), os.path.dirname(os.path.abspath(__file__))):
            c = os.path.join(base, "refinitiv-data.config.json")
            if os.path.exists(c):
                cfg = c; break
        if cfg:
            try:
                rd.load_config(cfg)
            except Exception:
                pass
        s = rd.session.desktop.Definition(name="idb-quotes").get_session()
        s.open()
        _IDB_RD["sess"] = s
        return s


def idb_rfx(rics):
    """Last/close/time per RIC from the IDB session (cached IDB_RFX_TTL s,
    exponential back-off on failure). Falls back to Nuke's cached rfx
    READ-ONLY so the tab still works when its own session is down."""
    now = time.time()
    rics = sorted({r for r in rics if r})
    if not rics:
        return {}, "no rics"
    if now - _IDB_RD["ts"] < IDB_RFX_TTL and all(r in _IDB_RD["data"] for r in rics):
        return _IDB_RD["data"], "idb-session (cached)"
    if now < _IDB_RD["hold_until"]:
        src_ = "idb-session backing off %ds: %s" % (int(_IDB_RD["hold_until"] - now), _IDB_RD["err"][:80])
    else:
        try:
            import refinitiv.data as rd
            s = _idb_rd_session()
            df = rd.content.pricing.Definition(universe=rics,
                                               fields=["CF_LAST", "CF_TIME", "CF_DATE", "CF_CLOSE"]
                                               ).get_data(session=s).data.df
            out = {}
            if df is not None and not df.empty:
                cols = {str(c).lower(): c for c in df.columns}
                ic = cols.get("instrument") or df.columns[0]
                for _, row in df.iterrows():
                    out[str(row[ic])] = {"last": _fnum(row.get(cols.get("cf_last"))),
                                         "last_time": str(row.get(cols.get("cf_time")) or ""),
                                         "last_date": str(row.get(cols.get("cf_date")) or ""),
                                         "close": _fnum(row.get(cols.get("cf_close"))),
                                         "close_date": ""}
            if out:
                _IDB_RD.update({"ts": now, "data": {**_IDB_RD["data"], **out}, "err": "", "fails": 0})
                return _IDB_RD["data"], "idb-session"
            raise RuntimeError("empty pricing snapshot")
        except Exception as e:
            _IDB_RD["fails"] += 1
            hold = min(300, 30 * (2 ** min(_IDB_RD["fails"] - 1, 3)))
            _IDB_RD.update({"err": str(e)[:200], "hold_until": now + hold})
            if "502" in str(e) or "Bad Gateway" in str(e) or _IDB_RD["fails"] >= 3:
                try:                          # drop the session so the next try reopens
                    _IDB_RD["sess"].close()
                except Exception:
                    pass
                _IDB_RD["sess"] = None
            src_ = "idb-session failed (%s) - using Nuke cache read-only" % str(e)[:60]
    mod = globals().get("_NUKE_MOD")
    fallback = dict((mod.snapshot().get("rfx") or {})) if mod else {}
    merged = {**fallback, **_IDB_RD["data"]}
    return merged, src_


def _idb_mapped_sids(tdate=None):
    """sec_id -> broker board row for every mapped broker name."""
    board, aliases = _idb_board(tdate or dt.date.today().isoformat())
    hist, _ = _idb_board(None)
    for k, b in hist.items():
        board.setdefault(k, b)
    out = {}
    for key, b in board.items():
        sid = _nk_resolve(aliases.get(b["broker_key"]) or {})
        if sid:
            out[int(sid)] = b
    return out


@app.post("/api/idb/fill")
def api_idb_fill(req: IdbFill, request: Request):
    """Toolbar fills for the IDB tab's OWN overrides (read Nuke's live /
    eod / Refinitiv values, write only IDB_OVD). Same semantics as Nuke's
    buttons; nothing is pushed to Nuke Station."""
    user = ((getattr(request.state, "auth", None) or {}).get("user") or "lagrange")
    mod = globals().get("_NUKE_MOD")
    if not mod:
        return JSONResponse(status_code=503, content={"ok": False, "error": "nuke module not loaded"})
    st = mod.snapshot()
    master = _conv_master().get("by_sid", {})
    mapped = _idb_mapped_sids()
    ids = req.sec_ids or list(mapped.keys())
    want = []
    for sid in ids:
        row = (st.get("rows") or {}).get(sid) or {}
        want += [master.get(sid, {}).get("ric") or ((st.get("nuke") or {}).get(sid) or {}).get("ric") or "",
                 (row.get("und_fx") or master.get(sid, {}).get("und_fx") or "").strip()]
    rfx, rfx_src = (idb_rfx(want) if req.kind in ("last", "close") else (st.get("rfx") or {}, "nuke"))
    n = 0; touched = []
    for sid in ids:
        row = (st.get("rows") or {}).get(sid) or {}
        nk = (st.get("nuke") or {}).get(sid) or {}
        ric = master.get(sid, {}).get("ric") or nk.get("ric") or ""
        fxr = (row.get("und_fx") or master.get(sid, {}).get("und_fx") or "").strip()
        srf, frf = rfx.get(ric) or {}, rfx.get(fxr) or {}
        van = (row.get("bond_type") or "").lower().startswith("vanil")
        if req.kind == "live":
            f = {"ovdSpot": nk.get("liveSpot"), "ovdCbFx": nk.get("liveCbFx"), "ovdUndFx": nk.get("liveUndFx")}
        elif req.kind == "eod":
            f = {"ovdSpot": nk.get("eodSpot"), "ovdCbFx": nk.get("eodCbFx"), "ovdUndFx": nk.get("eodUndFx")}
        elif req.kind in ("last", "close"):
            k = "last" if req.kind == "last" else "close"
            sp, fx = srf.get(k), frf.get(k)
            if sp is None and fx is None:
                continue
            f = {"ovdSpot": sp, "ovdUndFx": fx, "ovdCbFx": (fx if van else nk.get("liveCbFx"))}
        elif req.kind == "clear":
            f = {"ovdSpot": "", "ovdCbFx": "", "ovdUndFx": ""}
        elif req.kind == "bref":
            ref, _side, _d = _idb_ref_for(mapped.get(sid) or {})
            if ref is None:
                continue
            # ovdSpot = the ref used; FX overrides from Nuke's LIVE section
            f = {"ovdSpot": ref, "ovdCbFx": nk.get("liveCbFx"), "ovdUndFx": nk.get("liveUndFx")}
            if van and f.get("ovdUndFx") is not None:
                f["ovdCbFx"] = f["ovdUndFx"]          # vanilla: cbFx follows undFx
        else:
            return JSONResponse(status_code=400, content={"ok": False, "error": "kind?"})
        f = {k: v for k, v in f.items() if v is not None}
        if f:
            _idb_ovd_set(sid, f, user); n += 1; touched.append(sid)
    IDB_REV["n"] += 1
    if touched:
        _idb_mark_pending(touched)
        _idb_run(touched, user)                      # one engine call for all changed rows
    return {"ok": True, "n": n, "src": rfx_src}


IDB_PENDING = set()             # sec_ids whose IDB inputs changed since their last run
_IDB_PENDING_LOCK = threading.Lock()


def _idb_inputs(sid, st, ov=None):
    """Effective IDB inputs for a run: IDB overrides first, Nuke LIVE as
    fallback (read-only), vanilla rule applied. Returns (spot, cb, uf)."""
    ov = ov if ov is not None else IDB_OVD.get(sid, {})
    nk = (st.get("nuke") or {}).get(sid) or {}
    row0 = (st.get("rows") or {}).get(sid) or {}
    van = (row0.get("bond_type") or "").lower().startswith("vanil")
    spot = _nk_num(ov.get("ovdSpot")) or _nk_num(nk.get("liveSpot"))
    uf = _nk_num(ov.get("ovdUndFx")) or _nk_num(nk.get("liveUndFx")) or 0.0
    cb = uf if van else (_nk_num(ov.get("ovdCbFx")) or _nk_num(nk.get("liveCbFx")) or 0.0)
    return spot, cb, uf


def _idb_run(sids, user, at_broker_ref=False, mapped=None):
    """Run Nuke's engine for the given securities with the IDB tab's own
    inputs and keep the results in IDB_NUKE (Nuke Station untouched).
    at_broker_ref=True first sets ovdSpot := broker @ref and FX := LIVE
    (the Re-nuke @ broker REF button); False runs at the inputs as they
    are (auto re-nuke after an edit / fill)."""
    mod = globals().get("_NUKE_MOD")
    if not mod or not hasattr(mod, "run_nuke_batches"):
        return {"ok": False, "error": "nuke engine not available here", "n": 0}
    if not _NUKE_RUN_LOCK.acquire(blocking=False):
        with _IDB_PENDING_LOCK:
            IDB_PENDING.update(int(s) for s in sids)
        return {"ok": False, "error": "a nuke run is already in progress", "n": 0, "queued": True}
    try:
        st = mod.snapshot()
        _idb_ovd_load()
        mapped = mapped if mapped is not None else _idb_mapped_sids()
        entries, used = [], {}
        for sid in sids:
            sid = int(sid)
            b = mapped.get(sid) or {}
            ref, _side, _d = _idb_ref_for(b)
            if at_broker_ref:
                nk = (st.get("nuke") or {}).get(sid) or {}
                row0 = (st.get("rows") or {}).get(sid) or {}
                van = (row0.get("bond_type") or "").lower().startswith("vanil")
                uf0 = _nk_num(nk.get("liveUndFx"))
                cb0 = uf0 if van else _nk_num(nk.get("liveCbFx"))
                fset = {}
                if ref is not None:
                    fset["ovdSpot"] = ref
                if uf0:
                    fset["ovdUndFx"] = uf0
                if cb0:
                    fset["ovdCbFx"] = cb0
                if fset:
                    _idb_ovd_set(sid, fset, user)
            spot, cb, uf = _idb_inputs(sid, st)
            if spot is None:
                continue
            entries.append({"secId": sid, "ovdSpot": float(spot), "ovdCbFx": float(cb), "ovdUndFx": float(uf)})
            used[sid] = {"spot": float(spot), "cb": float(cb), "uf": float(uf), "bref": ref}
        if not entries:
            return {"ok": True, "n": 0, "note": "no rows with a spot to price"}
        data = mod.run_nuke_batches(entries)
        ts = dt.datetime.now().strftime("%H:%M:%S"); n = 0
        for r in data.get("rows", []):
            if r.get("secId") is None:
                continue
            sid = int(r["secId"])
            u = used.get(sid, {})
            IDB_NUKE[sid] = {"ref": u.get("spot"), "inputs": (u.get("spot"), u.get("cb"), u.get("uf")),
                             "bref": u.get("bref"), "res": r, "ts": ts, "by": user}
            with _IDB_PENDING_LOCK:
                IDB_PENDING.discard(sid)
            n += 1
        IDB_REV["n"] += 1
        return {"ok": True, "n": n, "elapsed": data.get("elapsed"), "at_ref": at_broker_ref}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300], "n": 0}
    finally:
        _NUKE_RUN_LOCK.release()


def _idb_mark_pending(sids):
    with _IDB_PENDING_LOCK:
        IDB_PENDING.update(int(s) for s in sids)


def _idb_auto_worker():
    """Every 1.5s: re-nuke (one engine call) every row whose IDB inputs
    changed - typed, filled or set from the broker ref."""
    while True:
        time.sleep(1.5)
        try:
            with _IDB_PENDING_LOCK:
                sids = sorted(IDB_PENDING)
            if not sids or os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
                continue
            _idb_run(sids, "auto")
        except Exception as e:
            print("[idb] auto re-nuke failed:", e)


if os.environ.get("LAGRANGE_TEST_LIVE") != "FILE":
    threading.Thread(target=_idb_auto_worker, daemon=True, name="idb-auto-renuke").start()


@app.post("/api/idb/nuke")
def api_idb_nuke(req: IdbFill, request: Request):
    """Re-nuke @ broker REF: ovdSpot := broker @ref (later side), FX := LIVE,
    then run with the IDB tab's inputs. Nuke Station's state is untouched."""
    user = ((getattr(request.state, "auth", None) or {}).get("user") or "lagrange")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "n": 0}
    mapped = _idb_mapped_sids()
    ids = req.sec_ids or list(mapped.keys())
    out = _idb_run(ids, user, at_broker_ref=True, mapped=mapped)
    if not out.get("ok"):
        return JSONResponse(status_code=(409 if out.get("queued") else 500), content=out)
    return out


class IdbAlias(BaseModel):
    broker_key: str = ""
    sec_id: str = ""
    my_short: str = ""
    note: str = ""
    status: str = ""


@app.post("/api/idb/alias")
def api_idb_alias(req: IdbAlias, request: Request):
    """Map a broker spelling (name+year) to my bond; applied at read time."""
    user = ((getattr(request.state, "auth", None) or {}).get("user") or "lagrange")
    bk = _re.sub(r"[^A-Z0-9]", "", (req.broker_key or "").upper())
    if not bk:
        return JSONResponse(status_code=400, content={"ok": False, "error": "broker key required"})
    sid = None
    if (req.sec_id or "").strip().isdigit():
        sid = int(req.sec_id)
    my_short = (req.my_short or "").strip()
    if sid and not my_short:
        m = _conv_master().get("by_sid", {}).get(sid, {})
        my_short = m.get("short_name") or my_short
    if my_short:
        hit = _nk_short_index().get(_nk_norm(my_short))
        if hit:
            sid = hit
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "broker_key": bk, "sec_id": sid, "my_short": my_short}
    _idb_ensure()
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO cba_app.idb_alias (broker_key, sec_id, my_short, "
                        "note, status, updated_by, updated_at) VALUES (%s,%s,%s,%s,%s,%s,NOW()) "
                        "ON DUPLICATE KEY UPDATE sec_id=VALUES(sec_id), my_short=VALUES(my_short), "
                        "note=VALUES(note), status=VALUES(status), updated_by=VALUES(updated_by), "
                        "updated_at=NOW()",
                        (bk, sid, my_short, (req.note or "")[:120],
                         (req.status or "ASSUMED").upper()[:10], user))
        conn.commit()
    finally:
        conn.close()
    IDB_REV["n"] += 1
    return {"ok": True, "broker_key": bk, "sec_id": sid, "my_short": my_short}


class IdbMark(BaseModel):
    mkey: str = ""
    my_bid: str = ""
    my_offer: str = ""
    spot: str = ""
    delta: str = ""
    parity: str = ""


@app.post("/api/idb/mark")
def api_idb_mark(req: IdbMark, request: Request):
    """Manual mark for a key (fallback when Nuke has no mapping)."""
    user = ((getattr(request.state, "auth", None) or {}).get("user") or "lagrange")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    _idb_ensure()
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO cba_app.idb_marks (mkey, my_bid, my_offer, spot, "
                        "delta, parity, updated_by, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) "
                        "ON DUPLICATE KEY UPDATE my_bid=VALUES(my_bid), my_offer=VALUES(my_offer), "
                        "spot=VALUES(spot), delta=VALUES(delta), parity=VALUES(parity), "
                        "updated_by=VALUES(updated_by), updated_at=NOW()",
                        (req.mkey.strip(), _fnum(req.my_bid), _fnum(req.my_offer),
                         _fnum(req.spot), _fnum(req.delta), _fnum(req.parity), user))
        conn.commit()
    finally:
        conn.close()
    IDB_REV["n"] += 1
    return {"ok": True}


@app.get("/api/dscan/status")
def api_dscan_status():
    return {"ok": _DSCAN["ok"], "note": _DSCAN["note"],
            "path": _DSCAN["path"]}


@app.get("/api/risk/latest")
def api_risk_latest():
    """Latest risk_positions: newest snapshot_date, then the
    freshest loaded_at batch within it, where every row whose
    loaded_at falls inside a 60-second buffer of the max
    counts as the same load."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "cols": [], "rows": [],
                "snap": "", "loaded": "", "table": "FILE"}
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT TABLE_SCHEMA FROM "
                    "information_schema.tables WHERE "
                    "TABLE_NAME='risk_positions' LIMIT 1")
                h = cur.fetchone()
                if not h:
                    return {"ok": False, "error":
                            "risk_positions table not found "
                            "in any schema"}
                tbl = "`%s`.`risk_positions`" % h[0]
                cur.execute("SELECT MAX(snapshot_date) "
                            "FROM " + tbl)
                snap = (cur.fetchone() or [None])[0]
                if snap is None:
                    return {"ok": False,
                            "error": "risk_positions is empty"}
                cur.execute("SELECT MAX(loaded_at) FROM " + tbl
                            + " WHERE snapshot_date=%s",
                            (snap,))
                mload = (cur.fetchone() or [None])[0]
                cur.execute(
                    "SELECT * FROM " + tbl +
                    " WHERE snapshot_date=%s AND "
                    "loaded_at >= (%s - INTERVAL 60 SECOND)",
                    (snap, mload))
                cols = [d[0] for d in cur.description]
                rows = [["" if v is None else str(v)
                         for v in r] for r in cur.fetchall()]
            return {"ok": True, "cols": cols, "rows": rows,
                    "snap": str(snap), "loaded": str(mload),
                    "table": h[0] + ".risk_positions"}
        finally:
            conn.close()
    except Exception as e:
        return {"ok": False, "error": str(e)}


class NkEdit(BaseModel):
    sec_id: str = ""
    field: str = ""
    value: str = ""


@app.post("/api/rfq/nkedit")
def api_rfq_nkedit(req: NkEdit, request: Request):
    """Edit a nuke-state field (x/or spreads) from the RFQ
    grid: shared STATE + cb_state persist + live push to every
    Nuke window."""
    user = ((getattr(request.state, "auth", None) or {})
            .get("user") or "trader")
    _F = ("x_bid", "x_ask", "x_both", "or_bid_sprd",
          "or_ask_sprd")
    if req.field not in _F:
        return JSONResponse(status_code=400, content={
            "ok": False, "error": "bad field"})
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    try:
        sid = int(req.sec_id)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={
            "ok": False, "error": "bad sec_id"})
    if _NUKE_MOD is None:
        return JSONResponse(status_code=503, content={
            "ok": False, "error": "nuke module not loaded"})
    try:
        row = _NUKE_MOD.STATE["rows"].setdefault(
            sid, _NUKE_MOD._blank_row())
        row[req.field] = req.value
        _NUKE_MOD.db_upsert_state_fields(sid, row, user,
                                         [req.field])
        lp = getattr(_NUKE_MOD, "MAIN_LOOP", None)
        if lp:
            import asyncio as _aio
            _aio.run_coroutine_threadsafe(
                _NUKE_MOD.broadcast({
                    "type": "snapshot",
                    "state": _NUKE_MOD.snapshot(),
                    "online": len(_NUKE_MOD.CLIENTS)}), lp)
        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": str(e)})


class ImpReq2(BaseModel):
    rfq_id: str = ""


def _rfq_log(cur, rfq_id, user, note):
    """Audit line in cba_app.rfq_log (field_name carries the
    event, new_value the note)."""
    try:
        cur.execute(
            "INSERT INTO cba_app.rfq_log (rfq_id, field_name, "
            "old_value, new_value, changed_by, changed_at) "
            "VALUES (%s,%s,NULL,%s,%s,NOW())",
            (rfq_id, "event", str(note)[:200], user))
    except Exception:
        pass



@app.post("/api/rfq/impreq")
def api_rfq_impreq(req: ImpReq2, request: Request):
    user = ((getattr(request.state, "auth", None) or {})
            .get("user") or "sales")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT sides, ord_level, ord_level2 FROM cba_app.rfq WHERE rfq_id=%s", (req.rfq_id,))
                h = cur.fetchone()
                if not h:
                    return JSONResponse(status_code=404, content={"ok": False, "error": "no such RFQ"})
                sides, l1, l2 = (h[0] or "two_way"), h[1], h[2]
                cur.execute("SELECT status FROM cba_app.rfq "
                            "WHERE rfq_id=%s", (req.rfq_id,))
                _st0 = (cur.fetchone() or [""])[0]
                if _st0 not in ("QUOTED", "WORKING"):
                    return JSONResponse(status_code=409, content={
                        "ok": False, "error": "improve is only available while QUOTED or WORKING"})
                miss = []
                if sides == "two_way":
                    if l1 is None and l2 is None:
                        miss.append("bid level or ask level")
                elif sides == "bid":
                    if l1 is None:
                        miss.append("bid level")
                else:
                    if l2 is None:
                        miss.append("ask level")
                if miss:
                    return JSONResponse(status_code=400, content={"ok": False, "miss": miss, "error": "improve needs: " + ", ".join(miss)})
                cur.execute("UPDATE cba_app.rfq SET status='IMPROVE', adj_req=1, last_updated=NOW(), updated_by=%s, row_version=row_version+1 WHERE rfq_id=%s", (user, req.rfq_id))
                _rfq_log(cur, req.rfq_id, user, "improve requested")
            conn.commit()
            return {"ok": True}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.post("/api/rfq/match")
def api_rfq_match(req: ImpReq2, request: Request):
    user = ((getattr(request.state, "auth", None) or {})
            .get("user") or "trader")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT ord_level, ord_level2, q_rev, ord_side FROM cba_app.rfq WHERE rfq_id=%s", (req.rfq_id,))
                h = cur.fetchone()
                if not h:
                    return JSONResponse(status_code=404, content={"ok": False, "error": "no such RFQ"})
                l1, l2, rev, ords = h[0], h[1], int(h[2] or 0) + 1, h[3]
                if l1 is None and l2 is None:
                    return JSONResponse(status_code=409, content={"ok": False, "error": "no improve terms to match"})
                _st = "WORKING" if ords else "QUOTED"
                cur.execute("UPDATE cba_app.rfq SET bid_px=COALESCE(%s, bid_px), ask_px=COALESCE(%s, ask_px), bid_at=NOW(), ask_at=NOW(), q_rev=%s, status=%s, adj_req=NULL, off_flag=NULL, off_by=NULL, off_at=NULL, last_updated=NOW(), updated_by=%s, row_version=row_version+1 WHERE rfq_id=%s", (l1, l2, rev, _st, user, req.rfq_id))
                cur.execute("INSERT INTO cba_app.rfq_quote_hist (rfq_id, rev, bid, ask, quoted_by, quoted_at, action) VALUES (%s,%s,%s,%s,%s,NOW(),'match')", (req.rfq_id, rev, l1, l2, user))
            conn.commit()
            return {"ok": True}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


class AdjReq(BaseModel):
    rfq_id: str = ""
    on: bool = True


@app.post("/api/rfq/adjreq")
def api_rfq_adjreq(req: AdjReq, request: Request):
    user = ((getattr(request.state, "auth", None) or {})
            .get("user") or "sales")
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True}
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE cba_app.rfq SET adj_req=%s, "
                            "last_updated=NOW(), updated_by=%s, "
                            "row_version=row_version+1 WHERE rfq_id=%s",
                            (1 if req.on else None, user, req.rfq_id))
                _rfq_log(cur, req.rfq_id, user,
                         "adj-req " + ("on" if req.on else "off"))
            conn.commit()
            return {"ok": True}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


class TwcbSendReq(BaseModel):
    to: str = ""
    cc: str = ""
    send: bool = False


@app.post("/api/twcb/send")
def api_twcb_send(req: TwcbSendReq):
    if not _LAST_TWCB["html"]:
        return JSONResponse(status_code=400, content={
            "ok": False,
            "error": "No TW CB report built yet - press Refresh first."})
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return {"ok": True, "message": "FILE mode: no Outlook here (draft prepared)", "subject": _LAST_TWCB["subject"]}
    try:
        import pythoncom
        pythoncom.CoInitialize()
        com_inited = True
    except ImportError:
        com_inited = False
    try:
        ok, msg = em.send_outlook(_LAST_TWCB["subject"],
                                  _LAST_TWCB["html"],
                                  req.to, req.cc, req.send)
    finally:
        if com_inited:
            pythoncom.CoUninitialize()
    return {"ok": ok, "message": msg,
            "subject": _LAST_TWCB["subject"]}


class DeltaSendReq(BaseModel):
    to: str = ""
    cc: str = ""
    send: bool = False


class DeltaRefreshReq(BaseModel):
    update_db: bool = False


@app.post("/api/delta/refresh")
def api_delta_refresh(req: DeltaRefreshReq = DeltaRefreshReq()):
    with _LOCK:
        upd_log = ""
        stale_warn = ""
        prev_snap = _latest_snap_ts() if req.update_db else None
        if req.update_db:
            ok, upd_log = _run_db_updater()
            if not ok:
                return JSONResponse(status_code=500, content={
                    "ok": False,
                    "error": "DB update failed - check NOT run "
                             "(avoid stale-data checks). Run plain Refresh "
                             "deliberately if you want the current DB state.",
                    "log": upd_log})
        try:
            dcmod = _import_delta()
        except Exception as e:
            return JSONResponse(status_code=500, content={
                "ok": False,
                "error": "delta_check_recovered.py not importable: %r" % e})
        try:
            df = dcmod.get_cba_delta_check(dcmod.CBA_SEC_IDS)
        except Exception as e:
            return JSONResponse(status_code=500,
                                content={"ok": False, "error": repr(e)})
        if df is None or len(df) == 0:
            return JSONResponse(status_code=400, content={
                "ok": False,
                "error": "CBA delta check returned no rows - check "
                         "cbanalytics tables / snap availability."})
        if req.update_db:
            new_snap = _latest_snap_ts()
            if prev_snap is not None and new_snap is not None \
                    and new_snap <= prev_snap:
                stale_warn = ("[warning] DB update wrote NO new snap - "
                              "API likely unavailable (weekend/holiday?). "
                              "Results below reflect the last good snap.")
                upd_log = (upd_log + "\n" + stale_warn).strip()

        html = dcmod.build_email_html(df)
        if "flag_reason" in df.columns:
            flagged = int((df["flag_reason"].fillna("").astype(str)
                           .str.strip() != "").sum())
        else:
            flagged = int((df["flag_delta"].fillna("").astype(str)
                           .str.strip() == "Y").sum())
        subject = ("CB Runs - Delta and Price Check (CBA) %s"
                   % dt.datetime.now().strftime("%Y/%m/%d %H:%M"))
        _LAST_DELTA.update(subject=subject, html=html,
                           built_at=dt.datetime.now().strftime("%H:%M:%S"))
        return {"ok": True, "rows": int(len(df)), "flagged": flagged,
                "subject": subject, "html": html, "log": upd_log,
                "stale": bool(stale_warn),
                "data_as_of": _df_data_as_of(df),
                "built_at": _LAST_DELTA["built_at"]}


@app.post("/api/delta/send")
def api_delta_send(req: DeltaSendReq):
    with _LOCK:
        if not _LAST_DELTA["html"]:
            return JSONResponse(status_code=400, content={
                "ok": False,
                "error": "No delta report built yet - press Refresh first."})
        subject, html = _LAST_DELTA["subject"], _LAST_DELTA["html"]
    try:
        import pythoncom
        pythoncom.CoInitialize()
        com_inited = True
    except ImportError:
        com_inited = False
    try:
        ok, msg = em.send_outlook(subject, html, req.to, req.cc, req.send)
    finally:
        if com_inited:
            pythoncom.CoUninitialize()
    return {"ok": ok, "message": msg, "subject": subject}




def _force_foreground(dcmod, hwnd):
    """Bring hwnd to foreground despite Windows focus-stealing rules.
    Tries: plain call -> Alt-keypress trick -> AttachThreadInput ->
    minimize/restore jolt. Returns True only if hwnd IS the foreground
    window afterwards (verified, so keystrokes cannot misfire)."""
    def _is_fg():
        try:
            return dcmod.win32gui.GetForegroundWindow() == hwnd
        except Exception:
            return False

    # attempt 0: plain
    try:
        dcmod.win32gui.SetForegroundWindow(hwnd)
    except Exception:
        pass
    if _is_fg():
        return True

    # attempt 1: Alt keypress grants SetForegroundWindow rights
    try:
        dcmod.pyautogui.press("alt")
        dcmod.time.sleep(0.1)
        dcmod.win32gui.SetForegroundWindow(hwnd)
    except Exception:
        pass
    if _is_fg():
        return True

    # attempt 2: AttachThreadInput to the current foreground thread
    try:
        import ctypes
        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32
        fg = user32.GetForegroundWindow()
        fg_tid = user32.GetWindowThreadProcessId(fg, None)
        cur_tid = kernel32.GetCurrentThreadId()
        if fg_tid and fg_tid != cur_tid:
            user32.AttachThreadInput(cur_tid, fg_tid, True)
            try:
                user32.BringWindowToTop(hwnd)
                dcmod.win32gui.SetForegroundWindow(hwnd)
            finally:
                user32.AttachThreadInput(cur_tid, fg_tid, False)
    except Exception:
        pass
    if _is_fg():
        return True

    # attempt 3: minimize/restore jolt
    try:
        dcmod.win32gui.ShowWindow(hwnd, dcmod.win32con.SW_MINIMIZE)
        dcmod.time.sleep(0.3)
        dcmod.win32gui.ShowWindow(hwnd, dcmod.win32con.SW_RESTORE)
        dcmod.time.sleep(0.3)
        dcmod.win32gui.SetForegroundWindow(hwnd)
    except Exception:
        pass
    return _is_fg()


def _scrape_derivation(dcmod):
    """Web-safe automated grab from the Derivation window.
    Forces foreground through Windows focus-stealing restrictions and
    VERIFIES focus before sending any keystroke. On any failure returns
    (None, reason) instead of opening Tk dialogs."""
    try:
        hwnd = dcmod.win32gui.FindWindow(None, DERIV_WINDOW_TITLE)
        matched = DERIV_WINDOW_TITLE if hwnd else None
        if not hwnd:
            # prefix fallback: survive version-string changes
            cands = []

            def _enum(h, _):
                try:
                    if dcmod.win32gui.IsWindowVisible(h):
                        t = dcmod.win32gui.GetWindowText(h)
                        if t.startswith(DERIV_TITLE_PREFIX):
                            cands.append((h, t))
                except Exception:
                    pass
                return True

            dcmod.win32gui.EnumWindows(_enum, None)
            if cands:
                hwnd, matched = cands[0]
    except Exception as e:
        return None, "win32gui unavailable: %r" % e
    if not hwnd:
        return None, ("Derivation window not found (looked for exact "
                      "title '%s' and any window starting with '%s'). "
                      "Use the paste box instead."
                      % (DERIV_WINDOW_TITLE, DERIV_TITLE_PREFIX))
    print("[grab] using window: %s" % matched)
    try:
        dcmod.win32gui.ShowWindow(hwnd, dcmod.win32con.SW_RESTORE)
        dcmod.time.sleep(0.5)
        if not _force_foreground(dcmod, hwnd):
            return None, ("Could not bring the Derivation window to the "
                          "foreground (Windows focus rules). Click the "
                          "Derivation window once yourself, then either "
                          "retry auto-grab immediately or just Ctrl+A / "
                          "Ctrl+C and use the paste box.")
        def _copy_rows(with_click_expand):
            """One grab attempt. Returns (df_or_None, rows, note)."""
            if dcmod.win32gui.GetForegroundWindow() != hwnd:
                if not _force_foreground(dcmod, hwnd):
                    return None, 0, "lost foreground"
            if with_click_expand:
                # click INTO the grid (read-only Name column, below header)
                l, t, r_, b_ = dcmod.win32gui.GetWindowRect(hwnd)
                cx = l + min(150, max(60, (r_ - l) // 8))
                cy = t + 230
                dcmod.pyautogui.click(cx, cy); dcmod.time.sleep(0.5)
                if dcmod.win32gui.GetForegroundWindow() != hwnd:
                    return None, 0, "lost foreground after click"
            dcmod.pyperclip.copy("")
            dcmod.pyautogui.hotkey('ctrl', 'a'); dcmod.time.sleep(1)
            if with_click_expand:
                dcmod.pyautogui.hotkey('ctrl', 'add'); dcmod.time.sleep(1)
            dcmod.pyautogui.hotkey('ctrl', 'c'); dcmod.time.sleep(5)
            clip = dcmod.pyperclip.paste()
            if not clip or len(clip.strip()) < 50:
                return None, 0, "clipboard empty"
            import pandas as _pd, io as _io
            try:
                d = _pd.read_csv(_io.StringIO(clip), sep="\t")
            except Exception as e:
                return None, 0, "parse failed: %r" % e
            return d, len(d), "ok"

        data, n1, note1 = _copy_rows(with_click_expand=True)
        print("[grab] click+expand sequence: %d rows (%s)" % (n1, note1))
        if n1 < DERIV_MIN_ROWS:
            data2, n2, note2 = _copy_rows(with_click_expand=False)
            print("[grab] simple sequence retry: %d rows (%s)" % (n2, note2))
            if n2 > n1:
                data, n1 = data2, n2
        if data is None or n1 == 0:
            return None, ("Both grab sequences failed (%s / see log). "
                          "Use the paste box instead." % note1)
        dcmod.win32gui.ShowWindow(hwnd, dcmod.win32con.SW_MINIMIZE)
        if data.empty:
            return None, "Grabbed data parsed to an empty table."
        print("[grab] columns: %s" % list(data.columns)[:15])
        return data, "auto-grab ok: %d rows" % len(data)
    except Exception as e:
        return None, ("Automated grab failed (%r). "
                      "Use the paste box instead." % e)


def run_full_delta_pipeline(dcmod, derivation_df):
    """Run the notebook __main__ flow (minus popup/auto-email) and return
    the combined derivation+CBA DataFrame. Mirrors delta_check_recovered's
    main cell; that cell remains the source of truth."""
    import pandas as pd
    import numpy as np

    dcmod.derivation_underlying_map = {}
    booking_cb = "4. Afternoon (All Regions)"
    print(f"Run mode: {booking_cb}")

    dcmod.data = derivation_df

    (dcmod.df, index_cn_hk_dict, index_tw_dict,
     index_kr_dict, index_jp_dict) = dcmod.get_cb_analyzer()
    dcmod.usdkrw, usdjpy = dcmod.get_fx_currency()

    col_num = 20  # FIXME(OCR): mirror of the notebook's unverified value
    num_range = range(2, col_num + 2)
    header_row = dcmod.df.iloc[1, 1:col_num].tolist()
    dcmod.header_row_map = dict(zip(num_range, header_row))
    dcmod.header_row_map.update({col_num + 1: "IVDelta",
                                 col_num + 2: "Delta_Now"})

    dcmod.derivation_map = {
        "ISIN": "Isin", "Bid": "Citi Bid", "Offer": "Citi Ask",
        "VS": "Last Price", "Delta": "HousePriceDelta",
        "IVDelta": "IVDelta", "Delta_Now": "Delta",
    }

    dcmod.full_runs_dict = {}
    dcmod.process_derivation_data(index_cn_hk_dict, "CN")
    dcmod.process_derivation_data(index_tw_dict,    "TW")
    dcmod.process_derivation_data(index_kr_dict,    "KR")
    dcmod.process_derivation_data(index_jp_dict,    "JP")

    runs_df = pd.DataFrame(dcmod.full_runs_dict).T
    runs_df = dcmod.get_security_data(runs_df)

    eqrms_df = pd.read_csv(
        r"\\apacdfs\HK\MKT\GROUPS\futures\CB\KK\Main Book by ACCTS.txt",
        sep="\t")  # FIXME(OCR): filename mirror
    runs_df = dcmod.process_quantities_and_currency(runs_df, eqrms_df,
                                                    dcmod.usdkrw)

    country_map = {"KR": "South Korea", "JP": "Japan",
                   "CN": "China / Hong Kong", "TW": "Taiwan"}
    country_runs = {
        country_map[c]: runs_df[runs_df["Country"] == c].sort_values("Name")
        for c in runs_df["Country"].unique()
    }
    combined_df = dcmod.combine_country_runs(country_runs, dcmod.usdkrw)

    final_df = dcmod.post_process(combined_df, booking_cb)
    final_df = dcmod.get_delta_qlx(final_df)
    final_df = dcmod.compute_delta_checks(final_df)

    output_cols = ["sec_id", "company_name", "expiry_date", "eqrms_snap_ts",
                   "eqrms_delta", "nuked_delta", "eqrms_vs_nuked",
                   "eqrms_fair_price", "nuked_mkt_price", "px_vs_nuked",
                   "flag_delta", "flag_price", "flag_reason"]
    output_cols = [c for c in output_cols if c in final_df.columns]
    derivation_out = final_df[output_cols].copy()
    derivation_out = derivation_out.dropna(subset=["sec_id"]) \
                                   .reset_index(drop=True)
    derivation_out.insert(0, "source_pricing", "derivation")
    derivation_out = dcmod.enrich_derivation_prices(derivation_out)

    cba_out = dcmod.get_cba_delta_check(dcmod.CBA_SEC_IDS)

    final_output_cols = ["source_pricing", "sec_id", "company_name",
                         "expiry_date", "eqrms_snap_ts", "eqrms_delta",
                         "nuked_delta", "eqrms_vs_nuked",
                         "eqrms_fair_price", "nuked_mkt_price", "px_vs_nuked",
                         "flag_delta", "flag_price", "flag_reason"]
    for col in final_output_cols:
        if col not in cba_out.columns:        cba_out[col] = np.nan
        if col not in derivation_out.columns: derivation_out[col] = np.nan
    cba_out        = cba_out[final_output_cols]
    derivation_out = derivation_out[final_output_cols]

    derivation_out["_sec_id_int"] = pd.to_numeric(
        derivation_out["sec_id"], errors="coerce").round(0).astype("Int64")
    cba_ids = set(pd.to_numeric(cba_out["sec_id"], errors="coerce")
                  .dropna().round(0).astype(int).tolist())
    derivation_out = derivation_out[
        ~derivation_out["_sec_id_int"].isin(cba_ids)
    ].drop(columns=["_sec_id_int"]).reset_index(drop=True)

    combined_output = pd.concat([derivation_out, cba_out],
                                ignore_index=True)
    print(f"combined output: {len(combined_output)} rows "
          f"({len(derivation_out)} derivation + {len(cba_out)} cba)")
    return combined_output


class DeltaFullReq(BaseModel):
    source: str = "paste"        # paste | auto
    pasted: str = ""
    update_db: bool = False


@app.post("/api/delta/full")
def api_delta_full(req: DeltaFullReq):
    with _LOCK:
        try:
            dcmod = _import_delta()
        except Exception as e:
            return JSONResponse(status_code=500, content={
                "ok": False,
                "error": "delta_check_recovered.py not importable: %r" % e})

        import pandas as pd
        log = io.StringIO()
        stale = False
        prev_snap = _latest_snap_ts() if req.update_db else None
        if req.update_db:
            ok, upd_log = _run_db_updater()
            print(upd_log, file=log)
            if not ok:
                return JSONResponse(status_code=500, content={
                    "ok": False,
                    "error": "DB update failed - pipeline NOT run "
                             "(avoid stale-data checks).",
                    "log": log.getvalue()})
        # -- acquire derivation data --
        if req.source == "auto":
            with contextlib.redirect_stdout(log):
                deriv_df, msg = _scrape_derivation(dcmod)
            print("[grab]", msg, file=log)
            if deriv_df is None:
                return JSONResponse(status_code=400, content={
                    "ok": False, "error": msg, "log": log.getvalue()})
        else:
            if not req.pasted.strip():
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": "Paste box is empty - copy the Derivation "
                             "grid (Ctrl+A, Ctrl+C) and paste it here."})
            try:
                deriv_df = pd.read_csv(io.StringIO(req.pasted), sep="\t")
            except Exception as e:
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": "Could not parse pasted data as "
                             "tab-separated: %r" % e})
            if deriv_df.empty:
                return JSONResponse(status_code=400, content={
                    "ok": False, "error": "Pasted data parsed to an "
                                          "empty table."})
            print("[paste] %d rows parsed" % len(deriv_df), file=log)

        # -- run the pipeline with captured output --
        try:
            with contextlib.redirect_stdout(log), \
                 contextlib.redirect_stderr(log):
                combined = run_full_delta_pipeline(dcmod, deriv_df)
        except Exception as e:
            import traceback
            tb = "\n".join(traceback.format_exc().splitlines()[-12:])
            print("--- traceback (tail) ---\n" + tb, file=log)
            return JSONResponse(status_code=500, content={
                "ok": False, "error": "pipeline failed: %r" % e,
                "log": log.getvalue()})

        if combined is None or len(combined) == 0:
            return JSONResponse(status_code=400, content={
                "ok": False, "error": "pipeline produced no rows",
                "log": log.getvalue()})

        if req.update_db:
            new_snap = _latest_snap_ts()
            if prev_snap is not None and new_snap is not None \
                    and new_snap <= prev_snap:
                stale = True
                print("[warning] DB update wrote NO new snap - results "
                      "reflect the last good snap.", file=log)

        html = dcmod.build_email_html(combined)
        if "flag_reason" in combined.columns:
            flagged = int((combined["flag_reason"].fillna("").astype(str)
                           .str.strip() != "").sum())
        else:
            flagged = int((combined["flag_delta"].fillna("").astype(str)
                           .str.strip() == "Y").sum())
        subject = ("CB Runs - Delta and Price Check (derivation + CBA) %s"
                   % dt.datetime.now().strftime("%Y/%m/%d %H:%M"))
        _LAST_DELTA.update(subject=subject, html=html,
                           built_at=dt.datetime.now().strftime("%H:%M:%S"))
        return {"ok": True, "rows": int(len(combined)), "flagged": flagged,
                "subject": subject, "html": html,
                "log": log.getvalue(), "stale": stale,
                "data_as_of": _df_data_as_of(combined),
                "built_at": _LAST_DELTA["built_at"]}




@app.get("/api/nuke/status")
def api_nuke_status():
    if NUKE_EMBED:
        return {"ok": True, "up": _NUKE_MOD is not None, "url": "/nuke/",
                "mode": "embedded", "autostart": False, "child": None,
                "build": LAGRANGE_BUILD, "note": _NUKE_NOTE}
    """Health-check the Nuke Station server so the tab can embed or
    explain. Uses stdlib urllib; 2s timeout."""
    import urllib.request
    try:
        with urllib.request.urlopen(NUKE_URL, timeout=2) as r:
            up = 200 <= r.status < 500
    except Exception:
        up = False
    note = _NUKE_NOTE
    if _NUKE_PROC is not None and _NUKE_PROC.poll() is not None:
        note = ("child EXITED with code %s - see nuke_station_console.log "
                "next to app.py" % _NUKE_PROC.returncode)
    child = (None if _NUKE_PROC is None
             else ("running" if _NUKE_PROC.poll() is None else "exited"))
    return {"ok": True, "up": up, "url": NUKE_URL,
            "autostart": NUKE_AUTOSTART, "child": child, "note": note}


@app.post("/api/nuke/start")
def api_nuke_start():
    """Connect-button path: (re)attempt start/embed, then report."""
    if NUKE_EMBED:
        if _NUKE_MOD is None:
            return JSONResponse(status_code=500, content={
                "ok": False, "up": False, "url": "/nuke/",
                "note": _NUKE_NOTE + " - fix and RESTART Lagrange "
                        "(embedding happens at startup)"})
        return api_nuke_status()
    _maybe_start_nuke()
    return api_nuke_status()


# ----------------------------------------------------------------------
# Trade Blotter tab (read + limited edit of eqrms.trade_blotter)
# Integration is DB-only: the blotter app runs separately; Lagrange never
# launches, imports, or calls it. Concurrency uses the blotter's own
# row_version counter; audits go to the blotter's own trade_blotter_audit
# so Lagrange edits appear in its Activity Log.
# ----------------------------------------------------------------------
BLOTTER_DB = os.environ.get("BLOTTER_DB", "trade_blotter")
BLOTTER_TABLE = "trade_blotter"
# Logical fields -> candidate physical column names, tried in order.
# The tab introspects SHOW COLUMNS once and adapts, so it survives the
# schema differences between main.py's model and the migrated table.
BLOTTER_CANDIDATES = {
    "trade_id":        ["trade_id", "id"],
    "trade_date":      ["trade_date"],
    "client_side":     ["client_side", "side"],
    "isin":            ["isin"],
    "bond_name":       ["bond_name"],
    "bond_type":       ["bond_type"],
    "bond_currency":   ["bond_currency", "bond_ccy", "currency", "ccy"],
    "fx_rate":         ["fx_rate"],
    "quantity":        ["quantity", "qty"],
    "price":           ["price"],
    "client_name":     ["client_name", "client"],
    "client_type":     ["client_type", "clienttype", "client_typ",
                        "cust_type"],
    "client_account":  ["client_account", "client_acct", "account", "acct"],
    "sales":           ["sales", "sales_person", "salesperson"],
    "trade_type":      ["trade_type"],
    "stock_ref":       ["stock_ref"],
    "fx_ref":          ["fx_ref"],
    "bond_fx_ref":     ["bond_fx_ref"],
    "stock_quantity":  ["stock_quantity", "stock_qty"],
    "delta":           ["delta"],
    "parity":          ["parity"],
    "bond_usd_settlement":  ["bond_usd_settlement", "bond_usd_settle"],
    "stock_usd_settlement": ["stock_usd_settlement", "stock_usd_settle"],
    "bond_settlement_ccy":  ["bond_settlement_ccy", "bond_settle_ccy"],
    "stock_settlement_ccy": ["stock_settlement_ccy", "stock_settle_ccy"],
    "working_stock_instruction": ["working_stock_instruction",
                                  "ws_instruction"],
    "working_stock_start":  ["working_stock_start", "ws_start"],
    "working_stock_end":    ["working_stock_end", "ws_end"],
    "working_fx_instruction": ["working_fx_instruction", "fx_instruction"],
    "working_fx_time":      ["working_fx_time", "fx_time"],
    "settlement_date": ["settlement_date", "settle_date", "settlement",
                        "value_date"],
    "trader_agree":    ["trader_agree", "trader", "trader_agreed",
                        "trader_ok"],
    "booked":          ["booked", "is_booked", "booked_flag"],
    "hedged_delta":    ["hedged_delta"],
    "hedged_fx":       ["hedged_fx"],
    "hedged_vol":      ["hedged_vol"],
    "hedged_credit":   ["hedged_credit"],
    "hedged_rates":    ["hedged_rates"],
    "internal_acct":   ["internal_acct", "internal_account", "int_acct"],
    "citi_give_up_stocks": ["citi_give_up_stocks", "citi_give_up",
                            "citi_gu"],
    "other_comments":  ["other_comments", "comments", "comment"],
    "cross_flag":      ["cross_flag", "cross", "is_cross"],
    "cross_quantity":  ["cross_quantity", "cross_qty"],
    "last_updated":    ["last_updated", "updated_at"],
    "updated_by":      ["updated_by", "update_by", "updated"],
    "row_version":     ["row_version"],
}
# main.py's rule: everything is editable EXCEPT identity/server-managed
BLOTTER_NON_EDITABLE = {"trade_id", "last_updated", "updated_by",
                        "row_version"}
BLOTTER_EDITABLE = [k for k in BLOTTER_CANDIDATES
                    if k not in BLOTTER_NON_EDITABLE]

# ---- transcribed from the blotter's main.py (types + rules) ------------
import re as _re
from decimal import Decimal as _Dec, InvalidOperation as _DecErr
from datetime import date as _date, datetime as _dt, timedelta as _td

BL_BOOL = {"bond_usd_settlement", "stock_usd_settlement", "booked",
           "citi_give_up_stocks", "cross_flag"}
BL_NUM = {"fx_rate", "quantity", "price", "stock_ref", "fx_ref",
          "bond_fx_ref", "stock_quantity", "delta", "parity",
          "cross_quantity"}
BL_DATE = {"trade_date"}                    # settlement_date is special
BL_TIME = {"working_stock_start", "working_stock_end", "working_fx_time"}
BL_HEDGE = {"hedged_delta", "hedged_fx", "hedged_vol", "hedged_credit",
            "hedged_rates"}
BL_HEDGE_STATES = {"N/A", "Open", "Done"}
BL_MAXLEN = {"client_side": 10, "isin": 12, "bond_name": 100,
             "bond_type": 50, "bond_currency": 10, "client_name": 100,
             "client_type": 50, "sales": 50, "trade_type": 50,
             "bond_settlement_ccy": 10, "stock_settlement_ccy": 10,
             "working_stock_start": 20, "working_stock_end": 20,
             "working_fx_time": 20, "trader_agree": 50, "internal_acct": 50,
             "client_account": 50}
BL_MAXLEN.update({h: 8 for h in BL_HEDGE})
BL_TPLUS = _re.compile(r"^[tT]\s*\+?\s*(-?\d+)$")
BL_DATE_FMTS = ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y")
# trade_field -> (map_table, key_col, {trade_col: map_col})
BL_AUTOFILL = {
    "isin": ("bond_mappings", "isin",
             {"bond_name": "bond_name", "bond_type": "bond_type",
              "bond_currency": "bond_currency", "fx_rate": "fx_rate"}),
    "bond_name": ("bond_mappings", "bond_name",
                  {"isin": "isin", "bond_type": "bond_type",
                   "bond_currency": "bond_currency", "fx_rate": "fx_rate"}),
    "client_name": ("client_mappings", "client_name",
                    {"client_type": "client_type",
                     "client_account": "client_account"}),
}


class BLVal(Exception):
    pass


def _bl_parse_date(v):
    if isinstance(v, _dt):
        return v.date()
    if isinstance(v, _date):
        return v
    sv = str(v).strip()
    for f in BL_DATE_FMTS:
        try:
            return _dt.strptime(sv, f).date()
        except ValueError:
            continue
    raise BLVal(f"Invalid date: {v!r}")


def _bl_bdays(start, n):
    if n == 0:
        return start
    step = 1 if n > 0 else -1
    d, rem = start, abs(n)
    while rem > 0:
        d += _td(days=step)
        if d.weekday() < 5:
            rem -= 1
    return d


def _bl_settlement(raw, trade_date):
    if raw is None:
        return None
    sv = str(raw).strip()
    if sv == "":
        return None
    mm = BL_TPLUS.match(sv)
    if mm:
        if trade_date in (None, ""):
            raise BLVal("Enter the Trade Date first, then use 't+N' for "
                        "settlement.")
        return _bl_bdays(_bl_parse_date(trade_date), int(mm.group(1)))
    return _bl_parse_date(sv)


def _bl_mmss(text):
    digits = "".join(ch for ch in str(text) if ch.isdigit())
    if not digits:
        return str(text).strip()
    digits = digits[:4]
    if len(digits) <= 2:
        return f"{int(digits):02d}:00"
    return f"{int(digits[:-2]):02d}:{digits[-2:]}"


def _bl_coerce(field, value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        if field in BL_BOOL:
            if isinstance(value, bool):
                return 1 if value else 0
            if isinstance(value, (int, float)):
                return 1 if value else 0
            return 1 if str(value).strip().lower() in (
                "1", "true", "yes", "y", "t") else 0
        if field in BL_NUM:
            return _Dec(str(value))
        if field in BL_DATE:
            return _bl_parse_date(value)
    except (_DecErr, ValueError):
        raise BLVal(f"Invalid number: {value!r}")
    text = str(value)
    if field == "client_side":
        text = text.strip().upper()
    if field in BL_TIME:
        text = _bl_mmss(text)
    return text


def _bl_validate(field, value):
    if value is None:
        return
    if field == "isin":
        if len(str(value)) != 12:
            raise BLVal("ISIN must be exactly 12 characters (or blank).")
    elif field == "delta":
        if not (_Dec("0") <= value <= _Dec("100")):
            raise BLVal("Delta must be between 0 and 100 (percent).")
    elif field == "quantity":
        if value < 0:
            raise BLVal("Quantity must be >= 0.")
    elif field == "client_side":
        if value not in ("BUY", "SELL"):
            raise BLVal("Client side must be BUY or SELL.")
    elif field in BL_HEDGE:
        if str(value) not in BL_HEDGE_STATES:
            raise BLVal("Hedge state must be N/A, Open or Done.")
    if field in BL_MAXLEN and value is not None:
        if len(str(value)) > BL_MAXLEN[field]:
            raise BLVal(f"{field} is too long (max {BL_MAXLEN[field]} "
                        f"characters).")


def _bl_ser(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    return str(v)

_BLOTTER_MAP = None            # logical -> physical (None if missing)
_BLOTTER_AUDIT_MODE = None     # "blotter" | "lagrange"


def _blotter_schema(conn):
    """Resolve logical->physical once per process; pick the audit target."""
    global _BLOTTER_MAP, _BLOTTER_AUDIT_MODE
    if _BLOTTER_MAP is not None:
        return _BLOTTER_MAP
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM {BLOTTER_DB}.{BLOTTER_TABLE}")
        _phys = [r[0] for r in cur.fetchall()]
        have = {c.lower() for c in _phys}
        globals()["_BLOTTER_PHYS"] = _phys
        mapping = {}
        for logical, cands in BLOTTER_CANDIDATES.items():
            mapping[logical] = next((c for c in cands if c.lower() in have),
                                    None)
        try:
            cur.execute(f"SHOW COLUMNS FROM {BLOTTER_DB}.trade_blotter_audit")
            acols = {r[0].lower() for r in cur.fetchall()}
            _BLOTTER_AUDIT_MODE = ("blotter" if
                {"trade_id", "field_name", "old_value", "new_value",
                 "changed_by", "changed_at"} <= acols else "lagrange")
        except Exception:
            _BLOTTER_AUDIT_MODE = "lagrange"
        if _BLOTTER_AUDIT_MODE == "lagrange":
            cur.execute(
                "CREATE TABLE IF NOT EXISTS cba_app.blotter_edit_log ("
                "log_id BIGINT PRIMARY KEY AUTO_INCREMENT, row_id INT, "
                "field_name VARCHAR(50), old_value TEXT, new_value TEXT, "
                "changed_by VARCHAR(50), changed_at DATETIME)")
    conn.commit()
    _BLOTTER_MAP = mapping
    return mapping


def _blotter_audit(cur, row_id, field, old, new, user):
    if _BLOTTER_AUDIT_MODE == "blotter":
        cur.execute(
            f"INSERT INTO {BLOTTER_DB}.trade_blotter_audit "
            f"(trade_id, field_name, old_value, new_value, changed_by, "
            f"changed_at) VALUES (%s,%s,%s,%s,%s,NOW())",
            (row_id, field, old, new, user))
    else:
        cur.execute(
            "INSERT INTO cba_app.blotter_edit_log "
            "(row_id, field_name, old_value, new_value, changed_by, "
            "changed_at) VALUES (%s,%s,%s,%s,%s,NOW())",
            (row_id, field, old, new, user))


def _blotter_payload(dfrom="", dto="", ticker="", ttype=""):
    """One source of truth for both /list and /stream."""
    conn = rc.connect()
    try:
        m = _blotter_schema(conn)
        sel = []
        for logical in BLOTTER_CANDIDATES:
            phys = m.get(logical)
            sel.append(f"`{phys}` AS `{logical}`" if phys
                       else f"'' AS `{logical}`")
        where, params = [], []
        if m["trade_date"]:
            if dfrom:
                where.append(f"`{m['trade_date']}` >= %s"); params.append(dfrom)
            if dto:
                where.append(f"`{m['trade_date']}` <= %s"); params.append(dto)
        if ticker:
            parts = [f"`{m[c]}` LIKE %s" for c in ("isin", "bond_name")
                     if m.get(c)]
            if parts:
                where.append("(" + " OR ".join(parts) + ")")
                params += [f"%{ticker}%"] * len(parts)
        if ttype and m.get("trade_type"):
            where.append(f"`{m['trade_type']}` = %s"); params.append(ttype)
        wsql = (" WHERE " + " AND ".join(where)) if where else ""
        order = m.get("trade_id") or m.get("last_updated") or "1"
        with conn.cursor() as cur:
            # change signature: inserts/deletes (count, max id),
            # edits (max ts + row_version sum survives same-second edits)
            sigcols = [f"COUNT(*)", f"IFNULL(MAX(`{order}`),0)"]
            if m.get("last_updated"):
                sigcols.append(f"IFNULL(MAX(`{m['last_updated']}`),'')")
            if m.get("row_version"):
                sigcols.append(f"IFNULL(SUM(`{m['row_version']}`),0)")
            cur.execute(f"SELECT {', '.join(sigcols)} FROM "
                        f"{BLOTTER_DB}.{BLOTTER_TABLE}{wsql}", params)
            sig = "|".join(str(x) for x in cur.fetchone())
            cur.execute(f"SELECT {', '.join(sel)} FROM "
                        f"{BLOTTER_DB}.{BLOTTER_TABLE}{wsql} "
                        f"ORDER BY `{order}` DESC LIMIT 500", params)
            cols = [d[0] for d in cur.description]
            rows = [{c: (str(v) if v is not None else "")
                     for c, v in zip(cols, r)} for r in cur.fetchall()]
            types = []
            if m.get("trade_type"):
                cur.execute(f"SELECT DISTINCT `{m['trade_type']}` "
                            f"FROM {BLOTTER_DB}.{BLOTTER_TABLE} "
                            f"WHERE `{m['trade_type']}` IS NOT NULL "
                            f"AND `{m['trade_type']}` <> '' ORDER BY 1")
                types = [r[0] for r in cur.fetchall()]
        tok_col = "row_version" if m.get("row_version") else "last_updated"
        for r in rows:
            r["_tok"] = r.get(tok_col, "")
        editable = [f for f in BLOTTER_EDITABLE if m.get(f)]
        missing = [k for k, v in m.items() if v is None]
        used = {v.lower() for v in m.values() if v}
        unmapped = [c for c in globals().get("_BLOTTER_PHYS", [])
                    if c.lower() not in used]
        return {"ok": True, "rows": rows, "types": types,
                "editable": editable, "audit": _BLOTTER_AUDIT_MODE,
                "missing": missing, "unmapped": unmapped, "sig": sig}
    finally:
        conn.close()


@app.get("/api/blotter/list")
def api_blotter_list(dfrom: str = "", dto: str = "", ticker: str = "",
                     ttype: str = ""):
    try:
        return _blotter_payload(dfrom, dto, ticker, ttype)
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.get("/api/blotter/stream")
def api_blotter_stream(dfrom: str = "", dto: str = "", ticker: str = "",
                       ttype: str = ""):
    """Server-Sent Events: pushes the filtered payload whenever the change
    signature moves (edits, inserts, deletes) - checked every 2s. Works
    with or without the blotter app running; integration stays DB-only."""
    def gen():
        import json as _json
        import time as _time
        last, last_ping = None, 0.0
        while True:
            woke = _BL_EVT.wait(timeout=0.5)
            if woke:
                _BL_EVT.clear()
                _time.sleep(0.03)          # let the txn land
            try:
                p = _blotter_payload(dfrom, dto, ticker, ttype)
                if p["sig"] != last:
                    last = p["sig"]
                    yield "event: rows\ndata: " + _json.dumps(p) + "\n\n"
                elif _time.time() - last_ping > 10:
                    last_ping = _time.time()
                    yield ": ping\n\n"
            except GeneratorExit:
                return
            except Exception as e:
                yield ("event: err\ndata: " +
                       _json.dumps({"error": str(e)}) + "\n\n")
                _time.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


class BlotterEdit(BaseModel):
    trade_id: int
    field: str = ""                    # legacy single-field form
    value: str = ""
    fields: Optional[Dict[str, Any]] = None   # excel-style multi-cell
    token: str = ""
    user: str = "lagrange"


# When the blotter app is RUNNING, route writes through its own WebSocket:
# main.py validates + writes + audits + broadcasts to every open blotter
# screen instantly (single-writer consistency). When it is down/unreachable,
# fall back to the direct-DB pipeline below (identical rules); its clients
# then sync on their next touch, as before.
BLOTTER_WS = os.environ.get("BLOTTER_WS", "ws://127.0.0.1:3306/ws")
import threading as _thr
_BL_EVT = _thr.Event()          # wakes SSE streams the instant anything changes


def _bl_notify():
    _BL_EVT.set()


async def _blotter_ws_listener():
    """Persistent subscriber to the blotter app's broadcast hub. Any update /
    insert / delete from ANY blotter user wakes Lagrange's SSE streams within
    milliseconds. Reconnects forever; harmless when the app is down."""
    if os.environ.get("BLOTTER_WS_MODE", "auto") == "off":
        return
    try:
        import websockets as _ws
    except Exception:
        return
    import asyncio as _aio
    while True:
        try:
            async with _ws.connect(BLOTTER_WS, open_timeout=2,
                                   close_timeout=1) as conn:
                async for raw in conn:
                    try:
                        t = json.loads(raw).get("type")
                    except Exception:
                        continue
                    if t in ("update", "insert", "delete", "delete_many"):
                        _bl_notify()
        except Exception:
            pass
        await _aio.sleep(2)

_BL_CONFLICT_MSG = ("This row was changed by someone else \u2014 reloaded "
                    "the latest values.")


def _bl_ws_ser(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    return str(v)


def _blotter_edit_via_app(req, user):
    """Returns a response dict/JSONResponse, or None to fall back to DB."""
    if os.environ.get("BLOTTER_WS_MODE", "auto") == "off":
        return None
    try:
        from websockets.sync.client import connect as _ws_connect
    except Exception:
        return None
    raw_fields = dict(req.fields) if req.fields else {req.field: req.value}
    try:
        with _ws_connect(BLOTTER_WS, open_timeout=1.2, close_timeout=0.5) as ws:
            ws.send(json.dumps({"action": "update", "trade_id": req.trade_id,
                                "row_version": req.token or "0",
                                "fields": raw_fields, "user": user}))
            changes, new_rv = {}, None
            deadline = time.time() + 4.0
            while time.time() < deadline:
                try:
                    raw = ws.recv(timeout=0.4 if changes else
                                  max(0.05, deadline - time.time()))
                except TimeoutError:
                    if changes:
                        break            # drain finished
                    continue
                try:
                    m = json.loads(raw)
                except Exception:
                    continue
                if m.get("trade_id") != req.trade_id and \
                        str(m.get("trade_id")) != str(req.trade_id):
                    continue
                t = m.get("type")
                if t == "reject":
                    msg = m.get("message", "rejected")
                    code = 409 if msg == _BL_CONFLICT_MSG else 400
                    return JSONResponse(status_code=code, content={
                        "ok": False, "error": msg, "via": "blotter-app"})
                if t == "delete":
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "trade not found",
                        "via": "blotter-app"})
                if t == "update" and m.get("user") == user:
                    changes[m.get("field")] = _bl_ws_ser(m.get("value"))
                    new_rv = m.get("row_version")
            if changes:
                _bl_notify()
                return {"ok": True, "token": str(new_rv),
                        "changes": changes, "via": "blotter-app"}
            return {"ok": True, "nochange": True,
                    "token": str(req.token or 0), "via": "blotter-app"}
    except Exception:
        return None                      # app down/unreachable -> direct DB


@app.post("/api/blotter/edit")
def api_blotter_edit(req: BlotterEdit, request: Request):
    raw_fields = dict(req.fields) if req.fields else {req.field: req.value}
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or (req.user or "lagrange").strip()[:50] or "lagrange")
    bad = [f for f in raw_fields if f not in BLOTTER_EDITABLE]
    if bad:
        return JSONResponse(status_code=400, content={
            "ok": False, "error": f"Field '{bad[0]}' is not editable."})
    via_app = _blotter_edit_via_app(req, user)
    if via_app is not None:
        return via_app
    try:
        conn = rc.connect()
        try:
            m = _blotter_schema(conn)
            if not m.get("row_version"):
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": "excel-style editing needs the row_version "
                             "column (this schema lacks it)"})
            missing_phys = [f for f in raw_fields if not m.get(f)]
            if missing_phys:
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": f"'{missing_phys[0]}' does not exist in "
                             f"{BLOTTER_DB}.{BLOTTER_TABLE}"})
            # settlement is resolved against the trade date, main.py-style
            settlement_raw = raw_fields.pop("settlement_date", "__UNSET__")
            coerced = {}
            for f, v in raw_fields.items():
                cv = _bl_coerce(f, v)
                _bl_validate(f, cv)
                coerced[f] = cv
            idc = m["trade_id"]
            sel = ", ".join(f"`{m[l]}` AS `{l}`" for l in BLOTTER_CANDIDATES
                            if m.get(l))
            with conn.cursor() as cur:
                cur.execute(f"SELECT {sel} FROM "
                            f"{BLOTTER_DB}.{BLOTTER_TABLE} "
                            f"WHERE `{idc}`=%s FOR UPDATE", (req.trade_id,))
                hit = cur.fetchone()
                if not hit:
                    conn.rollback()
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "trade not found"})
                cols = [d[0] for d in cur.description]
                current = dict(zip(cols, hit))
                if int(current.get("row_version") or 0) != int(req.token or 0):
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False,
                        "error": "Row changed underneath you (someone else "
                                 "edited it) - refreshing; please re-apply."})
                # booked lock, hedge-exempt - main.py's exact rule + message
                if current.get("booked"):
                    attempted = set(coerced)
                    if settlement_raw != "__UNSET__":
                        attempted.add("settlement_date")
                    if not attempted <= (BL_HEDGE | {"booked"}):
                        conn.rollback()
                        return JSONResponse(status_code=423, content={
                            "ok": False,
                            "error": "This trade is booked and locked. "
                                     "Un-tick Booked first to edit it "
                                     "(hedge state can still be changed)."})

                def cur_norm(f):
                    v = current.get(f)
                    if f in BL_BOOL:
                        return None if v is None else (1 if v else 0)
                    if f in BL_NUM and v is not None:
                        return _Dec(str(v))
                    if f in BL_DATE | {"settlement_date"} and v not in (None, ""):
                        return _bl_parse_date(v)
                    return v if v not in ("",) else None

                changes = {f: v for f, v in coerced.items()
                           if cur_norm(f) != v}
                if settlement_raw != "__UNSET__":
                    eff_td = changes.get("trade_date", current.get("trade_date"))
                    sval = _bl_settlement(settlement_raw, eff_td)
                    if cur_norm("settlement_date") != sval:
                        changes["settlement_date"] = sval
                # server-side auto-fill, never overriding explicit fields
                for f in list(changes):
                    if f in BL_AUTOFILL and changes[f] is not None:
                        tbl, keyc, colmap = BL_AUTOFILL[f]
                        cur.execute(
                            f"SELECT {', '.join(set(colmap.values()))} "
                            f"FROM {BLOTTER_DB}.{tbl} WHERE `{keyc}`=%s "
                            f"LIMIT 1", (str(changes[f]),))
                        row = cur.fetchone()
                        if row is not None:
                            mv = dict(zip([d[0] for d in cur.description], row))
                            for tcol, mcol in colmap.items():
                                nv = mv.get(mcol)
                                if tcol not in changes and \
                                        cur_norm(tcol) != (
                                            _Dec(str(nv)) if tcol in BL_NUM
                                            and nv is not None else nv):
                                    changes[tcol] = nv
                if not changes:
                    conn.rollback()
                    return {"ok": True, "nochange": True,
                            "token": str(current.get("row_version"))}
                sets = [f"`{m[f]}`=%s" for f in changes]
                vals = [None if v is None else
                        (v.isoformat() if isinstance(v, _date) else v)
                        for v in changes.values()]
                sets += [f"`{m['last_updated']}`=NOW()",
                         f"`{m['updated_by']}`=%s",
                         f"`{m['row_version']}`=`{m['row_version']}`+1"]
                vals += [user, req.trade_id, int(req.token or 0)]
                cur.execute(
                    f"UPDATE {BLOTTER_DB}.{BLOTTER_TABLE} "
                    f"SET {', '.join(sets)} "
                    f"WHERE `{idc}`=%s AND `{m['row_version']}`=%s", vals)
                if cur.rowcount != 1:
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False,
                        "error": "Row changed underneath you - refreshing."})
                for f, nv in changes.items():
                    _blotter_audit(cur, req.trade_id, f,
                                   _bl_ser(current.get(f)) or None,
                                   _bl_ser(nv) or None, user)
            conn.commit()
            _bl_notify()
            return {"ok": True,
                    "token": str(int(req.token or 0) + 1),
                    "changes": {f: _bl_ser(v) for f, v in changes.items()},
                    "via": "direct-db"}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


# ----------------------------------------------------------------------
# RFQ Station (cba_app.rfq / rfq_log)
# Lines mirror trade_blotter vocabulary on purpose: when an RFQ reaches
# DONE it should map field-for-field onto a trade_blotter insert
# (isin->isin, short_name->bond_name [autofill via bond_mappings],
#  ccy->bond_currency, qty->quantity, client->client_name,
#  style outright/vs -> trade_type, hit side -> client_side,
#  executed px -> price, stock_ref->stock_ref, delta->delta).
# The actual upload is deliberately NOT implemented yet.
# ----------------------------------------------------------------------
# ======================================================================
#  AUTH: login + trader / sales roles
#  Users live in cba_app.app_user (PBKDF2-SHA256). Sessions are
#  in-memory HttpOnly cookies (12h sliding) - a desk-LAN level of
#  security, not internet-grade. FILE test mode swaps the table for an
#  in-memory dict so the whole flow is testable without MariaDB.
# ======================================================================
AUTH_COOKIE = "lagr_sess"
# sliding idle expiry: a session lives as long as it is used at least
# once per AUTH_TTL (default 60 days). Sessions persist in
# cba_app.app_session so restarts never log anyone out.
AUTH_TTL = int(float(os.environ.get("LAGRANGE_AUTH_TTL_DAYS", "60"))
               * 86400)
AUTH_INVITE = os.environ.get("LAGRANGE_INVITE", "citi-cb")
AUTH_ROLES = ("trader", "sales")
SESSIONS: Dict[str, Dict[str, Any]] = {}
_AUTH_READY = False
_AUTH_FILE_USERS: Dict[str, Dict[str, str]] = {}


def _ensure_auth():
    global _AUTH_READY
    if _AUTH_READY or os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        _AUTH_READY = True
        return
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.app_user (
              user_id INT PRIMARY KEY AUTO_INCREMENT,
              username VARCHAR(50) UNIQUE,
              pw_hash VARCHAR(200),
              role ENUM('trader','sales') DEFAULT 'sales',
              created_at DATETIME, last_login DATETIME NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.app_session (
              tok VARCHAR(64) PRIMARY KEY,
              username VARCHAR(50), role VARCHAR(10),
              created_at DATETIME, last_seen DATETIME
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        conn.commit()
        _AUTH_READY = True
    finally:
        conn.close()


def _hash_pw(pw: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), bytes.fromhex(salt),
                            200_000).hex()
    return f"pbkdf2$200000${salt}${h}"


def _check_pw(pw: str, stored: str) -> bool:
    try:
        _algo, iters, salt, h = (stored or "").split("$")
        calc = hashlib.pbkdf2_hmac("sha256", pw.encode(),
                                   bytes.fromhex(salt),
                                   int(iters)).hex()
        return hmac.compare_digest(calc, h)
    except Exception:
        return False


def _auth_get_user(username: str):
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return _AUTH_FILE_USERS.get(username)
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT username, pw_hash, role FROM "
                        "cba_app.app_user WHERE username=%s",
                        (username,))
            r = cur.fetchone()
            return None if not r else {"username": r[0],
                                       "pw_hash": r[1], "role": r[2]}
    finally:
        conn.close()


def _auth_add_user(username: str, pw_hash: str, role: str):
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        if username in _AUTH_FILE_USERS:
            raise BLVal("Username already taken.")
        _AUTH_FILE_USERS[username] = {"username": username,
                                      "pw_hash": pw_hash, "role": role}
        return
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM cba_app.app_user WHERE "
                        "username=%s", (username,))
            if cur.fetchone():
                raise BLVal("Username already taken.")
            cur.execute("INSERT INTO cba_app.app_user (username, "
                        "pw_hash, role, created_at) VALUES "
                        "(%s,%s,%s,NOW())", (username, pw_hash, role))
        conn.commit()
    finally:
        conn.close()


def _auth_touch_login(username: str):
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE cba_app.app_user SET "
                            "last_login=NOW() WHERE username=%s",
                            (username,))
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _sess_db(sql, params=(), fetch=False):
    """Best-effort session persistence; never breaks login."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        return None
    try:
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone() if fetch else None
            conn.commit()
            return row
        finally:
            conn.close()
    except Exception:
        return None


def _new_sess(user: str, role: str) -> str:
    tok = secrets.token_urlsafe(32)
    SESSIONS[tok] = {"user": user, "role": role, "ts": time.time(),
                     "db_ts": time.time()}
    _sess_db("INSERT INTO cba_app.app_session (tok, username, role, "
             "created_at, last_seen) VALUES (%s,%s,%s,NOW(),NOW())",
             (tok, user, role))
    return tok


def _drop_sess(tok: str):
    SESSIONS.pop(tok or "", None)
    if tok:
        _sess_db("DELETE FROM cba_app.app_session WHERE tok=%s", (tok,))


def _get_sess(tok: str):
    if not tok:
        return None
    s = SESSIONS.get(tok)
    if not s:
        # not in memory (fresh process after a restart): rehydrate
        row = _sess_db("SELECT username, role, last_seen FROM "
                       "cba_app.app_session WHERE tok=%s", (tok,),
                       fetch=True)
        if not row:
            return None
        try:
            ls = row[2].timestamp() if row[2] else 0.0
        except Exception:
            ls = time.time()
        if time.time() - ls > AUTH_TTL:
            _drop_sess(tok)
            return None
        s = {"user": row[0], "role": row[1], "ts": time.time(),
             "db_ts": 0.0}
        SESSIONS[tok] = s
    if time.time() - s["ts"] > AUTH_TTL:
        _drop_sess(tok)
        return None
    s["ts"] = time.time()                      # sliding expiry
    if time.time() - s.get("db_ts", 0) > 600:   # touch DB <= 1x/10min
        s["db_ts"] = time.time()
        _sess_db("UPDATE cba_app.app_session SET last_seen=NOW() "
                 "WHERE tok=%s", (tok,))
    return s


class AuthLogin(BaseModel):
    username: str
    password: str


class AuthRegister(BaseModel):
    username: str
    password: str
    role: str = "sales"
    invite: str = ""


@app.post("/api/auth/login")
def api_auth_login(req: AuthLogin):
    try:
        _ensure_auth()
        u = _auth_get_user(req.username.strip())
        if not u or not _check_pw(req.password, u["pw_hash"]):
            return JSONResponse(status_code=401, content={
                "ok": False, "error": "Wrong username or password."})
        _auth_touch_login(u["username"])
        tok = _new_sess(u["username"], u["role"])
        resp = JSONResponse(content={"ok": True, "user": u["username"],
                                     "role": u["role"]})
        resp.set_cookie(AUTH_COOKIE, tok, httponly=True,
                        samesite="lax", max_age=AUTH_TTL)
        return resp
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.post("/api/auth/register")
def api_auth_register(req: AuthRegister):
    try:
        user = req.username.strip()[:50]
        if not user or len(req.password) < 4:
            raise BLVal("Username required; password min 4 chars.")
        if req.role not in AUTH_ROLES:
            raise BLVal("Role must be trader or sales.")
        if req.invite != AUTH_INVITE:
            raise BLVal("Wrong invite code - ask the desk.")
        _ensure_auth()
        _auth_add_user(user, _hash_pw(req.password), req.role)
        return {"ok": True, "user": user, "role": req.role}
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.post("/api/auth/logout")
def api_auth_logout(request: Request):
    _drop_sess(request.cookies.get(AUTH_COOKIE, ""))
    resp = JSONResponse(content={"ok": True})
    resp.delete_cookie(AUTH_COOKIE)
    return resp


@app.get("/api/auth/me")
def api_auth_me(request: Request):
    s = getattr(request.state, "auth", None)
    if not s:
        return JSONResponse(status_code=401,
                            content={"ok": False,
                                     "error": "login required"})
    return {"ok": True, "user": s["user"], "role": s["role"]}


_PREF_FILE: Dict[str, Dict[str, str]] = {}
_PREF_READY = False


def _ensure_pref():
    global _PREF_READY
    if _PREF_READY or os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        _PREF_READY = True
        return
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.app_pref (
              username VARCHAR(50), pref_key VARCHAR(80),
              pref_val MEDIUMTEXT, updated_at DATETIME,
              PRIMARY KEY (username, pref_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        conn.commit()
        _PREF_READY = True
    finally:
        conn.close()


class PrefSet(BaseModel):
    key: str
    val: str = ""


@app.get("/api/pref")
def api_pref_get(request: Request):
    try:
        user = (getattr(request.state, "auth", None) or {}).get("user")
        if not user:
            return JSONResponse(status_code=401, content={
                "ok": False, "error": "login required"})
        _ensure_pref()
        if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
            return {"ok": True,
                    "prefs": dict(_PREF_FILE.get(user, {}))}
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pref_key, pref_val FROM "
                            "cba_app.app_pref WHERE username=%s",
                            (user,))
                return {"ok": True,
                        "prefs": {k: v for k, v in cur.fetchall()}}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.post("/api/pref")
def api_pref_set(req: PrefSet, request: Request):
    try:
        user = (getattr(request.state, "auth", None) or {}).get("user")
        if not user:
            return JSONResponse(status_code=401, content={
                "ok": False, "error": "login required"})
        key = (req.key or "").strip()[:80]
        if not key:
            return JSONResponse(status_code=400, content={
                "ok": False, "error": "key required"})
        _ensure_pref()
        if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
            _PREF_FILE.setdefault(user, {})[key] = req.val
            return {"ok": True}
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO cba_app.app_pref "
                            "(username, pref_key, pref_val, "
                            "updated_at) VALUES (%s,%s,%s,NOW()) "
                            "ON DUPLICATE KEY UPDATE "
                            "pref_val=VALUES(pref_val), "
                            "updated_at=NOW()",
                            (user, key, req.val))
            conn.commit()
            return {"ok": True}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


AUTH_OPEN = ("/login", "/api/auth/login", "/api/auth/register",
             "/api/auth/logout", "/favicon.ico")
SALES_BLOCK_PREFIX = ("/api/delta/", "/api/nuke/", "/api/blotter/",
                      "/api/bau/", "/api/twcb/")
SALES_BLOCK_EXACT = ("/api/refresh", "/api/send", "/api/rfq/ack",
                     "/api/rfq/quote", "/api/rfq/pull",
                     "/api/rfq/upload", "/api/rfq/reject", "/api/rfq/delete", "/api/rfq/nkedit",
                     "/api/rfq/expire")


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in AUTH_OPEN:
        return await call_next(request)
    s = _get_sess(request.cookies.get(AUTH_COOKIE, ""))
    if not s:
        if path.startswith("/api/") or "/api/" in path:
            # API callers (incl. mounted apps at /dscan/api/...)
            # must get JSON, never a login-page redirect
            return JSONResponse(status_code=401, content={
                "ok": False, "error": "login required",
                "login": "/login"})
        return RedirectResponse("/login", status_code=302)
    request.state.auth = s
    if (s["role"] == "sales"
            and (s.get("user") or "") != "jb33880") and (
            path in SALES_BLOCK_EXACT or
            any(path.startswith(p) for p in SALES_BLOCK_PREFIX)):
        return JSONResponse(status_code=403, content={
            "ok": False,
            "error": "sales role - trader-only function"})
    resp = await call_next(request)
    if request.method == "POST" and \
            path.startswith("/api/rfq/"):
        _rfq_snap_bust()             # writers invalidate instantly
        if resp.status_code == 200:
            _rfq_touch(path.rsplit("/", 1)[-1])   # wake every browser
    return resp


PAGE_LOGIN = r"""<!doctype html><html><head><meta charset="utf-8">
<title>Lagrange - sign in</title><style>
body{font:13px 'Segoe UI',Consolas,sans-serif;background:#f6f4ef;
  display:flex;align-items:center;justify-content:center;height:100vh;
  margin:0}
.card{background:#fff;border:1px solid #d8d4cc;border-top:3px solid
  #1c1c1c;padding:22px 26px;width:320px;
  box-shadow:0 6px 18px rgba(0,0,0,.08)}
h1{font-size:14px;letter-spacing:1.4px;margin:0 0 2px}
.sub{color:#8a857b;font-size:11px;margin-bottom:14px}
label{display:block;color:#6e6a63;font-size:10px;letter-spacing:.6px;
  text-transform:uppercase;margin:9px 0 3px}
input,select{width:100%;box-sizing:border-box;padding:6px 8px;
  border:1px solid #c9c4b8;font:inherit;background:#fffdf6}
input:focus,select:focus{outline:none;border-color:#8a5b00}
button{width:100%;margin-top:14px;padding:7px;border:1px solid
  #1c1c1c;background:#1c1c1c;color:#fff;font:inherit;font-weight:700;
  letter-spacing:.8px;cursor:pointer}
button:hover{background:#3a3a3a}
.alt{margin-top:12px;text-align:center;font-size:11px;color:#6e6a63;
  cursor:pointer;text-decoration:underline}
#reg{display:none}
.err{color:#a8231b;font-size:11px;margin-top:9px;min-height:14px}
</style></head><body>
<div class="card">
  <h1>LAGRANGE</h1><div class="sub">CB desk workstation - sign in</div>
  <div id="lg">
    <label>username</label><input id="l_user" autocomplete="username">
    <label>password</label><input id="l_pw" type="password"
      autocomplete="current-password">
    <button id="b_login">Sign in</button>
    <div class="alt" id="to_reg">first time? create a user</div>
  </div>
  <div id="reg">
    <label>username</label><input id="r_user">
    <label>password</label><input id="r_pw" type="password">
    <label>role</label><select id="r_role">
      <option value="trader">trader</option>
      <option value="sales">sales</option></select>
    <label>desk invite code</label><input id="r_inv">
    <button id="b_reg">Create user</button>
    <div class="alt" id="to_lg">back to sign in</div>
  </div>
  <div class="err" id="l_err"></div>
</div>
<script>
const $=i=>document.getElementById(i);
$('to_reg').onclick=()=>{$('lg').style.display='none';
  $('reg').style.display='block';$('l_err').textContent='';};
$('to_lg').onclick=()=>{$('reg').style.display='none';
  $('lg').style.display='block';$('l_err').textContent='';};
async function post(u,b){const r=await fetch(u,{method:'POST',
  headers:{'Content-Type':'application/json'},
  body:JSON.stringify(b)});return r.json();}
$('b_login').onclick=async()=>{
  const j=await post('/api/auth/login',
    {username:$('l_user').value.trim(),password:$('l_pw').value});
  if(j.ok) location.href='/';
  else $('l_err').textContent=j.error||'login failed';
};
$('l_pw').addEventListener('keydown',e=>{
  if(e.key==='Enter')$('b_login').click();});
$('b_reg').onclick=async()=>{
  const j=await post('/api/auth/register',
    {username:$('r_user').value.trim(),password:$('r_pw').value,
     role:$('r_role').value,invite:$('r_inv').value.trim()});
  if(j.ok){$('l_err').textContent='Created - sign in now.';
    $('to_lg').onclick();}
  else $('l_err').textContent=j.error||'register failed';
};
</script></body></html>"""


@app.get("/login", response_class=HTMLResponse)
def page_login():
    return PAGE_LOGIN


RFQ_STYLES = ("outright", "vs", "working")
RFQ_SIDES = ("two_way", "bid", "ask")
RFQ_STATUS = ("REQUESTED", "QUOTED", "WORKING", "HIT", "IMPROVE",
              "DONE", "CANCELLED", "EXPIRED")
RFQ_LIST_TTL = float(os.environ.get("LAGRANGE_LIST_TTL", "2.0"))
RFQ_SNAP: Dict[str, Any] = {"ts": 0.0, "data": None}


# ---- live push: one monotonic rev; peers get a 30-byte signal and
# fetch the list (hot path O(peers) per change, no per-client build)
RFQ_PEERS: set = set()
RFQ_REV: Dict[str, Any] = {"n": 0, "sig": None, "loop": None}


def _rfq_touch(reason=""):
    """Bump the dataset rev and wake every connected browser.
    Thread-safe (engine thread / request threads / event loop)."""
    import asyncio as _aio
    RFQ_REV["n"] += 1
    RFQ_SNAP["ts"] = 0.0
    loop = RFQ_REV.get("loop")
    if not loop or not RFQ_PEERS:
        return RFQ_REV["n"]
    msg = json.dumps({"type": "rev", "rev": RFQ_REV["n"],
                      "why": reason[:40]})
    for ws in list(RFQ_PEERS):
        try:
            _aio.run_coroutine_threadsafe(ws.send_text(msg), loop)
        except Exception:
            RFQ_PEERS.discard(ws)
    return RFQ_REV["n"]


def _rfq_signature(rows):
    """Cheap change fingerprint: any writer (this process, another
    Lagrange, the engine) bumps row_version, so this catches it."""
    return hash(tuple((r.get("rfq_id"), r.get("row_version"),
                       r.get("q_rev"), r.get("status"),
                       r.get("off_flag"), r.get("bid_px"),
                       r.get("ask_px")) for r in rows))


def _rfq_snap_bust():
    RFQ_SNAP["ts"] = 0.0   # stale-mark only; keep last data
    # (data retained: mutations serve stale instantly, bg refreshes)
RFQ_EDITABLE = {"isin", "ccy", "style", "sides", "qty", "client",
                "stock_ref", "fx_ref", "delta", "notes", "tol",
                "ord_level", "ord_level2", "req_vs", "req_fx", "q_delta", "db1", "db1s", "db2", "db2s", "da1", "da1s", "da2", "da2s", "auto_q",
                "status", "hit", "trade_date"}
_RFQ_READY = False


def _ensure_rfq():
    global _RFQ_READY
    if _RFQ_READY:
        return
    conn = rc.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.rfq (
              rfq_id INT PRIMARY KEY AUTO_INCREMENT,
              created_at DATETIME, created_by VARCHAR(50),
              sec_id BIGINT NULL, short_name VARCHAR(64),
              isin VARCHAR(12) NULL, ccy VARCHAR(10) NULL,
              style ENUM('outright','vs') DEFAULT 'outright',
              sides ENUM('two_way','bid','ask') DEFAULT 'two_way',
              qty DECIMAL(20,2) NULL, client VARCHAR(100) NULL,
              bid_px DECIMAL(18,6) NULL, ask_px DECIMAL(18,6) NULL,
              stock_ref DECIMAL(18,6) NULL, delta DECIMAL(10,4) NULL,
              notes TEXT NULL, hit ENUM('','bid','ask') DEFAULT '',
              ric VARCHAR(50) NULL, und_fx VARCHAR(24) NULL,
              sec_fx VARCHAR(10) NULL, fx_ref DECIMAL(18,6) NULL,
              bid_at DATETIME NULL, ask_at DATETIME NULL,
              q_spot DECIMAL(18,6) NULL, q_fx DECIMAL(18,6) NULL,
              status ENUM('OPEN','QUOTED','DONE','CANCELLED')
                DEFAULT 'OPEN',
              last_updated TIMESTAMP DEFAULT current_timestamp()
                ON UPDATE current_timestamp(),
              updated_by VARCHAR(50), row_version INT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("""CREATE TABLE IF NOT EXISTS cba_app.rfq_log (
              log_id BIGINT PRIMARY KEY AUTO_INCREMENT, rfq_id INT,
              field_name VARCHAR(50), old_value TEXT, new_value TEXT,
              changed_by VARCHAR(50), changed_at DATETIME)""")
            for ddl in (
                "ALTER TABLE cba_app.rfq ADD COLUMN hit "
                "ENUM('','bid','ask') DEFAULT ''",
                "ALTER TABLE cba_app.rfq ADD COLUMN bid_at DATETIME NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN ask_at DATETIME NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN q_spot DECIMAL(18,6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN q_fx DECIMAL(18,6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN ric VARCHAR(50) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN und_fx VARCHAR(24) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN sec_fx VARCHAR(10) NULL",
                "ALTER TABLE cba_app.rfq MODIFY sec_fx VARCHAR(10) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN fx_ref DECIMAL(18,6) NULL",
                "ALTER TABLE cba_app.rfq MODIFY style ENUM('outright','vs','working') DEFAULT 'outright'",
                "ALTER TABLE cba_app.rfq MODIFY status ENUM('OPEN','QUOTED','WORKING','DONE','CANCELLED') DEFAULT 'OPEN'",
                "ALTER TABLE cba_app.rfq ADD COLUMN ack_by VARCHAR(50) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN ack_at DATETIME NULL",
                "ALTER TABLE cba_app.rfq MODIFY status ENUM('OPEN','QUOTED','QUOTING','WORKING','DONE','CANCELLED') DEFAULT 'QUOTING'",
                "UPDATE cba_app.rfq SET status='QUOTING' WHERE status IN ('OPEN','QUOTED')",
                "ALTER TABLE cba_app.rfq MODIFY status ENUM('QUOTING','WORKING','DONE','CANCELLED') DEFAULT 'QUOTING'",
                "ALTER TABLE cba_app.rfq ADD COLUMN trade_date DATE NULL",
                "UPDATE cba_app.rfq SET trade_date=DATE(created_at) WHERE trade_date IS NULL",
                "ALTER TABLE cba_app.rfq MODIFY status ENUM('QUOTING','REQUESTED','QUOTED','WORKING','DONE','CANCELLED') DEFAULT 'REQUESTED'",
                "UPDATE cba_app.rfq SET status='QUOTED' WHERE status='QUOTING' AND (bid_px IS NOT NULL OR ask_px IS NOT NULL)",
                "UPDATE cba_app.rfq SET status='REQUESTED' WHERE status='QUOTING'",
                "ALTER TABLE cba_app.rfq MODIFY status ENUM('REQUESTED','QUOTED','WORKING','DONE','CANCELLED') DEFAULT 'REQUESTED'",
                "ALTER TABLE cba_app.rfq ADD COLUMN q_rev INT DEFAULT 0",
                "CREATE TABLE IF NOT EXISTS cba_app.rfq_quote_hist ("
                "  hist_id BIGINT PRIMARY KEY AUTO_INCREMENT, rfq_id INT, "
                "  rev INT, bid DECIMAL(18,6) NULL, ask DECIMAL(18,6) NULL, "
                "  spot DECIMAL(18,6) NULL, fx DECIMAL(18,6) NULL, "
                "  delta DECIMAL(10,4) NULL, quoted_by VARCHAR(50), "
                "  quoted_at DATETIME, KEY idx_rfq (rfq_id, rev)) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
                "ALTER TABLE cba_app.rfq_quote_hist ADD COLUMN action VARCHAR(12) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN refresh_by VARCHAR(50) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN refresh_at DATETIME NULL",
                "ALTER TABLE cba_app.rfq MODIFY status ENUM('REQUESTED','QUOTED','WORKING','HIT','DONE','CANCELLED') DEFAULT 'REQUESTED'",
                "ALTER TABLE cba_app.rfq MODIFY status ENUM('OPEN','REQUESTED','QUOTED','QUOTING','WORKING','IMPROVE','HIT','DONE','CANCELLED','EXPIRED') DEFAULT 'REQUESTED'",
                "UPDATE cba_app.rfq SET status='HIT' WHERE status='DONE' AND (ack_by IS NULL OR ack_by='')",
                "ALTER TABLE cba_app.rfq ADD COLUMN tol DECIMAL(12,6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN off_flag VARCHAR(12) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN off_by VARCHAR(50) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN off_at DATETIME NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN ord_side VARCHAR(6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN ord_level DECIMAL(14,6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN auto_q TINYINT NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN ord_level2 DECIMAL(14,6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN adj_req TINYINT NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN req_vs DECIMAL(14,6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN req_fx DECIMAL(14,6) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN q_delta DECIMAL(8,2) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN q_delta_ovd TINYINT DEFAULT 0",
                # r107 repair: rows given the terminal status EXPIRED by
                # the r80/r81 E button become the correct quote-expired
                # state (open, off_flag=expired). Idempotent.
                "UPDATE cba_app.rfq SET status='REQUESTED', "
                "off_flag='expired', off_by=COALESCE(off_by, updated_by, "
                "'trader'), off_at=COALESCE(off_at, last_updated, NOW()), "
                "bid_px=NULL, ask_px=NULL WHERE status='EXPIRED'",
                "ALTER TABLE cba_app.rfq ADD COLUMN off_bid DECIMAL(12,4) NULL",
                "ALTER TABLE cba_app.rfq ADD COLUMN off_ask DECIMAL(12,4) NULL",
                "CREATE TABLE IF NOT EXISTS cba_app.rfq_bond_cfg (isin VARCHAR(20) PRIMARY KEY, short_name VARCHAR(64), tol DECIMAL(12,6) NULL, autopilot TINYINT NULL, updated_by VARCHAR(50), updated_at DATETIME)"):
                try:
                    cur.execute(ddl)
                except Exception:
                    pass                       # column already there
        conn.commit()
        _RFQ_READY = True
    finally:
        conn.close()


RFQ_STALE_SEC = int(os.environ.get("RFQ_STALE_SEC", "120"))
RFQ_SPOT_TOL_PCT = float(os.environ.get("RFQ_SPOT_TOL_PCT", "0.5"))
RFQ_FX_TOL_PCT = float(os.environ.get("RFQ_FX_TOL_PCT", "0.25"))
RFQ_PX_TOL = float(os.environ.get("RFQ_PX_TOL", "0.05"))
RFQ_QUOTE_TTL = float(os.environ.get("RFQ_QUOTE_TTL", "600"))   # quote timeout s


def _rfq_refdata(sec_id):
    """One security's refdata row: ric / sec_fx (ccy) / und_fx / isin.
    Uses the embedded Nuke's build_refdata; FILE fixture for tests."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        st = _rfq_state() or {}
        d = st.get("refdata", {}) or {}
        return d.get(str(sec_id)) or d.get(sec_id) or {}
    try:
        if _NUKE_MOD and sec_id not in (None, ""):
            rows, _err = _NUKE_MOD.build_refdata([int(sec_id)])
            return rows[0] if rows else {}
    except Exception:
        pass
    return {}


def _rfq_stock_ric(st, sec_id, fallback=""):
    """stock RIC lives in STATE['stockRics'], not the nuke entry."""
    sr = (st or {}).get("stockRics", {}) or {}
    return (sr.get(sec_id) or sr.get(str(sec_id)) or
            (sr.get(int(sec_id)) if str(sec_id).isdigit() else None) or
            fallback or "")


def _rfq_state():
    """Nuke shared state; LAGRANGE_TEST_LIVE=FILE reads a JSON fixture."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        try:
            with open("/tmp/rfq_test_live.json") as fh:
                return json.load(fh)
        except Exception:
            return None
    try:
        return _NUKE_MOD.STATE if _NUKE_MOD else None
    except Exception:
        return None


def _fnum(v):
    try:
        if v is None or str(v).strip() == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


_NUKE_PX = {}


def _nuke_px_prime(reqs):
    """One batched pricing call primes _NUKE_PX for a whole list
    pass - kills the serial cold-start cost."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE" or \
            not _NUKE_MOD or not reqs:
        return
    now = time.time()
    ents, keys = [], []
    for sid, spot, fx, cbfx in reqs:
        if spot is None:
            continue
        key = (int(sid), round(float(spot), 6),
               None if fx is None else round(float(fx), 6))
        hit = _NUKE_PX.get(key)
        if hit and now - hit["ts"] < 45:
            continue
        ents.append({"secId": int(sid), "ovdSpot": float(spot),
                     "ovdCbFx": float(cbfx or 0),
                     "ovdUndFx": float(fx or 0)})
        keys.append(key)
    if not ents:
        return
    try:
        out = _NUKE_MOD.call_nuked_api(ents)
        by_sid = {r.get("secId"): r for r in out.get("rows", [])}
        for ent, key in zip(ents, keys):
            r = by_sid.get(ent["secId"]) or {}
            b = _fnum(r.get("ovdMktBid"))
            a = _fnum(r.get("ovdMktAsk"))
            _NUKE_PX[key] = {"ts": now, "v": None if (b is None
                and a is None) else (b, a)}
    except Exception:
        pass


def _nuke_px(sid, spot, fx, cbfx):
    """TRUE nuke price at arbitrary refs via the same CB pricing
    service the Nuke button uses (call_nuked_api). 45s memo per
    (sid, spot, fx). None -> caller falls back to the delta/gamma
    expansion (flagged est)."""
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE" or \
            not _NUKE_MOD or spot is None:
        return None
    key = (int(sid), round(float(spot), 6),
           None if fx is None else round(float(fx), 6))
    now = time.time()
    hit = _NUKE_PX.get(key)
    if hit and now - hit["ts"] < 45:
        return hit["v"]
    try:
        out = _NUKE_MOD.call_nuked_api([{
            "secId": int(sid), "ovdSpot": float(spot),
            "ovdCbFx": float(cbfx or 0),
            "ovdUndFx": float(fx or 0)}])
        row = (out.get("rows") or [{}])[0]
        b = _fnum(row.get("ovdMktBid") if "ovdMktBid" in row
                  else row.get("ovd_bid"))
        a = _fnum(row.get("ovdMktAsk") if "ovdMktAsk" in row
                  else row.get("ovd_ask"))
        v = None if (b is None and a is None) else (b, a)
    except Exception:
        v = None
    _NUKE_PX[key] = {"ts": now, "v": v}
    if len(_NUKE_PX) > 4000:
        _NUKE_PX.clear()
    return v


def _rfq_expire_quote(cur, r, by):
    """Pull the standing quote as EXPIRED: bid/ask off (kept in
    off_bid/off_ask), status back to REQUESTED, off_flag='expired',
    history row action 'expired'. The RFQ stays active - Q re-quotes.
    Shared by the engine timeout and the trader's E button, so both
    end in the identical state (only the 'by' differs).
    Returns the new q_rev, or None if the row moved underneath."""
    rv = int(r.get("row_version") or 0)
    rev = int(r.get("q_rev") or 0) + 1
    cur.execute(
        "UPDATE cba_app.rfq SET bid_px=NULL, ask_px=NULL, q_rev=%s, "
        "status='REQUESTED', off_flag='expired', off_by=%s, "
        "off_at=NOW(), off_bid=COALESCE(%s, off_bid), "
        "off_ask=COALESCE(%s, off_ask), last_updated=NOW(), "
        "updated_by=%s, row_version=row_version+1 "
        "WHERE rfq_id=%s AND row_version=%s",
        (rev, by, _fnum(r.get("bid_px")), _fnum(r.get("ask_px")), by,
         r["rfq_id"], rv))
    if not cur.rowcount:
        return None
    cur.execute(
        "INSERT INTO cba_app.rfq_quote_hist (rfq_id, rev, bid, ask, "
        "quoted_by, quoted_at, action) VALUES "
        "(%s,%s,NULL,NULL,%s,NOW(),'expired')", (r["rfq_id"], rev, by))
    return rev


def _rfq_pick_delta(q_delta, q_delta_ovd, ticket_delta, live_delta):
    """Delta used to price/stamp a quote, in priority order:
    1) QUOTE-band Delta typed by the trader (q_delta_ovd=1),
    2) ticket-band Delta override (L / manual),
    3) live Nuke nDelta%."""
    if int(q_delta_ovd or 0) == 1 and _fnum(q_delta) is not None:
        return _fnum(q_delta)
    if _fnum(ticket_delta) is not None:
        return _fnum(ticket_delta)
    return live_delta


def _rfq_live(sec_id, style, ovd_spot=None, ovd_delta=None,
              ovd_fx=None):
    """Nuke QuoteBid/QuoteAsk, server-side, priced at the caller's
    refs. theo = nBid + (nD/100)*m + 0.5*(gamma/100)*m^2 with
    m = 100*(und - nSpot)/nSpot; und = ovd_spot > nuke-row ovdSpot >
    rfx last > liveSpot; nD = ovd_delta > nDelta*100. bid = theo,
    ask = theo + nSpread; outright adds or_bid/ask_sprd; ALL styles
    then add x_bid / x_ask / x_both (QuoteBid/QuoteAsk parity)."""
    st = _rfq_state()
    if not st or sec_id in (None, ""):
        return {}
    def pick(d, k):
        if not isinstance(d, dict):
            return None
        return d.get(k) if k in d else d.get(str(k)) if str(k) in d \
            else d.get(int(k)) if str(k).isdigit() and int(k) in d else None
    try:
        sid = int(sec_id)
    except (TypeError, ValueError):
        sid = sec_id
    nk = pick(st.get("nuke", {}), sid) or {}
    row = pick(st.get("rows", {}), sid) or {}
    rfx = st.get("rfx", {}) or {}
    n_bid, n_delta = _fnum(nk.get("nBid")), _fnum(nk.get("nDelta"))
    n_spot = _fnum(nk.get("nSpot"))
    out = {"ts": st.get("rfxTs") or ""}
    ric = _rfq_stock_ric(st, sid, nk.get("ric") or "")
    fx_ric = (row.get("und_fx") or "").strip()
    fx_live = _fnum((rfx.get(fx_ric) or {}).get("last")) if fx_ric else None
    out["fx"] = fx_live
    out["fx_nuke"] = _fnum(nk.get("nSpotFx"))
    und = ovd_spot if ovd_spot is not None \
        else _fnum(row.get("ovdSpot"))
    if und is None and ric:
        und = _fnum((rfx.get(ric) or {}).get("last"))
    if und is None:
        und = _fnum(nk.get("liveSpot"))
    out["spot"] = und
    if None in (n_bid, n_delta, n_spot) or not n_spot or und is None:
        return out
    m = 100.0 * (und - n_spot) / n_spot
    g = _fnum(row.get("n_gamma"))
    nD = ovd_delta if ovd_delta is not None else n_delta * 100.0
    theo = n_bid + (nD / 100.0) * m + (0.5 * (g / 100.0) * m * m
                                       if g is not None else 0.0)
    spread = _fnum(nk.get("nSpread")) or 0.0
    bid, ask = theo, theo + spread
    out["und"] = und
    out["src"] = "est"
    if ovd_spot is not None:
        _sv = _nuke_px(sid, ovd_spot,
                       ovd_fx if ovd_fx is not None else
                       _fnum(row.get("ovdUndFx")),
                       _fnum(row.get("ovdCbFx")))
        if _sv:
            _sb, _sa = _sv
            if _sb is not None:
                bid = _sb
            if _sa is not None:
                ask = _sa
            elif _sb is not None:
                ask = _sb + spread
            out["src"] = "nuke-svc"
    x2 = _fnum(row.get("x_both")) or 0.0
    bid += (_fnum(row.get("x_bid")) or 0.0) + x2
    ask += (_fnum(row.get("x_ask")) or 0.0) + x2
    out["bid"], out["ask"] = round(bid, 4), round(ask, 4)
    _mb = _fnum(nk.get("ovdMktBid"))
    _ma = _fnum(nk.get("ovdMktAsk"))
    _xb = (_fnum(row.get("x_bid")) or 0.0) + x2
    _xa = (_fnum(row.get("x_ask")) or 0.0) + x2
    out["nqb"] = None if _mb is None else round(_mb + _xb, 4)
    out["nqa"] = None if _ma is None else round(_ma + _xa, 4)
    _ndraw = None
    _nkkey = None
    for _src in (nk, row):
        for _k in ("n_delta", "nDeltaPct", "nDelta", "delta",
                   "m_delta"):
            _v = _src.get(_k)
            if _v not in (None, ""):
                _ndraw = _v
                _nkkey = _k
                break
        if _ndraw not in (None, ""):
            break
    out["nd"] = (_fnum(str(_ndraw).replace("%", "").strip())
                 if _ndraw not in (None, "") else None)
    # n_delta is stored as a fraction (0.56 = 56%): scale to
    # percent, magnitude-guarded so an already-percent value is
    # never double-scaled.
    if (_nkkey in ("n_delta", "nDelta") and out["nd"] is not None
            and abs(out["nd"]) <= 1.5):
        out["nd"] = round(out["nd"] * 100.0, 2)
    out["sn"] = str(row.get("short_name") or "").strip()
    out["xb"] = _fnum(row.get("x_bid"))
    out["xa"] = _fnum(row.get("x_ask"))
    out["x2"] = _fnum(row.get("x_both"))
    out["orb"] = _fnum(row.get("or_bid_sprd"))
    out["ora"] = _fnum(row.get("or_ask_sprd"))
    out["nvs"] = _fnum(row.get("ovdSpot"))
    out["nfx"] = _fnum(row.get("ovdUndFx"))
    out["trace"] = (("[" + out.get("src", "est") + "] ") if
                    ovd_spot is not None else "") + ("nBid %.4f + d %.2f%%*m %.3f%% (%.4f) + "
                    ".5g %s*m^2 (%s) | sprd %.2f "
                    "x(%s/%s/%s) und %.4f nSpot %.4f") % (
        n_bid, nD, m, (nD / 100.0) * m,
        "%.3f" % g if g is not None else "-",
        "%.4f" % (0.5 * (g / 100.0) * m * m) if g is not None
        else "-", spread,
        row.get("x_bid") or 0, row.get("x_ask") or 0, x2,
        und, n_spot)
    if style == "outright":
        _ob3 = out.get("orb") or 0.0
        _oa3 = out.get("ora") or 0.0
        for _kk, _vv in (("nqb", _ob3), ("nqa", _oa3),
                         ("bid", _ob3), ("ask", _oa3)):
            if out.get(_kk) is not None:
                out[_kk] = round(out[_kk] + _vv, 4)
    return out


def _rfq_qty_map(sec_ids):
    """Live position quantity per sec_id - eqrms via the embedded
    Nuke's fetch_ric_map, ONE batched query per /list poll.
    FILE fixture: refdata[sid][quantity_live]."""
    ids = sorted({int(s) for s in sec_ids
                  if str(s or "").strip().isdigit()})
    if not ids:
        return {}
    if os.environ.get("LAGRANGE_TEST_LIVE") == "FILE":
        st = _rfq_state() or {}
        d = st.get("refdata", {}) or {}
        out = {}
        for s in ids:
            r = d.get(str(s)) or d.get(s) or {}
            q = _fnum(r.get("quantity_live"))
            if q is not None:
                out[s] = q
        return out
    import time as _t
    key = tuple(ids)
    if (_RFQ_QTY_CACHE["key"] == key and
            _t.time() - _RFQ_QTY_CACHE["ts"] < 60):
        return _RFQ_QTY_CACHE["map"]
    try:
        if _NUKE_MOD:
            m = _NUKE_MOD.fetch_ric_map(ids)
            out = {sid: v.get("qty") for sid, v in m.items()
                   if v.get("qty") is not None}
            _RFQ_QTY_CACHE.update(ts=_t.time(), key=key, map=out)
            return out
    except Exception:
        pass
    return {}


_RFQ_QTY_CACHE = {"ts": 0.0, "key": None, "map": {}}


def _rfq_flag(r, calc):
    """Is the STANDING quote good to trade?
    NO QUOTE  - nothing confirmed yet (press Q)
    GOOD      - standing bid/ask within RFQ_PX_TOL of the model now
    REQUOTE   - drifted; press Q again (old quote goes to history)."""
    qb, qa = _fnum(r.get("bid_px")), _fnum(r.get("ask_px"))
    _t = _fnum(r.get("tol"))
    tol = _t if (_t is not None and _t > 0) else RFQ_PX_TOL
    if qb is None and qa is None:
        if r.get("off_flag") and r.get("status") == "REQUESTED":
            auto = r.get("off_flag") == "auto"
            _by = r.get("off_by") or ""
            _at = str(r.get("off_at") or "")[11:16]
            return {"txt": "OFF >tol" if auto else "OFF",
                    "cls": "fl-moved",
                    "title": (("auto-offed: model moved beyond the tol " if auto else f"offed by {_by} ")
                        + f"at {_at} - trader is adjusting; Q re-quotes")}
        if r.get("status") == "QUOTED":
            return {"txt": "PULL", "cls": "fl-moved",
                    "title": "standing quote lost (empty px) - auto-off will move this to ADJUSTING; Q re-quotes"}
        return {"txt": "NO QUOTE", "cls": "fl-none",
                "title": "no standing quote - press Q to send rev 1"}
    rev = r.get("q_rev") or "1"
    at = str(r.get("bid_at") or r.get("ask_at") or "")[11:16]
    if calc.get("bid") is None and calc.get("ask") is None:
        return {"txt": f"GOOD r{rev}", "cls": "fl-ok",
                "title": f"standing quote rev {rev} \u00b7 {at} \u00b7 no model to compare"}
    if (r.get("style") or "") == "outright":
        # DIRECTIONAL slippage: +ve = client dealing through your
        # standing quote is better-than-model FOR YOU.
        #   bid_slip = model bid - your bid (you buy below fair)
        #   ask_slip = your ask - model ask (you sell above fair)
        slips = []
        if qb is not None and calc.get("bid") is not None:
            slips.append(("bid", calc["bid"] - qb))
        if qa is not None and calc.get("ask") is not None:
            slips.append(("ask", qa - calc["ask"]))
        stxt = " / ".join(f"{s}{v:+.2f}" for s, v in slips)
        bad = [s for s, v in slips if v < -tol]
        if bad:
            return {"txt": "PULL", "cls": "fl-moved",
                    "title": f"rev {rev} slippage {stxt} - "
                             f"{'/'.join(bad)} side(s) beyond "
                             f"-{tol:.2f}: off the quote "
                             "(\u2a2f in the cell) or refresh "
                             "(\u27f3 / Q)"}
        return {"txt": f"GOOD r{rev}", "cls": "fl-ok",
                "title": f"rev {rev} \u00b7 {at} \u00b7 slippage "
                         f"{stxt or 'n/a'} - favorable or within "
                         f"{tol:.2f}"}
    drift = 0.0
    for q, c in ((qb, calc.get("bid")), (qa, calc.get("ask"))):
        if q is not None and c is not None:
            drift = max(drift, abs(q - c))
    if drift > tol:
        return {"txt": "REQUOTE", "cls": "fl-moved",
                "title": f"standing rev {rev} off model by {drift:.2f} "
                         f"(tol {tol:.2f}) - press Q; rev {rev} "
                         "is kept in history"}
    return {"txt": f"GOOD r{rev}", "cls": "fl-ok",
            "title": f"standing quote rev {rev} \u00b7 {at} \u00b7 "
                     f"within {tol:.2f} of model - good to trade"}


@app.get("/api/rfq/secs")
def api_rfq_secs():
    """RFQ security universe = the Nuke Station book: sec_id +
    short_name from shared state, ISIN / company from refdata.
    The entry box matches on either short name or ISIN.
    (This route was referenced by the page but never existed -
    the picker has been silently empty until now.)"""
    try:
        st = _rfq_state() or {}
        ids = st.get("ids") or sorted(
            (st.get("rows") or {}).keys(), key=str)
        out = []
        for sid in ids:
            rows = st.get("rows") or {}
            row = rows.get(str(sid)) or rows.get(sid) or {}
            try:
                rd = _rfq_refdata(sid) or {}
            except Exception:
                rd = {}
            out.append({"sec_id": str(sid),
                        "short_name": (row.get("short_name") or
                                       rd.get("short_name") or ""),
                        "isin": rd.get("isin") or "",
                        "company": rd.get("company_name") or ""})
        return {"ok": True, "secs": out}
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.get("/api/rfq/list")
def api_rfq_list(_bg: int = 0):
    try:
        now = time.time()
        if not _bg and RFQ_SNAP["data"] is not None:
            fresh = now - RFQ_SNAP["ts"] < RFQ_LIST_TTL
            if not fresh and not RFQ_SNAP.get("busy"):
                RFQ_SNAP["busy"] = True
                def _rebuild():
                    try:
                        api_rfq_list(_bg=1)
                        RFQ_SNAP["err"] = None
                    except Exception as _e1:
                        RFQ_SNAP["err"] = str(_e1)
                    finally:
                        RFQ_SNAP["busy"] = False
                threading.Thread(target=_rebuild,
                                 daemon=True).start()
            return RFQ_SNAP["data"]   # instant: stale-while-revalidate
        if not _bg and RFQ_SNAP["data"] is None:
            # cold start: answer NOW, build behind the paint
            if not RFQ_SNAP.get("busy"):
                RFQ_SNAP["busy"] = True
                def _rb0():
                    try:
                        api_rfq_list(_bg=1)
                        RFQ_SNAP["err"] = None
                    except Exception as _e0:
                        RFQ_SNAP["err"] = str(_e0)
                    finally:
                        RFQ_SNAP["busy"] = False
                threading.Thread(target=_rb0,
                                 daemon=True).start()
            return {"ok": True, "rows": [], "cold": 1,
                    "err": RFQ_SNAP.get("err"),
                    "ms": 0, "qttl": RFQ_QUOTE_TTL,
                    "editable": sorted(RFQ_EDITABLE)}
        _t0 = time.time()
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT rfq_id, created_at, trade_date, created_by, sec_id, "
                    "short_name, isin, ric, und_fx, sec_fx, fx_ref, ccy, "
                    "style, sides, hit, qty, client, "
                    "bid_px, ask_px, bid_at, ask_at, q_spot, q_fx, "
                    "stock_ref, delta, notes, status, ack_by, ack_at, q_rev, "
                    "refresh_by, refresh_at, tol, off_flag, off_by, off_at, "
                    "req_vs, req_fx, q_delta, COALESCE(q_delta_ovd,0) AS q_delta_ovd, ord_side, ord_level, ord_level2, adj_req, auto_q, off_bid, off_ask, "
                    "last_updated, updated_by, row_version "
                    "FROM cba_app.rfq ORDER BY rfq_id DESC LIMIT 200")
                cols = [d[0] for d in cur.description]
                _did_off = False
                rows = [{c: ("" if v is None else str(v))
                         for c, v in zip(cols, r)} for r in cur.fetchall()]
            qmap = _rfq_qty_map([r.get("sec_id") for r in rows])
            _pr = []
            for _r0 in rows:
                _ps = _fnum(_r0.get("stock_ref"))
                if _ps is not None and _r0.get("status") not in\
                        ("DONE", "CANCELLED"):
                    _st2 = _rfq_state() or {}
                    _rw2 = (_st2.get("rows") or {}).get(
                        int(_r0.get("sec_id") or 0), {}) or {}
                    _pr.append((_r0.get("sec_id"), _ps,
                        _fnum(_r0.get("fx_ref")) if
                        _fnum(_r0.get("fx_ref")) is not None else
                        _fnum(_rw2.get("ovdUndFx")),
                        _fnum(_rw2.get("ovdCbFx"))))
            # true-pricing service is ALWAYS async, single-flight:
            # a pass never waits on the nuke wire; rows use the
            # last completed prime (45s memo) and the trace names
            # the source.
            if _pr and not RFQ_SNAP.get("pbusy"):
                RFQ_SNAP["pbusy"] = True
                def _pw(_p=_pr):
                    try:
                        _nuke_px_prime(_p)
                    except Exception:
                        pass
                    finally:
                        RFQ_SNAP["pbusy"] = False
                threading.Thread(target=_pw, daemon=True).start()
            conn2 = rc.connect()
            cur2 = conn2.cursor()
            for r in rows:
                r["_tok"] = r["row_version"]
                _s = str(r.get("sec_id") or "")
                _q = qmap.get(int(_s)) if _s.isdigit() else None
                r["live_qty"] = "" if _q is None else f"{_q:,.0f}"
                frozen = r.get("status") == "CANCELLED"
                hit_watch = r.get("status") == "HIT"
                ovd_s = _fnum(r.get("stock_ref"))
                if ovd_s is None:
                    ovd_s = _fnum(r.get("req_vs"))
                ovd_d = _fnum(r.get("delta"))
                if int(r.get("q_delta_ovd") or 0) == 1 and _fnum(r.get("q_delta")) is not None:
                    ovd_d = _fnum(r.get("q_delta"))
                ovd_f = _fnum(r.get("fx_ref"))
                if ovd_f is None:
                    ovd_f = _fnum(r.get("req_fx"))
                if frozen:
                    live = {}
                    trk = {"stock_ref": None, "fx_ref": None,
                           "delta": None}
                else:
                    trk = _rfq_track(r.get("sec_id"), r.get("ric"),
                                     r.get("und_fx"))
                    live = _rfq_live(r.get("sec_id"), r.get("style"))
                    q = live if (ovd_s is None and ovd_d is None) \
                        else _rfq_live(r.get("sec_id"),
                                       r.get("style"),
                                       ovd_spot=ovd_s,
                                       ovd_delta=ovd_d,
                                       ovd_fx=ovd_f)
                    r["calc_bid"] = "" if q.get("bid") is None \
                        else f"{q['bid']:.2f}"
                    r["calc_ask"] = "" if q.get("ask") is None \
                        else f"{q['ask']:.2f}"
                    _qb = _fnum(r.get("bid_px"))
                    _qa = _fnum(r.get("ask_px"))
                    r["slip_bid"] = "" \
                        if (_qb is None or q.get("bid") is None) \
                        else f"{q['bid'] - _qb:+.2f}"
                    r["slip_ask"] = "" \
                        if (_qa is None or q.get("ask") is None) \
                        else f"{_qa - q['ask']:+.2f}"
                _nvs = live.get("nvs")
                r["live_spot"] = (f"{_nvs:g}" if _nvs is not None
                    else "" if trk["stock_ref"] is None
                    else f"{trk['stock_ref']:g}")
                _nfx = live.get("nfx")
                r["live_und"] = (f"{_nfx:g}" if _nfx is not None
                    else "" if trk["fx_ref"] is None
                    else f"{trk['fx_ref']:g}")
                r["live_delta"] = "" if trk["delta"] is None \
                    else f"{trk['delta']:g}"
                _qb2 = live.get("nqb")
                _qb2 = live.get("bid") if _qb2 is None else _qb2
                _qa2 = live.get("nqa")
                _qa2 = live.get("ask") if _qa2 is None else _qa2
                r["live_bid"] = "" if _qb2 is None \
                    else f"{_qb2:.2f}"
                r["live_ask"] = "" if _qa2 is None \
                    else f"{_qa2:.2f}"
                r["live_ts"] = live.get("ts") or ""
                _sn2 = (live.get("sn") or "").strip()
                if _sn2:
                    r["short_name"] = _sn2
                _nd = live.get("nd")
                r["live_delta"] = "" if _nd is None \
                    else "%.0f %%" % _nd
                _qbF = _fnum(r.get("bid_px"))
                _qaF = _fnum(r.get("ask_px"))
                _lbF = _fnum(r.get("live_bid"))
                _laF = _fnum(r.get("live_ask"))
                r["pd_bid"] = ("" if (_qbF is None or not _lbF)
                    else "%+.2f%%" % ((_qbF - _lbF) / _lbF * 100))
                r["pd_ask"] = ("" if (_qaF is None or not _laF)
                    else "%+.2f%%" % ((_qaF - _laF) / _laF * 100))
                r["nk_xb"] = "" if live.get("xb") is None else str(live["xb"])
                r["nk_xa"] = "" if live.get("xa") is None else str(live["xa"])
                r["nk_x"] = "" if live.get("x2") is None else str(live["x2"])
                r["orb"] = "" if live.get("orb") is None else str(live["orb"])
                r["ora"] = "" if live.get("ora") is None else str(live["ora"])
                qb, qa = r.get("bid_at"), r.get("ask_at")
                r["quoted"] = max([t for t in (qb, qa) if t], default="")
                if hit_watch:
                    r["flag"] = ("HIT " + str(r.get("hit") or "").upper()).strip()
                    r["flag_cls"] = "fl-moved"
                    r["flag_title"] = ("client dealt - awaiting "
                                       "trader ACK / REJ; quote "
                                       "frozen at the deal, live "
                                       "still tracking")
                elif frozen:
                    r["flag"] = "DONE" if r["status"] == "DONE" \
                        else "CXL"
                    r["flag_cls"] = "fl-none"
                    r["flag_title"] = ("updates stopped - refs "
                                       "frozen at status change")
                else:
                    if r.get("status") == "QUOTED":
                        try:
                            import datetime as _dt2
                            _qt = (r.get("bid_at") or
                                   r.get("ask_at") or "")[:19]
                            _age = ((_dt2.datetime.now() -
                                _dt2.datetime.strptime(_qt,
                                "%Y-%m-%d %H:%M:%S"))
                                .total_seconds()) if _qt else 0
                            if _age > RFQ_QUOTE_TTL:
                                _revE = _rfq_expire_quote(cur2, r, 'timeout')
                                if _revE:
                                    _did_off = True
                                    r.update(status="REQUESTED",
                                        off_flag="expired",
                                        bid_px="", ask_px="",
                                        q_rev=str(_revE),
                                        row_version=str(_rvE+1),
                                        _tok=str(_rvE+1))
                        except Exception:
                            pass
                    q2 = {"bid": _fnum(r.get("live_bid")),
                          "ask": _fnum(r.get("live_ask"))}
                    if q2["bid"] is None and q2["ask"] is None:
                        q2 = q
                    fl = _rfq_flag(r, q2)
                    if (fl["txt"] == "PULL"
                            and str(r.get("auto_q") or "") in
                            ("2", "2.0")
                            and r.get("status") in ("QUOTED",
                                                    "WORKING")):
                        try:
                            cbf = q2["bid"]
                            caf = q2["ask"]
                            _rvf = int(r.get("row_version") or 0)
                            _revf = int(r.get("q_rev") or 0) + 1
                            if cbf is None and caf is None:
                                raise Exception(
                                    "no live to follow")
                            nbf = None if cbf is None else \
                                round(round(cbf / 0.05) * 0.05, 2)
                            naf = None if caf is None else \
                                round(round(caf / 0.05) * 0.05, 2)
                            cur2.execute(
                                "UPDATE cba_app.rfq SET "
                                "bid_px=COALESCE(%s, bid_px), "
                                "ask_px=COALESCE(%s, ask_px), "
                                "bid_at=NOW(), "
                                "ask_at=NOW(), q_spot=%s, q_fx=%s, q_delta=%s, "
                                "q_delta_ovd=0, "
                                "stock_ref=COALESCE(%s, stock_ref), "
                                "fx_ref=COALESCE(%s, fx_ref), "
                                "delta=COALESCE(%s, delta), "
                                "q_rev=%s, last_updated=NOW(), "
                                "updated_by='autopilot', "
                                "row_version=row_version+1 "
                                "WHERE rfq_id=%s AND row_version=%s",
                                (nbf, naf,
                                 _fnum(r.get("live_spot")),
                                 _fnum(r.get("live_und")),
                                 live.get("nd"),
                                 _fnum(r.get("live_spot")),
                                 _fnum(r.get("live_und")),
                                 live.get("nd"),
                                 _revf, r["rfq_id"], _rvf))
                            if cur2.rowcount:
                                cur2.execute(
                                    "INSERT INTO cba_app.rfq_quote_hist (rfq_id, rev, bid, ask, spot, fx, delta, quoted_by, quoted_at, action) VALUES (%s,%s,%s,%s,%s,%s,%s,'autopilot',NOW(),'auto-follow')",
                                    (r["rfq_id"], _revf, nbf, naf,
                                     _fnum(r.get("live_spot")),
                                     _fnum(r.get("live_und")),
                                     _fnum(r.get("delta"))))
                                _did_off = True
                                r.update(bid_px="" if nbf is None else "%.2f" % nbf,
                                    ask_px="" if naf is None else "%.2f" % naf,
                                    q_rev=str(_revf),
                                    row_version=str(_rvf + 1),
                                    _tok=str(_rvf + 1))
                                fl = _rfq_flag(r, q2)
                        except Exception as _e2:
                            fl["txt"] += " \u26a0"
                            fl["title"] = (fl.get("title") or ""
                                ) + " [follow err: %s]" % _e2
                    elif (fl["txt"] == "PULL"
                            and str(r.get("auto_q") or "") in
                            ("1", "1.0")
                            and r.get("status") in ("QUOTED",
                                                    "WORKING")):
                        try:
                            _rv = int(r.get("row_version") or 0)
                            _rev = int(r.get("q_rev") or 0) + 1
                            _ob = _fnum(r.get("bid_px"))
                            _oa = _fnum(r.get("ask_px"))
                            cur2.execute(
                                "UPDATE cba_app.rfq SET "
                                "bid_px=NULL, ask_px=NULL, "
                                "bid_at=NULL, ask_at=NULL, "
                                "q_rev=%s, status=%s, "
                                "off_flag='auto', "
                                "off_by='auto-tol', "
                                "off_at=NOW(), "
                                "off_bid=%s, off_ask=%s, "
                                "refresh_by=NULL, refresh_at=NULL, "
                                "last_updated=NOW(), "
                                "updated_by='auto-tol', "
                                "row_version=row_version+1 "
                                "WHERE rfq_id=%s AND "
                                "row_version=%s",
                                (_rev, "REQUESTED", _ob, _oa,
                                 r["rfq_id"], _rv))
                            if not cur2.rowcount:
                                fl["txt"] += " \u26a0"
                                fl["title"] = (fl.get("title") or "") + " [auto-off skipped: version race]"
                            if cur2.rowcount:
                                cur2.execute(
                                    "INSERT INTO cba_app.rfq_quote_hist (rfq_id, rev, bid, ask, "
                                    "spot, fx, delta, quoted_by, quoted_at, action) VALUES "
                                    "(%s,%s,%s,%s,%s,%s,%s,"
                                    "'auto-tol',NOW(),'auto-off')",
                                    (r["rfq_id"], _rev, _ob, _oa,
                                     _fnum(r.get("stock_ref")),
                                     _fnum(r.get("fx_ref")),
                                     _fnum(r.get("delta"))))
                                cur2.execute(
                                    "INSERT INTO cba_app.rfq_log (rfq_id, field_name, old_value, "
                                    "new_value, changed_by, changed_at) VALUES "
                                    "(%s,'auto-off',%s,%s,"
                                    "'auto-tol',NOW())",
                                    (r["rfq_id"],
                                     fl["title"][:180],
                                     "breach -> quote OFF \u00b7 ADJUSTING"))
                                _did_off = True
                                r["status"] = "REQUESTED"
                                r["off_flag"] = "auto"
                                r["off_by"] = "auto-tol"
                                r["bid_px"] = r["ask_px"] = ""
                                r["bid_at"] = r["ask_at"] = ""
                                r["q_rev"] = str(_rev)
                                r["row_version"] = str(_rv + 1)
                                r["_tok"] = str(_rv + 1)
                                fl = _rfq_flag(r, q)
                        except Exception:
                            pass
                    if (_fnum(r.get("bid_px")) is None
                            and _fnum(r.get("ask_px")) is None
                            and r.get("off_flag") != "expired"
                            and str(r.get("auto_q") or "") in
                            ("1", "1.0", "True")):
                        try:
                            lb2 = q2["bid"]
                            la2 = q2["ask"]
                            cb2 = _fnum(r.get("calc_bid"))
                            ca2 = _fnum(r.get("calc_ask"))
                            _t2 = _fnum(r.get("tol"))
                            tl = _t2 if (_t2 and _t2 > 0) else RFQ_PX_TOL
                            back = all(
                                abs(x - y) <= tl for x, y in
                                ((cb2, lb2), (ca2, la2))
                                if x is not None and y is not None) and ((cb2 is not None and lb2 is not None) or (ca2 is not None and la2 is not None))
                            if back:
                                _rv2 = int(r.get("row_version") or 0)
                                _rev2 = int(r.get("q_rev") or 0) + 1
                                nb2 = None if cb2 is None else round(round(cb2 / 0.05) * 0.05, 2)
                                na2 = None if ca2 is None else round(round(ca2 / 0.05) * 0.05, 2)
                                cur2.execute(
                                    "UPDATE cba_app.rfq SET "
                                    "bid_px=%s, ask_px=%s, "
                                    "bid_at=NOW(), ask_at=NOW(), "
                                    "q_spot=%s, q_fx=%s, "
                                    "q_rev=%s, status=%s, "
                                    "off_flag=NULL, off_by=NULL, "
                                    "off_at=NULL, off_bid=NULL, "
                                    "off_ask=NULL, "
                                    "last_updated=NOW(), "
                                    "updated_by='autopilot', "
                                    "row_version=row_version+1 "
                                    "WHERE rfq_id=%s AND "
                                    "row_version=%s",
                                    (nb2, na2,
                                     _fnum(r.get("live_spot")),
                                     _fnum(r.get("live_und")),
                                     _rev2,
                                     "WORKING" if r.get("ord_side") else "QUOTED",
                                     r["rfq_id"], _rv2))
                                if cur2.rowcount:
                                    cur2.execute(
                                        "INSERT INTO cba_app.rfq_quote_hist (rfq_id, rev, bid, ask, spot, fx, delta, quoted_by, quoted_at, action) VALUES (%s,%s,%s,%s,%s,%s,%s,'autopilot',NOW(),'auto-restore')",
                                        (r["rfq_id"], _rev2, nb2, na2,
                                         _fnum(r.get("live_spot")),
                                         _fnum(r.get("live_und")),
                                         _fnum(r.get("delta"))))
                                    _did_off = True
                                    r.update(status=("WORKING" if r.get("ord_side") else "QUOTED"),
                                        off_flag="", off_by="",
                                        bid_px="" if nb2 is None else "%.2f" % nb2,
                                        ask_px="" if na2 is None else "%.2f" % na2,
                                        q_rev=str(_rev2),
                                        row_version=str(_rv2 + 1), _tok=str(_rv2 + 1))
                                    fl = _rfq_flag(r, q2)
                        except Exception as _e:
                            fl["txt"] += " \u26a0"
                            fl["title"] = (fl.get("title") or ""
                                ) + " [autopilot err: %s]" % _e
                    _aq = str(r.get("auto_q") or "")
                    _mode = ("FOLW(2)" if _aq in ("2", "2.0")
                             else "TOL(1)" if _aq in
                             ("1", "1.0", "True") else "manual")
                    r["flag"], r["flag_cls"] = fl["txt"], fl["cls"]
                    r["flag_title"] = (fl["title"] or "") + \
                        " \u00b7 algo: " + _mode
            try:
                if _did_off:
                    conn2.commit()
            finally:
                try:
                    cur2.close()
                    conn2.close()
                except Exception:
                    pass
            payload = {"ok": True, "rows": rows,
                       "ms": int((time.time() - _t0) * 1000),
                       "qttl": RFQ_QUOTE_TTL,
                       "build": "r107",
                       "editable": sorted(RFQ_EDITABLE)}
            RFQ_SNAP["data"] = payload
            RFQ_SNAP["ts"] = time.time()
            try:
                _sg = _rfq_signature(rows)
                if RFQ_REV["sig"] is not None and _sg != RFQ_REV["sig"]:
                    _rfq_touch("engine")
                    RFQ_SNAP["data"] = payload   # keep the fresh build
                    RFQ_SNAP["ts"] = time.time()
                RFQ_REV["sig"] = _sg
            except Exception:
                pass
            return payload
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


class RfqCreate(BaseModel):
    sec_id: str = ""
    short_name: str = ""
    style: str = "outright"
    sides: str = "two_way"
    qty: str = ""
    client: str = ""
    user: str = "lagrange"
    ord_side: str = ""      # "", buy, sell, two -> working order
    ord_level: str = ""
    ord_level2: str = ""   # ask-side level for two-way orders
    vs: str = ""            # stock ref (required for versus orders)
    fx: str = ""
    delta: str = ""


@app.post("/api/rfq/create")
def api_rfq_create(req: RfqCreate, request: Request):
    if req.style not in RFQ_STYLES:
        return JSONResponse(status_code=400, content={
            "ok": False, "error": "style must be outright, vs or working"})
    if req.sides not in RFQ_SIDES:
        return JSONResponse(status_code=400, content={
            "ok": False, "error": "sides must be two_way, bid or ask"})
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or (req.user or "lagrange").strip()[:50] or "lagrange")
    try:
        _ensure_rfq()
        qty = None
        if str(req.qty).strip():
            try:
                qty = _Dec(str(req.qty).replace(",", ""))
            except _DecErr:
                return JSONResponse(status_code=400, content={
                    "ok": False, "error": f"Invalid number: {req.qty!r}"})
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                # identifier mapping at submission - refdata is the
                # authority (ric / sec_fx=ccy / und_fx / isin), latest
                # cb_nuke batch is the fallback
                m_isin = m_ric = m_undfx = m_secfx = None
                if req.sec_id:
                    rd = _rfq_refdata(req.sec_id)
                    m_isin = (rd.get("isin") or "").strip() or None
                    m_ric = (rd.get("ric") or "").strip() or None
                    m_undfx = (rd.get("und_fx") or "").strip() or None
                    m_secfx = (rd.get("sec_fx") or "").strip() or None
                    if not (m_isin and m_ric and m_undfx):
                        try:
                            cur.execute(
                                "SELECT isin, ric, und_fx FROM "
                                "cba_app.cb_nuke WHERE sec_id=%s AND "
                                "saved_at=(SELECT MAX(saved_at) FROM "
                                "cba_app.cb_nuke) LIMIT 1", (req.sec_id,))
                            hit = cur.fetchone()
                            if hit:
                                m_isin = m_isin or hit[0]
                                m_ric = m_ric or hit[1]
                                m_undfx = m_undfx or hit[2]
                        except Exception:
                            pass
                    st = _rfq_state() or {}
                    m_ric = _rfq_stock_ric(st, req.sec_id, m_ric)
                    def _pk(d, k):
                        return (d.get(k) or d.get(str(k)) or
                                (d.get(int(k)) if str(k).isdigit() else None))
                    row = _pk(st.get("rows", {}) or {}, req.sec_id) or {}
                    m_undfx = (row.get("und_fx") or m_undfx or None)
                _ords = (req.ord_side or "").strip().lower()
                if _ords not in ("buy", "sell", "two"):
                    _ords = ""
                # order direction never forces the side: the
                # side stays as chosen (two-way / bid / ask);
                # client BUYS only makes the bid level compulsory,
                # client SELLS the ask level.
                _sides = req.sides
                _st0 = "REQUESTED"   # orders show ORD REQ until quoted
                def _cn(x):
                    return _fnum(str(x or "").replace("%", "")
                                 .replace(",", ""))
                _lvl = _cn(req.ord_level)
                _lvl2 = _cn(req.ord_level2)
                _vs = _cn(req.vs)
                _fx = _cn(req.fx)
                _dl = _cn(req.delta)
                _btol = _baq = None
                if m_isin:
                    try:
                        cur.execute("SELECT tol, autopilot FROM "
                                    "cba_app.rfq_bond_cfg "
                                    "WHERE isin=%s", (m_isin,))
                        _h2 = cur.fetchone()
                        if _h2:
                            _btol = _h2[0]
                            _baq = int(_h2[1]) if _h2[1] else None
                    except Exception:
                        pass
                if _ords:
                    if _ords in ("buy", "two") and _lvl is None:
                        raise BLVal("client BUYS: bid level is compulsory")
                    if _ords in ("sell", "two") and _lvl2 is None:
                        raise BLVal("client SELLS: ask level is compulsory")
                cur.execute(
                    "INSERT INTO cba_app.rfq (created_at, trade_date, created_by, "
                    "sec_id, short_name, isin, ric, und_fx, sec_fx, ccy, "
                    "style, sides, qty, client, ord_side, ord_level, ord_level2, "
                    "req_vs, req_fx, delta, tol, auto_q, "
                    "status, updated_by, row_version) VALUES "
                    "(NOW(), CURDATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)",
                    (user, req.sec_id or None, req.short_name[:64],
                     m_isin, m_ric,
                     (m_undfx or "")[:24] or None,
                     (m_secfx or "")[:10] or None,
                     (m_secfx or "")[:10] or None,
                     req.style, _sides, qty,
                     (req.client or "").strip()[:100] or None,
                     _ords or None, _lvl, _lvl2, _vs, _fx, _dl,
                     _btol, _baq,
                     _st0, user))
                rid = cur.lastrowid
                cur.execute(
                    "INSERT INTO cba_app.rfq_log (rfq_id, field_name, "
                    "old_value, new_value, changed_by, changed_at) "
                    "VALUES (%s,'created','',%s,%s,NOW())",
                    (rid, (f"{req.style}/{_sides} qty "
                           f"{req.qty or '-'} "
                           + (f"ORDER client {_ords} @ {req.ord_level or '-'} "
                              if _ords else "")
                           + f"{req.short_name}")[:200], user))
            conn.commit()
            return {"ok": True, "rfq_id": rid}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


class RfqEdit(BaseModel):
    rfq_id: int
    field: str
    value: str = ""
    token: str = "0"
    user: str = "lagrange"


@app.post("/api/rfq/edit")
def api_rfq_edit(req: RfqEdit, request: Request):
    f = req.field
    if f not in RFQ_EDITABLE:
        return JSONResponse(status_code=400, content={
            "ok": False, "error": f"Field '{f}' is not editable."})
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or (req.user or "lagrange").strip()[:50] or "lagrange")
    v = req.value.strip()
    try:
        if f == "hit" and v not in ("", "bid", "ask"):
            raise BLVal("hit must be blank, bid or ask")
        if f == "status" and v not in RFQ_STATUS:
            raise BLVal("Status must be one of " + ", ".join(RFQ_STATUS) + ".")
        if f == "style" and v not in RFQ_STYLES:
            raise BLVal("style must be outright, vs or working")
        if f == "sides" and v not in RFQ_SIDES:
            raise BLVal("sides must be two_way, bid or ask")
        if f == "isin" and v and len(v) != 12:
            raise BLVal("ISIN must be exactly 12 characters (or blank).")
        if f == "trade_date" and v:
            try:
                _dt.strptime(v, "%Y-%m-%d")
            except ValueError:
                raise BLVal("trade_date must be YYYY-MM-DD.")
        val = v or None
        if f in ("qty", "stock_ref", "fx_ref", "delta") and v:
            try:
                val = _Dec(v.replace(",", ""))
            except _DecErr:
                raise BLVal(f"Invalid number: {v!r}")
            if f == "delta" and not (_Dec("0") <= val <= _Dec("100")):
                raise BLVal("Delta must be between 0 and 100 (percent).")
            if f == "qty" and val < 0:
                raise BLVal("Quantity must be >= 0.")
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT `%s`, row_version, sec_id, style "
                            "FROM cba_app.rfq "
                            "WHERE rfq_id=%%s FOR UPDATE" % f,
                            (req.rfq_id,))
                hit = cur.fetchone()
                if not hit:
                    conn.rollback()
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "rfq not found"})
                old, rv, r_sec, r_style = hit
                _LWW = ("tol", "auto_q", "notes", "client",
                        "qty", "ord_level", "ord_level2",
                        "req_vs", "req_fx", "q_delta", "db1", "db1s",
                        "db2", "db2s", "da1", "da1s",
                        "da2", "da2s",
                        "stock_ref", "fx_ref", "delta")
                if f not in _LWW and \
                        int(rv or 0) != int(req.token or 0):
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False,
                        "error": "Row changed underneath you (someone else "
                                 "edited it) - refreshing; please re-apply."})
                extra_sql, extra_vals = "", []
                if f == "q_delta":
                    # trader typed (or cleared) the quote delta:
                    # remember it is an override, not a live stamp
                    extra_sql += ", q_delta_ovd=%s"
                    extra_vals.append(1 if val is not None else 0)
                if f == "hit":
                    cur.execute("SELECT status, bid_px, ask_px, "
                                "ric, und_fx, stock_ref, fx_ref, "
                                "delta, ord_side FROM cba_app.rfq "
                                "WHERE rfq_id=%s", (req.rfq_id,))
                    (h_st, h_pb, h_pa, h_ric, h_ufx, h_os, h_of,
                     h_od, h_ord) = cur.fetchone()
                    if v in ("bid", "ask"):
                        if h_st in ("DONE", "CANCELLED"):
                            raise BLVal("Line is booked / "
                                        "cancelled - hit is "
                                        "locked.")
                        if h_st not in ("QUOTED", "WORKING",
                                        "HIT"):
                            raise BLVal("Client can only deal on "
                                        "a standing quote - "
                                        "status must be QUOTED "
                                        "or WORKING (press Q "
                                        "first).")
                        px = h_pb if v == "bid" else h_pa
                        if _fnum(px) is None:
                            raise BLVal(f"No standing {v} to "
                                        "deal on - quote that "
                                        "side first.")
                        trk = _rfq_track(r_sec, h_ric, h_ufx)
                        eff = {
                            "stock_ref": _fnum(h_os)
                            if _fnum(h_os) is not None
                            else trk["stock_ref"],
                            "fx_ref": _fnum(h_of)
                            if _fnum(h_of) is not None
                            else trk["fx_ref"],
                            "delta": _fnum(h_od)
                            if _fnum(h_od) is not None
                            else trk["delta"]}
                        for col in ("stock_ref", "fx_ref",
                                    "delta"):
                            if eff[col] is not None:
                                extra_sql += f", `{col}`=%s"
                                extra_vals.append(eff[col])
                        extra_sql += (", status='HIT', "
                                      "refresh_by=NULL, "
                                      "refresh_at=NULL, "
                                      "q_spot=%s, q_fx=%s")
                        extra_vals += [eff["stock_ref"],
                                       eff["fx_ref"]]
                    elif v == "" and h_st == "HIT":
                        rev_st = "WORKING" if (r_style ==
                            "working" or h_ord) else (
                            "QUOTED" if (_fnum(h_pb) is not None
                                         or _fnum(h_pa)
                                         is not None)
                            else "REQUESTED")
                        extra_sql += ", status=%s"
                        extra_vals.append(rev_st)
                if f == "status" and v in ("DONE", "CANCELLED", "EXPIRED"):
                    cur.execute("SELECT sec_id, ric, und_fx, stock_ref, "
                                "fx_ref, delta, style FROM cba_app.rfq "
                                "WHERE rfq_id=%s", (req.rfq_id,))
                    (fs, fric, fufx, fo_s, fo_f, fo_d,
                     fsty) = cur.fetchone()
                    trk = _rfq_track(fs, fric, fufx)
                    # trader overrides win; live only fills blanks
                    eff = {
                        "stock_ref": _fnum(fo_s)
                        if _fnum(fo_s) is not None
                        else trk["stock_ref"],
                        "fx_ref": _fnum(fo_f)
                        if _fnum(fo_f) is not None
                        else trk["fx_ref"],
                        "delta": _fnum(fo_d)
                        if _fnum(fo_d) is not None
                        else trk["delta"]}
                    frz = []
                    for col, oldv in (("stock_ref", fo_s),
                                      ("fx_ref", fo_f),
                                      ("delta", fo_d)):
                        if eff[col] is not None:
                            extra_sql += f", `{col}`=%s"
                            extra_vals.append(eff[col])
                            if str(oldv) != str(eff[col]):
                                frz.append((col, oldv, eff[col]))
                    cur.execute("SELECT bid_px, ask_px FROM "
                                "cba_app.rfq WHERE rfq_id=%s",
                                (req.rfq_id,))
                    _qb, _qa = cur.fetchone()
                    lvq = _rfq_live(fs, fsty,
                                    ovd_spot=eff["stock_ref"],
                                    ovd_delta=eff["delta"])
                    for col, cur_v, pv in (
                            ("bid_px", _qb, lvq.get("bid")),
                            ("ask_px", _qa, lvq.get("ask"))):
                        if _fnum(cur_v) is None and pv is not None:
                            extra_sql += f", `{col}`=%s"
                            extra_vals.append(pv)
                    extra_sql += (", bid_at=NOW(), ask_at=NOW(), "
                                  "q_spot=%s, q_fx=%s")
                    extra_vals += [eff["stock_ref"], eff["fx_ref"]]
                cur.execute(
                    ("UPDATE cba_app.rfq SET `%s`=%%s" % f) + extra_sql +
                    ", last_updated=NOW(), updated_by=%s, "
                    "row_version=row_version+1 "
                    "WHERE rfq_id=%s AND row_version=%s",
                    [val] + extra_vals + [user, req.rfq_id, rv])
                cur.execute(
                    "INSERT INTO cba_app.rfq_log (rfq_id, field_name, "
                    "old_value, new_value, changed_by, changed_at) "
                    "VALUES (%s,%s,%s,%s,%s,NOW())",
                    (req.rfq_id, f,
                     None if old is None else str(old),
                     None if val is None else str(val), user))
                if f == "status" and v in ("DONE", "CANCELLED", "EXPIRED"):
                    for col, oldv, newv in frz:
                        cur.execute(
                            "INSERT INTO cba_app.rfq_log (rfq_id, "
                            "field_name, old_value, new_value, changed_by, "
                            "changed_at) VALUES (%s,%s,%s,%s,%s,NOW())",
                            (req.rfq_id, col,
                             None if oldv is None else str(oldv),
                             str(newv), user))
            conn.commit()
            if f == "hit" and v:
                if True:   # side-consistency: ALL roles
                    with conn.cursor() as c5:
                        c5.execute("SELECT sides FROM cba_app.rfq "
                                   "WHERE rfq_id=%s",
                                   (req.rfq_id,))
                        _sd = ((c5.fetchone() or ["two_way"])[0]
                               or "two_way")
                    _okb = _sd in ("two_way", "bid")
                    _oka = _sd in ("two_way", "ask", "offer")
                    if ((v == "bid" and not _okb)
                            or (v == "ask" and not _oka)):
                        return JSONResponse(status_code=409,
                            content={"ok": False, "error":
                            "side is '%s' - only the %s can be hit on this trade" % (_sd, "bid" if _okb else "ask")})
            if f == "auto_q" and (_fnum(v) or 0) in (1, 2):
                with conn.cursor() as c4:
                    c4.execute("SELECT tol FROM cba_app.rfq "
                               "WHERE rfq_id=%s", (req.rfq_id,))
                    _ht = c4.fetchone()
                if not _ht or _ht[0] is None:
                    return JSONResponse(status_code=409,
                        content={"ok": False, "error":
                        "arming TOL/FOLW needs a Tol value "
                        "on the row first"})
            if f in ("tol", "auto_q"):
                try:
                    with conn.cursor() as c3:
                        c3.execute("SELECT isin, short_name FROM "
                                   "cba_app.rfq WHERE rfq_id=%s",
                                   (req.rfq_id,))
                        _bi = c3.fetchone()
                        if _bi and (_bi[0] or "").strip():
                            _col = ("tol" if f == "tol" else
                                    "autopilot")
                            c3.execute(
                                "INSERT INTO cba_app.rfq_bond_cfg (isin, short_name, " + _col + ", updated_by, updated_at) VALUES (%s,%s,%s,%s,NOW()) ON DUPLICATE KEY UPDATE " + _col +
                                "=VALUES(" + _col + "), updated_by=VALUES(updated_by), updated_at=NOW()",
                                (_bi[0].strip().upper()[:20],
                                 (_bi[1] or "")[:64],
                                 _fnum(v) if f == "tol" else
                                 (int(_fnum(v) or 0) or None),
                                 user))
                    conn.commit()
                except Exception:
                    pass
            return {"ok": True, "token": str(int(rv or 0) + 1),
                    "value": "" if val is None else str(val)}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


def _rfq_track(sec_id, r_ric="", r_undfx=""):
    """Live-tracked refs: stock_ref <- override spot chain, fx_ref <-
    override und-fx chain, delta <- nDelta*100. Pure computation - never
    writes, never bumps row_version."""
    st = _rfq_state() or {}
    def _pk(d, k):
        return (d.get(k) or d.get(str(k)) or
                (d.get(int(k)) if str(k).isdigit() else None))
    row = _pk(st.get("rows", {}) or {}, sec_id) or {}
    nk = _pk(st.get("nuke", {}) or {}, sec_id) or {}
    rfx = st.get("rfx", {}) or {}
    ric = _rfq_stock_ric(st, sec_id, nk.get("ric") or r_ric or "")
    fxric = (row.get("und_fx") or r_undfx or "")
    sref = _fnum(row.get("ovdSpot"))
    if sref is None and ric:
        sref = _fnum((rfx.get(ric) or {}).get("last"))
    if sref is None:
        sref = _fnum(nk.get("liveSpot"))
    fref = _fnum(row.get("ovdUndFx"))
    if fref is None and fxric:
        fref = _fnum((rfx.get(fxric) or {}).get("last"))
    if fref is None:
        fref = _fnum(nk.get("nSpotFx"))
    nd = _fnum(nk.get("nDelta"))
    return {"stock_ref": sref, "fx_ref": fref,
            "delta": None if nd is None else round(nd * 100.0, 2)}


class RfqQuote(BaseModel):
    rfq_id: int
    token: str = "0"
    user: str = "lagrange"
    side: str = "both"


@app.post("/api/rfq/quote")
def api_rfq_quote(req: RfqQuote, request: Request):
    """Confirm the current computed Bid/Ask as the STANDING client
    quote: snap to the 0.05 grid, bump q_rev, stamp q_spot/q_fx/
    bid_at/ask_at, append to rfq_quote_hist, and auto-advance
    REQUESTED -> QUOTED. The previous standing quote survives in
    history."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or (req.user or "lagrange").strip()[:50] or "lagrange")
    try:
        if os.environ.get("LAGRANGE_TEST_LIVE") != "FILE":
            _c0 = rc.connect()
            try:
                with _c0.cursor() as _k0:
                    _k0.execute("SELECT stock_ref, fx_ref FROM "
                                "cba_app.rfq WHERE rfq_id=%s",
                                (req.rfq_id,))
                    _h0 = _k0.fetchone()
                if _h0 and (_h0[0] is None or _h0[1] is None):
                    return JSONResponse(status_code=400, content={
                        "ok": False, "error": "cannot quote: "
                        "OvdSpot and OvdFx are required"})
            finally:
                _c0.close()
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                if req.side not in ("both", "bid", "ask"):
                    raise BLVal("side must be both, bid or ask")
                cur.execute("SELECT status, style, sec_id, ric, "
                            "und_fx, stock_ref, fx_ref, delta, "
                            "q_rev, row_version, bid_px, ask_px, "
                            "ord_side, q_delta, "
                            "COALESCE(q_delta_ovd,0) FROM cba_app.rfq "
                            "WHERE rfq_id=%s FOR UPDATE",
                            (req.rfq_id,))
                r = cur.fetchone()
                if not r:
                    conn.rollback()
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "rfq not found"})
                (status, style, sec_id, ric, undfx, o_s, o_f, o_d,
                 q_rev, rv, cur_b, cur_a, q_ord, q_qd, q_qd_ovd) = r
                if int(rv or 0) != int(req.token or 0):
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False,
                        "error": "Row changed underneath you - "
                                 "refreshing; please re-quote."})
                if status in ("HIT", "DONE", "CANCELLED"):
                    raise BLVal("Frozen line - cannot requote a "
                                "HIT / DONE / CANCELLED RFQ.")
                trk = _rfq_track(sec_id, ric, undfx)
                eff_s = _fnum(o_s) if _fnum(o_s) is not None \
                    else trk["stock_ref"]
                eff_f = _fnum(o_f) if _fnum(o_f) is not None \
                    else trk["fx_ref"]
                eff_d = _rfq_pick_delta(q_qd, q_qd_ovd, o_d,
                                        trk["delta"])
                lv = _rfq_live(sec_id, style, ovd_fx=eff_f,
                               ovd_spot=eff_s,
                               ovd_delta=eff_d)
                if lv.get("bid") is None and lv.get("ask") is None:
                    raise BLVal("No model quote - nuke this "
                                "security first.")
                def grid(x):
                    return None if x is None else \
                        round(round(x / 0.05) * 0.05, 2)
                qb = grid(lv.get("bid")) if req.side in \
                    ("both", "bid") else _fnum(cur_b)
                qa = grid(lv.get("ask")) if req.side in \
                    ("both", "ask") else _fnum(cur_a)
                if req.side == "bid" and grid(lv.get("bid")) \
                        is None:
                    raise BLVal("No model bid to refresh to.")
                if req.side == "ask" and grid(lv.get("ask")) \
                        is None:
                    raise BLVal("No model ask to refresh to.")
                if qb is None and qa is None:
                    raise BLVal("cannot quote: no model prices "
                                "to confirm (both sides empty)")
                rev = int(q_rev or 0) + 1
                new_status = (("WORKING" if q_ord else "QUOTED")
                              if status in ("REQUESTED",
                                            "IMPROVE")
                              else status)
                cur.execute(
                    "UPDATE cba_app.rfq SET bid_px=%s, ask_px=%s, "
                    "q_spot=%s, q_fx=%s, "
                    "refresh_by=NULL, refresh_at=NULL, "
                    "off_flag=NULL, off_by=NULL, off_at=NULL, "
                    + ("bid_at=NOW(), " if req.side in
                       ("both", "bid") else "")
                    + ("ask_at=NOW(), " if req.side in
                       ("both", "ask") else "")
                    + "q_rev=%s, status=%s, q_delta=%s, "
                    "adj_req=NULL, "
                    "last_updated=NOW(), updated_by=%s, "
                    "row_version=row_version+1 "
                    "WHERE rfq_id=%s AND row_version=%s",
                    (qb, qa, eff_s, eff_f, rev, new_status,
                     (eff_d if eff_d is not None
                      else lv.get("nd")), user,
                     req.rfq_id, rv))
                cur.execute(
                    "INSERT INTO cba_app.rfq_quote_hist (rfq_id, "
                    "rev, bid, ask, spot, fx, delta, quoted_by, "
                    "quoted_at, action) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)",
                    (req.rfq_id, rev, qb, qa, eff_s, eff_f, eff_d,
                     user, req.side))
                cur.execute(
                    "INSERT INTO cba_app.rfq_log (rfq_id, "
                    "field_name, old_value, new_value, changed_by,"
                    " changed_at) VALUES "
                    "(%s,'quote',%s,%s,%s,NOW())",
                    (req.rfq_id, f"rev {rev - 1}",
                     f"rev {rev}: {qb} / {qa}", user))
            conn.commit()
            return {"ok": True, "token": str(int(rv or 0) + 1),
                    "rev": rev, "bid": qb, "ask": qa,
                    "status": new_status}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


class RfqRefresh(BaseModel):
    rfq_id: int
    token: str = "0"
    user: str = "lagrange"


@app.post("/api/rfq/refresh")
def api_rfq_refresh(req: RfqRefresh, request: Request):
    """Sales asks the trader to refresh the quote: stamps
    refresh_by / refresh_at on an active line. Cleared
    automatically by any trader quote op (Q / side refresh /
    pull)."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or (req.user or "lagrange").strip()[:50] or "lagrange")
    try:
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status, refresh_by, row_version "
                            "FROM cba_app.rfq WHERE rfq_id=%s "
                            "FOR UPDATE", (req.rfq_id,))
                r = cur.fetchone()
                if not r:
                    conn.rollback()
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "rfq not found"})
                status, old_rf, rv = r
                if int(rv or 0) != int(req.token or 0):
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False,
                        "error": "Row changed underneath you - "
                                 "refreshing; please retry."})
                if status in ("HIT", "DONE", "CANCELLED"):
                    raise BLVal("Frozen line - nothing to refresh.")
                cur.execute(
                    "UPDATE cba_app.rfq SET refresh_by=%s, "
                    "refresh_at=NOW(), last_updated=NOW(), "
                    "updated_by=%s, row_version=row_version+1 "
                    "WHERE rfq_id=%s AND row_version=%s",
                    (user, user, req.rfq_id, rv))
                cur.execute(
                    "INSERT INTO cba_app.rfq_log (rfq_id, "
                    "field_name, old_value, new_value, changed_by,"
                    " changed_at) VALUES "
                    "(%s,'refresh',%s,%s,%s,NOW())",
                    (req.rfq_id, old_rf or "", user, user))
            conn.commit()
            return {"ok": True, "token": str(int(rv or 0) + 1),
                    "refresh_by": user}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


class RfqPull(BaseModel):
    rfq_id: int
    token: str = "0"
    user: str = "lagrange"
    side: str = "both"


@app.post("/api/rfq/pull")
def api_rfq_pull(req: RfqPull, request: Request):
    """OFF the quote: clear the standing bid / ask / both. Pulling
    the last standing side reverts QUOTED -> REQUESTED. Logged as
    a revision (action pull*) so the withdrawal is auditable."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or (req.user or "lagrange").strip()[:50] or "lagrange")
    try:
        if req.side not in ("both", "bid", "ask"):
            raise BLVal("side must be both, bid or ask")
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status, bid_px, ask_px, q_rev, "
                            "stock_ref, fx_ref, delta, row_version "
                            "FROM cba_app.rfq WHERE rfq_id=%s "
                            "FOR UPDATE", (req.rfq_id,))
                r = cur.fetchone()
                if not r:
                    conn.rollback()
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "rfq not found"})
                (status, cur_b, cur_a, q_rev, o_s, o_f, o_d,
                 rv) = r
                if int(rv or 0) != int(req.token or 0):
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False,
                        "error": "Row changed underneath you - "
                                 "refreshing; please retry."})
                if status in ("HIT", "DONE", "CANCELLED"):
                    raise BLVal("Frozen line - nothing to pull.")
                pull_b = req.side in ("both", "bid")
                pull_a = req.side in ("both", "ask")
                if pull_b and pull_a and _fnum(cur_b) is None \
                        and _fnum(cur_a) is None:
                    raise BLVal("No standing quote to pull.")
                if req.side == "bid" and _fnum(cur_b) is None:
                    raise BLVal("No standing bid to pull.")
                if req.side == "ask" and _fnum(cur_a) is None:
                    raise BLVal("No standing ask to pull.")
                nb = None if pull_b else _fnum(cur_b)
                na = None if pull_a else _fnum(cur_a)
                rev = int(q_rev or 0) + 1
                _pulled_all = nb is None and na is None
                new_status = "REQUESTED" if (_pulled_all and
                    status == "QUOTED") else status
                _off = _pulled_all and status in ("QUOTED",
                                                 "WORKING")
                cur.execute(
                    "UPDATE cba_app.rfq SET bid_px=%s, ask_px=%s, "
                    "refresh_by=NULL, refresh_at=NULL, "
                    + ("off_flag='manual', off_by=%s, "
                       "off_at=NOW(), " if _off else "")
                    + "q_rev=%s, status=%s, last_updated=NOW(), "
                    "updated_by=%s, row_version=row_version+1 "
                    "WHERE rfq_id=%s AND row_version=%s",
                    ([nb, na] + ([user] if _off else [])
                     + [rev, new_status, user, req.rfq_id,
                        rv]))
                act = "pull" if req.side == "both" \
                    else "pull_" + req.side
                cur.execute(
                    "INSERT INTO cba_app.rfq_quote_hist (rfq_id, "
                    "rev, bid, ask, spot, fx, delta, quoted_by, "
                    "quoted_at, action) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)",
                    (req.rfq_id, rev, nb, na, _fnum(o_s),
                     _fnum(o_f), _fnum(o_d), user, act))
                cur.execute(
                    "INSERT INTO cba_app.rfq_log (rfq_id, "
                    "field_name, old_value, new_value, changed_by,"
                    " changed_at) VALUES "
                    "(%s,'quote',%s,%s,%s,NOW())",
                    (req.rfq_id, f"rev {rev - 1}",
                     f"rev {rev}: {act}", user))
            conn.commit()
            return {"ok": True, "token": str(int(rv or 0) + 1),
                    "rev": rev, "status": new_status}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.get("/api/rfq/history")
def api_rfq_history(rfq_id: int = 0):
    """Quoting history for one RFQ, newest first, for the side
    panel."""
    try:
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT rev, action, bid, ask, spot, fx, delta, "
                    "quoted_by, quoted_at FROM "
                    "cba_app.rfq_quote_hist WHERE rfq_id=%s "
                    "ORDER BY rev DESC LIMIT 100", (rfq_id,))
                cols = ["rev", "action", "bid", "ask", "spot", "fx",
                        "delta", "quoted_by", "quoted_at"]
                rows = [{c: ("" if v is None else str(v))
                         for c, v in zip(cols, rr)}
                        for rr in cur.fetchall()]
                cur.execute(
                    "SELECT field_name, old_value, new_value, "
                    "changed_by, changed_at FROM cba_app.rfq_log "
                    "WHERE rfq_id=%s ORDER BY log_id DESC "
                    "LIMIT 120", (rfq_id,))
                ecols = ["field", "old", "new", "by", "at"]
                events = [{c: ("" if v is None else str(v))
                           for c, v in zip(ecols, rr)}
                          for rr in cur.fetchall()]
            return {"ok": True, "rows": rows, "events": events}
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


class RfqAck(BaseModel):
    rfq_id: int
    token: str = "0"
    user: str = "lagrange"


@app.post("/api/rfq/ack")
def api_rfq_ack(req: RfqAck, request: Request):
    """Trader acknowledgment of a hit & done trade: stamps ack_by /
    ack_at. Requires status DONE and a hit side; optimistic lock;
    logged to rfq_log."""
    user = ((getattr(request.state, "auth", None) or {}).get("user")
            or (req.user or "lagrange").strip()[:50] or "lagrange")
    try:
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status, hit, ack_by, row_version "
                            "FROM cba_app.rfq WHERE rfq_id=%s "
                            "FOR UPDATE", (req.rfq_id,))
                r = cur.fetchone()
                if not r:
                    conn.rollback()
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "rfq not found"})
                status, hitside, old_ack, rv = r
                if int(rv or 0) != int(req.token or 0):
                    conn.rollback()
                    return JSONResponse(status_code=409, content={
                        "ok": False,
                        "error": "Row changed underneath you - "
                                 "refreshing; please re-ack."})
                if status not in ("HIT", "DONE"):
                    raise BLVal("Only HIT (client-dealt) trades "
                                "can be acknowledged.")
                if hitside not in ("bid", "ask"):
                    raise BLVal("Set Hit (bid or ask) first - ack "
                                "confirms the dealt side.")
                cur.execute("UPDATE cba_app.rfq SET ack_by=%s, "
                            "ack_at=NOW(), status='DONE', "
                            "last_updated=NOW(), "
                            "updated_by=%s, "
                            "row_version=row_version+1 "
                            "WHERE rfq_id=%s AND row_version=%s",
                            (user, user, req.rfq_id, rv))
                cur.execute("INSERT INTO cba_app.rfq_log (rfq_id, "
                            "field_name, old_value, new_value, "
                            "changed_by, changed_at) VALUES "
                            "(%s,'ack',%s,%s,%s,NOW())",
                            (req.rfq_id, old_ack, user, user))
            conn.commit()
            return {"ok": True, "token": str(int(rv or 0) + 1),
                    "ack_by": user}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


RFQ_UPLOAD_ENABLED = False        # flip to True to write trade_blotter


class RfqUpload(BaseModel):
    rfq_id: int
    user: str = "lagrange"


class RfqReject(BaseModel):
    rfq_id: int
    token: str = ""
    user: str = ""


@app.post("/api/rfq/reject")
def api_rfq_reject(req: RfqReject, request: Request):
    """Trader busts a HIT: back to the pre-hit state, hit
    cleared, audited as 'reject'."""
    try:
        _ensure_rfq()
        user = ((getattr(request.state, "auth", None) or {}).get("user")
                or (req.user or "lagrange").strip()[:50] or "lagrange")
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status, hit, bid_px, ask_px, "
                            "style, row_version, q_rev, stock_ref, "
                            "fx_ref, delta, ord_side FROM cba_app.rfq "
                            "WHERE rfq_id=%s", (req.rfq_id,))
                row = cur.fetchone()
                if not row:
                    raise BLVal("RFQ not found.")
                (status, hitside, pb, pa, style, rv, qrev,
                 s_ref, f_ref, dlt, r_ord) = row
                if status != "HIT":
                    raise BLVal("Only HIT (client-dealt) lines "
                                "can be rejected.")
                if req.token and str(rv) != str(req.token):
                    return JSONResponse(status_code=409, content={
                        "ok": False, "error": "Row changed elsewhere - refreshed."})
                rev_st = "REQUESTED"
                # bust kills the quote; an ORDER row keeps
                # ord_side and shows ORD REQ until re-quoted
                cur.execute("UPDATE cba_app.rfq SET "
                            "status=%s, "
                            "hit=NULL, bid_px=NULL, ask_px=NULL, "
                            "bid_at=NULL, ask_at=NULL, "
                            "q_rev=q_rev+1, refresh_by=NULL, "
                            "refresh_at=NULL, "
                            "last_updated=NOW(), "
                            "updated_by=%s, "
                            "row_version=row_version+1 "
                            "WHERE rfq_id=%s AND row_version=%s",
                            (rev_st, user, req.rfq_id, rv))
                # (rev_st already order-aware)
                if cur.rowcount == 0:
                    return JSONResponse(status_code=409, content={
                        "ok": False, "error": "Row changed elsewhere - refreshed."})
                cur.execute(
                    "INSERT INTO cba_app.rfq_quote_hist (rfq_id, "
                    "rev, bid, ask, spot, fx, delta, quoted_by, "
                    "quoted_at, action) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,NOW(),'reject')",
                    (req.rfq_id, (qrev or 0) + 1, _fnum(pb),
                     _fnum(pa), _fnum(s_ref), _fnum(f_ref),
                     _fnum(dlt), user))
                cur.execute(
                    "INSERT INTO cba_app.rfq_log (rfq_id, field_name, "
                    "old_value, new_value, changed_by, changed_at) "
                    "VALUES (%s,'reject',%s,%s,%s,NOW())",
                    (req.rfq_id, f"hit {hitside or '-'}",
                     "busted -> REQUESTED \u00b7 quote pulled", user))
            conn.commit()
            return {"ok": True, "status": rev_st,
                    "token": str(rv + 1)}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


@app.post("/api/rfq/upload")
def api_rfq_upload(req: RfqUpload, request: Request):
    """Builds the exact trade_blotter row a DONE RFQ maps to. The INSERT is
    fully implemented below but gated off (RFQ_UPLOAD_ENABLED=False) per
    the current no-connection requirement - flipping the flag arms it."""
    try:
        _ensure_rfq()
        conn = rc.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT short_name, isin, ccy, style, sides, hit, qty, "
                    "sec_fx, fx_ref, "
                    "client, bid_px, ask_px, stock_ref, delta, status, ack_by "
                    "FROM cba_app.rfq WHERE rfq_id=%s", (req.rfq_id,))
                r = cur.fetchone()
                if not r:
                    return JSONResponse(status_code=404, content={
                        "ok": False, "error": "rfq not found"})
                (short_name, isin, ccy, style, sides, hitside, qty,
                 sec_fx, fx_ref, client,
                 bid_px, ask_px, stock_ref, delta, status,
                 ack_by) = r
                if status != "DONE":
                    raise BLVal("Only DONE RFQs can be staged for upload.")
                if hitside not in ("bid", "ask"):
                    raise BLVal("Set Hit (bid or ask) first - it decides "
                                "client side and price.")
                if not ack_by:
                    raise BLVal("Trader ack required before upload - "
                                "click ACK on the line first.")
                px = bid_px if hitside == "bid" else ask_px
                if px is None:
                    raise BLVal(f"No {hitside} px on this RFQ.")
                if not client:
                    raise BLVal("Client is required before upload.")
                # our bid dealt -> client SELLS to us; our ask -> client BUYS
                side = "SELL" if hitside == "bid" else "BUY"
                b_isin, b_type, b_ccy, b_fx = isin, None, ccy, None
                cur.execute(
                    f"SELECT isin, bond_type, bond_currency, fx_rate "
                    f"FROM {BLOTTER_DB}.bond_mappings "
                    f"WHERE bond_name=%s LIMIT 1", (short_name,))
                mrow = cur.fetchone()
                if mrow:
                    b_isin = b_isin or mrow[0]
                    b_type = mrow[1]
                    b_ccy = b_ccy or mrow[2]
                    b_fx = mrow[3]
            blotter_row = {
                "trade_date": _date.today().isoformat(),
                "client_side": side, "isin": b_isin or "",
                "bond_name": short_name, "bond_type": b_type or "",
                "bond_currency": b_ccy or sec_fx or "",
                "fx_rate": "" if b_fx is None else str(b_fx),
                "quantity": "" if qty is None else str(qty),
                "price": str(px), "client_name": client,
                "trade_type": style,          # blotter vocabulary already
                "stock_ref": "" if stock_ref is None else str(stock_ref),
                "fx_ref": "" if fx_ref is None else str(fx_ref),
                "delta": "" if delta is None else str(delta),
                "booked": "0",
            }
            if RFQ_UPLOAD_ENABLED:
                cols = ", ".join(f"`{k}`" for k in blotter_row)
                ph = ", ".join(["%s"] * len(blotter_row))
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {BLOTTER_DB}.{BLOTTER_TABLE} "
                        f"({cols}, updated_by, row_version) "
                        f"VALUES ({ph}, %s, 0)",
                        list(blotter_row.values()) +
                        [((getattr(request.state, "auth", None) or {})
                          .get("user") or req.user)])
                conn.commit()
                _bl_notify()
                return {"ok": True, "uploaded": True,
                        "blotter_row": blotter_row}
            return {"ok": True, "staged": True, "uploaded": False,
                    "blotter_row": blotter_row,
                    "note": "trade_blotter connection disabled "
                            "(RFQ_UPLOAD_ENABLED=False) - row shown is "
                            "exactly what will be inserted"}
        finally:
            conn.close()
    except BLVal as e:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(e)})


# ----------------------------------------------------------------------
# BAU task checklist (cba_app.bau_task / bau_task_log)
# ----------------------------------------------------------------------
_BAU_READY = False


def _bau_conn():
    return rc.connect()


def _ensure_bau_schema():
    global _BAU_READY
    if _BAU_READY:
        return
    conn = _bau_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS cba_app")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cba_app.bau_task (
                  task_id     INT AUTO_INCREMENT PRIMARY KEY,
                  task_name   VARCHAR(200) NOT NULL,
                  sched_time  TIME NULL,
                  category    VARCHAR(50) NULL,
                  notes       VARCHAR(500) NULL,
                  sort_order  INT NOT NULL DEFAULT 100,
                  is_active   TINYINT(1) NOT NULL DEFAULT 1,
                  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cba_app.bau_task_log (
                  task_id    INT NOT NULL,
                  task_date  DATE NOT NULL,
                  done       TINYINT(1) NOT NULL DEFAULT 0,
                  done_at    DATETIME NULL,
                  comment    VARCHAR(300) NULL,
                  PRIMARY KEY (task_id, task_date)
                )""")
            cur.execute("SELECT COUNT(*) FROM cba_app.bau_task")
            if cur.fetchone()[0] == 0:
                cur.executemany(
                    "INSERT INTO cba_app.bau_task "
                    "(task_name, sched_time, category, sort_order) "
                    "VALUES (%s, %s, %s, %s)",
                    [("Export EQRMS Trade History txt + Refresh recon",
                      "08:45:00", "AM", 10),
                     ("Update cbanalytics DB + Delta & Price check",
                      "09:15:00", "AM", 20),
                     ("Send Trade Booking Recon email",
                      "17:30:00", "EOD", 30)])
        conn.commit()
        _BAU_READY = True
    finally:
        conn.close()


class BauLogReq(BaseModel):
    task_id: int
    date: str
    done: Optional[bool] = None
    comment: Optional[str] = None


class BauTaskReq(BaseModel):
    task_id: Optional[int] = None
    task_name: str = ""
    sched_time: Optional[str] = None    # "HH:MM"
    category: Optional[str] = None
    notes: Optional[str] = None
    sort_order: int = 100
    is_active: bool = True


@app.get("/api/bau/list")
def api_bau_list(date: str, include_inactive: bool = False):
    _ensure_bau_schema()
    day = rc.parse_date(date)
    conn = _bau_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT t.task_id, t.task_name, t.sched_time, t.category, "
                "       t.notes, t.sort_order, t.is_active, "
                "       COALESCE(l.done, 0), l.done_at, l.comment "
                "FROM cba_app.bau_task t "
                "LEFT JOIN cba_app.bau_task_log l "
                "  ON l.task_id = t.task_id AND l.task_date = %s "
                + ("" if include_inactive else "WHERE t.is_active = 1 ")
                + "ORDER BY t.sort_order, t.sched_time IS NULL, "
                  "t.sched_time, t.task_id", (day,))
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for (tid, name, st, cat, notes, so, act, done, dat, com) in rows:
        if isinstance(st, dt.timedelta):
            tot = int(st.total_seconds())
            st_str = "%02d:%02d" % (tot // 3600, (tot % 3600) // 60)
        elif st is not None:
            st_str = str(st)[:5]
        else:
            st_str = None
        out.append(dict(
            task_id=tid, task_name=name,
            sched_time=st_str,
            category=cat, notes=notes, sort_order=so,
            is_active=bool(act), done=bool(done),
            done_at=str(dat)[:16] if dat else None,
            comment=com))
    n_done = sum(1 for r in out if r["done"] and r["is_active"])
    n_act = sum(1 for r in out if r["is_active"])
    return {"ok": True, "date": str(day), "tasks": out,
            "done": n_done, "total": n_act}


@app.post("/api/bau/log")
def api_bau_log(req: BauLogReq):
    _ensure_bau_schema()
    day = rc.parse_date(req.date)
    conn = _bau_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT IGNORE INTO cba_app.bau_task_log "
                        "(task_id, task_date) VALUES (%s, %s)",
                        (req.task_id, day))
            if req.done is not None:
                cur.execute(
                    "UPDATE cba_app.bau_task_log "
                    "SET done=%s, done_at=%s "
                    "WHERE task_id=%s AND task_date=%s",
                    (1 if req.done else 0,
                     dt.datetime.now().replace(microsecond=0)
                     if req.done else None,
                     req.task_id, day))
            if req.comment is not None:
                cur.execute(
                    "UPDATE cba_app.bau_task_log SET comment=%s "
                    "WHERE task_id=%s AND task_date=%s",
                    (req.comment.strip()[:300] or None, req.task_id, day))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/bau/task")
def api_bau_task(req: BauTaskReq):
    _ensure_bau_schema()
    name = (req.task_name or "").strip()
    if not name:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": "task_name required"})
    st = (req.sched_time or "").strip() or None
    conn = _bau_conn()
    try:
        with conn.cursor() as cur:
            if req.task_id:
                cur.execute(
                    "UPDATE cba_app.bau_task SET task_name=%s, sched_time=%s, "
                    "category=%s, notes=%s, sort_order=%s, is_active=%s "
                    "WHERE task_id=%s",
                    (name, st, req.category, req.notes, req.sort_order,
                     1 if req.is_active else 0, req.task_id))
            else:
                cur.execute(
                    "INSERT INTO cba_app.bau_task "
                    "(task_name, sched_time, category, notes, sort_order, "
                    " is_active) VALUES (%s, %s, %s, %s, %s, %s)",
                    (name, st, req.category, req.notes, req.sort_order,
                     1 if req.is_active else 0))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


class BauDeleteReq(BaseModel):
    task_id: int


@app.post("/api/bau/delete")
def api_bau_delete(req: BauDeleteReq):
    """Hard delete: removes the task AND every daily record for it.
    Archive is the safe alternative that keeps history."""
    _ensure_bau_schema()
    conn = _bau_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM cba_app.bau_task_log WHERE task_id=%s",
                        (req.task_id,))
            logs_deleted = cur.rowcount
            cur.execute("DELETE FROM cba_app.bau_task WHERE task_id=%s",
                        (req.task_id,))
            task_deleted = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    if not task_deleted:
        return JSONResponse(status_code=404,
                            content={"ok": False, "error": "task not found"})
    return {"ok": True, "logs_deleted": logs_deleted}


class BauArchiveReq(BaseModel):
    task_id: int
    is_active: bool = False


@app.post("/api/bau/archive")
def api_bau_archive(req: BauArchiveReq):
    _ensure_bau_schema()
    conn = _bau_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE cba_app.bau_task SET is_active=%s "
                        "WHERE task_id=%s",
                        (1 if req.is_active else 0, req.task_id))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


# ----------------------------------------------------------------------
# UI (single embedded page, no external assets)
# ----------------------------------------------------------------------
PAGE = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>Lagrange</title>
<style>
 body{margin:0;font:13px 'Segoe UI',Consolas,sans-serif;background:#f4f4f4;color:#1c1c1c}
 header{background:#f0f0f0;border-bottom:1px solid #d8d8d8;padding:12px 18px;
        font-weight:700;letter-spacing:2px}
 header small{color:#6e6a63;font-weight:400;letter-spacing:0;margin-left:12px}
 #tabs{display:flex;background:#fafafa;border-bottom:1px solid #d8d8d8}
 .tab{padding:9px 18px;cursor:pointer;color:#6e6a63;border-bottom:3px solid transparent;
      letter-spacing:1px;font-weight:700;user-select:none}
 .tab.active{color:#1c1c1c;border-bottom-color:#0a7f45;background:#fff}
 .bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;
      padding:10px 18px;background:#fafafa;border-bottom:1px solid #d8d8d8}
 select,input,button{font:13px 'Segoe UI',Consolas,sans-serif;padding:5px 8px;
      border:1px solid #c9c9c9;background:#fff}
 button{cursor:pointer}
 .refresh{background:#0a7f45;color:#fff;border-color:#0a7f45;font-weight:700}
 .sendnow{background:#8f5f00;color:#fff;border-color:#8f5f00;font-weight:700}
 button:disabled{opacity:.45;cursor:not-allowed}
 .status{padding:7px 18px;color:#6e6a63;background:#fff;
         border-bottom:1px solid #e5e5e5;white-space:pre-wrap}
 .status.err{color:#b3261e}
 .status.ok{color:#0a7f45}
 iframe{width:100%;height:calc(100vh - 205px);border:0;background:#fff}
 label{color:#6e6a63}
 .hide{display:none}
/* ---- Trade Blotter: visual clone of the CB Trade Blotter app ---- */
.blgrid{border-collapse:collapse;font:12px 'Segoe UI',Consolas,sans-serif;
  white-space:nowrap;width:max-content}
.blgrid th,.blgrid td{border:1px solid #d5d9d5;padding:2px 7px;
  text-align:left;background:#fff}
.blgrid th{background:#f2f3f2;position:sticky;top:0;z-index:2;
  font-weight:600;color:#333;border-bottom:2px solid #c3c7c3}
.blgrid tr.bdone td{background:#e7f4ea}
.blgrid tr:hover td{background:#dcefe1}
.blgrid tr.bunb:hover td{background:#f0f0f0}
.blgrid td.bnum{text-align:right}
.blgrid td.bst{font-weight:600;color:#1d7a3d}
.blgrid tr.bunb td.bst{color:#c62828}
.blgrid .dot{display:inline-block;width:8px;height:8px;border-radius:50%;
  margin-right:5px;vertical-align:1px}
.blgrid .dot.dg{background:#2e9e53} .blgrid .dot.dr{background:#d43a3a}
.blgrid td.bck{text-align:center}
.blgrid .cb{display:inline-block;width:13px;height:13px;
  border:1px solid #9aa0a6;border-radius:2px;background:#fff;
  font:11px/13px sans-serif;color:#fff;text-align:center}
.blgrid .cb.on{background:#1a73e8;border-color:#1a73e8}
.blgrid td.bna{color:#9aa0a6;text-align:center}
.blgrid td.hd{color:#1d7a3d;font-weight:600;text-align:center}
.blgrid td.ho{color:#b26a00;font-weight:700;text-align:center;background:#fdf3e0}
.blgrid .dot.dy{background:#e0a100}
.blgrid tr.bhedge td.bst{color:#b26a00}
.blgrid th{position:relative}
.blgrid .blh{cursor:pointer;user-select:none}
.blgrid .blh:hover{color:#000}
.blgrid .blrz{position:absolute;right:-3px;top:0;width:7px;height:100%;
  cursor:col-resize;z-index:3}
.blgrid .blrz:hover{background:#8a5b00;opacity:.4}
.blgrid tr.blfr th{top:25px;padding:0;background:#fafbfa}
.blgrid .blf{width:100%;border:0;background:transparent;padding:1px 6px;
  font:11px 'Segoe UI',Consolas,sans-serif;outline:none;color:#444}
.blgrid .blf:focus{background:#fff;box-shadow:inset 0 0 0 1px #8a5b00}
.blgrid td.rq-open{color:#8a5b00} .blgrid td.rq-quoted{color:#274f8f}
.blgrid td.rq-done{color:#1d7a3d}
.blgrid td.rq-cxl{color:#9aa0a6;text-decoration:line-through}
.blgrid td.rq-chip{color:#444}
.blgrid td.rq-live{text-align:right}
.blgrid td.rq-q{text-align:right}
.blgrid td.rq-dp{text-align:right}
.blgrid td.rq-dn{text-align:right}

#rfq_tbl td.bed input[data-rf="trade_date"]{text-align:left;width:88px}
#rfq_hist{position:fixed;top:52px;right:0;width:360px;bottom:0;
  transform:translateX(105%);
  background:var(--bg);border-left:1px solid var(--border2);z-index:60;
  box-shadow:-4px 0 14px rgba(0,0,0,.18);padding:10px 12px;
  overflow:auto;transition:transform .18s ease}
#rfq_hist.on{transform:none}
#rh_rz{position:absolute;left:0;top:0;bottom:0;width:6px;
  cursor:ew-resize}
#rh_rz:hover{background:rgba(28,28,28,.10)}
.rh-sub{margin:14px 0 4px;font-weight:700;font-size:10px;
  letter-spacing:1px;text-transform:uppercase;
  border-bottom:1px solid var(--text);padding-bottom:3px}
.rh-sub small{color:var(--faint);font-weight:400;letter-spacing:.2px;
  text-transform:none;margin-left:6px}
#rh_ev{border:1px solid var(--border2);width:100%}
#rh_ev th{background:var(--panel);color:var(--muted);
  font-weight:700;font-size:9.5px;letter-spacing:.4px;
  text-transform:uppercase;border:none;
  border-bottom:1px solid var(--text)}
#rh_ev td{border:none;border-bottom:1px solid var(--border);
  font-size:11px}
#rh_ev th,#rh_ev td{padding:2px 6px;text-align:left}
#rh_ev tbody tr:nth-child(even) td{background:var(--row)}
#rh_ev td.rh-st{font-weight:700}
#rf_cfg{font-size:13px}
#rf_cfgp{position:fixed;top:84px;right:14px;z-index:65;
  background:var(--bg);border:1px solid var(--border2);
  border-top:2px solid var(--text);
  box-shadow:0 6px 18px rgba(0,0,0,.12);
  font:11px var(--mono);padding:10px 12px;width:200px}
#rf_cfgp.hide{display:none}
#rf_cfgp .cfg-t{font-weight:700;letter-spacing:1px;font-size:11px;
  display:flex;justify-content:space-between;margin-bottom:6px}
#rf_cfg_x{cursor:pointer;color:#a8231b}
#rf_cfgp .cfg-s{color:var(--muted);font-size:9.5px;letter-spacing:.6px;
  text-transform:uppercase;margin:8px 0 3px;border-bottom:1px
  solid #efede7}
#rf_cfgp label{display:inline-block;margin:1px 8px 1px 0;
  cursor:pointer}
#cfg_cols label{display:inline-block;width:60px}
#cfg_reset,#cf_clear{margin-top:6px;width:100%;font:inherit;
  border:1px solid var(--border2);background:var(--panel);
  cursor:pointer;padding:3px}
#cfg_reset:hover,#cf_clear:hover{border-color:var(--text)}
#rf_cfgp .cf-row{display:flex;align-items:center;
  justify-content:space-between;margin:3px 0}
#rf_cfgp .cf-row span{color:var(--muted);font-size:10.5px}
#rf_cfgp .cf-row input[type=color]{width:34px;height:18px;
  padding:0;border:1px solid var(--border2)}
#cf_fgx,#cf_bgx{cursor:pointer;color:#a8231b;padding:0 3px}
#cf_w{font:inherit;border:1px solid var(--border2);padding:1px 4px;
  width:52px;text-align:right}
#rf_cfgp .cf-note{color:var(--faint);font-size:10px;margin:6px 0 2px}
#rfq_hist .rh-head{display:flex;justify-content:space-between;
  font-weight:700;font-size:13px;margin-bottom:4px}
#rh_close{cursor:pointer;color:#a8231b;padding:0 4px}
#rh_close:hover{background:#a8231b;color:#fff}
#rfq_hist .rh-meta{color:var(--muted);font-size:11px;margin-bottom:8px}
#rh_tbl th,#rh_tbl td{padding:2px 6px;font-size:11.5px;text-align:right}
#rh_tbl th:nth-child(-n+3),#rh_tbl td:nth-child(-n+3){text-align:left}
.blgrid td.rq-qd{font-style:italic;text-align:right}
/* ---- RFQ: Nuke Station design system ---- */
#tab-rfq{--bg:#ffffff;--panel:#f2f3f5;--panel2:#f7f8f9;
  --row:#f7f8fa;--hover:#eef0f3;--border:#e3e6ea;
  --border2:#c9ced4;--text:#16181d;--muted:#5a6068;
  --faint:#8b919a;--amber:#8a5b00;--amber-dim:#f7f1e2;
  --green:#106b3f;--red:#a8231b;--blue:#274f8f;--teal:#0b6e66;
  --mono:'Consolas','JetBrains Mono',monospace}
#tab-rfq .bar button{background:var(--bg);color:var(--text);
  border:1px solid var(--border2);border-radius:0;
  padding:3px 9px;font:11px var(--mono);cursor:pointer}
#tab-rfq .bar button:hover{border-color:var(--text)}
#tab-rfq .bar button#rf_send{background:var(--text);
  border-color:var(--text);color:#fff;font-weight:700}
#tab-rfq .bar input,#tab-rfq .bar select{
  font:11px var(--mono);border:1px solid var(--border2);
  border-radius:0;padding:3px 6px;background:var(--bg);
  color:var(--text)}
#tab-rfq .bar input:focus,#tab-rfq .bar select:focus{
  outline:none;border-color:var(--text)}
#tab-rfq .status{font:11px var(--mono);color:var(--muted)}
/* ---- RFQ neat theme (Runs-style) ---- */
#rf_cap{font:12px var(--mono);margin:10px 0 6px;
  font-weight:700;letter-spacing:1.5px;color:var(--text);
  text-transform:uppercase}
#rf_cap small{color:var(--faint);font-weight:400;letter-spacing:.2px;
  margin-left:10px;text-transform:none;font-size:10px}
#rfq_tbl{font:11.5px var(--mono);border-collapse:separate;
  border-spacing:0;border:1px solid var(--border2);
  background:var(--bg)}
#rfq_tbl.fs-s{font-size:10.5px}
#rfq_tbl.fs-l{font-size:12.5px}
#rfq_tbl select.rq-sel,#rfq_tbl td.bed input{
  font:11.5px var(--mono)}
#rfq_tbl.fs-s select.rq-sel,#rfq_tbl.fs-s td.bed input{
  font-size:10.5px}
#rfq_tbl.fs-l select.rq-sel,#rfq_tbl.fs-l td.bed input{
  font-size:12.5px}
#rfq_tbl th{background:var(--panel);color:var(--muted);
  font-weight:700;font-size:9.5px;letter-spacing:.4px;
  text-transform:uppercase;padding:2px 5px;border:none;
  border-bottom:1px solid var(--text)}
#rfq_tbl th.rq-band{color:var(--faint);font-size:9px;
  letter-spacing:.7px;text-align:left;border-bottom:none;
  padding:3px 7px 0}
#rfq_tbl th.gsep,#rfq_tbl thead tr:last-child th.gsep{
  border-left:1px solid var(--border2)}
#rfq_tbl th.rq-band{border-bottom:none;color:#e8eaee;
  background:#2b3038;font-weight:700;font-size:8.5px;
  letter-spacing:1.8px;padding:3px 9px}
#rfq_tbl th.bd-T{box-shadow:inset 0 -2px 0 #8b919a}
#rfq_tbl th.bd-Q{box-shadow:inset 0 -2px 0 #4d79c7}
#rfq_tbl th.bd-M{box-shadow:inset 0 -2px 0 #17a091}
#rfq_tbl th.bd-C{box-shadow:inset 0 -2px 0 #c99420}
#rfq_tbl th.bd-X{box-shadow:inset 0 -2px 0 #9d7ad8}
#rfq_tbl th.bd-F{box-shadow:inset 0 -2px 0 #8b919a}
#rfq_tbl thead tr:last-child th{background:var(--panel)}
#rfq_tbl thead tr:last-child th.g-Q{box-shadow:inset 0 -2px 0 #4d79c7}
#rfq_tbl thead tr:last-child th.g-M{box-shadow:inset 0 -2px 0 #17a091}
#rfq_tbl thead tr:last-child th.g-C{box-shadow:inset 0 -2px 0 #c99420}
#rfq_tbl thead tr:last-child th.g-X{box-shadow:inset 0 -2px 0 #9d7ad8}
#rfq_tbl td,#rfq_tbl td.bed input,#rfq_tbl td select,#rfq_tbl .rq-num{color:#000}
#rfq_tbl td{border:none;border-bottom:1px solid var(--border);
  padding:3px 7px}
#rfq_tbl.den-c td{padding:2px 4px}
#rfq_tbl.den-c th{padding:2px 4px}
#rfq_tbl tbody tr:nth-child(even) td{background:var(--row)}
#rfq_tbl tbody tr:hover td{background:var(--hover) !important}
#rfq_tbl tr.bdone td{background:#b9e2c1 !important}
#rfq_tbl tr.bdone:hover td{background:#a9d9b3 !important}
#rfq_tbl tr.bwork td{background:#b8ded6 !important}
#rfq_tbl tr.bwork:hover td{background:#a9d5cb !important}
#rfq_tbl tr.bnack td{background:#f8cf9a !important}
#rfq_tbl tr.bnack:hover td{background:#f5c384 !important}
#rfq_tbl tr.breq td{background:#d4bcee !important}
#rfq_tbl tr.breq:hover td{background:#c8ace8 !important}
#rfq_tbl tr.badj td{background:#f6d3ce !important}
#rfq_tbl tr.badj:hover td{background:#f0c2bb !important}
#rfq_tbl tr.bqtd td{background:#dfeafc !important}
#rfq_tbl tr.bqtd:hover td{background:#d2e2f9 !important}
#rfq_tbl tr.bcxl td{background:#d9dce2 !important}
#rfq_tbl tr.bcxl:hover td{background:#ccd0d8 !important}
#rfq_tbl td.bed{background:transparent;padding:0}
#rfq_tbl td.bed input{padding:2px 4px;width:58px;
  background:transparent;border:1px solid transparent;
  color:var(--text);text-align:right}
#rfq_tbl td.bed input::placeholder{color:var(--faint);
  font-style:italic}
#rfq_tbl tbody tr:hover td.bed input{border-color:var(--border2)}
#rfq_tbl td.bed input:focus{border-color:var(--amber);
  background:var(--bg)}
#rfq_tbl.den-c td.bed input{padding:2px 4px}
#rfq_tbl td.bed input[data-rf="qty"]{width:66px}
#rfq_tbl td.bed input[data-rf="client"],
#rfq_tbl td.bed input[data-rf="notes"],
#rfq_tbl td.bed input[data-rf="trade_date"],
#rfq_tbl td.bed input[data-rf="isin"]{color:var(--text);
  text-align:left}
#rfq_tbl td.bed input[data-rf="client"],
#rfq_tbl td.bed input[data-rf="notes"],
#rfq_tbl td.bed input[data-rf="isin"]{width:72px}
#rfq_tbl td.bed input[data-rf="trade_date"]{width:70px}
#rfq_tbl select.rq-sel{border:1px solid transparent;
  background:transparent;padding:2px 1px;
  max-width:160px}
#rfq_tbl select.rq-sel:hover{border-color:var(--border2);
  background:var(--bg)}
#rfq_tbl select.rq-sel.rq-open{color:#7b5cc4}
#rfq_tbl select.rq-sel.rq-quoted{color:var(--blue)}
#rfq_tbl select.rq-sel.rq-work{color:var(--teal)}
#rfq_tbl select.rq-sel.rq-hit{color:#b26a00}
#rfq_tbl select.rq-sel.rq-done{color:var(--green)}
#rfq_tbl select.rq-sel.rq-cxl{color:var(--faint)}
#rfq_tbl select.rq-sel.rq-rfsh{color:var(--blue)}
#rfq_tbl .rq-st{font:inherit;letter-spacing:.2px}
#rfq_tbl .rq-st.rq-open{color:#7b5cc4}
#rfq_tbl .rq-st.rq-quoted{color:var(--blue)}
#rfq_tbl .rq-st.rq-work{color:var(--teal)}
#rfq_tbl .rq-st.rq-hit{color:#b26a00}
#rfq_tbl .rq-st.rq-done{color:var(--green)}
#rfq_tbl .rq-st.rq-cxl{color:var(--faint)}
#rfq_tbl .rq-st.rq-rfsh{color:var(--blue)}
#rfq_tbl .rq-st.rq-adj{color:var(--red)}
#rfq_tbl td.rq-ordc{white-space:nowrap}
.rq-ap{cursor:pointer;border:1px solid var(--border2);padding:0 4px;font-weight:700;font-size:9.5px;color:var(--muted)}
.rq-ap.on{background:#106b3f;color:#fff;border-color:#106b3f}
.rq-tag{display:inline-block;border:1px solid var(--border2);
  padding:0 3px;margin:0 1px;font-size:8px;font-weight:700;
  color:var(--muted);letter-spacing:.3px}
.rq-tag.t-tol{cursor:pointer}
.rq-tag.t-tol.on{background:#c99420;color:#fff;border-color:#c99420}
.rq-tag.t-tol.fw{background:#0b6e66;color:#fff;border-color:#0b6e66}
.rq-adjb{font-size:8.5px;border:1px solid #b34700;color:#b34700;background:#fff;cursor:pointer;padding:0 4px}
.rq-adjb.on{background:#b34700;color:#fff}
tr.bimp td{background:#fdf3d7}
.rq-mtb{font-size:8.5px;border:1px solid #106b3f;color:#106b3f;background:#fff;cursor:pointer;padding:0 4px}
.rq-mtb:hover{background:#106b3f;color:#fff}
.missb{border:2px solid #b3261e !important;background:#fde7e5 !important}
#rfq_tbl input[data-rf="stock_ref"],#rfq_tbl input[data-rf="fx_ref"]{color:#000;font-style:normal}
#rfq_tbl input[data-rf="stock_ref"]::placeholder,#rfq_tbl input[data-rf="fx_ref"]::placeholder{color:#000;opacity:1;font-style:normal}
.rq-st.rq-adjrq{color:#b34700}
#rfq_tbl td.rq-algos{white-space:nowrap}
.twnew{color:#8a5b00;background:#f7f1e2;border:1px solid #8a5b00;font-size:8.5px;padding:1px 4px;letter-spacing:.5px}
#rfq_tbl td[class*="fl-"]{text-align:center;letter-spacing:.4px}
#rfq_tbl td.fl-ok{color:#106b3f}
#rfq_tbl td.rq-tbtn{white-space:nowrap;text-align:center;
  padding-left:1px;padding-right:1px}
#rfq_tbl .rq-qb,#rfq_tbl .rq-h,#rfq_tbl .rq-cp,#rfq_tbl .rq-x,#rfq_tbl .rq-ex,
#rfq_tbl .rq-pb,#rfq_tbl .rq-lv,
#rfq_tbl .rq-ab{display:inline-block;line-height:15px;
  min-width:16px;text-align:center;vertical-align:middle}
#rfq_tbl .rq-qb,#rfq_tbl .rq-h,#rfq_tbl .rq-cp,#rfq_tbl .rq-x,#rfq_tbl .rq-ex,
#rfq_tbl .rq-pb,#rfq_tbl .rq-lv{
  cursor:pointer;border:1px solid var(--border2);
  background:var(--bg);color:var(--muted);font-weight:700;
  font-size:9.5px;padding:0 3px;margin:0;border-radius:0}
#rfq_tbl .rq-qb{color:#274f8f}
#rfq_tbl .rq-qb:hover{background:#274f8f;border-color:#274f8f;
  color:#fff}
#rfq_tbl .rq-h:hover,#rfq_tbl .rq-cp:hover{background:var(--text);
  border-color:var(--text);color:#fff}
#rfq_tbl .rq-x{color:#a8231b}
#rfq_tbl .rq-ex{color:#b45309}
#rfq_tbl .rq-ex:hover{background:#b45309;border-color:#b45309;color:#fff}
#rfq_tbl .rq-x:hover{background:#a8231b;border-color:#a8231b;
  color:#fff}
#rfq_tbl .rq-pb{color:#a8231b}
#rfq_tbl .rq-pb:hover{background:#a8231b;border-color:#a8231b;
  color:#fff}
#rfq_tbl .rq-lv{color:#0b6e66}
#rfq_scroll{overflow:auto;max-height:calc(100vh - 148px)}
#ib_tbl{font-size:10px;line-height:1.15;border-collapse:collapse}
#ib_tbl th{font-size:8.5px;padding:2px 4px;letter-spacing:.4px;text-align:right}
#ib_tbl td{padding:1px 4px;text-align:right;white-space:nowrap;font-family:Consolas,Menlo,monospace;font-variant-numeric:tabular-nums}
#ib_tbl tr.band td{background:#f3f2ef;font-family:'Segoe UI',system-ui,sans-serif;font-size:8.5px;font-weight:700;letter-spacing:.6px;text-align:left;text-transform:uppercase;border-top:1px solid #d8d4cc;border-bottom:1px solid #d8d4cc;color:#5a5650}
#ib_tbl td.l,#ib_tbl th.l{text-align:left}#ib_tbl td.gcol,#ib_tbl th.gcol{border-left:1px solid #bbb}
#ib_tbl input.ibi{width:64px;background:#fff3cd;border:1px solid #e0c46a;font:inherit;font-size:10px;text-align:right;padding:0 3px}
#ib_tbl input.ibi.w{width:110px;text-align:left}
#ib_tbl td.calc{background:#eef3ff}#ib_tbl td.mine{background:#f6f3ea}
#ib_tbl .fl-x{background:#c62828;color:#fff;font-weight:700;padding:0 4px;border-radius:2px}
#ib_tbl .fl-g{background:#e8a020;color:#fff;font-weight:700;padding:0 4px;border-radius:2px}
#ib_tbl .fl-m{color:#667;font-style:italic}#ib_tbl .fl-s{color:#b45309;font-weight:700}
#ib_tbl .assumed{color:#b45309;font-size:8.5px;font-family:'Segoe UI',system-ui,sans-serif;font-style:italic}
#ib_tbl .confirmed{color:#0b6e66;font-size:8.5px;font-family:'Segoe UI',system-ui,sans-serif}
#tab-idb button.on{background:#0b6e66;color:#fff;border-color:#0b6e66}
#tab-idb button.k{background:#111;color:#fff;border-color:#111}#tab-idb button.g{background:#0b6e66;color:#fff;border-color:#0b6e66}
#ib_tbl td.nsec{background:#fafafa}#ib_tbl td.gM{background:#ffe082;font-weight:700}#ib_tbl td.pc{background:#fff9c4}
#ib_tbl td.qcell{background:#c8e6c9;font-weight:700}#ib_tbl td.uinp{background:#fff3cd}#ib_tbl td.uinp input{width:70px;background:transparent;border:0;font:inherit;text-align:right}
#ib_tbl td.lv{background:#e0f7fa}#ib_tbl td.eo{background:#eef2f7}#ib_tbl td.stk{background:#b2ebf2;font-weight:700}#ib_tbl td.fxc{background:#e1bee7;font-weight:700}
/* section palette: pale tint on cells, darker shade on the band header, rule between sections */
#ib_tbl td.mapc{background:#fdf8ec}#ib_tbl tr.band td.b-mapc{background:#f3e7c3;color:#5a4a1c}
#ib_tbl td.nsec{background:#f7f7f7}#ib_tbl tr.band td.b-nsec{background:#e2e2e2;color:#333}
#ib_tbl td.mdl{background:#eef3fb}#ib_tbl tr.band td.b-mdl{background:#cfdcf3;color:#1e3a6e}
#ib_tbl td.res{background:#eef7f0}#ib_tbl tr.band td.b-res{background:#cde7d3;color:#1f4d2b}
#ib_tbl td.ibq{background:#f3effa}#ib_tbl tr.band td.b-ibq{background:#dcd3f0;color:#3b2a6e}
#ib_tbl td.ibc{background:#e2f0e6;font-weight:600}#ib_tbl tr.band td.b-myq{background:#cde7d3;color:#1f4d2b}#ib_tbl td.myq{background:#eef7f0}
#ib_tbl td.uinp{background:#fff3cd}#ib_tbl tr.band td.b-uinp{background:#f5dd8a;color:#5a4300}
#ib_tbl td.lv{background:#e9f6f8}#ib_tbl tr.band td.b-lv{background:#c4e5ec;color:#0d4a56}
#ib_tbl td.eo{background:#eff2f5}#ib_tbl tr.band td.b-eo{background:#d3dbe3;color:#2b3a49}
#ib_tbl td.stk{background:#fbf8e7}#ib_tbl tr.band td.b-stk{background:#efe4a8;color:#5a4d0a}
#ib_tbl td.fxc{background:#faeff0}#ib_tbl tr.band td.b-fxc{background:#efcdd1;color:#6b1f28}
#ib_tbl td.chk{background:#f6f6f6}#ib_tbl tr.band td.b-chk{background:#dedede;color:#333}
#ib_tbl td.trd{background:#fbf8e7}#ib_tbl tr.band td.b-trd{background:#efe4a8;color:#5a4d0a}
#ib_tbl tr.band td{border-left:2px solid #fff}#ib_tbl td.gcol,#ib_tbl th.gcol{border-left:2px solid #c9c9c9}
#ib_tbl thead{position:sticky;top:0;z-index:3}#ib_tbl th{background:#fafafa;color:#222;border-bottom:2px solid #888}
#ib_tbl tbody tr:nth-child(even) td{filter:brightness(0.975)}#ib_tbl tbody tr:hover td{filter:brightness(0.93)}
#ib_tbl td.qcell{background:#cfe9d3;font-weight:700}
#ib_tbl td.mktpx{font-size:12px;font-weight:700;color:#2b1a5e;background:#e9e2f7}#ib_tbl td.sub{font-size:9.5px;color:#777}
#ib_tbl .cvacc{font-size:10px;padding:0 5px;background:#0b6e66;color:#fff;border-color:#0b6e66;cursor:pointer}#ib_tbl td.mapc{background:#fbfbf7}
#cv_tbl{font-size:10px;line-height:1.15;border-collapse:collapse}
#cv_tbl th{font-size:8.5px;padding:2px 4px;letter-spacing:.4px;text-align:right}
#cv_tbl td{padding:1px 4px;text-align:right;white-space:nowrap;font-family:Consolas,Menlo,monospace;font-variant-numeric:tabular-nums}
#cv_tbl tr.band td{background:#f3f2ef;font-family:'Segoe UI',system-ui,sans-serif;font-size:8.5px;font-weight:700;letter-spacing:.6px;text-align:left;text-transform:uppercase;border-top:1px solid #d8d4cc;border-bottom:1px solid #d8d4cc;color:#5a5650}
#cv_tbl td.l,#cv_tbl th.l{text-align:left}
#cv_tbl input.cvi{width:64px;background:#fff3cd;border:1px solid #e0c46a;font:inherit;font-size:10px;text-align:right;padding:0 3px}
#cv_tbl td.calc{background:#eef3ff}#cv_tbl td.bid{background:#dcedc8;font-weight:700}
#cv_tbl td.id{background:#f7f7f7;color:#333;max-width:150px;overflow:hidden;text-overflow:ellipsis}
#cv_tbl .cvdel{cursor:pointer;color:#c62828;font-weight:700}#cv_tbl .cvdel:hover{color:#fff;background:#c62828;border-radius:2px;padding:0 3px}
#cv_tbl td.gcol{border-left:1px solid #bbb}
#cv_tbl .est{color:#b45309;font-size:8px;font-family:'Segoe UI',system-ui,sans-serif;font-style:italic}
#cv_tbl .st-ok{background:#c62828;color:#fff;font-weight:700;padding:0 4px;border-radius:2px}
#cv_tbl .st-fl{color:#667;font-style:italic}#cv_tbl .st-tm{color:#b45309;font-weight:700}
#cv_tbl .cvopen{cursor:pointer;color:#1d4ed8;text-decoration:underline}
#rfq_tbl{font-size:10px;line-height:1.15}
#rfq_tbl th{font-size:8.5px;padding:2px 3px;letter-spacing:.4px}
#rfq_tbl td{padding:1px 3px}
#rfq_tbl input,#rfq_tbl select{font-size:10px;padding:0 2px}
#rfq_tbl input[data-rf="stock_ref"],#rfq_tbl input[data-rf="fx_ref"]{width:50px !important}
#rfq_tbl input.nkv{width:36px}
#rfq_tbl .rq-hb,#rfq_tbl .rq-pb,#rfq_tbl .rq-lv,#rfq_tbl .rq-qb,#rfq_tbl .rq-cp,#rfq_tbl .rq-x,#rfq_tbl .rq-ex,#rfq_tbl .rq-mtb{font-size:8.5px;padding:0 3px}
#rfq_tbl .rq-lv:hover{background:#0b6e66;border-color:#0b6e66;
  color:#fff}
#rfq_tbl td.rq-qcell{position:relative}
#rfq_tbl .qc{position:absolute;left:2px;top:50%;
  transform:translateY(-50%);display:none;gap:1px}
#rfq_tbl td.rq-qcell:hover .qc{display:inline-flex}
#rfq_tbl .qc.qr{left:auto;right:2px}
#rfq_tbl .qc b.qc-l{color:#0b6e66}
#rfq_tbl .qc b.qc-l:hover{background:#0b6e66;color:#fff;
  border-color:#0b6e66}
#rfq_tbl .qc b{font:8px/1 var(--mono);font-weight:700;
  padding:1px 2px;border:1px solid var(--border2);
  background:var(--bg);cursor:pointer;color:var(--blue)}
#rfq_tbl .qc b.qc-r:hover{background:#274f8f;color:#fff;
  border-color:#274f8f}
#rfq_tbl .qc b.qc-x{color:#a8231b}
#rfq_tbl .qc b.qc-x:hover{background:#a8231b;color:#fff;
  border-color:#a8231b}
#rfq_tbl td.rq-hitc{text-align:center;white-space:nowrap}
.rq-hb{display:inline-block;min-width:16px;text-align:center;
  cursor:pointer;border:1px solid var(--border2);
  background:var(--bg);color:var(--muted);font-weight:700;
  font-size:9.5px;line-height:14px;margin:0 1px}
.rq-hb:hover{border-color:var(--text);color:var(--text)}
.rq-hb.on{background:var(--text);border-color:var(--text);
  color:#fff}
#rfq_tbl td.rq-rfc{text-align:center;white-space:nowrap}
.rq-rf{cursor:pointer;border:1px solid var(--border2);
  background:var(--bg);color:var(--muted);font-size:9.5px;
  font-weight:700;padding:1px 5px;line-height:14px;
  display:inline-block}
.rq-rf:hover{border-color:#b26a00;color:#b26a00}
.rq-rf.rq-rfp{background:#b26a00;border-color:#b26a00;color:#fff;
  cursor:default}
#who_wrap{position:fixed;top:6px;right:10px;z-index:70;
  font:11px 'Consolas','JetBrains Mono',monospace;color:#5a6068;
  background:#fff;border:1px solid #c9ced4;padding:2px 4px 2px 8px}
#who_badge{margin-right:6px;font-weight:700}
#btn_logout{font:10px 'Consolas','JetBrains Mono',monospace;
  border:1px solid #c9ced4;background:#f2f3f5;cursor:pointer;
  padding:1px 6px;border-radius:0}
#btn_logout:hover{border-color:#a8231b;color:#a8231b}
.rq-await{letter-spacing:.4px;text-transform:uppercase}
#rfq_tbl tr.brefr td{background:#b9d4f2 !important}
#rfq_tbl tr.brefr:hover td{background:#a8c8ee !important}
#rfq_tbl tbody tr.rowsel td{background:#e8eef7 !important;
  border-top:1px solid var(--teal) !important;
  border-bottom:1px solid var(--teal) !important}
#rfq_tbl tbody tr.rowsel td:first-child{
  border-left:3px solid var(--teal) !important}
#rf_fields input[type=date]{width:118px}
#rfq_mon{position:fixed;top:52px;right:0;width:340px;bottom:0;
  min-width:220px;max-width:60vw;resize:horizontal;
  background:var(--bg);border-left:1px solid var(--border2);
  z-index:61;transform:translateX(105%);
  transition:transform .18s ease;padding:6px 8px;
  font:11px var(--mono);overflow:auto}
#rfq_mon.on{transform:none}
body.amdock-b #rfq_mon{top:auto;left:0;right:0;bottom:0;
  width:auto;min-width:0;max-width:none;height:240px;
  min-height:110px;max-height:60vh;resize:vertical;
  border-left:none;border-top:2px solid var(--border2);
  transform:translateY(105%)}
body.amdock-b #rfq_mon.on{transform:none}
#rm_dock,#rm_zi,#rm_zo{cursor:pointer;color:var(--blue);
  margin-left:8px;font-weight:700}
#rm_dock:hover,#rm_zi:hover,#rm_zo:hover{color:var(--text)}
#rfq_mon .rm-head{display:flex;justify-content:space-between;
  font-weight:700;font-size:11px;text-transform:uppercase;
  letter-spacing:1.2px;border-bottom:1px solid var(--text);
  padding-bottom:5px;margin-bottom:4px}
#rm_close{cursor:pointer;color:var(--red)}
#rfq_mon .rm-meta{color:var(--faint);font-size:10px;
  margin-bottom:8px}
.rm-it{display:grid;
  grid-template-columns:70px 48px minmax(86px,118px) 104px
    58px 38px 62px 1fr;
  gap:0 7px;align-items:center;padding:2px 6px;border:0;
  border-left:3px solid var(--border2);
  border-bottom:1px solid var(--border);margin:0;
  cursor:pointer;background:var(--bg)}
.rm-it:hover{background:var(--hover)}
.rm-sec{font-weight:700;white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis}
.rm-t,.rm-isin,.rm-ty,.rm-sd,.rm-by{color:var(--muted);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  font-size:10px}
.rm-t{color:var(--faint)}
.rm-why{color:var(--faint);white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;text-align:right}
#rm_list .rm-x{margin-left:auto;padding:0 6px;color:#999;cursor:pointer;font-weight:700;flex:0 0 auto}#rm_list .rm-x:hover{color:#fff;background:#c62828;border-radius:2px}
body.amdock-b #rm_list{display:grid;
  grid-template-columns:repeat(auto-fill,minmax(640px,1fr));
  gap:0 16px}
#rm_grip{position:absolute;left:0;top:0;bottom:0;width:6px;
  cursor:ew-resize;z-index:2}
#rm_grip:hover{background:var(--border2)}
body.amdock-b #rm_grip{left:0;right:0;top:0;bottom:auto;
  height:6px;width:auto;cursor:ns-resize}
.rm-b{font-size:8.5px;font-weight:700;letter-spacing:.5px;
  padding:1px 4px;border:1px solid;min-width:38px;
  text-align:center}
.rm-hit{color:#b26a00;border-color:#b26a00;background:#fbe3c4}
.rm-it[data-k="hit"]{border-left-color:#b26a00}
.rm-rf{color:var(--blue);border-color:var(--blue);
  background:#d9e7f8}
.rm-it[data-k="rf"]{border-left-color:var(--blue)}
.rm-pl{color:var(--red);border-color:var(--red);
  background:#f6dcd9}
.rm-it[data-k="pl"]{border-left-color:var(--red)}
.rm-rq{color:#7b5cc4;border-color:#7b5cc4;background:#eadef7}
.rm-it[data-k="rq"]{border-left-color:#7b5cc4}
.rm-re{color:var(--teal);border-color:var(--teal);
  background:#d4ebe7}
.rm-it[data-k="re"]{border-left-color:var(--teal)}
.rm-aj{color:#a8231b;border-color:#a8231b;background:#f6d3ce}
.rm-it[data-k="aj"]{border-left-color:#a8231b}
.rm-st{color:var(--muted);border-color:var(--border2);
  background:var(--panel)}
.rm-it[data-k="st"]{border-left-color:var(--border2)}
.rm-sec{font-weight:700}
.rm-why{color:var(--muted);font-size:10.5px;margin-left:auto;
  text-align:right}
@keyframes rflash{0%{outline:2px solid var(--teal);
  outline-offset:-2px}100%{outline:2px solid transparent}}
#rfq_tbl tr.rflash td{animation:rflash 1.6s ease-out}
.rq-rfb{margin-left:4px}
#rfq_hist{font:11.5px var(--mono)}
#rfq_hist .rh-head{text-transform:uppercase;letter-spacing:1.2px;
  font-size:11px;border-bottom:1px solid var(--text);
  padding-bottom:5px}
#rh_tbl{border:1px solid var(--border2);width:100%}
#rh_tbl th{background:var(--panel);color:var(--muted);
  font-weight:700;font-size:9.5px;letter-spacing:.4px;
  text-transform:uppercase;border:none;
  border-bottom:1px solid var(--text)}
#rh_tbl td{border:none;border-bottom:1px solid var(--border)}
#rh_tbl tbody tr:nth-child(even) td{background:var(--row)}
.blgrid td.fl-ok{color:#1d7a3d;font-weight:600;text-align:center}
.blgrid td.fl-stale{color:#b26a00;font-weight:700;text-align:center}
.blgrid td.fl-moved{text-align:center}
.blgrid td.fl-none{text-align:center}
.rq-up{cursor:pointer;color:#274f8f;font-weight:700;margin-left:4px}
.rq-up:hover{color:#0b3d91}
.blgrid td.rq-tbtn{text-align:center}
.rq-x{cursor:pointer;color:#c62828;font-weight:700;border:1px solid #c62828;
  border-radius:3px;padding:0 5px;font-size:10.5px}
.rq-x:hover{background:#c62828;color:#fff}
.blgrid td.rq-ref{text-align:right}
.blgrid tr.bwork td{background:#e6f2f1}
.blgrid td.rq-na{color:#b8bcc2;text-align:center}
.blgrid td.rq-qty{text-align:right}
.rq-cp{cursor:pointer;color:#274f8f;font-weight:700;border:1px solid #274f8f;
  border-radius:3px;padding:0 4px;font-size:10.5px;margin-right:4px}
.rq-cp:hover{background:#274f8f;color:#fff}
#rfq_tbl td.bed{padding:0;background:#fffdf2}
#rfq_tbl td.bed input:focus{box-shadow:inset 0 0 0 1px #8a5b00}
#rfq_tbl td.bed input[data-rf="client"],#rfq_tbl td.bed input[data-rf="notes"],
#rfq_tbl td.bed input[data-rf="isin"]{text-align:left;width:104px}
.rf-f{border:1px solid #c9c9c9;background:#fff;padding:2px 10px;
  cursor:pointer;font-size:11px}
.rf-f.on{background:#1c1c1c;color:#fff;border-color:#1c1c1c;font-weight:700}
.bar input.opt{background:#f2f3f5}
.bar input.req{background:#d6e5fa;border-color:#274f8f;border-width:2px}
.bar input.miss{border-color:#a8231b !important;background:#f6d3ce !important}
.rq-sel{font:11.5px var(--mono);border:1px solid #d5d9d5;
  background:#fff;color:var(--text);padding:1px 2px;max-width:150px}
#rfq_tbl td.rq-selc{padding:1px 3px;text-align:left}
.rq-rj{display:inline-block;min-width:16px;text-align:center;
  cursor:pointer;border:1px solid var(--red);
  background:var(--bg);color:var(--red);font-weight:700;
  font-size:9.5px;line-height:14px;margin-left:3px;
  padding:0 3px}
.rq-rj:hover{background:var(--red);color:#fff}
.blgrid td.rq-ackd{text-align:center}
.blgrid td.rq-ackn{text-align:center}
.blgrid tr.bnack td{background:#fdf3e0}
.rq-ab{cursor:pointer;color:#fff;background:#b26a00;
  border:1px solid #b26a00;border-radius:3px;padding:0 6px;
  font-size:10.5px;font-weight:700}
.rq-ab:hover{background:#8a5b00}
#bl_live.liveon{background:#e7f4ea;border-color:#2e9e53;color:#1d7a3d;
  font-weight:700}
#bl_tbl td.bed{padding:0;background:#fffdf2}
#bl_tbl td.bce{cursor:pointer}
#bl_tbl td.bce:hover{box-shadow:inset 0 0 0 1px #8a5b0055}
#bl_tbl td.bed input{width:140px;border:0;background:transparent;
  font:12px 'Segoe UI',Consolas,sans-serif;color:#8a5b00;padding:2px 7px;
  outline:none}
#bl_tbl td.bed input:focus{box-shadow:inset 0 0 0 1px #8a5b00}
#bl_tbl td.selcell{outline:2px solid #1a73e8;outline-offset:-2px}
#bl_tbl td.pend{color:#8a8f98;font-style:italic}
#bl_tbl td.bederr{animation:blerr .9s}
@keyframes blerr{0%{background:#fbdada}100%{background:inherit}}
 .toInput{width:280px}
 .note{padding:5px 18px;background:#f6ead2;color:#8f5f00;border-bottom:1px solid #e5d9bd;
       font-size:12px}
 .panel{margin:10px 18px;border:1px solid #d8d8d8;background:#fff}
 .panel .cap{padding:6px 12px;background:#f0f0f0;border-bottom:1px solid #d8d8d8;
       font-weight:700;letter-spacing:1px;color:#1c1c1c;font-size:12px}
 .panel .cap small{color:#6e6a63;font-weight:400;letter-spacing:0;margin-left:8px}
 .panel .body{padding:10px 12px}
 .panel textarea{width:100%;box-sizing:border-box;font:12px Consolas,monospace;
       border:1px solid #c9c9c9;padding:6px;resize:vertical}
 .btnrow{display:flex;gap:8px;align-items:center;margin-top:8px;flex-wrap:wrap}
 .btnrow .hint{color:#6e6a63;font-size:12px}
</style></head><body>
<header>LAGRANGE <small>CB Runs desk console &middot; build 2026-08-19.r107 &middot; one port (59988)</small>
  <small id="built"></small></header>
<div id="tabs">
  <div class="tab active" id="tabbtn-recon" onclick="showTab('recon')">TRADE BOOKING RECONCILIATION</div>
  <div class="tab" id="tabbtn-delta" onclick="showTab('delta')">DELTA CHECK</div>
  <div class="tab" id="tabbtn-dscan" onclick="showTab('dscan')" title="delta risk scanner · standalone app on :59966">DELTA SCAN</div>
  <div class="tab" id="tabbtn-risk" onclick="showTab('risk')" title="latest risk_positions snapshot (freshest loaded_at batch, 60s buffer)">RISK POSITIONS</div>
  <div class="tab" id="tabbtn-bau" onclick="showTab('bau')">BAU TASKS</div>
  <div class="tab" id="tabbtn-twcb" onclick="showTab('twcb')" title="TW Convertible Bond Issuance Pipeline Monitor">TW CB PIPELINE</div>
  <div class="tab" id="tabbtn-blotter" onclick="showTab('blotter')">TRADE BLOTTER</div>
  <div class="tab" id="tabbtn-rfq" onclick="showTab('rfq')">RFQ STATION</div>
  <div class="tab" id="tabbtn-nuke" onclick="showTab('nuke')">NUKE STATION</div>
  <div class="tab" id="tabbtn-conv" onclick="showTab('conv')" title="conversion-trade bid sheet: bid = parity - X">CB CONVERSION</div>
  <div class="tab" id="tabbtn-idb" onclick="showTab('idb')" title="broker runs: paste, board, compare vs my marks">IDB QUOTES</div>
  <span id="who_wrap"><span id="who_badge"></span><button id="btn_logout" title="log out">logout</button></span>
</div>

<!-- ============ TAB 1 : RECON ============ -->
<div id="tab-recon">
<div class="bar">
  <label>mode</label>
  <select id="mode">
    <option value="today">Today</option>
    <option value="date">Single date</option>
    <option value="range">Date range</option>
  </select>
  <input type="date" id="d1" class="hide">
  <input type="date" id="d2" class="hide">
  <label><input type="checkbox" id="skiploader"> skip txt loader (DB only)</label>
  <button id="refresh" class="refresh">&#8635; Refresh</button>
</div>
<div class="bar">
  <label>To</label><input id="to" class="toInput" placeholder="a@citi.com; b@citi.com">
  <label>Cc</label><input id="cc" class="toInput">
  <button id="draft" disabled>Open Draft</button>
  <button id="sendnow" class="sendnow" disabled>Send Now</button>
</div>
<div class="status" id="status">Press Refresh to load the latest EQRMS export and build the report.</div>
<iframe id="frame"></iframe>
</div>

<!-- ============ TAB 2 : DELTA CHECK ============ -->
<div id="tab-delta" class="hide">
<div class="note">Quick refresh = CBA source only. Full pipeline (below) adds the
derivation leg &mdash; recovered code: verify FIXME items and compare the first run
against a notebook-generated report.</div>
<div class="bar">
  <button id="dupdate">&#8645; Update DB</button>
  <label><input type="checkbox" id="dupdfirst"> update DB first (applies to all checks below)</label>
  <button id="drefresh" class="refresh">&#8635; Refresh (CBA only)</button>
  <span id="dstats" style="color:#6e6a63"></span>
</div>
<div class="panel">
  <div class="cap">FULL PIPELINE <small>derivation + CBA &mdash; paste the grid, or let Lagrange grab it</small></div>
  <div class="body">
    <textarea id="dpaste" rows="4" placeholder="Paste the Derivation grid here:  click the Derivation window &rarr; Ctrl+A &rarr; Ctrl+C &rarr; Ctrl+V here"></textarea>
    <div class="btnrow">
      <button id="dfull" class="refresh">&#9654; Run full pipeline (pasted data)</button>
      <button id="dauto">&#9889; Auto-grab from Derivation window &amp; run</button>
      <span class="hint">auto-grab sends Ctrl+E / Ctrl+A / Ctrl+C to the Derivation window &mdash; hands off until it finishes</span>
    </div>
  </div>
</div>
<div class="bar">
  <label>To</label><input id="dto" class="toInput" placeholder="a@citi.com; b@citi.com">
  <label>Cc</label><input id="dcc" class="toInput">
  <button id="ddraft" disabled>Open Draft</button>
  <button id="dsendnow" class="sendnow" disabled>Send Now</button>
</div>
<div class="status" id="dstatus">Press Refresh to run the CBA delta + price check
(flags: |&Delta; vs nuked| &gt; 5 points, |mkt bid &minus; fair| &gt; 2).</div>
<iframe id="dframe"></iframe>
</div>

<!-- ============ TAB 3 : BAU TASKS ============ -->
<div id="tab-dscan" class="hide">
  <div class="bar">
    <span class="status" id="ds_status">checking delta scan mount…</span>
    <a id="ds_open" target="_blank" style="margin-left:10px">open in new window ↗</a>
  </div>
  <iframe id="ds_frame" style="width:100%;border:0;height:calc(100vh - 120px);background:#101418"></iframe>
</div>
<div id="tab-risk" class="hide">
  <div class="bar">
    <button id="rk_reload">&#8635; Reload</button>
    <input type="text" id="rk_filt" placeholder="filter\u2026" size="18">
    <span id="rk_meta" class="status"></span>
  </div>
  <div id="rk_scroll" style="overflow:auto;max-height:calc(100vh - 128px)">
    <table id="rk_tbl" class="report blgrid"><thead></thead><tbody></tbody></table>
  </div>
</div>
<div id="tab-bau" class="hide">
<div class="bar">
  <label>date</label><input type="date" id="bdate">
  <button id="bload" class="refresh">&#8635; Load</button>
  <span id="bprog" style="color:#6e6a63"></span>
  <span style="flex:1"></span>
  <label><input type="checkbox" id="bmanage"> manage tasks</label>
</div>
<div class="status" id="bstatus">Pick a date and Load. Tick tasks as you complete them &mdash; each date keeps its own record.</div>
<div class="panel" id="bpanel" style="display:none">
  <div class="cap">TASK TEMPLATE <small>add or edit &mdash; changes apply to every day going forward; history is kept</small></div>
  <div class="body">
    <div class="btnrow">
      <input id="bt_name" placeholder="task name" style="flex:2;min-width:220px">
      <input id="bt_time" type="time" title="scheduled time">
      <input id="bt_cat" placeholder="category (AM/PM/EOD)" style="width:140px">
      <input id="bt_sort" type="number" value="100" title="sort order" style="width:70px">
      <input id="bt_notes" placeholder="notes / how-to" style="flex:2;min-width:180px">
      <button id="bt_save" class="refresh">Save task</button>
      <button id="bt_clear">Clear form</button>
      <input type="hidden" id="bt_id">
    </div>
  </div>
</div>
<div style="margin:10px 18px;background:#fff;border:1px solid #d8d8d8">
  <table id="btable" style="width:100%;border-collapse:collapse;font:13px Consolas,monospace"></table>
</div>
</div>

<!-- ============ TAB 4 : TRADE BLOTTER ============ -->
<div id="tab-blotter" class="hide">
<div class="bar">
  <label>from <input type="date" id="bl_from"></label>
  <label>to <input type="date" id="bl_to"></label>
  <input type="text" id="bl_ticker" placeholder="ISIN / name" size="14">
  <select id="bl_type"><option value="">all types</option></select>
  <button id="bl_load" class="refresh">&#8635; Load</button>
  <button id="bl_live">&#9679; live: off</button>
  <input type="text" id="bl_q" placeholder="search loaded rows" size="16">
  <button id="bl_reset" title="clear saved column widths + sort">reset layout</button>
  <label style="margin-left:auto">you: <input type="text" id="bl_user" size="10"
    placeholder="name"></label>
  <span id="bl_meta" style="color:#6e6a63"></span>
</div>
<div class="status" id="bl_status">Read-only view of eqrms.trade_blotter; only
Comments and Trader Agree are editable. Auto-refreshes every 30s.</div>
<div style="overflow:auto;max-height:calc(100vh - 210px)">
<style id="blcolstyle"></style>
<table id="bl_tbl" class="report blgrid"><thead></thead><tbody></tbody></table>
</div>
</div>

<!-- ============ TAB 5 : RFQ STATION ============ -->
<div id="tab-rfq" class="hide">
<div class="bar">
  <input type="text" id="rf_sec" list="rf_secdl" placeholder="short name or ISIN"
    style="min-width:190px" autocomplete="off">
  <datalist id="rf_secdl"></datalist>
  <select id="rf_style"><option value="outright">outright</option>
    <option value="vs">versus</option>
    <option value="working">working stock</option></select>
  <select id="rf_sides"><option value="two_way">two-way</option>
    <option value="bid">bid only</option><option value="ask">ask only</option>
  </select>
  <select id="rf_ord" title="send as a plain RFQ, or as a WORKING client order (client has given a level / instruction; the desk works toward the fill)">
    <option value="">RFQ</option>
    <option value="buy">order: client BUYS</option>
    <option value="sell">order: client SELLS</option>
    <option value="two">order: two-way</option></select>
  <input type="text" id="rf_lvl" placeholder="bid level" size="9"
    title="client target level / limit \u00b7 REQUIRED for working orders">
  <input type="text" id="rf_lvl2" placeholder="ask level" size="8"
    title="ask-side target \u00b7 REQUIRED for two-way working orders">
  <input type="text" id="rf_vs" placeholder="vs" size="8"
    title="stock reference \u00b7 REQUIRED for versus working orders">
  <input type="text" id="rf_fx" placeholder="fx" size="7"
    title="fx reference (optional)">
  <input type="text" id="rf_delta" placeholder="delta" size="5"
    title="delta % (optional)">
  <input type="text" id="rf_qty" placeholder="qty" size="10">
  <input type="text" id="rf_client" placeholder="client" size="12">
  <button id="rf_send" class="refresh">Send RFQ</button>
  <button id="rf_reload">&#8635;</button>
  <button id="rf_mon" title="activity monitor - what needs a trader now, most urgent first">&#9873; <b id="rf_mon_n">0</b></button>
  <button id="rf_cfg" title="display config">&#9881;</button>
  <div id="rf_cfgp" class="hide">
    <div class="cfg-t">DISPLAY CONFIG
      <span id="rf_cfg_x" title="close">&#10005;</span></div>
    <div class="cfg-s">density</div>
    <label><input type="radio" name="cfg_den" value="compact"> compact</label>
    <label><input type="radio" name="cfg_den" value="cozy"> cozy</label>
    <div class="cfg-s">text</div>
    <label><input type="radio" name="cfg_fs" value="s"> S</label>
    <label><input type="radio" name="cfg_fs" value="m"> M</label>
    <label><input type="radio" name="cfg_fs" value="l"> L</label>
    <div class="cfg-s">columns</div>
    <div id="cfg_cols"></div>
    <div class="cfg-s">column format</div>
    <select id="cf_col" style="width:100%"></select>
    <div class="cf-row"><span>width px (blank = auto)</span>
      <input id="cf_w" size="4"></div>
    <div class="cf-row"><span>foreground</span>
      <input type="color" id="cf_fg" value="#1c1c1c">
      <b id="cf_fgx" title="clear">&#10005;</b></div>
    <div class="cf-row"><span>background</span>
      <input type="color" id="cf_bg" value="#ffffff">
      <b id="cf_bgx" title="clear">&#10005;</b></div>
    <div class="cf-row"><span>bold</span>
      <input type="checkbox" id="cf_bold"></div>
    <div class="cf-note">changes preview instantly &middot; drag the right edge
      of any column header to resize &middot; all of it is saved in this
      browser</div>
    <button id="cf_clear">Clear this column</button>
    <button id="cfg_reset">Reset to defaults</button>
  </div>
  <span id="rf_filt" style="margin-left:8px">
    <button class="rf-f" data-f="open">Open</button><button class="rf-f" data-f="all">All</button><button class="rf-f" data-f="bondcfg" id="rf_bcfg" title="per-bond defaults: Tol + Autopilot (spreadsheet)">Bond Cfg</button><button class="rf-f on" data-f="nocxl">All &minus;CXL</button><button class="rf-f" data-f="done">Done</button></span>
  <span id="rf_fields" style="margin-left:10px">
    from <input type="date" id="rf_f_from" title="trade date from">
    to <input type="date" id="rf_f_to" title="trade date to">
    <input id="rf_f_txt" placeholder="ISIN / name / client" size="15">
    <select id="rf_f_type"><option value="">all types</option>
      <option>outright</option><option>vs</option>
      <option value="working">working stock</option></select>
  </span>
  <span id="rf_meta" style="margin-left:auto;color:#5a6068"></span>
</div>
<div class="status" id="rf_status">RFQs on Nuke Station securities - outright, versus
or working stock. Runs-format line per RFQ; Status / Type / Side / Hit are dropdowns.
Grey Bid/Ask = draft (model at your refs); press Q to confirm it as the
standing client quote - every rev is kept, H opens the history panel.
Fresh (outright): GOOD = slippage favorable / tiny, PULL = a side has
gone -ve beyond tolerance - hover the cell: \u27f3 refresh that side,
\u2a2f pull it (Q refreshes both). vs / working use symmetric REQUOTE.
Type a short name OR an ISIN to send. DONE lines need the trader's
ACK (amber), then they're shaped for blotter upload (upload itself:
coming later).</div>
<div style="overflow:auto;max-height:calc(100vh - 210px)">
<style id="rfq_grpcss"></style>
<style id="rfq_colcss"></style>
<div id="rf_cap"><b>RFQ STATION</b><small>grey italic = ghost (empty cell, info only) \u00b7 black italic Bid/Ask = draft (Q confirms) \u00b7 black = standing / pinned &middot; Hit: B / A \u2192 HIT (amber \u00b7 live keeps tracking), ACK books it DONE / REJ busts it \u00b7 rows: violet=req salmon=adjusting (quote off) skyblue=quoted teal=working amber=hit green=done grey=cxl blue=refresh (status reads REFRESH while pending) &middot; Refresh: sales &#10227; req tints the row blue until the trader answers (Q / &#10227; / &#10759;) &middot; H / &#10697; by the ID &middot; trader ctrl: Q quote &middot; &#10680; off &middot; L live &middot; &#10005; cancel &middot; E expire</small></div>
<div id="rfq_scroll"><table id="rfq_tbl" class="report blgrid"><thead></thead><tbody></tbody></table></div>
<div id="rfq_bcfg" class="hide" style="padding:8px 10px">
  <b style="letter-spacing:1px">PER-BOND DEFAULTS \u00b7 applied when a row has no explicit Tol / Auto</b>
  <table class="blgrid" id="bc_tbl" style="margin-top:6px"><thead><tr><th>ISIN</th><th>Security</th><th style="width:80px">Tol</th><th style="width:70px">Autopilot</th><th style="width:60px"></th></tr></thead>
  <tbody id="bc_body"></tbody></table>
  <div class="bar"><input id="bc_isin" placeholder="ISIN" size="14"><input id="bc_name" placeholder="security" size="12"><input id="bc_tol" placeholder="tol" size="6">
  <label><input type="checkbox" id="bc_ap"> autopilot</label>
  <button id="bc_add" class="refresh">Save</button>
  <span id="bc_status"></span></div>
</div>
<div id="rfq_mon"><div id="rm_grip" title="drag to resize"></div>
  <div class="rm-head">ACTIVITY MONITOR<span><span id="rm_zo" title="smaller">A&#8722;</span><span id="rm_zi" title="bigger">A+</span><span id="rm_dock" title="dock to bottom / right">&#8681;</span><span id="rm_close" title="close" style="margin-left:8px">&#10005;</span></span></div>
  <div class="rm-meta">most urgent first &middot; click to jump &middot; refreshes with the poll</div>
  <div id="rm_list"></div>
</div>
<div id="rfq_hist">
  <div id="rh_rz" title="drag &#8596; to resize &middot; double-click resets"></div>
  <div class="rh-head"><span id="rh_title">Quoting history</span>
    <span id="rh_close" title="close">&#10005;</span></div>
  <div id="rh_meta" class="rh-meta"></div>
  <table class="report blgrid" id="rh_tbl"><thead><tr>
    <th>Rev</th><th>Act</th><th>Time</th><th>By</th><th>Bid</th><th>Ask</th>
    <th>Move</th><th>Vs</th><th>Fx</th><th>&Delta;</th>
  </tr></thead><tbody></tbody></table>
  <div class="rh-sub">EVENT LOG <small>created &middot; status &middot; hit &middot; refresh &middot; ack &middot; edits</small></div>
  <table class="report blgrid" id="rh_ev"><thead><tr>
    <th>Time</th><th>By</th><th>What</th><th>Change</th>
  </tr></thead><tbody></tbody></table>
</div>
</div>
</div>

<!-- ============ TAB 6 : NUKE STATION ============ -->
<div id="tab-twcb" class="hide">
<div class="bar">
  <b style="letter-spacing:1px">TW CONVERTIBLE BOND ISSUANCE PIPELINE MONITOR</b>
  <button id="twrun" class="refresh">&#8635; Refresh</button>
  <button id="twbf" title="one-time: walk ~92 days of SFB daily-news pages to seed the LIVE SHELF with June / July effective registrations; runs in the background (a few minutes), then the view refreshes itself">Backfill 92d</button>
  <label title="update the seen-store so these events stop counting as NEW (same dedupe the scheduled run uses)">
    <input type="checkbox" id="twmark"> mark as seen</label>
  <span id="twstatus" style="color:#6e6a63"></span>
</div>
<div class="bar">
  To <input type="text" id="twto" size="34" placeholder="a@citi.com; b@citi.com">
  Cc <input type="text" id="twcc" size="24">
  <button id="twdraft" disabled>Open Draft</button>
  <button id="twsendnow" class="sendnow" disabled>Send Now</button>
  <span id="twmeta" style="color:#6e6a63"></span>
</div>
<div style="padding:0 10px 10px">
<table class="blgrid" id="tw_tbl"><thead><tr>
  <th style="width:44px">New</th><th>Source</th><th style="width:86px">Date</th>
  <th>Company</th><th>Detail</th><th>Detail (EN)</th></tr></thead>
<tbody id="tw_body"><tr><td colspan="6" style="color:#8b919a">Press Refresh to pull TWSE / TPEx / SFB.</td></tr></tbody></table>
<div id="tw_shelf_wrap" class="hide" style="margin-top:14px">
  <b style="letter-spacing:1px" title="every SFB effective-registration stays here for the 92-day issuance window; seed history once with: python tw_cb_pipeline_monitor.py --backfill">LIVE SHELF \u00b7 effective registrations inside the 92-day issuance window</b>
  <table class="blgrid" id="tws_tbl" style="margin-top:6px"><thead><tr>
    <th style="width:70px">Days left</th><th style="width:86px">Effective</th>
    <th>Company</th><th>Detail</th><th>Detail (EN)</th></tr></thead>
  <tbody id="tws_body"></tbody></table>
</div>
</div>
</div>
<div id="tab-conv" class="hide">
  <div class="bar">
    <button id="cv_reload">&#8635; Reload</button>
    <input type="text" id="cv_add" placeholder="add bond: SECID or ISIN" size="22">
    <button id="cv_addb">+ Add</button>
    <button id="cv_exb" title="seed the seven example lines (est terms) - shared, persisted">Load examples</button>
    <span class="status" id="cv_meta">bid = parity &minus; X &middot; X = tax + fees + slippage + borrow + funding + FX + other + edge &middot; amber = editable (persisted per ISIN) &middot; <i>est</i> = placeholder/estimate</span>
  </div>
  <div id="cv_scroll" style="overflow:auto;max-height:calc(100vh - 128px)">
    <table id="cv_tbl" class="report blgrid"><thead></thead><tbody></tbody></table>
  </div>
</div>
<div id="tab-idb" class="hide">
  <div class="bar" style="align-items:flex-start">
    <textarea id="ib_paste" rows="4" placeholder="paste a broker run here (Bloomberg chat text, Ctrl+V) - one quote per line, e.g.  09:20:05 ANTA 29 96.75 offered r 71.95" style="width:46%;font:10.5px Consolas,Menlo,monospace"></textarea>
    <div style="display:flex;flex-direction:column;gap:4px">
      <div><label>source</label> <input type="text" id="ib_src" value="IDB1" size="6"> <label>date</label> <input type="date" id="ib_date"> <button id="ib_ingest">Ingest run</button></div>
      <div><button id="ib_nuke" class="k" title="re-nuke every mapped bond at the broker's @REF (IDB tab only - Nuke Station untouched)">Re-nuke @ broker REF</button> <button data-fill="live">Live &rarr; ovd</button> <button data-fill="eod">EOD &rarr; ovd</button> <button data-fill="last">Last &rarr; ovd</button> <button id="ib_auto">AUTO last: OFF</button> <button data-fill="close">Close &rarr; ovd</button> <button data-fill="bref" title="ovdSpot = broker @REF (ref used, more recently quoted side); ovdCbFx / ovdUndFx = Nuke LIVE fx - IDB tab only, no re-nuke">Broker Ref &rarr; ovd</button> <button data-fill="clear">Clear overrides</button></div>
      <div><button id="ib_accall" title="accept every suggested mapping (rows keep ASSUMED status until you confirm)">Accept all suggestions</button> <span class="sm">IDB tab r107</span></div>
      <div><button id="ib_v_grid" class="on">Grid</button> <button id="ib_v_cmp">Compare</button> <button id="ib_v_board">Board</button> <button id="ib_v_alias">Aliases</button> <button id="ib_v_unp">Unparsed</button> <button id="ib_reload">&#8635;</button></div>
      <span class="status" id="ib_meta">IDB tab prices with its OWN override inputs (amber): any change to a row's inputs re-nukes that row automatically (Nuke's engine + model/X settings; Nuke Station untouched); override result, my bid/offer, gap and flags come only from those runs</span>
    </div>
  </div>
  <div id="ib_scroll" style="overflow:auto;max-height:calc(100vh - 190px)">
    <table id="ib_tbl" class="report blgrid"><thead></thead><tbody></tbody></table>
  </div>
</div>
<div id="tab-nuke" class="hide">
<div class="bar">
  <button id="nretry" class="refresh">&#8635; Connect</button>
  <a id="nopen" href="#" target="_blank"><button>Open in new window</button></a>
  <span id="nstat" style="color:#6e6a63"></span>
</div>
<div class="status" id="nstatus">Checking Nuke Station server ...</div>
<iframe id="nframe" style="height:calc(100vh - 160px)"></iframe>
</div>

<script>
const $=id=>document.getElementById(id);
let AUTH={user:"",role:""};
async function authBoot(){
  try{
    const r=await fetch('/api/auth/me');
    if(r.status===401){ location.href='/login'; return; }
    const j=await r.json();
    if(!j.ok){ location.href='/login'; return; }
    AUTH={user:j.user,role:j.role};
    try{
      const pr=await (await fetch('/api/pref')).json();
      if(pr.ok&&pr.prefs){
        ['lagrange.cfg','lagrange.rhw','lagrange.twto','lagrange.twcc'].forEach(k=>{
          if(pr.prefs[k]!=null&&pr.prefs[k]!=='')
            localStorage.setItem(k,pr.prefs[k]);
        });
        RFQ_CFG=rfqLoadCfg(); rfqApplyCfg();
        if(rfqRows.length) rfqRender(); else rfqHeader();
      }
    }catch(e){}
    $('who_badge').textContent=AUTH.user+' \u00b7 '+AUTH.role;
    if(AUTH.role==='sales'){
      ['recon','delta','bau','blotter','twcb','nuke'].forEach(x=>{
        const b=$('tabbtn-'+x); if(b) b.style.display='none';
        const d=$('tab-'+x); if(d) d.classList.add('hide');
      });
      showTab('rfq');
      if($('rf_mon')) $('rf_mon').style.display='none';
    }
  }catch(e){}
}
authBoot();
function showTab(t){
  window.curTab=t;
  ['recon','delta','dscan','risk','bau','blotter','rfq','twcb','nuke','conv','idb'].forEach(x=>{
    if(t==='conv' && x==='conv') convLoad();
    if(t==='idb' && x==='idb') idbLoad();
    if(t==='dscan' && x==='dscan'){
      const u='/dscan/';
      $('ds_open').href=u;
      fetch('/api/dscan/status').then(r=>r.json()).then(j=>{
        const st=$('ds_status'); if(!st) return;
        st.textContent=j.ok?('delta scan \u00b7 '+j.note)
          :('DELTA SCAN NOT MOUNTED \u2014 '+j.note);
        st.className='status '+(j.ok?'ok':'err');
        if(j.ok && !$('ds_frame').src) $('ds_frame').src=u+'?v='+Date.now();
      }).catch(()=>{ if(!$('ds_frame').src) $('ds_frame').src=u; }); }
    if(t==='risk' && x==='risk') riskLoad();
    if(t==='nuke' && x==='nuke') nukeConnect();
    if(t==='blotter' && x==='blotter') blotterLoad();
    if(t==='rfq' && x==='rfq') rfqLoad();
    $('tab-'+x).classList.toggle('hide', x!==t);
    $('tabbtn-'+x).classList.toggle('active', x===t);
  });
}
$('mode').onchange=()=>{
  const m=$('mode').value;
  $('d1').classList.toggle('hide', m==='today');
  $('d2').classList.toggle('hide', m!=='range');
};
function setS(id,t,cls){const s=$(id);s.textContent=t;s.className='status '+(cls||'');}
async function post(url,body){
  const r=await fetch(url,{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
  return [r.ok, await r.json()];
}

/* ---- tab: risk positions ---- */
let RK={cols:[],rows:[]};
async function riskLoad(){
  setS('rk_meta','loading\u2026','');
  let j; try{ j=await (await fetch('/api/risk/latest')).json();
  }catch(e){ setS('rk_meta','load failed: '+e,'err'); return; }
  if(!j.ok){ setS('rk_meta',j.error||'error','err'); return; }
  RK=j; riskRender();
  setS('rk_meta','snapshot '+(j.snap||'?')+' \u00b7 batch '
    +(j.loaded||'?')+' (\u226460s window) \u00b7 '
    +j.rows.length+' rows \u00b7 '+(j.table||''),'ok');
}
function riskRender(){
  const f=($('rk_filt').value||'').toLowerCase();
  const rows=f?RK.rows.filter(r=>r.join(' ')
    .toLowerCase().includes(f)):RK.rows;
  $('rk_tbl').querySelector('thead').innerHTML='<tr>'+
    RK.cols.map(c=>'<th>'+blEsc(c)+'</th>').join('')+'</tr>';
  $('rk_tbl').querySelector('tbody').innerHTML=rows.map(r=>
    '<tr>'+r.map((v,i)=>'<td class="'+
    (/^-?[0-9,.]+$/.test(v)?'num':'')+'">'+blEsc(v)+'</td>')
    .join('')+'</tr>').join('');
}
if($('rk_reload')) $('rk_reload').onclick=riskLoad;
if($('rk_filt')) $('rk_filt').oninput=riskRender;

/* ---- tab 1: recon ---- */
$('refresh').onclick=async()=>{
  const m=$('mode').value;
  const body={mode:m, skip_loader:$('skiploader').checked};
  if(m==='date')  body.date=$('d1').value;
  if(m==='range'){body.date_from=$('d1').value; body.date_to=$('d2').value;}
  $('refresh').disabled=true;
  setS('status','Refreshing: loading txt exports, querying DB, reconciling ...');
  try{
    const [ok,j]=await post('/api/refresh',body);
    if(!ok||!j.ok){
      setS('status','ERROR: '+(j.error||'refresh failed')+'\n'+(j.loader||''),'err');
      return;
    }
    $('frame').srcdoc=j.html;
    $('built').textContent='| recon built '+j.built_at+' | '+j.label;
    $('draft').disabled=false; $('sendnow').disabled=false;
    const tag=j.alerts===0?'CLEAN':'ALERTS: '+j.alerts;
    setS('status',tag+' | matched '+j.matched+' | '+j.subject+
      (j.loader_ok?'':'\n[loader warning] '+j.loader),
      j.alerts===0?'ok':'err');
  }catch(e){ setS('status','ERROR: '+e,'err'); }
  finally{ $('refresh').disabled=false; }
};
async function reconSend(sendNow){
  if(sendNow && !confirm('Send the recon email NOW?')) return;
  $('draft').disabled=true; $('sendnow').disabled=true;
  setS('status',sendNow?'Sending via Outlook ...':'Opening Outlook draft ...');
  try{
    const [ok,j]=await post('/api/send',
      {to:$('to').value, cc:$('cc').value, send:sendNow});
    setS('status',(j.ok?'Outlook: ':'Outlook ERROR: ')+j.message, j.ok?'ok':'err');
  }catch(e){ setS('status','ERROR: '+e,'err'); }
  finally{ $('draft').disabled=false; $('sendnow').disabled=false; }
}
$('draft').onclick=()=>reconSend(false);
$('sendnow').onclick=()=>reconSend(true);

/* ---- tab 2: delta check ---- */
$('dupdate').onclick=async()=>{
  $('dupdate').disabled=true;
  setS('dstatus','Updating DB via cba_mariadb.py - this can take several minutes ...');
  try{
    const [ok,j]=await post('/api/delta/update_db');
    if(!ok||!j.ok){ setS('dstatus','ERROR: '+(j.error||'update failed')+'\n'+(j.log||''),'err'); return; }
    setS('dstatus','DB updated.\n'+j.log,'ok');
  }catch(e){ setS('dstatus','ERROR: '+e,'err'); }
  finally{ $('dupdate').disabled=false; }
};
$('drefresh').onclick=async()=>{
  $('drefresh').disabled=true;
  const upd=$('dupdfirst').checked;
  setS('dstatus',(upd?'Updating DB, then running':'Running')+
    ' CBA delta + price check ...'+(upd?' (DB update can take several minutes)':''));
  try{
    const [ok,j]=await post('/api/delta/refresh',{update_db:upd});
    if(!ok||!j.ok){ setS('dstatus','ERROR: '+(j.error||'refresh failed'),'err'); return; }
    $('dframe').srcdoc=j.html;
    $('dstats').textContent=j.rows+' rows | '+j.flagged+' flagged | data as of '
      +(j.data_as_of||'?')+' | built '+j.built_at;
    $('ddraft').disabled=false; $('dsendnow').disabled=false;
    setS('dstatus',(j.flagged===0?'ALL WITHIN TOLERANCE':'FLAGGED: '+j.flagged)
      +' | '+j.subject+(j.log?'\n'+j.log:''),
      j.stale?'err':(j.flagged===0?'ok':'err'));
  }catch(e){ setS('dstatus','ERROR: '+e,'err'); }
  finally{ $('drefresh').disabled=false; }
};
async function deltaSend(sendNow){
  if(sendNow && !confirm('Send the delta check email NOW?')) return;
  $('ddraft').disabled=true; $('dsendnow').disabled=true;
  setS('dstatus',sendNow?'Sending via Outlook ...':'Opening Outlook draft ...');
  try{
    const [ok,j]=await post('/api/delta/send',
      {to:$('dto').value, cc:$('dcc').value, send:sendNow});
    setS('dstatus',(j.ok?'Outlook: ':'Outlook ERROR: ')+j.message, j.ok?'ok':'err');
  }catch(e){ setS('dstatus','ERROR: '+e,'err'); }
  finally{ $('ddraft').disabled=false; $('dsendnow').disabled=false; }
}
$('ddraft').onclick=()=>deltaSend(false);
$('dsendnow').onclick=()=>deltaSend(true);

async function runFull(source){
  if(source==='auto' && !confirm(
    'Auto-grab will focus the Derivation window and send Ctrl+E / Ctrl+A / Ctrl+C.\n'+
    'Do not touch keyboard/mouse until it finishes. Continue?')) return;
  $('dfull').disabled=true; $('dauto').disabled=true; $('drefresh').disabled=true;
  setS('dstatus',($('dupdfirst').checked?'Updating DB, then running':'Running')+
    ' FULL pipeline (derivation + CBA) - Bloomberg + Excel + DB, this can take a while ...');
  try{
    const [ok,j]=await post('/api/delta/full',
      {source:source, pasted:$('dpaste').value, update_db:$('dupdfirst').checked});
    if(!ok||!j.ok){
      setS('dstatus','ERROR: '+(j.error||'pipeline failed')+
        (j.log?'\n--- log ---\n'+j.log:''),'err');
      return;
    }
    $('dframe').srcdoc=j.html;
    $('dstats').textContent=j.rows+' rows | '+j.flagged+' flagged | data as of '
      +(j.data_as_of||'?')+' | built '+j.built_at+' (full)';
    $('ddraft').disabled=false; $('dsendnow').disabled=false;
    setS('dstatus',(j.flagged===0?'ALL WITHIN TOLERANCE':'FLAGGED: '+j.flagged)
      +' | '+j.subject+'\n--- log ---\n'+j.log, j.flagged===0?'ok':'err');
  }catch(e){ setS('dstatus','ERROR: '+e,'err'); }
  finally{ $('dfull').disabled=false; $('dauto').disabled=false; $('drefresh').disabled=false; }
}
$('dauto').onclick=()=>runFull('auto');
$('dfull').onclick=()=>runFull('paste');

/* ---- tab 3: BAU tasks ---- */
$('bdate').value = new Date().toISOString().slice(0,10);
$('bmanage').onchange=()=>{ $('bpanel').style.display=$('bmanage').checked?'':'none'; bauLoad(); };
function esc(t){const d=document.createElement('div');d.textContent=t==null?'':t;return d.innerHTML;}
async function bauLoad(){
  const date=$('bdate').value;
  if(!date) return;
  const inc=$('bmanage').checked?'&include_inactive=true':'';
  try{
    const r=await fetch('/api/bau/list?date='+date+inc);
    const j=await r.json();
    if(!j.ok){ setS('bstatus','ERROR loading tasks','err'); return; }
    $('bprog').textContent=j.done+' / '+j.total+' done';
    const now=new Date(); const isToday=date===now.toISOString().slice(0,10);
    const hhmm=now.toTimeString().slice(0,5);
    let h='<tr style="background:#f0f0f0;font-weight:700">'
      +'<td style="padding:6px 10px;width:36px"></td>'
      +'<td style="padding:6px 10px;width:60px">TIME</td>'
      +'<td style="padding:6px 10px">TASK</td>'
      +'<td style="padding:6px 10px;width:70px">CAT</td>'
      +'<td style="padding:6px 10px;width:120px">DONE AT</td>'
      +'<td style="padding:6px 10px;width:260px">COMMENT</td>'
      +'<td style="padding:6px 10px;width:130px"></td></tr>';
    for(const t of j.tasks){
      const overdue=isToday&&!t.done&&t.sched_time&&t.sched_time<hhmm;
      const bg=!t.is_active?'#f3f3f3':t.done?'#f0f7f0':overdue?'#f6ead2':'#fff';
      h+='<tr style="background:'+bg+';border-top:1px solid #e5e5e5'
        +(t.is_active?'':';color:#9a9a9a')+'">'
        +'<td style="padding:5px 10px;text-align:center">'
        +'<input type="checkbox" '+(t.done?'checked':'')
        +' onchange="bauTick('+t.task_id+',this.checked)"></td>'
        +'<td style="padding:5px 10px">'+(t.sched_time||'')+'</td>'
        +'<td style="padding:5px 10px" title="'+esc(t.notes)+'">'
        +esc(t.task_name)+(t.is_active?'':' (archived)')+'</td>'
        +'<td style="padding:5px 10px">'+esc(t.category)+'</td>'
        +'<td style="padding:5px 10px;color:#6e6a63">'+(t.done_at||'')+'</td>'
        +'<td style="padding:3px 6px"><input value="'+esc(t.comment)
        +'" style="width:100%;border:1px solid #ddd;padding:3px 5px" '
        +'onchange="bauComment('+t.task_id+',this.value)"></td>'
        +'<td style="padding:3px 6px">'
        +'<button onclick=\'bauEdit('+JSON.stringify(t).replace(/'/g,"&#39;")+')\'>edit</button> '
        +'<button onclick="bauArch('+t.task_id+','+(t.is_active?'false':'true')+')">'
        +(t.is_active?'archive':'restore')+'</button>'
        +($('bmanage').checked
          ?' <button style="color:#b3261e" onclick="bauDel('+t.task_id
            +',\''+esc(t.task_name).replace(/'/g,"&#39;")+'\')">delete</button>'
          :'')
        +'</td></tr>';
    }
    $('btable').innerHTML=h;
    setS('bstatus','Loaded '+j.date+'.','ok');
  }catch(e){ setS('bstatus','ERROR: '+e,'err'); }
}
async function bauTick(id,done){
  await post('/api/bau/log',{task_id:id,date:$('bdate').value,done:done});
  bauLoad();
}
async function bauComment(id,c){
  await post('/api/bau/log',{task_id:id,date:$('bdate').value,comment:c});
}
function bauEdit(t){
  $('bmanage').checked=true; $('bpanel').style.display='';
  $('bt_id').value=t.task_id; $('bt_name').value=t.task_name;
  $('bt_time').value=t.sched_time||''; $('bt_cat').value=t.category||'';
  $('bt_sort').value=t.sort_order; $('bt_notes').value=t.notes||'';
}
async function bauDel(id,name){
  if(!confirm('DELETE "'+name+'" permanently?\n\n'+
    'This removes the task AND every day\'s completion record for it.\n'+
    'It cannot be undone. (Use archive instead to keep history.)')) return;
  const [ok,j]=await post('/api/bau/delete',{task_id:id});
  if(!ok||!j.ok){ setS('bstatus','ERROR: '+(j.error||'delete failed'),'err'); return; }
  setS('bstatus','Task deleted ('+j.logs_deleted+' day record(s) removed).','ok');
  bauLoad();
}
async function bauArch(id,act){
  await post('/api/bau/archive',{task_id:id,is_active:act}); bauLoad();
}
$('bt_save').onclick=async()=>{
  const body={task_id:$('bt_id').value?parseInt($('bt_id').value):null,
    task_name:$('bt_name').value, sched_time:$('bt_time').value||null,
    category:$('bt_cat').value||null, notes:$('bt_notes').value||null,
    sort_order:parseInt($('bt_sort').value)||100, is_active:true};
  const [ok,j]=await post('/api/bau/task',body);
  if(!ok||!j.ok){ setS('bstatus','ERROR: '+(j.error||'save failed'),'err'); return; }
  $('bt_clear').click(); bauLoad();
};
$('bt_clear').onclick=()=>{ ['bt_id','bt_name','bt_time','bt_cat','bt_notes'].forEach(i=>$(i).value=''); $('bt_sort').value=100; };
$('bload').onclick=bauLoad;
$('bdate').onchange=bauLoad;

/* ---- tab: Trade Blotter (DB-only view of the separate blotter app) ---- */
const BLOTTER_COLS = [   /* order mirrors the CB Trade Blotter app; kinds:
   t text  n num  d date  b checkbox  e editable  h hedge(Done/Open/N-A)  s status */
  ["status","Status","s"], ["trade_id","ID","n"], ["trade_date","Trade Date","d"],
  ["client_side","Side","t"], ["isin","ISIN","t"], ["bond_name","Bond Name","t"],
  ["bond_type","Bond Type","t"], ["bond_currency","Bond CCY","t"],
  ["fx_rate","FX","n"], ["quantity","Quantity","n"], ["price","Price","n"],
  ["client_name","Client","t"], ["client_type","Client Type","t"],
  ["client_account","Client Acct","t"], ["sales","Sales","t"],
  ["trade_type","Trade Type","t"], ["stock_ref","Stock Ref","n"],
  ["fx_ref","FX Ref","n"], ["bond_fx_ref","Bond FX Ref","n"],
  ["stock_quantity","Stock Qty","n"], ["delta","Delta","n"],
  ["parity","Parity","n"], ["bond_usd_settlement","Bond FCS","b"],
  ["stock_usd_settlement","Stock FCS","b"],
  ["bond_settlement_ccy","Bond SetCcy","t"],
  ["stock_settlement_ccy","Stock SetCcy","t"],
  ["working_stock_instruction","WS Instruction","t"],
  ["working_stock_start","WS Start","t"], ["working_stock_end","WS End","t"],
  ["working_fx_instruction","FX Instr","t"], ["working_fx_time","FX Time","t"],
  ["settlement_date","Settlement","d"], ["trader_agree","Trader Agree","t"],
  ["booked","Booked","b"], ["internal_acct","Internal Acct","t"],
  ["citi_give_up_stocks","Citi Give-up","b"],
  ["other_comments","Comments","t"], ["cross_flag","Cross","b"],
  ["cross_quantity","Cross Qty","n"], ["last_updated","Last Updated","t"],
  ["updated_by","Updated","t"],
  ["hedged_delta","Hedged \u0394","h"], ["hedged_fx","Hedged FX","h"],
  ["hedged_vol","Hedged Vol","h"], ["hedged_credit","Hedged Credit","h"],
  ["hedged_rates","Hedged Rates","h"]];
const HEDGE_KEYS = ["hedged_delta","hedged_fx","hedged_vol","hedged_credit",
                    "hedged_rates"];
let blTimer=null, blEditable=[], blRows=[], blES=null, blPending=null;
let blFilt={};                    /* per-column filters (session only) */
const blEditing=()=>{ const a=document.activeElement;
  return !!(a && ((a.dataset&&a.dataset.bf) || a.classList.contains("blf"))); };
document.addEventListener("focusout",()=>{ setTimeout(()=>{
  if(blPending && !blEditing()){ const p=blPending; blPending=null;
    blApply(p,true); } }, 60); });
const BLKEY="lagrange.blotter.layout";
let blLay; try{ blLay=JSON.parse(localStorage.getItem(BLKEY))||{}; }
catch(e){ blLay={}; }
blLay.w = blLay.w||{}; blLay.sort = blLay.sort||{k:"trade_id",dir:-1};
const blSave=()=>localStorage.setItem(BLKEY, JSON.stringify(blLay));
const blEsc=v=>String(v??"").replace(/&/g,"&amp;").replace(/</g,"&lt;")
  .replace(/"/g,"&quot;");
const blNum=v=>{const n=Number(v);return v===""||isNaN(n)?blEsc(v)
  :n.toLocaleString("en-US",{maximumFractionDigits:6});};
const truthy=v=>v==="1"||v==="True"||v==="true";

function blHeader(){
  const sk=blLay.sort.k, sd=blLay.sort.dir;
  $('bl_tbl').querySelector("thead").innerHTML = "<tr>" +
    BLOTTER_COLS.map(([k,label],i)=>
      `<th data-k="${k}"><span class="blh" onclick="blSort('${k}')">${label}` +
      (k===sk ? (sd>0?" &#9650;":" &#9660;") : "") + `</span>` +
      `<span class="blrz" data-i="${i}" data-k="${k}"` +
      ` onmousedown="blRzDown(event,this)"></span></th>`).join("") + "</tr>" +
    "<tr class=\"blfr\">" + BLOTTER_COLS.map(([k])=>
      `<th class="blft"><input class="blf" data-fk="${k}" ` +
      `value="${blEsc(blFilt[k]||"")}" oninput="blFiltChange(this)" ` +
      `placeholder="&#8981;"></th>`).join("") + "</tr>";
}
function blFiltChange(el){
  const v=el.value.trim();
  if(v) blFilt[el.dataset.fk]=v; else delete blFilt[el.dataset.fk];
  blApplyFilters();
}
function blApplyWidths(){
  $('blcolstyle').textContent = Object.entries(blLay.w).map(([k,px])=>{
    const i = BLOTTER_COLS.findIndex(c=>c[0]===k);
    if(i<0) return "";
    return `#bl_tbl th:nth-child(${i+1}),#bl_tbl td:nth-child(${i+1})` +
      `{min-width:${px}px;max-width:${px}px;overflow:hidden;` +
      `text-overflow:ellipsis}`;
  }).join("\n");
}
let _rz=null;
function blRzDown(e,el){
  e.preventDefault(); e.stopPropagation();
  const k=el.dataset.k;
  _rz={k, x:e.clientX, w: blLay.w[k] ||
       (el.parentElement.offsetWidth||100)};
  document.onmousemove=ev=>{
    if(!_rz) return;
    blLay.w[_rz.k]=Math.max(40, _rz.w + (ev.clientX-_rz.x));
    blApplyWidths();
  };
  document.onmouseup=()=>{ if(_rz){ blSave(); }
    _rz=null; document.onmousemove=null; document.onmouseup=null; };
}
function blSort(k){
  if(blLay.sort.k===k) blLay.sort.dir=-blLay.sort.dir;
  else blLay.sort={k, dir:1};
  blSave(); blRender();
}
function blStatus(row){
  if(!truthy(row.booked))
    return {cls:"bunb", cell:'<span class="dot dr"></span>UNBOOKED'};
  if(HEDGE_KEYS.some(h=>row[h]==="Open"))
    return {cls:"bdone bhedge", cell:'<span class="dot dy"></span>HEDGING'};
  return {cls:"bdone", cell:'<span class="dot dg"></span>DONE'};
}
function blRender(){
  blHeader(); blApplyWidths();
  const {k,dir}=blLay.sort;
  const kind=(BLOTTER_COLS.find(c=>c[0]===k)||[])[2];
  const rows=[...blRows].sort((a,b)=>{
    let x=a[k]??"", y=b[k]??"";
    if(kind==="n"||k==="trade_id"){
      x=Number(String(x).replace(/[, ]/g,""))||0;
      y=Number(String(y).replace(/[, ]/g,""))||0;
      return (x-y)*dir;
    }
    return String(x).localeCompare(String(y))*dir;
  });
  const body=rows.map(row=>{
    const st=blStatus(row);
    const searchable=(st.cell.replace(/<[^>]*>/g,"")+" "+
      BLOTTER_COLS.map(c=>row[c[0]]??"").join(" ")).toLowerCase();
    return `<tr class="${st.cls}" data-id="${row.trade_id}" ` +
      `data-tok="${blEsc(row._tok||"")}" data-s="${blEsc(searchable)}">` +
      BLOTTER_COLS.map(([ck,_,ckind])=>{
        const v=row[ck]??"";
        const ed=blEditable.includes(ck);
        const dk=`data-k="${ck}"`;
        if(ckind==="s") return `<td ${dk} class="bst">${st.cell}</td>`;
        if(ckind==="b") return `<td ${dk} class="bck${ed?" bce":""}">${truthy(v)
          ?'<span class="cb on">&#10003;</span>':'<span class="cb"></span>'}</td>`;
        if(ckind==="h"){
          const cls = v==="Done"?"hd":(v==="Open"?"ho":"bna");
          return `<td ${dk} class="${cls}${ed?" bce":""}">${v===""?"N/A":blEsc(v)}</td>`;
        }
        if(ckind==="n") return `<td ${dk} class="bnum${ed?" bce":""}">${blNum(v)}</td>`;
        return `<td ${dk} class="${ed?"bce":""}">${blEsc(v)}</td>`;
      }).join("")+"</tr>";
  }).join("");
  $('bl_tbl').querySelector("tbody").innerHTML=body;
  blApplyFilters();
  blSelApply();
}
const blCellText=td=>{ const i=td.querySelector("input");
  return (i? i.value : td.textContent).toLowerCase(); };
function blApplyFilters(){
  const q=($('bl_q').value||"").trim().toLowerCase();
  const act=Object.entries(blFilt).map(([k,v])=>
    [BLOTTER_COLS.findIndex(c=>c[0]===k), v.toLowerCase()])
    .filter(([i])=>i>=0);
  document.querySelectorAll("#bl_tbl tbody tr").forEach(tr=>{
    let vis = !q || tr.dataset.s.includes(q);
    if(vis && act.length){
      const tds=tr.children;
      vis = act.every(([i,v])=>blCellText(tds[i]).includes(v));
    }
    tr.style.display = vis ? "" : "none";
  });
}
function blotterInit(){
  const today=new Date().toISOString().slice(0,10);
  if(!$('bl_from').value){$('bl_from').value=today;$('bl_to').value=today;}
  if(!$('bl_user').value)
    $('bl_user').value=localStorage.getItem("lagrange.user")||"";
}
async function blotterLoad(quiet){
  blotterInit();
  try{
    const q=new URLSearchParams({dfrom:$('bl_from').value,dto:$('bl_to').value,
      ticker:$('bl_ticker').value.trim(),ttype:$('bl_type').value});
    const r=await fetch('/api/blotter/list?'+q); const j=await r.json();
    if(!j.ok){setS('bl_status','ERROR: '+j.error,'err');return;}
    blApply(j, quiet);
    if(blES) blLiveStart(true);            // filters may have changed
    if(blTimer) clearInterval(blTimer);
    blTimer=setInterval(()=>{
      if(blES) return;                     // live stream owns refresh
      if(blEditing()) return;              // user mid-edit
      if(window.curTab==='blotter') blotterLoad(true);
    },30000);
  }catch(e){ setS('bl_status','ERROR: '+e,'err'); }
}

function blApply(j, quiet){
  blEditable=j.editable||[]; blRows=j.rows||[];
  const sel=$('bl_type'), cur=sel.value;
  sel.innerHTML='<option value="">all types</option>'+
    (j.types||[]).map(t=>`<option${t===cur?" selected":""}>${blEsc(t)}</option>`).join("");
  blRender();
  $('bl_meta').textContent=blRows.length+" trades"+(blES?" \u00b7 live":"");
  $('bl_meta').title='unresolved logical: '+(j.missing||[]).join(', ')+
    String.fromCharCode(10)+'table columns not yet mapped: '+
    (j.unmapped||[]).join(', ');
  if(!quiet) setS('bl_status','Loaded '+blRows.length+
    ' trades. Click headers to sort, drag edges to resize (saved), '+
    'filter per column below the headers. Only Comments / Trader Agree '+
    'editable.','ok');
}

function blLiveParams(){
  return new URLSearchParams({dfrom:$('bl_from').value,dto:$('bl_to').value,
    ticker:$('bl_ticker').value.trim(),ttype:$('bl_type').value}).toString();
}
function blLiveStart(restart){
  if(blES){ try{blES.close();}catch(e){} blES=null; }
  blES = new EventSource('/api/blotter/stream?'+blLiveParams());
  blES.addEventListener('rows', ev=>{
    let j; try{ j=JSON.parse(ev.data); }catch(e){ return; }
    blEditable=j.editable||[];
    blMerge(j.rows||[]);
    $('bl_meta').textContent=(j.rows||[]).length+" trades \u00b7 live";
  });
  blES.addEventListener('err', ev=>{
    try{ setS('bl_status','stream: '+JSON.parse(ev.data).error,'err'); }
    catch(e){}
  });
  blES.onerror = ()=>setS('bl_status',
    'live stream interrupted - reconnecting ...','err');
  $('bl_live').textContent='\u25cf live: on';
  $('bl_live').classList.add('liveon');
  if(!restart) setS('bl_status',
    'LIVE - pushes within ~2s of any blotter change (edits, new trades, '+
    'deletes). Toggle again to stop.','ok');
}
function blLiveStop(){
  if(blES){ try{blES.close();}catch(e){} blES=null; }
  $('bl_live').textContent='\u25cf live: off';
  $('bl_live').classList.remove('liveon');
  setS('bl_status','Live off - back to manual Load / 30s refresh.','ok');
}
$('bl_live').onclick=()=>{ blES ? blLiveStop() : blLiveStart(false); };
/* ---- excel-style editing: selection model, optimistic commits, ---- */
/* ---- surgical cell patches (no full re-renders on edit/push)    ---- */
const BL_CYCLE = { client_side:["BUY","SELL"],
  hedged_delta:["N/A","Open","Done"], hedged_fx:["N/A","Open","Done"],
  hedged_vol:["N/A","Open","Done"], hedged_credit:["N/A","Open","Done"],
  hedged_rates:["N/A","Open","Done"] };
const blKind=k=>(BLOTTER_COLS.find(c=>c[0]===k)||[])[2];
const blRow=id=>blRows.find(x=>String(x.trade_id)===String(id));
const blTd=(id,k)=>document.querySelector(
  `#bl_tbl tr[data-id="${id}"] td[data-k="${k}"]`);
const BL_COLKEYS = BLOTTER_COLS.map(c=>c[0]);
let blSel=null;                       /* {id,k} selected cell */
let blQ={};                           /* per-row promise queues */

function blCellHTML(row, ck, ckind, st){
  const v=row[ck]??"";
  if(ckind==="s") return st.cell;
  if(ckind==="b") return truthy(v)
    ?'<span class="cb on">&#10003;</span>':'<span class="cb"></span>';
  if(ckind==="h") return v===""?"N/A":blEsc(v);
  if(ckind==="n") return blNum(v);
  return blEsc(v);
}
function blCellCls(row, ck, ckind, ed){
  if(ckind==="s") return "bst";
  if(ckind==="b") return "bck"+(ed?" bce":"");
  if(ckind==="h") return (row[ck]==="Done"?"hd":(row[ck]==="Open"?"ho":"bna"))
                  +(ed?" bce":"");
  if(ckind==="n") return "bnum"+(ed?" bce":"");
  return ed?"bce":"";
}
function blPatchRow(row){
  const tr=document.querySelector(`#bl_tbl tr[data-id="${row.trade_id}"]`);
  if(!tr) return false;
  const st=blStatus(row);
  tr.className=st.cls; tr.dataset.tok=row._tok||"";
  tr.dataset.s=(st.cell.replace(/<[^>]*>/g,"")+" "+
    BLOTTER_COLS.map(c=>row[c[0]]??"").join(" ")).toLowerCase();
  BLOTTER_COLS.forEach(([ck,_,ckind],i)=>{
    const td=tr.children[i];
    if(!td || td.querySelector("input")) return;    // never touch open editor
    if(td.classList.contains("pend")) return;       // optimistic in flight
    const ed=blEditable.includes(ck);
    const html=blCellHTML(row,ck,ckind,st);
    if(td.innerHTML!==html) td.innerHTML=html;
    const cls=blCellCls(row,ck,ckind,ed);
    if(td.className.replace(" selcell","")!==cls)
      td.className=cls+(blSel&&blSel.id==String(row.trade_id)&&blSel.k===ck
                        ?" selcell":"");
  });
  return true;
}
function blMerge(newRows){
  const oldIds=new Set(blRows.map(r=>String(r.trade_id)));
  const newIds=new Set(newRows.map(r=>String(r.trade_id)));
  const structural=oldIds.size!==newIds.size ||
    [...newIds].some(id=>!oldIds.has(id));
  const oldBy={}; blRows.forEach(r=>oldBy[String(r.trade_id)]=r);
  blRows=newRows;
  if(structural){ blRender(); blSelApply(); return; }
  newRows.forEach(r=>{
    const o=oldBy[String(r.trade_id)];
    if(!o || o._tok!==r._tok ||
       BL_COLKEYS.some(k=>(o[k]??"")!==(r[k]??""))) blPatchRow(r);
  });
  blApplyFilters();
}
function blSelApply(){
  document.querySelectorAll("#bl_tbl td.selcell")
    .forEach(td=>td.classList.remove("selcell"));
  if(!blSel) return;
  const td=blTd(blSel.id, blSel.k);
  if(td){ td.classList.add("selcell");
    if(td.scrollIntoView) td.scrollIntoView({block:"nearest",inline:"nearest"}); }
}
function blSelect(id,k){ blSel={id:String(id),k}; blSelApply(); }
function blVisibleTrs(){
  return [...document.querySelectorAll("#bl_tbl tbody tr")]
    .filter(t=>t.style.display!=="none");
}
function blMoveSel(dr,dc){
  if(!blSel) return;
  const trs=blVisibleTrs();
  const ri=trs.findIndex(t=>t.dataset.id===blSel.id);
  const ci=BL_COLKEYS.indexOf(blSel.k);
  const nr=Math.min(Math.max(ri+dr,0),trs.length-1);
  const nc=Math.min(Math.max(ci+dc,0),BL_COLKEYS.length-1);
  if(nr>=0 && trs[nr]) blSelect(trs[nr].dataset.id, BL_COLKEYS[nc]);
}
function blQueue(id, fieldsObj, before){
  blQ[id]=(blQ[id]||Promise.resolve()).then(()=>blSend(id,fieldsObj,before))
    .catch(()=>{});
}
async function blSend(id, fieldsObj, before){
  const tr=document.querySelector(`#bl_tbl tr[data-id="${id}"]`);
  const user=($('bl_user').value.trim()||"lagrange");
  localStorage.setItem("lagrange.user",user);
  try{
    const r=await fetch('/api/blotter/edit',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({trade_id:+id, fields:fieldsObj,
        token:tr?tr.dataset.tok:"", user})});
    const j=await r.json();
    const row=blRow(id);
    Object.keys(fieldsObj).forEach(k=>{
      const td=blTd(id,k); if(td) td.classList.remove("pend");
    });
    if(j.ok && j.nochange){
      if(row && before) Object.assign(row,before), blPatchRow(row);
      return;
    }
    if(j.ok){
      if(row){ Object.assign(row, j.changes); row._tok=j.token; blPatchRow(row); }
      const n=Object.keys(j.changes||{}).length,
            m=Object.keys(fieldsObj).length;
      setS('bl_status','Saved '+Object.keys(fieldsObj).join(", ")+
        (n>m?' (+'+(n-m)+' auto-filled)':'')+
        (j.via==="blotter-app"?' \u00b7 blotter screens updated live':' \u00b7 direct DB')
        +'.','ok');
    } else {
      if(row && before){ Object.assign(row,before); blPatchRow(row);
        const td=blTd(id,Object.keys(fieldsObj)[0]);
        if(td){ td.classList.add("bederr");
          setTimeout(()=>td.classList.remove("bederr"),900); } }
      setS('bl_status', j.error, 'err');
      if(r.status===409) blotterLoad(true);
    }
  }catch(e){ setS('bl_status','ERROR: '+e,'err'); }
}
function blCommitOptimistic(id, k, v){
  const row=blRow(id); if(!row) return;
  const before={[k]:row[k], _tok:row._tok};
  row[k]=v;
  blPatchRow(row);
  const td=blTd(id,k); if(td) td.classList.add("pend");
  blQueue(id, {[k]:v}, before);
}
function blOpenEd(td, seed){
  if(!td || td.querySelector("input")) return;
  const tr=td.closest("tr"), id=tr.dataset.id, k=td.dataset.k;
  const row=blRow(id); if(!row) return;
  const raw=row[k]??"";
  td.classList.add("bed");
  td.innerHTML=`<input data-bf="${k}" data-id="${id}">`;
  const inp=td.querySelector("input");
  inp.value = (seed!==undefined ? seed : raw);
  inp.focus();
  if(seed===undefined && inp.select) inp.select();
  let doneFlag=false;
  const close=()=>{ doneFlag=true; td.classList.remove("bed");
    const st=blStatus(row);
    td.innerHTML=blCellHTML(row,k,blKind(k),st);
    td.className=blCellCls(row,k,blKind(k),blEditable.includes(k)); };
  const commit=(mv)=>{
    if(doneFlag) return; doneFlag=true;
    let v=inp.value;
    if(blKind(k)==="n") v=v.replace(/[,\s]/g,"");
    td.classList.remove("bed");
    if(String(v)!==String(raw)){
      row[k]=v; const st=blStatus(row);
      td.innerHTML=blCellHTML(row,k,blKind(k),st);
      td.className=blCellCls(row,k,blKind(k),true)+" pend";
      blQueue(id,{[k]:v},{[k]:raw,_tok:row._tok});
    } else close();
    if(mv) blMoveSel(mv[0],mv[1]);
  };
  inp.onkeydown=ev=>{
    ev.stopPropagation();
    if(ev.key==="Enter"){ ev.preventDefault(); commit([1,0]); }
    else if(ev.key==="Tab"){ ev.preventDefault(); commit([0,ev.shiftKey?-1:1]); }
    else if(ev.key==="Escape"){ close(); blSelApply(); }
  };
  inp.onblur=()=>setTimeout(()=>{ if(!doneFlag) commit(null); },40);
}
function blActivate(td, seed){
  const k=td.dataset.k, id=td.closest("tr").dataset.id;
  if(!blEditable.includes(k)) return;
  const row=blRow(id); if(!row) return;
  if(blKind(k)==="b"){
    blCommitOptimistic(id,k, truthy(row[k])?"0":"1"); return;
  }
  if(k in BL_CYCLE){
    const arr=BL_CYCLE[k], cur=(row[k]||arr[0]);
    blCommitOptimistic(id,k, arr[(arr.indexOf(cur)+1)%arr.length]); return;
  }
  blOpenEd(td, seed);
}
function blCellClick(ev){
  const td=ev.target.closest("td[data-k]");
  if(!td || td.querySelector("input")) return;
  const id=td.closest("tr").dataset.id, k=td.dataset.k;
  const was=blSel && blSel.id===id && blSel.k===k;
  blSelect(id,k);
  if(was || blKind(k)==="b" || k in BL_CYCLE) blActivate(td);
}
document.addEventListener("keydown",ev=>{
  if(window.curTab!=="blotter" || !blSel) return;
  const ae=document.activeElement;
  if(ae && ae.tagName==="INPUT") return;         // filters / editors own keys
  const td=blTd(blSel.id, blSel.k);
  if(!td) return;
  if(ev.key==="ArrowDown"){ ev.preventDefault(); blMoveSel(1,0); }
  else if(ev.key==="ArrowUp"){ ev.preventDefault(); blMoveSel(-1,0); }
  else if(ev.key==="ArrowRight"){ ev.preventDefault(); blMoveSel(0,1); }
  else if(ev.key==="ArrowLeft"){ ev.preventDefault(); blMoveSel(0,-1); }
  else if(ev.key==="Enter"||ev.key==="F2"){ ev.preventDefault(); blActivate(td); }
  else if(ev.key===" "&&(blKind(blSel.k)==="b"||blSel.k in BL_CYCLE)){
    ev.preventDefault(); blActivate(td); }
  else if(ev.key.length===1 && !ev.ctrlKey && !ev.metaKey
          && blEditable.includes(blSel.k)
          && blKind(blSel.k)!=="b" && !(blSel.k in BL_CYCLE)){
    ev.preventDefault(); blActivate(td, ev.key);  // type-to-edit, excel style
  }
});
$('bl_tbl').addEventListener("click", blCellClick);
$('bl_load').onclick=()=>blotterLoad();
$('bl_q').oninput=blApplyFilters;
$('bl_reset').onclick=()=>{ blLay={w:{},sort:{k:"trade_id",dir:-1}};
  blSave(); blRender();
  setS('bl_status','Layout reset (widths + sort).','ok'); };

/* ---- tab: RFQ Station (runs-format lines; upload comes later) ---- */
const RFQ_COLS = [
  ["status","Status"], ["refresh","Refresh"], ["imp_act","Improve"], ["hit","Hit"],
  ["hist","H"], ["copy","\u29c9"],
  ["rfq_id","ID"], ["trade_date","Date"],
  ["isin","ISIN"], ["short_name","Security"],
  ["style","Type"], ["sides","Side"], ["ord","Ord"],
  ["ord_lb","Bid Lvl"], ["ord_la","Ask Lvl"],
  ["qty","Qty"],
  ["eff_vs","Vs"], ["eff_fx","Fx"], ["delta","Delta"],
  ["client","Client"],
  ["bid_px","Bid"], ["ask_px","Ask"],
  ["stock_ref","OvdSpot"], ["fx_ref","OvdFx"], ["q_delta","Delta"],
  ["nk_xb","XBid"], ["orb","orBs"], ["ora","orAs"],
  ["nk_xa","XAsk"], ["nk_x","X"],
  ["vs_usd","Vs$"],
  ["live_bid","LBid"], ["live_ask","LAsk"],
  ["live_spot","LVs"], ["live_und","LFx"], ["live_delta","Delta"],
  ["live_qty","LQty"],
  ["tol","Tol"],
  ["diff_bid","dBid"], ["diff_ask","dAsk"],
  ["flag","Drift"], ["ack","Ack"],
  ["q_act","Q"], ["off_act","\u2298"], ["lv_act","L"], ["mt_act","Match"],
  ["cx_act","\u2715"], ["ex_act","\u23f1"], ["ap_act","Algos"],
  ["notes","Notes"], ["updated_by","By"]];
const RFQ_COL_TOGGLE=[["trade_date","Date"],["isin","ISIN"],
  ["eff_vs","Vs"],["eff_fx","Fx"],["delta","Delta"],["vs_usd","Vs$"],["nk_xb","XBid"],["orb","orBs"],["ora","orAs"],["nk_xa","XAsk"],["nk_x","X"],
  ["live_bid","LBid"],["live_ask","LAsk"],["live_spot","LVs"],
  ["live_und","LFx"],["live_qty","LQty"],["diff_bid","dBid"],
  ["diff_ask","dAsk"],["tol","Tol"],["ord","Lvl"],["hist","H"],["copy","Copy"],
  ["q_act","Q"],["off_act","Off"],["lv_act","Live"],
  ["cx_act","Cxl"],["ex_act","Exp"],["ap_act","Auto"],["notes","Notes"],["updated_by","By"]];
const RFQ_GROUPS={status:"T",refresh:"T",hit:"T",hist:"T",ord:"T",
  copy:"T",rfq_id:"T",
  trade_date:"T",sides:"T",isin:"T",short_name:"T",style:"T",
  qty:"T",eff_vs:"T",eff_fx:"T",client:"T",
  bid_px:"Q",ask_px:"Q",stock_ref:"Q",fx_ref:"Q",vs_usd:"Q",
  delta:"T",imp_act:"T",mt_act:"X",ord_lb:"T",ord_la:"T",q_delta:"Q",live_delta:"M",pd_bid:"Q",db1:"Q",db1s:"Q",db2:"Q",db2s:"Q",da1:"Q",da1s:"Q",da2:"Q",da2s:"Q",nk_xb:"Q",orb:"Q",ora:"Q",nk_xa:"Q",nk_x:"Q",pd_ask:"M",
  live_bid:"M",live_ask:"M",live_spot:"M",live_und:"M",
  live_qty:"M",
  tol:"C",diff_bid:"C",diff_ask:"C",flag:"C",
  ack:"X",q_act:"X",off_act:"X",lv_act:"X",cx_act:"X",ex_act:"X",ap_act:"X",
  notes:"F",updated_by:"F"};
const RFQ_GROUP_LAB={T:"ticket",Q:"QUOTE",
  M:"model \u00b7 live",C:"check",X:"trader ctrl",F:"notes"};
const RFQ_CFG_KEY="lagrange.cfg";
const RFQ_CFG_DEF={density:"compact",font:"m",
  cols:{isin:false,vs_usd:false},colfmt:{}};
function rfqLoadCfg(){
  try{
    const j=JSON.parse(localStorage.getItem(RFQ_CFG_KEY)||"{}");
    return {density:j.density||RFQ_CFG_DEF.density,
      font:j.font||RFQ_CFG_DEF.font,
      cols:Object.assign({},RFQ_CFG_DEF.cols,j.cols||{}),
      colfmt:j.colfmt||{}};
  }catch(e){
    return JSON.parse(JSON.stringify(RFQ_CFG_DEF));
  }
}
let RFQ_CFG=rfqLoadCfg();
let _prefT=null;
function prefPush(k,v){
  clearTimeout(_prefT);
  _prefT=setTimeout(()=>{
    fetch('/api/pref',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({key:k,val:v})}).catch(()=>{});
  },600);
}
function rfqSaveCfg(){
  const s=JSON.stringify(RFQ_CFG);
  localStorage.setItem(RFQ_CFG_KEY,s);
  prefPush(RFQ_CFG_KEY,s);
}
function rfqColVis(k){
  const c=RFQ_CFG.cols; return (k in c)?c[k]!==false:true;
}
function rfqVisCols(){
  return RFQ_COLS.filter(([k])=>rfqColVis(k));
}
function rfqApplyColFmt(){
  const vis=rfqVisCols(); let css="";
  vis.forEach(([k],i)=>{
    const f=(RFQ_CFG.colfmt||{})[k]; if(!f) return;
    const th=`#rfq_tbl thead tr:last-child th:nth-child(${i+1})`;
    const td=`#rfq_tbl tbody td:nth-child(${i+1})`;
    if(f.w) css+=`${th},${td}{width:${f.w}px;min-width:${f.w}px}\n`+
      `${td} input{width:${Math.max(20,f.w-14)}px}\n`;
    const d=[];
    if(f.fg) d.push("color:"+f.fg);
    if(f.bg) d.push("background:"+f.bg+" !important");
    if(f.bold) d.push("font-weight:700");
    if(d.length) css+=`${td}{${d.join(";")}}\n`;
  });
  $('rfq_colcss').textContent=css;
}
function rfqApplyCfg(){
  const t=$('rfq_tbl');
  t.classList.toggle("den-c",RFQ_CFG.density==="compact");
  t.classList.toggle("fs-s",RFQ_CFG.font==="s");
  t.classList.toggle("fs-l",RFQ_CFG.font==="l");
}
const RFQ_EDIT_TXT = ["isin","qty","client","notes","trade_date"];
const RFQ_STYLE_OPTS = [["outright","outright"],["vs","versus"],
                        ["working","working stock"]];
const RFQ_SIDE_OPTS = [["two_way","both"],["bid","bid"],
                       ["ask","offer"]];
const RFQ_NUM = ["qty","stock_ref","fx_ref","delta","tol","ord_level","ord_level2","req_vs","req_fx","q_delta","db1","db1s","db2","db2s","da1","da1s","da2","da2s"];
let rfqRows=[], rfqTimer=null, rfqFilt="nocxl", rfqSig="";
const RFQ_SEL=new Set();
let rhId=null, rhSig="";
let RFQ_SECMAP={};
const rfqStCls={REQUESTED:"rq-open",QUOTED:"rq-quoted",
                WORKING:"rq-work",HIT:"rq-hit",DONE:"rq-done",
                CANCELLED:"rq-cxl"};
const rqF=v=>parseFloat(String(v??"").replace(/,/g,""));
const rqN=v=>{
  if(v===""||v==null) return "";
  const n=Number(String(v).replace(/,/g,""));
  if(!isFinite(n)) return String(v);
  return n.toLocaleString("en-US",{maximumFractionDigits:6});
};
const rqG=v=>{ const n=rqF(v); return isFinite(n)?String(+n.toFixed(4)):""; };
const rqBA=v=>{ const n=rqF(v); if(!isFinite(n)) return "";
  return (Math.round(n/0.05)*0.05).toFixed(2); };
const rqQty=v=>{ const n=rqF(v);
  return isFinite(n)?Math.round(n).toLocaleString("en-US"):""; };
const effS=r=>{ const o=rqF(r.stock_ref);
  return isFinite(o)?o:rqF(r.live_spot); };
const effF=r=>{ const o=rqF(r.fx_ref);
  return isFinite(o)?o:rqF(r.live_und); };
const effD=r=>{ const o=rqF(r.delta);
  return isFinite(o)?o:rqF(r.live_delta); };
function rfqUser(){
  return AUTH.user||($('bl_user')&&$('bl_user').value.trim())||
    localStorage.getItem("lagrange.user")||"lagrange";
}
function rfqStatusPath(row){
  return row.style==="working"
    ? ["REQUESTED","QUOTED","WORKING","HIT","DONE","CANCELLED"]
    : ["REQUESTED","QUOTED","HIT","DONE","CANCELLED"];
}
function rfqActive(r){
  return ["REQUESTED","QUOTED","WORKING","IMPROVE"].includes(r.status);
}
function rfqOpen(r){
  return rfqActive(r) || r.status==="HIT"; // dealt, awaiting ack
}
function rfqFieldPass(r){
  const f=$('rf_f_from')&&$('rf_f_from').value;
  const t=$('rf_f_to')&&$('rf_f_to').value;
  const d=(r.trade_date||"").slice(0,10);
  if(f&&(!d||d<f)) return false;
  if(t&&(!d||d>t)) return false;
  const q=($('rf_f_txt')&&$('rf_f_txt').value.trim().toUpperCase())||"";
  if(q && !((r.isin||"").toUpperCase().includes(q)||
            (r.short_name||"").toUpperCase().includes(q)||
            (r.client||"").toUpperCase().includes(q)))
    return false;
  const ty=($('rf_f_type')&&$('rf_f_type').value)||"";
  if(ty && r.style!==ty) return false;
  return true;
}
function rfqShown(){
  return rfqRows.filter(r=> (rfqFilt==="all" ? true
    : rfqFilt==="nocxl" ? r.status!=="CANCELLED"
    : rfqFilt==="done" ? r.status==="DONE" : rfqOpen(r))
    && rfqFieldPass(r));
}
function rfqQuoteStr(r){
  // Nuke-station copy grammar, adapted per style + requested side:
  //  versus  two-way: NAME b / a vs S fx F Dd
  //  versus  bid:     NAME b bid vs S fx F Dd
  //  versus  offer:   NAME vs S fx F Dd a offer
  //  outright two-way: NAME b / a ref S fx F
  //  outright bid:     NAME b bid ref S fx F
  //  outright offer:   NAME ref S fx F a offer
  const nm=r.short_name||("#"+r.rfq_id);
  const b=rqBA(r.bid_px!==""?r.bid_px:r.calc_bid);
  const a=rqBA(r.ask_px!==""?r.ask_px:r.calc_ask);
  const sE=effS(r); const s=isFinite(sE)?String(+sE.toFixed(4)):"";
  const fE=effF(r); const f=isFinite(fE)?fE.toFixed(4):"";
  const dE=effD(r);
  const d=isFinite(dE)?" "+Math.round(dE)+"d":"";
  const refs=(r.style==="outright"
    ?((s?" ref "+s:"")+(f?" fx "+f:"")+d)
    :((s?" vs "+s:"")+(f?" fx "+f:"")+d));
  const askOnly=r.sides==="ask"||r.sides==="offer";
  let q;
  if(askOnly){
    q = a ? nm+refs+" "+a+" offer" : nm+" no offer";
  } else if(r.sides==="bid"){
    q = b ? nm+" "+b+" bid"+refs : nm+" no bid";
  } else {
    if(!(b||a)) return nm+" (no px yet)";
    q = nm+" "+(b||"?")+" / "+(a||"?")+refs;
  }
  if(r.style==="working") q+=" working stock";
  return q.replace(/ +/g," ").trim();
}
async function rfqCopy(txt){
  try{ await navigator.clipboard.writeText(txt); return true; }
  catch(e){
    const ta=document.createElement("textarea");
    ta.style.position="fixed"; ta.style.left="-9999px"; ta.value=txt;
    document.body.appendChild(ta); ta.select();
    let ok=false; try{ ok=document.execCommand("copy"); }catch(_){}
    ta.remove(); return ok;
  }
}
function rfqSelHtml(field, v, opts, cls){
  return `<select class="rq-sel${cls?" "+cls:""}" data-rs="${field}" onchange="rfqSel(this)">`+
    opts.map(([val,lab])=>`<option value="${val}"${val===String(v)?" selected":""}>${lab}</option>`).join("")+
    `</select>`;
}
async function rfqSel(sel){
  const tr=sel.closest("tr");
  const j=await rfqEditSend(tr.dataset.id, sel.dataset.rs, sel.value,
                            tr.dataset.tok);
  if(j.ok){ tr.dataset.tok=j.token;
    const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
    if(row){ row[sel.dataset.rs]=j.value; row._tok=j.token; }
    setS('rf_status','Saved '+sel.dataset.rs+' on RFQ '+tr.dataset.id+'.','ok');
    rfqRender(); }
  else{ setS('rf_status',j.error,'err'); rfqLoad(true); }
}
function rfqApplyGroupCss(){
  const vis=rfqVisCols(); let css="";
  vis.forEach(([k],i)=>{
    const g=RFQ_GROUPS[k]||"T";
    if(i>0 && (RFQ_GROUPS[vis[i-1][0]]||"T")!==g)
      css+=`#rfq_tbl tbody td:nth-child(${i+1}){border-left:1px solid var(--border2) !important}\n`;
  });
  $('rfq_grpcss').textContent=css;
}
function rfqHeader(){
  const vis=rfqVisCols();
  let bands=[], cur=null;
  vis.forEach(([k])=>{
    const g=RFQ_GROUPS[k]||"T";
    if(cur&&cur.g===g) cur.n++;
    else { cur={g,n:1}; bands.push(cur); }
  });
  const bandTr="<tr>"+bands.map((b,i)=>
    `<th class="rq-band bd-${b.g}${i>0?" gsep":""}" colspan="${b.n}">${RFQ_GROUP_LAB[b.g]}</th>`).join("")+"</tr>";
  const colTr="<tr>"+vis.map(([k,l],i)=>
    `<th class="g-${RFQ_GROUPS[k]||"T"}${(i>0&&(RFQ_GROUPS[vis[i-1][0]]||"T")!==(RFQ_GROUPS[k]||"T"))?" gsep":""}">${l}</th>`).join("")+"</tr>";
  $('rfq_tbl').querySelector("thead").innerHTML=bandTr+colTr;
  rfqApplyGroupCss();
  rfqApplyColFmt();
}
let _rfqPtr=0;
document.addEventListener('pointerdown',ev=>{
  if(ev.target&&ev.target.closest&&ev.target.closest('#rfq_tbl')) _rfqPtr=Date.now();
},true);
function rfqRender(){
  if(Date.now()-_rfqPtr<450){
    window._rfqDefer=1;
    setTimeout(()=>{ if(window._rfqDefer){window._rfqDefer=0; rfqRender(); } },500);
    return;
  }
  const ae=document.activeElement;
  let keep=null;
  if(ae&&ae.closest&&ae.closest('#rfq_tbl')&&
     (ae.tagName==='INPUT'||ae.tagName==='SELECT')){
    const tr=ae.closest('tr');
    const key=ae.dataset.rf?('rf:'+ae.dataset.rf):
      (ae.dataset.rs?('rs:'+ae.dataset.rs):null);
    if(tr&&key) keep={id:tr.dataset.id,key,val:ae.value,
      s:ae.selectionStart,e:ae.selectionEnd};
  }
  window._rfqDefer=0;
  rfqHeader();
  const shown=rfqShown();
  $('rfq_tbl').querySelector("tbody").innerHTML = shown.map(row=>{
    return `<tr data-id="${row.rfq_id}" data-tok="${blEsc(row._tok||"0")}"`+
      ` class="${row.status==="DONE"?"bdone":row.status==="HIT"?"bnack":row.status==="WORKING"?((row.off_flag||String(row.adj_req||"")==="1")&&rfqActive(row)?"badj":"bwork"):row.status==="REQUESTED"?(row.off_flag&&rfqActive(row)?"badj":"breq"):row.status==="QUOTED"?"bqtd":row.status==="IMPROVE"?"bimp":row.status==="CANCELLED"?"bcxl":""}${(row.refresh_by&&rfqActive(row))?" brefr":""}${RFQ_SEL.has(String(row.rfq_id))?" rowsel":""}">`+
      rfqVisCols().map(([k])=>{
        const sales=AUTH.role==="sales"&&(AUTH.user||"")!=="jb33880";
        const v=row[k]??"";
        if(sales&&["orb","ora","live_bid","live_ask","live_spot","live_und","live_delta","pd_ask","nk_xb","nk_xa","nk_x"].includes(k))
          return '<td class="rq-num" style="color:#c9c9c9" title="trader-only">\u2014</td>';
        if(k==="status"){
          const up=(v==="DONE"&&AUTH.role!=="sales")
            ?' <span class="rq-up" data-up="1" title="stage for '+
             'blotter upload (connection off)">&#8686;</span>':'';
          const pend=!!(row.refresh_by&&rfqActive(row));
          const adj=!pend&&row.off_flag&&(v==="REQUESTED"||v==="WORKING")&&
            rfqActive(row);
          const expd=!pend&&row.off_flag==="expired"&&
            rfqActive(row);
          const adjrq=!pend&&!adj&&!expd&&String(row.adj_req||"")==="1"&&rfqActive(row);
          const ordreq=!pend&&!adj&&!adjrq&&row.ord_side&&
            v==="REQUESTED";
          const lab=pend?"REFRESH"
            :expd?"EXPIRED"
            :adj?"ADJUSTING"
            :adjrq?"ADJ REQ"
            :ordreq?"ORD REQ"
            :(v==="HIT"&&row.hit)
              ?"HIT "+String(row.hit).toUpperCase():String(v);
          const scls=(rfqStCls[v]||"")+(pend?" rq-rfsh":"")+
            (adj?" rq-adj":"");
          return `<td class="rq-selc" `+
            `title="RFQ #${blEsc(row.rfq_id)} \u00b7 status is driven by actions (Send / Q / Hit / ACK / REJ / \u2715)`+
            `${row.ord_side?` \u00b7 WORKING ORDER: client ${row.ord_side==="buy"?"BUYS - improve the ASK":row.ord_side==="sell"?"SELLS - improve the BID":"two-way - work both"}${row.ord_level?" toward "+blEsc(rqG(row.ord_level)):""}${v==="REQUESTED"?" \u00b7 quote (Q) to start WORKING":""}`:""}`+
            `${row.style==="working"?" \u00b7 working stock path":""}`+
            `${pend?` \u00b7 REFRESH requested by ${blEsc(row.refresh_by)} at ${blEsc(row.refresh_at||"")} (underlying ${blEsc(v)}; Q / \u27f3 / \u2a2f answers it)`:""}`+
            `${adj?` \u00b7 quote OFF ${row.off_flag==="auto"?"(model moved beyond Tol)":"by "+blEsc(row.off_by||"")} - trader is adjusting the price; Q re-quotes`:""}">`+
            `<span class="rq-st ${scls}">${lab}</span>`+up+`</td>`;
        }
        if(k==="rfq_id")
          return `<td class="rq-id" title="created ${blEsc(row.created_at||"")}`+
            ` by ${blEsc(row.created_by||"")}">#${blEsc(v)}</td>`;
        if(k==="trade_date")
          return `<td class="bed" title="trade date (YYYY-MM-DD)">`+
            `<input data-rf="trade_date" value="${blEsc(v)}" `+
            `onchange="rfqEdit(this)"></td>`;
        if(k==="sides")
          return `<td class="rq-selc" title="what we show the client: `+
            `bid / offer / both">`+
            rfqSelHtml("sides", v, RFQ_SIDE_OPTS)+`</td>`;
        if(k==="short_name")
          return `<td title="sec ${blEsc(row.sec_id||"-")} \u00b7 ${blEsc(row.ric||"-")} \u00b7 ${blEsc(row.und_fx||"-")} \u00b7 ${blEsc(row.ccy||"-")}">${blEsc(v)}</td>`;
        if(k==="style")
          return `<td class="rq-selc" title="trade type">`+
            rfqSelHtml("style", v, RFQ_STYLE_OPTS)+`</td>`;
        if(k==="hit"){
          if(sales&&!["QUOTED","WORKING"].includes(row.status))
            return '<td class="rq-hitc"></td>';
          const sd=String(row.sides||"two_way");
          const canB=sd==="two_way"||sd==="bid";
          const canA=sd==="two_way"||sd==="ask"||sd==="offer";
          const mk=(s,lab,ttl)=>
            `<b class="rq-hb${v===s?" on":""}" data-hit="${s}" `+
            `title="${ttl}${v===s?" \u00b7 click again to clear":""}">${lab}</b>`;
          return `<td class="rq-hitc">`+
            (canB?mk("bid","B","hit bid \u00b7 client SOLD to us at our bid"):"")+
            (canA?mk("ask","A","hit ask \u00b7 client BOUGHT from us at our ask"):"")+
            `</td>`;
        }
        if(k==="eff_vs"||k==="eff_fx"){
          const fld=k==="eff_vs"?"req_vs":"req_fx";
          const typed=row[fld]??"";
          const e=k==="eff_vs"?effS(row):effF(row);
          const d=!isFinite(e)?"":(k==="eff_vs"?rqN(String(+e.toFixed(4)))
                                              :e.toFixed(4));
          if(!rfqActive(row))
            return `<td class="rq-ref" title="ref on the quote `+
              `(override wins, else live)">${blEsc(d)}</td>`;
          return `<td class="bed" title="SALES-REQUESTED ref (the ticket) \u00b7 `+
            `distinct from the trader\u2019s OvdSpot/OvdFx `+
            `override \u00b7 prices the quote when no trader `+
            `override stands \u00b7 ghost = effective ref now">`+
            `<input data-rf="${fld}" style="width:64px" oninput="rqPrev(this)" `+
            `value="${blEsc(rqG(typed))}" placeholder="${blEsc(d)}" `+
            `onchange="rfqEdit(this)"></td>`;
        }
        if(k==="bid_px"||k==="ask_px"){
          const calc=k==="bid_px"?row.calc_bid:row.calc_ask;
          const nm=k==="bid_px"?"Bid":"Ask";
          const side=k==="bid_px"?"bid":"ask";
          if(v!==""){
            const slip=k==="bid_px"?row.slip_bid:row.slip_ask;
            const lt="standing "+nm+" \u00b7 rev "+(row.q_rev||"1")+(row.trace?" \u00b7 "+row.trace:"")+
              " \u00b7 model now "+(rqBA(calc)||"-")+
              (slip?(" \u00b7 slippage "+slip+" (+ve = favorable)"):"")+
              " \u00b7 \u27f3 refresh this side, \u2a2f pull it, Q both";
            const ch=(AUTH.role!=="sales")&&rfqActive(row)
              ?`<span class="qc"><b class="qc-r" data-side="${side}" `+
               `title="refresh ${side} to model">\u27f3</b>`+
               `<b class="qc-x" data-side="${side}" `+
               `title="pull ${side} (off the quote)">\u2a2f</b></span>`:"";
            return `<td class="rq-q rq-qcell" title="${blEsc(lt)}">${ch}${rqBA(v)}</td>`;
          }
          if(k==="ask_px"){
            const lq=parseFloat(String(row.live_qty||"").replace(/,/g,""));
            if(isFinite(lq)&&lq===0)
              return `<td class="rq-qd" title="LQty is 0 \u2014 no live quantity, ask suppressed"></td>`;
          }
          const lt="draft "+nm+" \u2014 model at your refs, not yet "+
            "quoted to the client \u00b7 press Q to confirm rev 1";
          return `<td class="rq-qd" title="${blEsc(lt)}">${rqBA(calc)}</td>`;
        }
        if(k==="stock_ref"||k==="fx_ref"){
          const std=row.bid_px!==""||row.ask_px!=="";
          const qref=k==="stock_ref"?row.q_spot:row.q_fx;
          const lref=k==="stock_ref"?row.live_spot:row.live_und;
          const ph=(std&&qref!==""&&qref!=null)?qref:lref;
          const nm = k==="stock_ref"?"ovdSpot":"ovdUndFx";
          const tt = nm+" override \u00b7 "+
            ((std&&qref!==""&&qref!=null)
              ?("refs behind standing quote rev "+
                (row.q_rev||"1")+" ("+ph+") \u00b7 live now "+
                (lref||"-"))
              :("blank = live ("+(lref||"-")+")"))+
            " \u00b7 type to PIN the model \u00b7 freezes when the client deals";
          const dv=k==="fx_ref"
            ?(v===""?"":rqF(v).toFixed(4)):rqN(rqG(v));
          const dp=k==="fx_ref"
            ?(ph===""?"":rqF(ph).toFixed(4)):rqN(ph||"");
          const lc=(k==="stock_ref"&&AUTH.role!=="sales"&&
            rfqActive(row)&&(row.live_spot||row.live_und))
            ?`<span class="qc qr"><b class="qc-l" title="copy LiveVs / LiveFx into OvdSpot / OvdFx">\u2b05</b></span>`:"";
          return `<td class="bed rq-qcell" title="${blEsc(tt)}">${lc}<input data-rf="${k}" `+
            `value="${blEsc(dv)}" placeholder="${blEsc(dp||"\u2014")}" `+
            `onchange="rfqEdit(this)"></td>`;
        }
        if(k==="delta"){
          const tt = "delta % override \u00b7 blank = live nDelta% ("+
            (row.live_delta||"-")+"%) \u00b7 prices the quote";
          return `<td class="bed" title="${blEsc(tt)}"><input data-rf="delta" `+
            `value="${blEsc(rqG(v))}" placeholder="" `+
            `onchange="rfqEdit(this)" style="width:34px">%</td>`;
        }
        if(k==="vs_usd"){
          const s=effS(row), f=effF(row);
          const u=(isFinite(s)&&isFinite(f)&&f!==0)?(s/f).toFixed(2):"";
          return `<td class="rq-ref" title="effective Vs / Fx">${u}</td>`;
        }
        if(k==="live_bid"||k==="live_ask")
          return `<td class="rq-live" title="live model quote (0.05 grid) \u00b7 ${blEsc(row.live_ts||"")}">${rqBA(v)}</td>`;
        if(k==="live_spot"||k==="live_und"){
          const d=k==="live_und"
            ?(v===""?"":rqF(v).toFixed(4)):rqN(rqG(v));
          return `<td class="rq-live" title="live \u00b7 ${blEsc(row.live_ts||"")}">${blEsc(d)}</td>`;
        }
        if(k==="live_qty")
          return `<td class="rq-qty" title="current position (eqrms)">${blEsc(rqN(v))}</td>`;
        if(k==="diff_bid"||k==="diff_ask"){
          const q=rqBA(k==="diff_bid"
            ?(row.bid_px!==""?row.bid_px:row.calc_bid)
            :(row.ask_px!==""?row.ask_px:row.calc_ask));
          const l=rqBA(k==="diff_bid"?row.live_bid:row.live_ask);
          let txt="", cls="rq-ref";
          if(q!==""&&l!==""){
            const d=+(q-l).toFixed(2);
            txt=(d>0?"+":"")+d.toFixed(2);
            if(d>0) cls+=" rq-dp"; else if(d<0) cls+=" rq-dn";
          }
          return `<td class="${cls}" title="quote minus live model `+
            `(0.05 grid)">${txt}</td>`;
        }
        if(k==="qty")
          return `<td class="bed" title="RFQ trade size"><input data-rf="qty" `+
            `value="${blEsc(rqQty(v))}" onchange="rfqEdit(this)"></td>`;
        if(k==="ack"){
          if(row.status==="DONE"&&row.ack_by)
            return `<td class="rq-ackd" title="confirmed DONE by ${blEsc(row.ack_by)} \u00b7 ${blEsc(row.ack_at||"")}">\u2713 ${blEsc(row.ack_by)}</td>`;
          if(row.status==="HIT")
            return AUTH.role==="sales"
              ? `<td class="rq-ackn" title="client dealt - waiting for the trader to confirm"><span class="rq-await">awaiting</span></td>`
              : `<td class="rq-ackn"><span class="rq-ab" title="confirm the dealt trade \u2192 books it DONE">ACK</span><span class="rq-rj" title="bust the hit \u2192 back to the standing quote">REJ</span></td>`;
          if(row.status==="DONE")
            return AUTH.role==="sales"
              ? `<td class="rq-ackn"><span class="rq-await">awaiting</span></td>`
              : `<td class="rq-ackn"><span class="rq-ab" title="confirm the dealt trade \u2192 books it DONE">ACK</span></td>`;
          return `<td class="rq-na">\u2014</td>`;
        }
        if(k==="imp_act"){
          if(!["QUOTED","WORKING"].includes(row.status))
            return '<td class="rq-tbtn"></td>';
          return `<td><button class="rq-adjb rq-impb" title="submit improve terms: fill the level(s) for the quoted side(s) first \u2014 missing cells go red">improve</button></td>`;
        }
        if(k==="mt_act"){
          if(sales||!rfqActive(row))
            return '<td class="rq-tbtn"></td>';
          const has=(row.ord_level??"")!==""||(row.ord_level2??"")!=="";
          return `<td>${has?`<button class="rq-mtb" title="match the sales improve terms: quote moves to the requested level(s)">match</button>`:""}</td>`;
        }
        if(k==="refresh"&&sales&&!["QUOTED","WORKING"].includes(row.status))
          return '<td></td>';
        if(k==="refresh"&&sales&&row.ord_side&&rfqActive(row)){
          const on=String(row.adj_req||"")==="1";
          return `<td><button class="rq-adjb${on?" on":""}" title="request the trader to adjust this working order">adj</button></td>`;
        }
        if(k==="refresh"){
          if(!rfqActive(row))
            return `<td class="rq-na">\u2014</td>`;
          if(row.refresh_by)
            return `<td class="rq-rfc"><span class="rq-rf rq-rfp" title="refresh requested by ${blEsc(row.refresh_by)} \u00b7 ${blEsc(row.refresh_at||"")} \u00b7 trader: Q / \u27f3 / \u2a2f clears it">\u27f3 ${blEsc(row.refresh_by)}</span></td>`;
          return `<td class="rq-rfc"><span class="rq-rf" title="sales: ask the trader to refresh this quote">\u27f3 req</span></td>`;
        }
        if(k==="flag"){
          let cd="";
          if(row.status==="QUOTED"&&window._rfqTtl){
            const t0=Date.parse((row.bid_at||row.ask_at||"")
              .replace(" ","T"));
            if(isFinite(t0)){
              const rem=Math.max(0,window._rfqTtl-
                (Date.now()-t0)/1000);
              cd=" \u00b7 "+Math.floor(rem/60)+":"+
                String(Math.floor(rem%60)).padStart(2,"0");
            }
          }
          return `<td class="${blEsc(row.flag_cls||"")}" `+
                 `title="${blEsc(row.flag_title||"")} \u00b7 quote expires after ${Math.round((window._rfqTtl||600)/60)}m; Q resets the clock">${blEsc(v)}${cd}</td>`;
        }
        if(k==="ord"){
          if(!row.ord_side) return '<td class="rq-num"></td>';
          const gl=row.ord_side==="buy"?"B"
            :row.ord_side==="sell"?"S":"2w";
          const hint=row.ord_side==="buy"
            ?"client BUYS - work the ask down toward the bid level"
            :row.ord_side==="sell"
            ?"client SELLS - improve (raise) the BID toward the ask level"
            :"two-way client order - work both sides";
          return `<td class="rq-num" title="${hint}">${gl}</td>`;
        }
        if(k==="ord_lb"||k==="ord_la"){
          const f=k==="ord_lb"?"ord_level":"ord_level2";
          const en=rfqActive(row);
          const vv=row[f]??"";
          if(!en)
            return `<td class="rq-num">${blEsc(rqG(vv))}</td>`;
          return `<td class="bed"><input data-rf="${f}" style="width:48px" value="${blEsc(rqG(vv))}" placeholder="\u2014" onchange="rfqEdit(this)" title="${k==="ord_lb"?"bid level (compulsory when client BUYS / two-way)":"ask level (compulsory when client SELLS / two-way)"}"></td>`;
        }
        if(["nk_xb","orb","ora","nk_xa","nk_x"].includes(k)){
          const NF={nk_xb:"x_bid",orb:"or_bid_sprd",ora:"or_ask_sprd",nk_xa:"x_ask",nk_x:"x_both"};
          if(sales)
            return '<td class="rq-num" style="color:#c9c9c9" title="trader-only">\u2014</td>';
          return `<td class="bed"><input class="nkv" style="width:44px" data-nf="${NF[k]}" data-sid="${row.sec_id||""}" value="${blEsc(v)}" placeholder="0" onchange="nkEdit(this)" title="nuke ${NF[k]} \u00b7 edits sync LIVE to the Nuke station (and back)"></td>`;
        }
        if(k==="q_delta"){
          if(!rfqActive(row)){
            const dv=row.q_delta;
            return `<td class="rq-num">${dv?blEsc(rqG(dv))+" %":""}</td>`;
          }
          const gh=(row.live_delta||"").replace(" %","");
          return `<td class="bed"><input data-rf="q_delta" style="width:40px" value="${blEsc(rqG(row.q_delta??""))}" placeholder="${blEsc(gh)}" oninput="rqPrev(this)" onchange="rfqEdit(this)" title="delta on the quote \u00b7 typed value = used at Q (wins over ticket Delta and live) \u00b7 blank = live nDelta% \u00b7 FOLW re-stamps live"></td>`;
        }
        if(k==="live_delta"){
          return `<td class="rq-num" title="model delta now">${blEsc(row.live_delta||"")}</td>`;
        }
        if(k==="tol"){
          if(sales||!rfqActive(row))
            return `<td class="rq-num" title="off-the-quote tolerance (Drift)">${blEsc(v)}</td>`;
          return `<td class="bed"><input data-rf="tol" style="width:36px" value="${blEsc(v)}" placeholder="0.05" onchange="rfqEdit(this)" title="per-line tolerance: if the model moves beyond this against a standing OUTRIGHT quote, the quote is auto-OFFed and the line shows ADJUSTING (blank = desk default 0.05)"></td>`;
        }
        if(k==="ap_act"){
          if(sales||!rfqActive(row))
            return '<td class="rq-tbtn"></td>';
          const m2=String(row.auto_q||"");
          if(sales||!rfqActive(row)||row.style!=="outright")
            return '<td class="rq-tbtn"></td>';
          const off='<span class="rq-tag" title="registry slot - not yet enabled (ALGOS panel, next phase)">';
          return `<td class="rq-tbtn rq-algos">`+
            `<span class="rq-ap rq-tag t-tol${m2==="1"?" on":""}" data-m="1" title="mode 1 \u00b7 OFF beyond Tol (ADJUSTING), re-quote when the model is back within Tol of the offed px \u00b7 never fires on HIT">TOL</span>`+
            `<span class="rq-ap rq-tag t-tol${m2==="2"?" fw":""}" data-m="2" title="mode 2 \u00b7 FOLLOW: on every breach re-quote straight at live (no off gap) \u00b7 never fires on HIT">FOLW</span>`+
            off+`SPRD</span>`+off+`STALE</span>`+off+`SKEW</span></td>`;
        }
        if(k==="hist")
          return '<td class="rq-tbtn"><span class="rq-h" '+
            'title="view quoting history + event log">H</span></td>';
        if(k==="copy")
          return '<td class="rq-tbtn"><span class="rq-cp" '+
            'title="copy the client quote (same clipboard '+
            'fallback as Nuke)">\u29c9</span></td>';
        if(k==="q_act"){
          const on=(!sales)&&rfqActive(row);
          return `<td class="rq-tbtn">${on?'<span class="rq-qb" title="Quote it: confirm the current Bid/Ask as the standing client quote (rev '+((+row.q_rev||0)+1)+'); the previous rev is kept in history">Q</span>':""}</td>`;
        }
        if(k==="off_act"){
          const on=(!sales)&&rfqActive(row)&&
            (row.bid_px!==""||row.ask_px!=="");
          return `<td class="rq-tbtn">${on?'<span class="rq-pb" title="OFF the quote: pull both sides for now (kept in history) - Q puts a fresh quote back">\u2298</span>':""}</td>`;
        }
        if(k==="lv_act"){
          const on=(!sales)&&rfqActive(row)&&
            (row.live_spot!==""||row.live_und!=="");
          return `<td class="rq-tbtn">${on?'<span class="rq-lv" title="set OvdSpot / OvdFx to the live refs (LVs / LFx) and reprice">L</span>':""}</td>`;
        }
        if(k==="ex_act"){
          const _adj=!!row.off_flag&&row.off_flag!=="expired";
          const on=(!sales)&&rfqActive(row)&&
            (row.bid_px!==""||row.ask_px!==""||_adj);
          return `<td class="rq-tbtn">${on?'<span class="rq-ex" title="expire the standing quote now - exactly what the timeout does: quote off (kept in history), row stays open, Q re-quotes">E</span>':""}</td>`;
        }
        if(k==="cx_act"&&!sales&&(row.status==="CANCELLED"||(row.status==="DONE"&&rfqUser()==="jb33880"))){
          return `<td class="rq-tbtn"><span class="rq-x rq-del" title="delete this cancelled line permanently (removes quote history too)">\ud83d\uddd1</span></td>`;
        }
        if(k==="cx_act"){
          const on=(!sales)&&(rfqActive(row)||row.status==="EXPIRED");
          return `<td class="rq-tbtn">${on?'<span class="rq-x" title="cancel this quote - stops all updates">&#10005;</span>':""}</td>`;
        }
        if(k==="updated_by")
          return `<td title="updated ${blEsc(row.last_updated||"")}">${blEsc(v)}</td>`;
        if(RFQ_EDIT_TXT.includes(k))
          return `<td class="bed"><input data-rf="${k}" `+
            `value="${blEsc(v)}" onchange="rfqEdit(this)"></td>`;
        return `<td>${blEsc(v)}</td>`;
      }).join("")+"</tr>";
  }).join("");
  if(keep){ try{
    const tr=document.querySelector('#rfq_tbl tr[data-id="'+keep.id+'"]');
    const [kind,name]=keep.key.split(':');
    const el=tr&&tr.querySelector((kind==='rf'?'[data-rf="':'[data-rs="')
      +name+'"]');
    if(el){ if(el.tagName==='INPUT'&&el.value!==keep.val) el.value=keep.val;
      el.focus({preventScroll:true});
      if(el.tagName==='INPUT'&&keep.s!=null)
        try{ el.setSelectionRange(keep.s,keep.e); }catch(_){}
    }
  }catch(_){} }
  const nOpen=rfqRows.filter(rfqOpen).length;
  window._rfqNOpen=nOpen; rfqMeta();
  if(window._rfqMs!=null&&$('rf_meta'))
    $('rf_meta').textContent+=' \u00b7 pass '+window._rfqMs+'ms \u00b7 '+(window._rfqBuild||'old-build');
  rfqMonRender();
}
function rfqMeta(){
  const shown=rfqShown();
  $('rf_meta').textContent=shown.length+"/"+rfqRows.length+
    " shown \u00b7 "+(window._rfqNOpen||0)+" open"+
    (RFQ_SEL.size?(" \u00b7 "+RFQ_SEL.size+" selected (Esc clears)"):"");
}
async function rfqSecs(){
  try{
    const j=await (await fetch('/api/rfq/secs')).json();
    if(!j.ok) return;
    RFQ_SECMAP={};
    const opts=[];
    for(const x of (j.secs||[])){
      const sn=(x.short_name||"").trim(), isin=(x.isin||"").trim();
      const rec={sec_id:String(x.sec_id||""), short_name:sn, isin:isin};
      if(sn) RFQ_SECMAP[sn.toUpperCase()]=rec;
      if(isin) RFQ_SECMAP[isin.toUpperCase()]=rec;
      if(sn) opts.push(`<option value="${blEsc(sn)}">${blEsc(isin||x.sec_id)}</option>`);
      if(isin) opts.push(`<option value="${blEsc(isin)}">${blEsc(sn||x.sec_id)}</option>`);
    }
    $('rf_secdl').innerHTML=opts.join("");
  }catch(e){}
}
async function rfqLoad(quiet){
  try{
    if(!Object.keys(RFQ_SECMAP).length) rfqSecs();
    if(!window._rfqSecT){ window._rfqSecT=1;
      setInterval(rfqSecs, 15000); }
    const j=await (await fetch('/api/rfq/list')).json();
    if(!j.ok){ setS('rf_status','ERROR: '+j.error,'err'); return; }
    const sig=JSON.stringify(j.rows||[]);
    window._rfqMs=j.ms; window._rfqTtl=j.qttl||600;
    window._rfqBuild=j.build||window._rfqBuild;
    window.addEventListener('error',ev=>{try{setS('rf_status','ERROR: '+ev.message+' @'+(ev.lineno||'?')+' \u00b7 '+(window._rfqBuild||'old-build'),'err');}catch(_){}});
window.addEventListener('unhandledrejection',ev=>{try{setS('rf_status','ERROR: '+(ev.reason&&ev.reason.message||ev.reason)+' \u00b7 '+(window._rfqBuild||'old-build'),'err');}catch(_){}});
    if(!window._rfqFast){ window._rfqFast=1;
      const FASTC={live_bid:"lb",live_ask:"la",live_spot:"lvs",live_und:"lfx"};
      setInterval(async()=>{
        if(document.hidden) return;
        let j2; try{ j2=await (await fetch("/api/rfq/live")).json(); }catch(e){ return; }
        if(!j2||!j2.ok||!j2.live) return;
        const vis=rfqVisCols().map(c=>c[0]);
        const idx={}; vis.forEach((k,i)=>idx[k]=i);
        document.querySelectorAll("#rfq_tbl tr[data-id]").forEach(tr=>{
          const lv=j2.live[tr.dataset.id]; if(!lv) return;
          const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
          const tds=tr.children;
          const setT=(k,v)=>{ const i=idx[k];
            if(i==null||!tds[i]) return;
            if(tds[i].querySelector("input,select,span.rq-st")) return;
            tds[i].textContent=v; };
          const f2=x=>x==null?"":Number(x).toFixed(2);
          setT("live_bid",f2(lv.lb)); setT("live_ask",f2(lv.la));
          setT("live_spot",lv.lvs==null?"":rqN(String(+Number(lv.lvs).toFixed(4))));
          setT("live_und",lv.lfx==null?"":Number(lv.lfx).toFixed(4));
          setT("live_delta",lv.ld==null?"":Math.round(lv.ld)+" %");
          if(row){ row.live_bid=f2(lv.lb); row.live_ask=f2(lv.la);
            const qb=parseFloat(row.bid_px), qa=parseFloat(row.ask_px);
            if(isFinite(qb)&&lv.lb!=null) setT("diff_bid",(qb-lv.lb>=0?"+":"")+(qb-lv.lb).toFixed(2));
            if(isFinite(qa)&&lv.la!=null) setT("diff_ask",(qa-lv.la>=0?"+":"")+(qa-lv.la).toFixed(2));
          }
        });
      },250);
    }
    if(j.cold){
      if(j.err) setS('rf_status',
        'station init failed: '+j.err+' \u2014 retrying','err');
      else setS('rf_status',
        'station warming up \u2014 first data in a moment','ok');
      setTimeout(()=>rfqLoad(true),1200);
      if(rfqRows.length) return; }
    if(!(quiet && sig===rfqSig)){ rfqRows=j.rows||[]; rfqRender(); }
    else rfqMonRender();       // ages / order keep ticking
    rfqSig=sig;
    if(rhId && $('rfq_hist').classList.contains('on'))
      rfqHist(rhId, true);
    if(!quiet) setS('rf_status','Loaded '+rfqRows.length+' RFQs \u00b7 showing '+
      rfqFilt.toUpperCase()+'. Status / Type / Side / Hit are dropdowns; '+
      'Bid/Ask auto-price on the 0.05 grid at your amber refs; '+
      '\u29c9 copies the quote; ACK confirms a hit+done trade.','ok');
    if(rfqTimer) clearInterval(rfqTimer);
    rfqWsConnect();
    rfqTimer=setInterval(()=>{
      if(window.curTab!=='rfq' || document.hidden) return;
      const live=window._rfqWs&&window._rfqWs.readyState===1;
      window._rfqTick=(window._rfqTick||0)+1;
      if(live && window._rfqTick%5!==0) return;   // push live: 5s safety poll
      rfqLoad(true);                                // no push: 1s poll
    },1000);
    if(!window._rfqVisT){ window._rfqVisT=1;
      document.addEventListener('visibilitychange',()=>{
        if(!document.hidden && window.curTab==='rfq') rfqLoad(true);});
      document.addEventListener('focusout',ev=>{
        if(window._rfqDefer && ev.target && ev.target.closest &&
           ev.target.closest('#rfq_tbl'))
          setTimeout(()=>{ if(window._rfqDefer){ window._rfqDefer=0;
            rfqRender(); } },80);
      },true); }
  }catch(e){ setS('rf_status','ERROR: '+e,'err'); }
}
async function rfqSend(){
  const typed=($('rf_sec').value||"").trim().toUpperCase();
  const rec=RFQ_SECMAP[typed];
  if(!rec){
    setS('rf_status','Unknown security \u2014 type or pick a short name '+
      'or ISIN from the list.','err');
    return;
  }
  const R=rfqBarReq();
  const miss=Object.entries(R).filter(([id,req])=>req&&
    !($(id).value||'').trim()).map(([id])=>id);
  if(miss.length){
    miss.forEach(id=>$(id).classList.add('miss'));
    setS('rf_status','Working order needs: '+miss.map(i=>
      i==='rf_lvl'?'bid level':i==='rf_lvl2'?'ask level':'vs ref').join(' + '),'err');
    return;
  }
  const user=rfqUser();
  try{
    const r=await fetch('/api/rfq/create',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({sec_id:rec.sec_id,
        short_name:rec.short_name,
        style:$('rf_style').value, sides:$('rf_sides').value,
        ord_side:$('rf_ord')?$('rf_ord').value:'',
        ord_level:$('rf_lvl')?$('rf_lvl').value.trim():'',
        ord_level2:$('rf_lvl2')?$('rf_lvl2').value.trim():'',
        vs:$('rf_vs')?$('rf_vs').value.trim():'',
        fx:$('rf_fx')?$('rf_fx').value.trim():'',
        delta:$('rf_delta')?$('rf_delta').value.trim():'',
        qty:$('rf_qty').value.trim(), client:$('rf_client').value.trim(),
        user})});
    const j=await r.json();
    if(j.ok){
      const wasOrd=$('rf_ord')&&$('rf_ord').value;
      $('rf_qty').value=""; $('rf_client').value="";
      if($('rf_ord')){ $('rf_ord').value=''; rfqOrdLab(); }
      if($('rf_lvl')) $('rf_lvl').value='';
      setS('rf_status',(wasOrd?'WORK ORDER #':'RFQ #')+
        j.rfq_id+' sent.','ok'); rfqLoad(true);
    }
    else setS('rf_status',j.error,'err');
  }catch(e){ setS('rf_status','ERROR: '+e,'err'); }
}
async function rfqEditSend(id, field, value, tok){
  const user=rfqUser();
  const r=await fetch('/api/rfq/edit',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({rfq_id:+id, field, value, token:tok, user})});
  return r.json();
}
let _rqPvT={};
function rqPrev(inp){
  const tr=inp.closest("tr"); if(!tr) return;
  const id=tr.dataset.id;
  clearTimeout(_rqPvT[id]);
  _rqPvT[id]=setTimeout(async()=>{
    const row=rfqRows.find(x=>x.rfq_id===id); if(!row) return;
    const gv=f=>{const i2=tr.querySelector(
      'input[data-rf="'+f+'"]');return i2?i2.value.trim():"";};
    let j; try{ j=await (await fetch("/api/rfq/reprice",{
      method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({sec_id:String(row.sec_id||""),
        style:row.style||"",ovd_spot:gv("stock_ref"),
        ovd_fx:gv("fx_ref"),ovd_delta:gv("delta"),
        q_delta:gv("q_delta")})})).json(); }catch(e){ return; }
    if(!j||!j.ok) return;
    const f2=x=>x==null?"":Number(x).toFixed(2);
    row.calc_bid=f2(j.bid); row.calc_ask=f2(j.ask);
    const vis=rfqVisCols().map(c=>c[0]);
    const idx={}; vis.forEach((k,i)=>idx[k]=i);
    const _lq0=parseFloat(String(row.live_qty||"").replace(/,/g,""));
    [["bid_px",row.calc_bid],["ask_px",row.calc_ask]].forEach(([k,val])=>{
      if(k==="ask_px"&&isFinite(_lq0)&&_lq0===0) return;
      const i3=idx[k]; const td=tr.children[i3];
      if(td&&td.classList.contains("rq-qd")) td.textContent=rqBA(val);
    });
  },180);
}
async function nkEdit(inp){
  const [ok,j]=await post('/api/rfq/nkedit',{
    sec_id:inp.dataset.sid, field:inp.dataset.nf,
    value:inp.value.trim()});
  setS('rf_status', j.ok?('nuke '+inp.dataset.nf+' saved \u2014 synced to Nuke station'):(j.error||'save failed'),
    j.ok?'ok':'err');
}
async function rfqEdit(inp){
  const tr=inp.closest("tr");
  let v=inp.value;
  if(RFQ_NUM.includes(inp.dataset.rf)) v=v.replace(/[,\s]/g,"");
  const j=await rfqEditSend(tr.dataset.id, inp.dataset.rf, v, tr.dataset.tok);
  if(j.ok){ tr.dataset.tok=j.token; inp.value=j.value;
    const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
    if(row){ row[inp.dataset.rf]=j.value; row._tok=j.token; }
    setS('rf_status','Saved '+inp.dataset.rf+' on RFQ '+tr.dataset.id+'.','ok');
    if(["stock_ref","fx_ref","delta"].includes(inp.dataset.rf))
      rfqLoad(true); }
  else{ setS('rf_status',j.error,'err'); rfqLoad(true); }
}
async function rfqHist(id, quiet){
  rhId=id;
  const row=rfqRows.find(x=>x.rfq_id===String(id))||{};
  $('rh_title').textContent="RFQ #"+id+" \u00b7 "+
    (row.short_name||"");
  if(!quiet) $('rh_meta').textContent="loading\u2026";
  $('rfq_hist').classList.add("on");
  $('rfq_mon').classList.remove('on');
  try{
    const j=await (await fetch('/api/rfq/history?rfq_id='+id)).json();
    if(!j.ok){ if(!quiet) $('rh_meta').textContent=j.error; return; }
    const sig=JSON.stringify(j);
    if(quiet && sig===rhSig) return;
    rhSig=sig;
    const rows=j.rows||[];
    $('rh_meta').textContent=rows.length+" revision(s) \u00b7 "+
      "newest first \u00b7 Move = bid change vs previous rev";
    $('rh_tbl').querySelector("tbody").innerHTML=
      rows.map((r,i)=>{
        const prev=rows[i+1];
        let mv="";
        if(prev){
          const d=rqF(r.bid)-rqF(prev.bid);
          if(isFinite(d)&&d!==0) mv=(d>0?"+":"")+d.toFixed(2);
          else if(isFinite(d)) mv="0.00";
        }
        return `<tr><td>r${blEsc(r.rev)}</td>`+
          `<td class="${String(r.action||"").startsWith("pull")?"rq-dn":""}">${blEsc(r.action||"both")}</td>`+
          `<td>${blEsc(String(r.quoted_at||"").slice(11,19))}</td>`+
          `<td>${blEsc(r.quoted_by||"")}</td>`+
          `<td>${rqBA(r.bid)}</td><td>${rqBA(r.ask)}</td>`+
          `<td class="${mv.startsWith("+")?"rq-dp":mv.startsWith("-")?"rq-dn":""}">${mv}</td>`+
          `<td>${blEsc(rqG(r.spot))}</td>`+
          `<td>${blEsc(r.fx===""?"":rqF(r.fx).toFixed(4))}</td>`+
          `<td>${blEsc(rqG(r.delta))}${r.delta!==""?"%":""}</td></tr>`;
      }).join("")||
      '<tr><td colspan="10">no revisions yet - press Q on the line</td></tr>';
    const evs=(j.events||[]).filter(e=>e.field!=="quote");
    $('rh_ev').querySelector("tbody").innerHTML=
      evs.map(e=>{
        const cut=s=>blEsc(String(s??"").slice(0,30));
        const chg=e.field==="created"?cut(e.new)
          :cut(e.old||"\u2205")+" \u2192 "+cut(e.new||"\u2205");
        return `<tr><td>${blEsc(String(e.at||"").slice(5,16))}</td>`+
          `<td>${blEsc(e.by||"")}</td>`+
          `<td class="${e.field==="status"?"rh-st":""}">${blEsc(e.field)}</td>`+
          `<td>${chg}</td></tr>`;
      }).join("")||
      '<tr><td colspan="4">no events yet</td></tr>';
  }catch(e){ $('rh_meta').textContent=String(e); }
}
function rmCap(m,cap){ return Math.min(m,cap); }
function rmAge(ts){
  if(!ts) return 0;
  const t=new Date(String(ts).replace(' ','T'));
  const m=(Date.now()-t.getTime())/60000;
  return isFinite(m)&&m>0?m:0;
}
function rmAgeTxt(m){
  if(m<1) return '<1m';
  if(m<60) return Math.round(m)+'m';
  return (m/60).toFixed(1)+'h';
}
function rfqMonItems(){
  if(AUTH.role==='sales') return [];
  const out=[];
  rfqRows.forEach(r=>{
    const id=r.rfq_id, sec=r.short_name||'';
    if(r.status==='HIT'){
      const qp=Number(r.hit==='ask'?r.ask_px:r.bid_px);
      const lp=Number(r.hit==='ask'?r.live_ask:r.live_bid);
      const mv=(isFinite(qp)&&isFinite(lp))?lp-qp:0;
      const a=rmAge(r.ack_at||r.last_updated||r.quoted);
      out.push({k:'hit',id,sec,
        why:'hit '+String(r.hit||'?').toUpperCase()+
          ' \u00b7 ACK / REJ \u00b7 mkt '+(mv>=0?'+':'')+
          mv.toFixed(2)+' vs deal \u00b7 '+rmAgeTxt(a),
        score:1000+rmCap(a,120)*1.5+rmCap(Math.abs(mv)*100,100)});
      return;
    }
    if(!rfqActive(r)) return;
    if(r.refresh_by){
      const a=rmAge(r.refresh_at);
      out.push({k:'rf',id,sec,
        why:'refresh req by '+r.refresh_by+' \u00b7 '+
          rmAgeTxt(a),score:600+rmCap(a,120)*3});
    }
    const sb=parseFloat(r.slip_bid), sa=parseFloat(r.slip_ask);
    const worst=Math.min(isFinite(sb)?sb:0, isFinite(sa)?sa:0);
    const std=r.bid_px!==''||r.ask_px!=='';
    if(std&&String(r.flag||'').indexOf('PULL')===0){
      out.push({k:'pl',id,sec,
        why:'moved '+worst.toFixed(2)+' against \u00b7 pull / '+
          'requote',score:500+rmCap(Math.abs(worst)*100,90)});
    } else if(std&&String(r.flag||'').indexOf('REQUOTE')===0){
      out.push({k:'re',id,sec,
        why:'refs drifted \u00b7 requote',
        score:300+rmCap(Math.abs(worst)*50,90)});
    }
    if(r.status==='REQUESTED'&&!std){
      const a=rmAge(r.created_at||r.last_updated);
      if(r.off_flag==='expired'){
        /* expired quotes are a normal end state, not an adjustment alert */
      } else if(r.off_flag){
        out.push({k:'aj',id,sec,
          why:'quote OFF '+(r.off_flag==='auto'?'(>tol)'
            :'(trader)')+' \u00b7 re-quote',
          score:495+rmCap(a,4)});
      } else if(!r.ord_side){
        out.push({k:'rq',id,sec,
          why:'no quote yet \u00b7 waiting '+rmAgeTxt(a),
          score:400+rmCap(a,90)});
      }
    }
    if(r.ord_side&&!std&&!r.off_flag){
      const a=rmAge(r.created_at||r.last_updated);
      out.push({k:'wk',id,sec,
        why:'work the order \u00b7 client '+
          (r.ord_side==='buy'?'BUYS':r.ord_side==='sell'
            ?'SELLS':'2-way')+
          (r.ord_level?' @ '+rqG(r.ord_level):'')+
          ' \u00b7 '+rmAgeTxt(a),
        score:400+rmCap(a,90)+3});
    }
    if(std&&String(r.flag||'').indexOf('GOOD')===0){
      const a=rmAge(r.quoted);
      if(a>10) out.push({k:'st',id,sec,
        why:'standing quote '+rmAgeTxt(a)+' old',
        score:100+rmCap(a,180)/2});
    }
  });
  const _ts=id=>{const r=rfqRows.find(x=>x.rfq_id===id)||{};
    return String(r.last_updated||r.created||'');};
  out.sort((a,b)=>_ts(b.id).localeCompare(_ts(a.id)));
  const dis=rmDismissed();
  return out.filter(it=>{ const r=rfqRows.find(x=>x.rfq_id===it.id)||{}; return !dis[it.k+':'+it.id+':'+String(r.row_version||'')]; });
}
function rmDismissed(){ try{ return JSON.parse(localStorage.getItem('rfq.amdis.'+((window.AUTH&&AUTH.user)||''))||'{}'); }catch(_){ return {}; } }
function rmDismiss(k,id){ const r=rfqRows.find(x=>x.rfq_id===id)||{}; const d=rmDismissed(); d[k+':'+id+':'+String(r.row_version||'')]=Date.now();
  const keys=Object.keys(d); if(keys.length>400) keys.sort((a,b)=>d[a]-d[b]).slice(0,keys.length-400).forEach(x=>delete d[x]);
  localStorage.setItem('rfq.amdis.'+((window.AUTH&&AUTH.user)||''),JSON.stringify(d)); rfqMonRender(); }
const RM_LAB={hit:'HIT',rf:'RFRSH',pl:'PULL',aj:'ADJ',
  wk:'WORK',rq:'REQ',re:'RQTE',st:'STALE'};
const RM_CLS={hit:'rm-hit',rf:'rm-rf',pl:'rm-pl',aj:'rm-aj',
  wk:'rm-re',rq:'rm-rq',re:'rm-re',st:'rm-st'};
function rfqMonRender(){
  const el=$('rm_list'); if(!el) return;
  let items=rfqMonItems();
  const _f=($('rf_f_from')||{}).value||'';
  const _t=($('rf_f_to')||{}).value||'';
  if(_f||_t){ items=items.filter(it=>{
    const r=rfqRows.find(x=>x.rfq_id===it.id)||{};
    const d=String(r.last_updated||r.created||'').slice(0,10);
    return (!_f||d>=_f)&&(!_t||d<=_t); }); }
  $('rf_mon_n').textContent=items.length;
  el.innerHTML=items.map(it=>{
    const R=rfqRows.find(x=>x.rfq_id===it.id)||{};
    const tm=String(R.last_updated||R.created||"")
      .slice(5,16).replace("T"," ");
    return `<div class="rm-it" data-k="${it.k}" data-id="${it.id}" title="score ${Math.round(it.score)}">`+
    `<span class="rm-t">${tm}</span>`+
    `<span class="rm-b ${RM_CLS[it.k]}">${RM_LAB[it.k]}`+
    `</span><span class="rm-sec">#${it.id} `+
    `${blEsc(it.sec)}</span>`+
    `<span class="rm-isin">${blEsc(R.isin||"")}</span>`+
    `<span class="rm-ty">${blEsc(R.style||"")}</span>`+
    `<span class="rm-sd">${blEsc(R.sides||"")}</span>`+
    `<span class="rm-by">${blEsc(R.by||R.updated_by||"")}</span>`+
    `<span class="rm-why">${blEsc(it.why)}</span>`+
    `<span class="rm-x" title="dismiss this item (comes back only if the RFQ changes)" onclick="event.stopPropagation();rmDismiss('${it.k}',${it.id})">&#10005;</span></div>`;})
    .join('')||'<div class="rm-meta">nothing needs you - '+
    'all quiet</div>';
}
(function(){
  const _amu=()=>((window.AUTH&&AUTH.user)||'');
  const AK=k=>'rfq.'+k+'.'+_amu();
  const dk=localStorage.getItem(AK('amdock'));
  if(dk==='b') document.body.classList.add('amdock-b');
  const z=parseFloat(localStorage.getItem(AK('amz'))||'11');
  const ap=v=>{const l=$('rm_list');
    if(l) l.style.fontSize=v+'px';
    localStorage.setItem(AK('amz'),String(v));};
  ap(z);
  const dkb=$('rm_dock');
  const lab=()=>{ if(dkb) dkb.innerHTML=
    document.body.classList.contains('amdock-b')
    ?'&#8680;':'&#8681;'; };
  lab();
  if(dkb) dkb.onclick=ev=>{ ev.stopPropagation();
    document.body.classList.toggle('amdock-b');
    localStorage.setItem(AK('amdock'),
      document.body.classList.contains('amdock-b')?'b':'r');
    lab(); };
  if($('rm_zi')) $('rm_zi').onclick=()=>ap(Math.min(15,
    (parseFloat($('rm_list').style.fontSize)||11)+1));
  if($('rm_zo')) $('rm_zo').onclick=()=>ap(Math.max(8,
    (parseFloat($('rm_list').style.fontSize)||11)-1));
  const mon=$('rfq_mon');
  const sz=()=>{ if(!mon) return;
    const w=localStorage.getItem(AK('amw'));
    const h=localStorage.getItem(AK('amh'));
    if(w) mon.style.width=w+'px';
    if(h&&document.body.classList.contains('amdock-b'))
      mon.style.height=h+'px'; };
  sz();
  const gr=$('rm_grip');
  if(gr&&mon){
    let drag=false;
    gr.addEventListener('pointerdown',ev=>{ drag=true;
      gr.setPointerCapture(ev.pointerId);
      ev.preventDefault(); });
    gr.addEventListener('pointermove',ev=>{ if(!drag) return;
      if(document.body.classList.contains('amdock-b')){
        const H=Math.min(window.innerHeight*.7,
          Math.max(90,window.innerHeight-ev.clientY));
        mon.style.height=H+'px';
        localStorage.setItem(AK('amh'),String(Math.round(H)));
      } else {
        const W=Math.min(window.innerWidth*.6,
          Math.max(220,window.innerWidth-ev.clientX));
        mon.style.width=W+'px';
        localStorage.setItem(AK('amw'),String(Math.round(W)));
      } });
    const end=ev=>{ drag=false; };
    gr.addEventListener('pointerup',end);
    gr.addEventListener('pointercancel',end);
  }
  if($('rm_dock')){ const _od=$('rm_dock').onclick;
    $('rm_dock').onclick=ev=>{ _od(ev);
      mon.style.height=''; mon.style.width=''; sz(); }; }
})();
function rfqWsConnect(){
  if(window._rfqWs && window._rfqWs.readyState<=1) return;
  const u=(location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws/rfq';
  let ws; try{ ws=new WebSocket(u); }catch(e){ return; }
  window._rfqWs=ws;
  ws.onmessage=ev=>{ try{
    const m=JSON.parse(ev.data);
    if(m.type==='rev' && m.rev>(window._rfqRev||0)){
      window._rfqRev=m.rev;
      clearTimeout(window._rfqPushT);
      window._rfqPushT=setTimeout(()=>{
        if(window.curTab==='rfq' && !document.hidden) rfqLoad(true);
      },40);                                          // coalesce bursts
    } }catch(_){} };
  ws.onopen=()=>{ window._rfqWsPing=setInterval(()=>{
    try{ ws.send('p'); }catch(_){} },20000); };
  ws.onclose=()=>{ clearInterval(window._rfqWsPing); window._rfqWs=null;
    setTimeout(rfqWsConnect, 2000+Math.random()*1000); };
  ws.onerror=()=>{ try{ ws.close(); }catch(_){} };
}
/* ---- IDB QUOTES tab ---- */
let IB={view:'grid',rows:[],aliases:{}};
const ibN=(v,d)=>(v==null||v==="")?'\u2014':Number(v).toFixed(d==null?2:d);
function ibFlags(f){ if(!f) return ''; return f.split(' | ').map(x=>{
  const c=x.startsWith('MKT')?'fl-x':(x.startsWith('GAP')?'fl-g':((x==='NO MARK'||x.startsWith('HIST')||x==='NO QUOTE')?'fl-m':'fl-s'));
  return '<span class="'+c+'">'+blEsc(x)+'</span>'; }).join(' '); }
function ibDate(){ return $('ib_date').value||''; }
function ibQ(){ return '?date='+encodeURIComponent(ibDate())+'&source='+encodeURIComponent(($('ib_src').value||'').trim()); }
async function idbLoad(){
  const v=IB.view; setS('ib_meta','loading\u2026','');
  ['grid','cmp','board','alias','unp'].forEach(k=>{ const b=$('ib_v_'+k); if(b) b.className=(k===v?'on':''); });
  let j; try{
    const u=v==='grid'?'/api/idb/grid'+ibQ():v==='cmp'?'/api/idb/compare'+ibQ():v==='board'?'/api/idb/board'+ibQ():v==='alias'?'/api/idb/board'+ibQ():'/api/idb/unparsed'+ibQ();
    j=await (await fetch(u)).json(); }catch(e){ setS('ib_meta','load failed: '+e,'err'); return; }
  if(!j.ok){ setS('ib_meta',j.error||'error','err'); return; }
  IB.rows=j.rows||[]; IB.aliases=j.aliases||IB.aliases; idbRender();
  const n=IB.rows.length;
  if(v==='grid'){ const nm=IB.rows.filter(r=>r.secId).length, nx=IB.rows.filter(r=>/MKT/.test(r.idb_flag||'')).length;
    const ns=IB.rows.filter(r=>!r.secId&&r.suggest).length;
    setS('ib_meta',(j.date||'')+' \u00b7 '+n+' broker names \u00b7 '+nm+' mapped \u00b7 '+ns+' suggested (\u2713 to accept) \u00b7 '+nx+' crossed \u00b7 refinitiv: '+(j.rfx_src||'')+' \u00b7 last nuke '+(j.nuke_ts||'\u2014')+(j.nuke_by?' by '+j.nuke_by:''),'ok'); }
  else if(v==='cmp'){ const nr=IB.rows.filter(r=>r.mark_src==='renuke').length; const nx=IB.rows.filter(r=>/MKT/.test(r.flags)).length, ng=IB.rows.filter(r=>/GAP/.test(r.flags)).length, nm=IB.rows.filter(r=>r.flags==='NO MARK').length;
    setS('ib_meta',(j.date||'')+' \u00b7 '+n+' bonds \u00b7 '+nr+' re-nuked @ broker ref \u00b7 '+nx+' crossed \u00b7 '+ng+' gap > 0.5 \u00b7 '+nm+' no mark \u00b7 mkt = broker as quoted, my = desk quote re-nuked at the broker ref','ok'); }
  else setS('ib_meta',(j.date||'')+' \u00b7 '+n+' rows','ok');
}
const IB_SECT=[
 ["idb \u2192 my bond","mapc",[["idb_name","idb name"],["my_short","my short_name"],["map","map"]]],
 ["securities \u00b7 yours","nsec",[["secId","secid"],["company","company"],["short_name","short_name"],["bond_type","bond_t"],["ric","ric"],["expiry","expiry"],["isin","isin"],["sec_fx","sec_fx"],["und_fx","und_fx"]]],
 ["model (last nuke)","mdl",[["n_bid","nbid"],["n_gamma","ngamma"],["n_spread","nspread"],["n_spot","nspot"],["n_spotfx","nspotfx"],["n_delta","ndelta%"],["parityPct","parity%"]]],
 ["override result","res",[["x_bid","xbid"],["or_bid_sprd","orbidsprd"],["ovd_bid","bid"],["ovd_ask","ask"],["or_ask_sprd","oraskspr"],["x_ask","xask"],["x_both","x"],["quote_bid","quotebid"],["quote_ask","quoteask"],["stk_move","stk%"]]],
 ["idb quotes \u00b7 mkt = broker as quoted \u00b7 my = re-nuked @ broker ref","ibq",[["idb_bid","mkt bid"],["idb_bref","@ref"],["idb_btime","time"],["idb_ask","mkt offer"],["idb_aref","@ref"],["idb_atime","time"],["idb_ref","ref used"],["idb_rb","my bid @ref"],["idb_ra","my offer @ref"],["idb_gap","gap"],["idb_flag","flags"]]],
 ["override inputs","uinp",[["ovdSpot","ovdspot"],["ovdCbFx","ovdcbfx"],["ovdUndFx","ovdundfx"]]],
 ["live","lv",[["live_bid","bid"],["live_ask","ask"],["live_spot","spot"],["live_cbfx","cbfx"],["live_undfx","undfx"]]],
 ["eod","eo",[["eod_bid","bid"],["eod_ask","ask"],["eod_spot","spot"],["eod_cbfx","cbfx"],["eod_undfx","undfx"]]],
 ["stock","stk",[["stk_last","last"],["stk_time","time"],["stk_date","date"],["stk_close","close"],["stk_closedt","close dt"]]],
 ["fx","fxc",[["fx_last","fx last"],["fx_time","fx time"],["fx_date","fx date"],["fx_close","fx close"],["fx_closedt","fx close dt"]]]];
const IB_LEFT=new Set(["idb_name","my_short","map","idb_nuked","company","short_name","ric","isin","bond_type","idb_flag","idb_btime","idb_atime","stk_time","stk_date","stk_closedt","fx_time","fx_date","fx_closedt"]);
function ibCell(k,v,sect){
  const cls=sect[1]+(IB_LEFT.has(k)?" l":"")+(k==="quote_bid"||k==="quote_ask"?" qcell":"")+(["idb_rb","idb_ra","idb_gap","idb_ref"].includes(k)?" ibc":"");
  if(sect[1]==="uinp") return '<td class="'+cls+'"><input data-f="'+k+'" value="'+blEsc(v==null?"":v)+'" onchange="ibOvd(this)"></td>';
  if(k==="my_short") return '<td class="'+cls+' uinp"><input data-ms="1" value="'+blEsc(v==null?"":v)+'" placeholder="my short_name (as in Nuke)" onchange="ibMapShort(this)" style="width:110px;text-align:left"></td>';
  if(k==="map"){ const r=IB.rows.find(x=>x.broker_key===CUR_BK)||{};
    if(v==="OK") return '<td class="'+cls+'"><span class="confirmed">mapped</span></td>';
    if(r.suggest) return '<td class="'+cls+'"><span class="assumed">suggest</span> <b>'+blEsc(r.suggest.short_name)+'</b> <button class="cvacc" onclick="ibAccept(this)" title="accept this mapping">&#10003;</button></td>';
    return '<td class="'+cls+'">'+(v==="NO MATCH"?'<span class="assumed">no match in Nuke</span>':'<span class="assumed">unmapped - type your short_name</span>')+'</td>'; }
  if(k==="idb_flag") return '<td class="'+cls+'">'+ibFlags(v)+'</td>';
  if(k==="short_name") return '<td class="'+cls+'"><b>'+blEsc(v==null?"":v)+'</b></td>';
  if(k==="secId") return '<td class="'+cls+'">'+blEsc(v==null?"":String(v))+'</td>';
  let s; if(v==null||v==="") s=""; else if(typeof v==="number") s=(["stk_last","stk_close","idb_bref","idb_aref","live_spot","eod_spot","n_spot"].includes(k)||Math.abs(v)>=1000)?Number(v).toLocaleString("en-US",{maximumFractionDigits:2}):(k==="idb_gap"?((v>0?"+":"")+v.toFixed(3)):(["idb_rb","idb_ra"].includes(k)?v.toFixed(2):(k==="n_delta"?v.toFixed(1)+"%":v.toFixed(2)))); else s=String(v);
  return '<td class="'+cls+'">'+blEsc(s)+'</td>';
}
function idbRenderGrid(){
  const th=$('ib_tbl').querySelector('thead'), tb=$('ib_tbl').querySelector('tbody');
  const ae=document.activeElement; let keep=null;
  if(ae && ae.closest && ae.closest('#ib_tbl') && ae.tagName==='INPUT'){ const tr=ae.closest('tr'); keep={bk:tr&&tr.dataset.bk, f:ae.dataset.f||(ae.dataset.ms?'ms':null), val:ae.value, s:ae.selectionStart, e:ae.selectionEnd}; }
  th.innerHTML='<tr class="band">'+IB_SECT.map(s=>'<td class="b-'+(s[1]||'chk')+'" colspan="'+s[2].length+'">'+s[0]+' &#9662;</td>').join('')+'</tr><tr>'+IB_SECT.map((s,si)=>s[2].map(([k,l],ci)=>'<th class="'+(IB_LEFT.has(k)?'l':'')+(ci===0&&si>0?' gcol':'')+'">'+l+'</th>').join('')).join('')+'</tr>';
  tb.innerHTML=IB.rows.map(r=>{ CUR_BK=r.broker_key; return '<tr data-id="'+(r.secId||'')+'" data-bk="'+blEsc(r.broker_key||'')+'">'+IB_SECT.map((s,si)=>s[2].map(([k],ci)=>ibCell(k,r[k],s).replace('<td class="','<td class="'+(ci===0&&si>0?'gcol ':''))).join('')).join('')+'</tr>'; }).join('');
  if(keep && keep.bk && keep.f){ try{ const tr=tb.querySelector('tr[data-bk="'+keep.bk+'"]'); const el=tr && (keep.f==='ms'?tr.querySelector('input[data-ms]'):tr.querySelector('input[data-f="'+keep.f+'"]'));
    if(el){ if(el.value!==keep.val) el.value=keep.val; el.focus({preventScroll:true}); if(keep.s!=null) try{ el.setSelectionRange(keep.s,keep.e); }catch(_){} } }catch(_){} }
}
let CUR_BK='';
async function ibAccept(el){ const tr=el.closest('tr'); const r=IB.rows.find(x=>x.broker_key===tr.dataset.bk); if(!r||!r.suggest) return;
  const [ok,j]=await post('/api/idb/alias',{broker_key:r.broker_key,my_short:r.suggest.short_name,sec_id:String(r.suggest.secId),status:'ASSUMED'});
  if(j&&j.ok){ setS('ib_meta','mapped '+r.broker_key+' \u2192 '+r.suggest.short_name,'ok'); idbLoad(); } else setS('ib_meta',(j&&j.error)||'accept failed','err'); }
async function ibAcceptAll(){ const s=IB.rows.filter(r=>!r.secId&&r.suggest); let n=0;
  for(const r of s){ const [ok,j]=await post('/api/idb/alias',{broker_key:r.broker_key,my_short:r.suggest.short_name,sec_id:String(r.suggest.secId),status:'ASSUMED'}); if(j&&j.ok) n++; }
  setS('ib_meta','accepted '+n+' suggested mapping(s) - review them in Aliases and mark CONFIRMED','ok'); idbLoad(); }
async function ibMapShort(el){ const tr=el.closest('tr'); const [ok,j]=await post('/api/idb/alias',{broker_key:tr.dataset.bk,my_short:el.value.trim(),status:'ASSUMED'});
  if(j&&j.ok){ setS('ib_meta', j.sec_id?('mapped '+tr.dataset.bk+' \u2192 '+j.my_short+' (secid '+j.sec_id+') - Nuke details loading'):('saved '+tr.dataset.bk+' \u2192 "'+el.value.trim()+'" - no Nuke bond with that short_name yet'), j.sec_id?'ok':'err'); idbLoad(); }
  else setS('ib_meta',(j&&j.error)||'map failed','err'); }
async function ibOvd(el){ if(!el.closest('tr').dataset.id){ setS('ib_meta','map this row to a Nuke bond first','err'); return; } const tr=el.closest('tr'); const [ok,j]=await post('/api/idb/ovd',{sec_id:tr.dataset.id,field:el.dataset.f,value:el.value});
  if(j&&j.ok){ setS('ib_meta','override saved (IDB tab) - '+(j.renuked?'row re-nuked':(j.queued?'re-nuke queued (engine busy)':'re-nuke pending')),'ok'); setTimeout(idbLoad,400); setTimeout(idbLoad,2500); } else setS('ib_meta',(j&&j.error)||'override failed','err'); }
async function ibFill(kind){ const [ok,j]=await post('/api/idb/fill',{kind}); if(j&&j.ok){ setS('ib_meta',(kind==='bref'?'Broker Ref':kind)+' \u2192 ovd applied to '+j.n+' row(s) (IDB tab only'+(j.src?' \u00b7 refinitiv: '+j.src:'')+')','ok'); idbLoad(); } else setS('ib_meta',(j&&j.error)||'fill failed','err'); }
async function ibNuke(){ setS('ib_meta','re-nuking at broker refs\u2026 (IDB overrides only)',''); const [ok,j]=await post('/api/idb/nuke',{}); if(j&&j.ok){ setS('ib_meta','re-nuked '+j.n+' mapped bond(s) at their broker @REF in '+(j.elapsed||'?')+'s'+(j.note?' - '+j.note:''),'ok'); idbLoad(); } else setS('ib_meta',(j&&j.error)||'re-nuke failed','err'); }
let IB_AUTO=null; function ibAutoToggle(){ const b=$('ib_auto'); if(IB_AUTO){ clearInterval(IB_AUTO); IB_AUTO=null; b.textContent='AUTO last: OFF'; b.className=''; } else { IB_AUTO=setInterval(async()=>{ await ibFill('last'); await ibNuke(); },15000); ibFill('last').then(ibNuke); b.textContent='AUTO last: ON'; b.className='g'; } }
function idbRender(){
  const th=$('ib_tbl').querySelector('thead'), tb=$('ib_tbl').querySelector('tbody'); const v=IB.view;
  if(v==='grid'){ idbRenderGrid(); return; }
  const hd=(bands,cols)=>'<tr class="band">'+bands.map(([l,n,b])=>'<td class="b-'+(b||'chk')+'" colspan="'+n+'">'+l+'</td>').join('')+'</tr><tr>'+cols.map(c=>'<th class="'+(c[1]||'')+'">'+c[0]+'</th>').join('')+'</tr>';
  if(v==='cmp'){
    th.innerHTML=hd([["idb \u2192 my bond",3,"mapc"],["mkt (broker as quoted)",6,"ibq"],["my quote (re-nuked @ broker ref)",3,"myq"],["check",3,"chk"]],
      [["idb name","l"],["my short_name","l"],["map",""],["mkt bid","gcol"],["@ref",""],["time",""],["mkt offer",""],["@ref",""],["time",""],["ref used","gcol"],["my bid",""],["my offer",""],["gap","gcol"],["src",""],["flags","l"]]);
    tb.innerHTML=IB.rows.map((r,i)=>'<tr data-i="'+i+'"><td class="mapc l"><b>'+blEsc((r.spellings||[r.broker_key]).join(', '))+'</b></td><td class="mapc l">'+blEsc(r.my_short||'')+'</td>'
      +'<td class="mapc">'+(r.sec_id?'<span class="confirmed">mapped</span>':'<span class="assumed">unmapped</span>')+'</td>'
      +'<td class="ibq mktpx gcol">'+ibN(r.mkt_bid,2)+'</td><td class="ibq sub">'+ibN(r.bid_ref,2)+'</td><td class="ibq sub l">'+blEsc(r.bid_time||'')+'</td><td class="ibq mktpx gcol">'+ibN(r.mkt_offer,2)+'</td><td class="ibq sub">'+ibN(r.offer_ref,2)+'</td><td class="ibq sub l">'+blEsc(r.offer_time||'')+'</td>'
      +'<td class="myq gcol">'+ibN(r.ref_used,2)+'</td><td class="qcell">'+ibN(r.my_bid,2)+'</td><td class="qcell">'+ibN(r.my_offer,2)+'</td>'
      +'<td class="chk ibc gcol">'+(r.gap==null?'\u2014':(r.gap>0?'+':'')+Number(r.gap).toFixed(3))+'</td><td class="chk">'+blEsc(r.mark_src||'')+'</td><td class="chk l">'+ibFlags(r.flags)+'</td></tr>').join('');
  } else if(v==='board'){
    th.innerHTML=hd([["idb \u2192 my bond",3,"mapc"],["latest bid",3,"ibq"],["latest offer",3,"ibq"],["latest trade",2,"trd"],["",2,"chk"]],
      [["idb name","l"],["my short_name","l"],["map",""],["bid","gcol"],["@ref",""],["time",""],["offer","gcol"],["@ref",""],["time",""],["trade","gcol"],["time",""],["quotes",""],["last",""]]);
    tb.innerHTML=IB.rows.map(r=>{ const a=(IB.aliases||{})[r.broker_key]||{}; return '<tr><td class="mapc l"><b>'+blEsc((r.spellings||[r.broker_key]).join(', '))+'</b></td><td class="mapc l">'+blEsc(a.my_short||'')+'</td><td class="mapc">'+(r.sec_id||a.my_short?'<span class="confirmed">mapped</span>':'<span class="assumed">unmapped</span>')+'</td>'
      +'<td class="ibq gcol">'+ibN(r.bid,2)+'</td><td class="ibq">'+ibN(r.bid_ref,2)+'</td><td class="ibq l">'+blEsc(r.bid_time||'')+'</td><td class="ibq gcol">'+ibN(r.offer,2)+'</td><td class="ibq">'+ibN(r.offer_ref,2)+'</td><td class="ibq l">'+blEsc(r.offer_time||'')+'</td><td class="trd gcol">'+ibN(r.trade,2)+'</td><td class="trd l">'+blEsc(r.trade_time||'')+'</td><td class="chk gcol">'+r.n+'</td><td class="chk l">'+blEsc(r.last_time||'')+'</td></tr>'; }).join('');
  } else if(v==='alias'){
    th.innerHTML=hd([["broker",2,"ibq"],["my bond (edit)",4,"mapc"]],[["broker key","l"],["spellings seen","l"],["secid","gcol"],["my short_name",""],["note",""],["status",""]]);
    tb.innerHTML=IB.rows.map(r=>{ const a=IB.aliases[r.broker_key]||{};
      return '<tr data-bk="'+blEsc(r.broker_key)+'"><td class="l"><b>'+blEsc(r.broker_key)+'</b></td><td class="l"><span class="sm">'+blEsc((r.spellings||[]).join(', '))+'</span></td>'
      +'<td class="gcol"><input class="ibi" data-k="sec_id" value="'+blEsc(a.sec_id||'')+'" onchange="ibAlias(this)" title="SECID from Nuke"></td>'
      +'<td><input class="ibi w" data-k="my_short" value="'+blEsc(a.my_short||'')+'" onchange="ibAlias(this)" title="my short_name (as in Nuke / RFQ)"></td>'
      +'<td><input class="ibi w" data-k="note" value="'+blEsc(a.note||'')+'" onchange="ibAlias(this)"></td>'
      +'<td><select data-k="status" onchange="ibAlias(this)"><option'+(a.status==='CONFIRMED'?'':' selected')+'>ASSUMED</option><option'+(a.status==='CONFIRMED'?' selected':'')+'>CONFIRMED</option></select></td></tr>'; }).join('');
  } else {
    th.innerHTML=hd([["lines to review",3,"chk"]],[["#",""],["line","l"],["reason","l"]]);
    tb.innerHTML=IB.rows.map(r=>'<tr><td>'+r.id+'</td><td class="l">'+blEsc(r.line)+'</td><td class="l">'+blEsc(r.reason)+'</td></tr>').join('');
  }
}
async function ibAlias(el){ const tr=el.closest('tr'); const g=k=>{ const e=tr.querySelector('[data-k="'+k+'"]'); return e?e.value.trim():''; };
  const [ok,j]=await post('/api/idb/alias',{broker_key:tr.dataset.bk,sec_id:g('sec_id'),my_short:g('my_short'),note:g('note'),status:g('status')});
  if(j&&j.ok){ setS('ib_meta','alias saved: '+j.broker_key+' \u2192 '+(j.my_short||'')+' ('+(j.sec_id||'no secid')+')','ok'); IB.aliases[j.broker_key]={sec_id:j.sec_id,my_short:j.my_short,note:g('note'),status:g('status')}; }
  else setS('ib_meta',(j&&j.error)||'alias save failed','err'); }
async function ibMark(el){ const tr=el.closest('tr'); const key=el.dataset.key; const g=k=>{ const e=tr.querySelector('[data-k="'+k+'"]'); return e?e.value.trim():''; };
  const [ok,j]=await post('/api/idb/mark',{mkey:key,my_bid:g('my_bid'),my_offer:g('my_offer'),spot:g('spot'),delta:g('delta'),parity:g('parity')});
  if(j&&j.ok){ setS('ib_meta','manual mark saved for '+key,'ok'); idbLoad(); } else setS('ib_meta',(j&&j.error)||'mark save failed','err'); }
async function ibIngest(){ const text=$('ib_paste').value; if(!text.trim()) return;
  const [ok,j]=await post('/api/idb/ingest',{text,source:($('ib_src').value||'').trim(),date:ibDate()});
  if(j&&j.ok){ setS('ib_meta','stored '+j.new+' new quote(s) across '+(j.bonds||0)+' bond(s), '+j.dup+' duplicate(s) skipped, '+j.review+' line(s) to review - latest levels updated','ok'); $('ib_paste').value=''; IB.view='cmp'; idbLoad(); }
  else setS('ib_meta',(j&&j.error)||'ingest failed','err'); }
if($('ib_ingest')) $('ib_ingest').onclick=ibIngest;
if($('ib_reload')) $('ib_reload').onclick=idbLoad;
['grid','cmp','board','alias','unp'].forEach(k=>{ const b=$('ib_v_'+k); if(b) b.onclick=()=>{ IB.view=k; idbLoad(); }; });
document.querySelectorAll('#tab-idb button[data-fill]').forEach(b=>b.onclick=()=>ibFill(b.dataset.fill));
if($('ib_nuke')) $('ib_nuke').onclick=ibNuke; if($('ib_auto')) $('ib_auto').onclick=ibAutoToggle;
function ibAutoToggleIdb(){ /* IDB-only auto-last: re-fill from Refinitiv last every 15s, then re-nuke */ }
if($('ib_accall')) $('ib_accall').onclick=ibAcceptAll;
function ibEditing(){ const a=document.activeElement; return !!(a && a.closest && a.closest('#ib_tbl') && (a.tagName==='INPUT'||a.tagName==='SELECT')); }
function idbTick(){ if(window.curTab!=='idb' || document.hidden) return false; if(ibEditing()) return false; if(Date.now()-(window._ibLastEdit||0)<3000) return false; idbLoad(); return true; }
setInterval(idbTick,5000);
document.addEventListener('input',ev=>{ if(ev.target && ev.target.closest && ev.target.closest('#ib_tbl')) window._ibLastEdit=Date.now(); },true);
if($('ib_date')&&!$('ib_date').value) $('ib_date').value=new Date().toISOString().slice(0,10);
if($('ib_paste')) $('ib_paste').addEventListener('keydown',ev=>{ if(ev.key==='Enter'&&(ev.ctrlKey||ev.metaKey)) ibIngest(); });

/* ---- CB CONVERSION bid sheet (JS mirrors conv_bid in Python) ---- */
let CV={rows:[]};
const CV_COLS=[
 ["secId","secid","id"],["company","company","id"],["short_name","short_name","id"],
 ["expiry","expiry","id"],["isin","isin","id"],["ric","ric","id"],
 ["sec_fx","sec_fx","id"],["und_fx","und_fx","id"],
 ["spot","Stock","in",1],["fx","FX","in",1],
 ["cp","Conv px","in",1],["fixed_fx","Fixed FX","in",1],["qty","Face","in",1],
 ["delta","Delta %","in",1],
 ["tax_pct","Tax %","in",0],["comm_bp","Comm bp","in",0],["slip_bp","Stk slip bp","in",0],
 ["fx_bp","FX sprd bp","in",0],["fx_slip_bp","FX slip bp","in",0],
 ["fund_pct","Fund %/y","in",0],["rebate_pct","Rebate %/y","in",0],["borrow_pct","Borrow %/y","in",0],
 ["lag_cd","Lag cd","in",0],["fee_usd","Fee USD","in",0],["edge","Edge","in",0],
 ["shares","Shr/100","calc"],["parity","Parity","calc"],
 ["c_tax","Tax","calc"],["c_fees","Fees","calc"],["c_stk_slip","Stk slip","calc"],
 ["c_fx_sprd","FX sprd","calc"],["c_fx_slip","FX slip","calc"],
 ["c_funding","Bond fund","calc"],["c_proceeds","Proceeds int","calc"],["c_borrow","Borrow","calc"],
 ["net_carry","Net carry","calc"],["c_other","Other","calc"],["x","X","calc"],
 ["pmx","P \u2212 X","bid"],["bid","Bid","bid"],["status","Status","st"],["open","","op"],["del","","del"]];
const CV_BANDS=[["SECURITY (nuke)",8],["INPUTS \u00b7 RFQ (versus)",6],["COST PARAMS (est)",11],["PARITY",2],["X = COSTS + EDGE (\u2212 = earned)",11],["BID",2],["",3]];
const CV_GRP=[8,14,25,27,38,40];
function cvCalc(p){
 const f=(k,d)=>(p[k]==null||p[k]==="")?d:parseFloat(p[k]);
 const spot=f("spot"),fx=f("fx"),cp=f("cp"),ffx=f("fixed_fx",1);
 const o={shares:null,parity:null,x:null,bid:null,status:"terms?",costs:{}};
 if(!(spot&&fx&&cp&&ffx)) return o;
 const shares=100*ffx/cp, parity=shares*spot/fx;
 const qty=f("qty",1e6)||1e6;
 const slip=f("slip_bp",0);   // versus: stock leg agreed -> 0 bp
 const lag=f("lag_cd",7);
 const c={tax:parity*f("tax_pct",0)/100, fees:parity*f("comm_bp",0)/1e4,
  stk_slip:parity*slip/1e4, fx_sprd:parity*f("fx_bp",0)/1e4, fx_slip:parity*f("fx_slip_bp",0)/1e4,
  funding:parity*f("fund_pct",0)/100*lag/360, proceeds:-parity*f("rebate_pct",0)/100*lag/360,
  borrow:parity*f("borrow_pct",0)/100*lag/365, other:f("fee_usd",0)/(qty/100)};
 for(const k in c) c[k]=+c[k].toFixed(4);            // shown parts define X
 const edge=f("edge",0.25);
 const x=+(Object.values(c).reduce((a,b)=>a+b,0)+edge).toFixed(4);
 const bid=Math.floor((parity-x)*100)/100;
 const net_carry=+(-(c.funding+c.proceeds+c.borrow)).toFixed(4);
 return {shares:+shares.toFixed(4),parity:+parity.toFixed(2),costs:c,net_carry,edge,x,
  bid,x_shown:+(parity-bid).toFixed(2),status:(f("delta",0)>=90?"CONVERSION BID":"floor only")};
}
const cvF2=v=>(v==null||!isFinite(v))?"\u2014":Number(v).toFixed(2);
function cvRow(p,i){
 const c=cvCalc(p); p._calc=c;
 const est=(k)=>{ if((p._typed||{})[k]) return ""; if((p.ref||[]).includes(k)) return '<span class="est" style="color:#1d4ed8">ref</span>';
  return (p.est||[]).length&&["cp","fixed_fx","tax_pct","comm_bp","borrow_pct","fund_pct","rebate_pct","lag_cd","fx_bp","fx_slip_bp","fee_usd"].includes(k)?'<span class="est">est</span>':""; };
 return '<tr data-i="'+i+'" data-key="'+blEsc(p.key||p.isin||"")+'">'+CV_COLS.map(([k,lab,kind,ro],ci)=>{
  const g=CV_GRP.includes(ci)?" gcol":"";
  if(kind==="id"){ const v=p[k]==null?"":p[k];
   return '<td class="id'+(k==="company"||k==="short_name"||k==="ric"||k==="isin"?" l":"")+g+'"'+(k==="company"?' title="'+blEsc(v)+'"':"")+'>'+(k==="short_name"?"<b>"+blEsc(v)+"</b>":blEsc(String(v).slice(0,k==="company"?22:40)))+(p.manual&&k==="secId"?' <span class="sm">manual</span>':"")+'</td>'; }
  if(kind==="del") return '<td class="l"><span class="rq-x cvdel" title="'+(p.manual?"delete this line":"hide this RFQ-sourced line")+'" onclick="cvDel(this)">\u2715</span></td>';
  if(kind==="in"){
   const v=(p[k]==null?"":p[k]);
   return '<td'+g+'><input class="cvi" data-k="'+k+'" data-i="'+i+'" value="'+blEsc(v)+'" oninput="cvEdit(this)" onchange="cvSave(this)">'+est(k)+'</td>';
  }
  if(kind==="calc"){
   const v=k.startsWith("c_")?c.costs[k.slice(2)]:c[k];
   return '<td class="calc'+g+'">'+(k==="shares"?(v==null?"\u2014":Number(v).toFixed(4)):cvF2(v))+'</td>';
  }
  if(kind==="bid"){
   if(k==="pmx") return '<td class="bid'+g+'">'+(c.bid==null?"\u2014":"P \u2212 "+cvF2(c.x_shown))+'</td>';
   return '<td class="bid">'+cvF2(c.bid)+'</td>';
  }
  if(kind==="st"){ const cls=c.status==="CONVERSION BID"?"st-ok":(c.status==="terms?"?"st-tm":"st-fl");
   return '<td class="l'+g+'"><span class="'+cls+'">'+c.status+'</span></td>'; }
  return '<td class="l"><span class="cvopen" onclick="rfqJump(\''+blEsc(String(p.rfq_id||""))+'\')">Open</span></td>';
 }).join("")+"</tr>";
}
function cvRender(){
 const th=$("cv_tbl").querySelector("thead"); const tb=$("cv_tbl").querySelector("tbody");
 const TIP={tax_pct:"transaction / stamp tax on the stock sale, % of parity",comm_bp:"broker commission on the stock sale, bp of parity",slip_bp:"stock execution slippage vs mark, bp of parity (versus: stock leg agreed, default 0)",fx_bp:"FX dealing spread converting proceeds to bond ccy, bp of parity",fx_slip_bp:"FX execution slippage vs mark, bp of parity",fund_pct:"funding cost of the bond position over the lag, %/y act/360 (paid)",rebate_pct:"interest earned on short-sale proceeds over the lag, %/y act/360 (earned, reduces X)",borrow_pct:"stock borrow fee over the lag, %/y act/365 (paid)",lag_cd:"calendar days until converted shares arrive - every carry item scales with it",fee_usd:"fixed conversion / agent / settlement fees per ticket, USD, spread over face",edge:"target profit per 100 face",net_carry:"proceeds interest - bond funding - borrow: working-capital edge earned (+) or lost (-)"};
 th.innerHTML='<tr class="band">'+CV_BANDS.map(([l,n])=>'<td colspan="'+n+'">'+l+'</td>').join("")+"</tr>"
  +"<tr>"+CV_COLS.map(([k,lab],ci)=>'<th title="'+blEsc(TIP[k]||"")+'" class="'+(k==="short_name"||k==="status"?"l":"")+(CV_GRP.includes(ci)?" gcol":"")+'">'+lab+"</th>").join("")+"</tr>";
 tb.innerHTML=CV.rows.map(cvRow).join("");
}
function cvEdit(el){ const p=CV.rows[+el.dataset.i]; p[el.dataset.k]=el.value.trim(); p._typed=p._typed||{}; p._typed[el.dataset.k]=1;
 const c=cvCalc(p); p._calc=c; const tr=el.closest("tr"); const cells=tr.children;
 CV_COLS.forEach(([k,lab,kind],ci)=>{ if(kind==="calc"){ const v=k.startsWith("c_")?c.costs[k.slice(2)]:c[k]; cells[ci].textContent=(k==="shares"?(v==null?"\u2014":Number(v).toFixed(4)):cvF2(v)); }
  else if(k==="pmx") cells[ci].textContent=c.bid==null?"\u2014":"P \u2212 "+cvF2(c.x_shown);
  else if(k==="bid") cells[ci].textContent=cvF2(c.bid);
  else if(k==="status"){ cells[ci].innerHTML='<span class="'+(c.status==="CONVERSION BID"?"st-ok":(c.status==="terms?"?"st-tm":"st-fl"))+'">'+c.status+"</span>"; } });
}
async function cvSave(el){ const p=CV.rows[+el.dataset.i]; const keep={};
 ["cp","fixed_fx","tax_pct","comm_bp","slip_bp","fx_bp","fx_slip_bp","fund_pct","rebate_pct","borrow_pct","lag_cd","fee_usd","edge","spot","fx","qty","delta"].forEach(k=>{ if(p[k]!=null&&p[k]!=="") keep[k]=p[k]; });
 try{ await post('/api/conv/save',{isin:p.isin,params:keep}); }catch(_){}
}
async function convLoad(){
 setS('cv_meta','loading\u2026','');
 let j; try{ j=await (await fetch('/api/conv/rows')).json(); }catch(e){ setS('cv_meta','load failed: '+e,'err'); return; }
 if(!j.ok){ setS('cv_meta',j.error||'error','err'); return; }
 CV=j; cvRender();
 const n=j.rows.length, nb=j.rows.filter(r=>r.calc&&r.calc.status==="CONVERSION BID").length;
 setS('cv_meta',n+' bonds (latest RFQ each) \u00b7 '+nb+' real conversion bid'+(nb===1?'':'s')+' \u00b7 others floor only (delta < '+j.deep_itm+'%) \u00b7 bid = parity \u2212 X \u00b7 amber = editable, persisted \u00b7 est = placeholder','ok');
}
if($('cv_reload')) $('cv_reload').onclick=convLoad;
async function cvAdd(){ const q=($('cv_add').value||'').trim(); if(!q) return;
 const [ok,j]=await post('/api/conv/add',{q});
 if(j&&j.ok){ $('cv_add').value=''; setS('cv_meta','added '+j.key+(j.known?'':' (not in Nuke master - identity blank, type the terms)'),'ok'); convLoad(); }
 else setS('cv_meta',(j&&j.error)||'add failed','err'); }
async function cvDel(el){ const tr=el.closest('tr'); const key=tr.dataset.key; const p=CV.rows[+tr.dataset.i];
 if(!confirm((p.manual?'Delete ':'Hide ')+(p.short_name||key)+' from the conversion sheet?')) return;
 const [ok,j]=await post('/api/conv/del',{q:key}); if(j&&j.ok) convLoad(); else setS('cv_meta',(j&&j.error)||'delete failed','err'); }
if($('cv_addb')) $('cv_addb').onclick=cvAdd;
if($('cv_exb')) $('cv_exb').onclick=async()=>{ const [ok,j]=await post('/api/conv/examples',{}); if(j&&j.ok){ setS('cv_meta','loaded '+j.n+' example lines (est)','ok'); convLoad(); } };
if($('cv_add')) $('cv_add').onkeydown=ev=>{ if(ev.key==='Enter') cvAdd(); };
function rfqJump(id){
  if(!rfqShown().some(r=>String(r.rfq_id)===String(id))){
    rfqFilt='all';
    document.querySelectorAll('#rf_filt .rf-f').forEach(x=>
      x.classList.toggle('on',x.dataset.f==='all'));
    ['rf_f_from','rf_f_to','rf_f_txt'].forEach(i=>{
      if($(i)) $(i).value='';
    });
    if($('rf_f_type')) $('rf_f_type').value='';
    rfqRender();
  }
  const tr=document.querySelector(
    `#rfq_tbl tbody tr[data-id="${id}"]`);
  if(!tr) return;
  if(tr.scrollIntoView) tr.scrollIntoView({block:'center'});
  tr.classList.remove('rflash'); void tr.offsetWidth;
  tr.classList.add('rflash');
}
$('rf_mon').onclick=()=>{
  rfqMonRender();
  const opening=!$('rfq_mon').classList.contains('on');
  $('rfq_mon').classList.toggle('on');
  if(opening){ $('rfq_hist').classList.remove('on'); rhId=null; }
};
$('rm_close').onclick=()=>$('rfq_mon').classList.remove('on');
$('rm_list').addEventListener('click',ev=>{
  const it=ev.target.closest('.rm-it');
  if(it) rfqJump(it.dataset.id);
});
$('rh_close').onclick=()=>{
  $('rfq_hist').classList.remove("on"); rhId=null;
};
document.addEventListener('keydown',ev=>{
  if(ev.key==='Escape'&&RFQ_SEL.size){
    RFQ_SEL.clear(); rfqRender();
  }
});
(function(){
  const P=$('rfq_hist'), H=$('rh_rz'), KEY='lagrange.rhw';
  const saved=parseInt(localStorage.getItem(KEY)||'',10);
  if(saved>=260&&saved<=720) P.style.width=saved+'px';
  let drag=false;
  H.addEventListener('mousedown',e=>{drag=true;e.preventDefault();});
  window.addEventListener('mousemove',e=>{
    if(!drag) return;
    const w=Math.min(720,Math.max(260,window.innerWidth-e.clientX));
    P.style.width=w+'px';
  });
  window.addEventListener('mouseup',()=>{
    if(!drag) return; drag=false;
    const wv=String(parseInt(P.style.width)||360);
    localStorage.setItem(KEY,wv); prefPush(KEY,wv);
  });
  H.addEventListener('dblclick',()=>{P.style.width='360px';
    localStorage.setItem(KEY,'360');});
})();
async function rfqCopyLive(tr){
  const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
  if(!row) return;
  if(row.live_spot==="" && row.live_und===""){
    setS('rf_status','No live refs to copy yet.','err'); return;
  }
  let tok=tr.dataset.tok, ok=true;
  if(row.live_spot!==""){
    const j=await rfqEditSend(tr.dataset.id,"stock_ref",
      row.live_spot,tok);
    if(j.ok) tok=j.token;
    else { ok=false; setS('rf_status',j.error,'err'); }
  }
  if(ok && row.live_und!==""){
    const j2=await rfqEditSend(tr.dataset.id,"fx_ref",
      row.live_und,tok);
    if(!j2.ok){ ok=false; setS('rf_status',j2.error,'err'); }
  }
  if(ok && row.live_delta!==""){
    const dv=String(row.live_delta).replace("%","").trim();
    const j3=await rfqEditSend(tr.dataset.id,"delta",dv,tok);
    if(j3.ok) tok=j3.token;
    else { ok=false; setS('rf_status',j3.error,'err'); }
  }
  if(ok) setS('rf_status','OvdSpot / OvdFx / Delta set to live (Delta = nDelta%) - repriced.','ok');
  rfqLoad(true);
}
async function rfqSideOp(tr, url, side){
  const j=await (await fetch(url,{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({rfq_id:+tr.dataset.id,
      token:tr.dataset.tok, user:rfqUser(), side})})).json();
  if(j.ok){
    const act=url.includes("pull")?"pulled":"refreshed";
    setS('rf_status','RFQ '+tr.dataset.id+' '+side+' '+act+
      ' (rev '+j.rev+')'+(j.status?' \u00b7 '+j.status:'')+'.','ok');
    rfqLoad(true);
  } else { setS('rf_status',j.error,'err'); rfqLoad(true); }
}
$('rfq_tbl').addEventListener('focusout',()=>{
  if(window._rfqDefer){ window._rfqDefer=0;
    setTimeout(rfqRender,60); }
});
$('rfq_tbl').addEventListener("click", async ev=>{
  const delb=ev.target.closest('.rq-del');
  if(delb){
    const tr=delb.closest('tr');
    if(!confirm('Delete cancelled RFQ #'+tr.dataset.id+
      ' permanently?')) return;
    post('/api/rfq/delete',{rfq_id:tr.dataset.id})
      .then(([ok,j])=>{ if(j.ok){
        rfqRows=rfqRows.filter(x=>x.rfq_id!==tr.dataset.id);
        rfqRender(); setS('rf_status','Deleted #'+
        tr.dataset.id+'.','ok'); }
        else setS('rf_status',j.error||'delete failed','err'); });
    return;
  }
  const ipb=ev.target.closest('.rq-impb');
  if(ipb){
    const tr=ipb.closest('tr');
    const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id)||{};
    const sides=row.sides||'two_way';
    const gvL=f=>{const inp=tr.querySelector(
      'input[data-rf="'+f+'"]');
      return inp?String(inp.value).trim()!=='':
        String(row[f]||'').trim()!=='';};
    const hasB=gvL('ord_level'), hasA=gvL('ord_level2');
    const markR=f=>{const inp=tr.querySelector(
      'input[data-rf="'+f+'"]');
      if(inp){ inp.classList.add('missb');
        inp.addEventListener('input',()=>
          inp.classList.remove('missb'),{once:true}); }};
    let bad=false, msg='';
    if(sides==='two_way'){
      if(!hasB&&!hasA){ bad=true; markR('ord_level');
        markR('ord_level2');
        msg='improve on two-way needs at least one level '+
          '(bid or ask) \u2014 fill a red cell'; }
    } else if(sides==='bid'){
      if(!hasB){ bad=true; markR('ord_level');
        msg='improve needs the bid level \u2014 fill the '+
          'red cell'; }
    } else {
      if(!hasA){ bad=true; markR('ord_level2');
        msg='improve needs the ask level \u2014 fill the '+
          'red cell'; }
    }
    if(bad){ setS('rf_status',msg,'err'); return; }
    setS('rf_status','improve: sending…','ok');
    post('/api/rfq/impreq',{rfq_id:tr.dataset.id})
      .then(([ok,j])=>{ if(j&&j.ok){ row.status='IMPROVE';
        rfqRender(); setS('rf_status','improve sent — row '+
          'is now IMPROVE; trader answers with match or Q','ok');
        setTimeout(()=>rfqLoad(true),400); }
        else setS('rf_status',(j&&(j.error||JSON.stringify(j)))||'rejected','err'); })
      .catch(e=>setS('rf_status',
        'improve failed: '+e+' \u00b7 '+(window._rfqBuild||'old-build'),'err'));
    ev.stopPropagation(); return;
  }
  const mtb=ev.target.closest('.rq-mtb');
  if(mtb){
    const tr=mtb.closest('tr');
    post('/api/rfq/match',{rfq_id:tr.dataset.id})
      .then(([ok,j])=>{ setS('rf_status',
        j.ok?'matched \u2014 quote at the improve terms'
        :(j.error||'match failed'), j.ok?'ok':'err');
        if(j.ok) rfqLoad(true); });
    ev.stopPropagation(); return;
  }
  const ajb=ev.target.closest('.rq-adjb');
  if(ajb){
    const tr=ajb.closest('tr');
    const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
    const nv=String(row&&row.adj_req||'')==='1'?false:true;
    post('/api/rfq/adjreq',{rfq_id:tr.dataset.id,on:nv})
      .then(([ok,j])=>{ if(j.ok&&row){ row.adj_req=nv?'1':'';
        rfqRender(); } });
    return;
  }
  const apb=ev.target.closest('.rq-ap');
  if(apb){
    const tr=apb.closest('tr');
    const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
    const cur=String(row&&row.auto_q||'0');
    const mm=apb.dataset.m||'1';
    const nv=cur===mm?'0':mm;
    if(nv!=='0'){
      const ti=tr.querySelector('input[data-rf="tol"]');
      const hasTol=(ti&&String(ti.value).trim()!=='')||
        String(row&&row.tol||'').trim()!=='';
      if(!hasTol){
        if(ti){ ti.classList.add('missb');
          ti.addEventListener('input',()=>
            ti.classList.remove('missb'),{once:true}); }
        setS('rf_status','set a tolerance first \u2014 the '+
          'Tol cell is required to arm TOL / FOLW','err');
        return;
      }
    }
    rfqEditSend(tr.dataset.id,'auto_q',nv,tr.dataset.tok)
      .then(j=>{ if(j.ok&&row){ row.auto_q=nv;
        row._tok=j.token; rfqRender(); } });
    return;
  }
  const hitb=ev.target.closest("b.rq-hb");
  if(hitb){
    const tr=hitb.closest("tr");
    const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
    if(row&&(row.status==="DONE"||row.status==="CANCELLED")){
      setS('rf_status','Line is booked / cancelled - hit is locked.','err');
      ev.stopPropagation(); return;
    }
    const nv=(row && row.hit===hitb.dataset.hit)?"":hitb.dataset.hit;
    const j=await rfqEditSend(tr.dataset.id,"hit",nv,tr.dataset.tok);
    if(j.ok){
      if(row){
        row.hit=j.value; row._tok=j.token;
        if(j.value){                       // dealt -> HIT now
          row.status="HIT";
          row.flag="HIT "+String(j.value).toUpperCase();
          row.flag_cls="fl-moved";
          row.refresh_by=""; row.refresh_at="";
        } else if(row.status==="HIT"){     // un-hit -> revert
          row.status=(row.style==="working"||row.ord_side)
            ?"WORKING"
            :((row.bid_px!==""||row.ask_px!=="")
              ?"QUOTED":"REQUESTED");
          row.flag=""; row.flag_cls="";
        }
      }
      setS('rf_status','RFQ '+tr.dataset.id+' hit = '+
        (j.value||'cleared')+'.','ok');
      rfqRender(); rfqLoad(true);          // reconcile in 1 RTT
    }
    else { setS('rf_status',j.error,'err'); rfqLoad(true); }
    ev.stopPropagation(); return;
  }
  const rfb=ev.target.closest("span.rq-rf");
  if(rfb && !rfb.classList.contains("rq-rfp")){
    const tr=rfb.closest("tr");
    const j=await (await fetch('/api/rfq/refresh',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rfq_id:+tr.dataset.id,
        token:tr.dataset.tok, user:rfqUser()})})).json();
    if(j.ok){ setS('rf_status','RFQ '+tr.dataset.id+': refresh '+
        'requested by '+j.refresh_by+' - the trader will see it '+
        'amber.','ok'); rfqLoad(true); }
    else { setS('rf_status',j.error,'err'); rfqLoad(true); }
    ev.stopPropagation(); return;
  }
  if(rfb){ ev.stopPropagation(); return; }
  const pbb=ev.target.closest("span.rq-pb");
  if(pbb){
    rfqSideOp(pbb.closest("tr"), '/api/rfq/pull', "both");
    ev.stopPropagation(); return;
  }
  const lvb=ev.target.closest("span.rq-lv");
  if(lvb){
    rfqCopyLive(lvb.closest("tr"));
    ev.stopPropagation(); return;
  }
  const lcb=ev.target.closest("b.qc-l");
  if(lcb){
    rfqCopyLive(lcb.closest("tr"));
    ev.stopPropagation(); return;
  }
  const qr=ev.target.closest("b.qc-r");
  if(qr){
    rfqSideOp(qr.closest("tr"), '/api/rfq/quote', qr.dataset.side);
    ev.stopPropagation(); return;
  }
  const qx=ev.target.closest("b.qc-x");
  if(qx){
    rfqSideOp(qx.closest("tr"), '/api/rfq/pull', qx.dataset.side);
    ev.stopPropagation(); return;
  }
  const qb=ev.target.closest("span.rq-qb");
  if(qb){
    const tr=qb.closest("tr");
    let missq=false;
    ["stock_ref","fx_ref"].forEach(f=>{
      const inp=tr.querySelector('input[data-rf="'+f+'"]');
      if(inp&&String(inp.value).trim()===""){ missq=true;
        inp.classList.add("missb");
        inp.addEventListener("input",()=>
          inp.classList.remove("missb"),{once:true}); }
    });
    if(missq){ setS('rf_status',
      'cannot quote: OvdSpot and OvdFx are required '+
      '\u2014 fill the red cells','err');
      ev.stopPropagation(); return; }
    const j=await (await fetch('/api/rfq/quote',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rfq_id:+tr.dataset.id,
        token:tr.dataset.tok, user:rfqUser(), side:"both"})})).json();
    if(j.ok){ setS('rf_status','RFQ '+tr.dataset.id+' quoted rev '+
        j.rev+': '+(j.bid??"-")+' / '+(j.ask??"-")+'.','ok');
      rfqLoad(true); }
    else { setS('rf_status',j.error,'err'); rfqLoad(true); }
    ev.stopPropagation(); return;
  }
  const selTr=ev.target.closest("#rfq_tbl tbody tr");
  if(selTr && !ev.target.closest(
      "input,select,button,b,span,svg")
     && (ev.ctrlKey||ev.metaKey||
         ev.target.closest("td.rq-id"))){
    const id=String(selTr.dataset.id);
    if(RFQ_SEL.has(id)) RFQ_SEL.delete(id);
    else RFQ_SEL.add(id);
    selTr.classList.toggle("rowsel", RFQ_SEL.has(id));
    rfqMeta();
    ev.stopPropagation(); return;
  }
  const hb=ev.target.closest("span.rq-h");
  if(hb){
    rfqHist(hb.closest("tr").dataset.id);
    ev.stopPropagation(); return;
  }
  const cp=ev.target.closest("span.rq-cp");
  if(cp){
    const tr=cp.closest("tr");
    const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
    if(row){
      const s=rfqQuoteStr(row);
      const ok=await rfqCopy(s);
      setS('rf_status',(ok?'copied: ':'copy blocked \u2014 ')+s,
           ok?'ok':'err');
    }
    ev.stopPropagation(); return;
  }
  const rj=ev.target.closest("span.rq-rj");
  if(rj){
    const tr=rj.closest("tr");
    fetch('/api/rfq/reject',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rfq_id:+tr.dataset.id,
        token:tr.dataset.tok,user:rfqUser()})})
      .then(r=>r.json()).then(j=>{
        if(j.ok){
          const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
          if(row){
            row.status=j.status||"REQUESTED"; row.hit="";
            row.bid_px=""; row.ask_px="";
            row.flag=""; row.flag_cls="";
            if(j.token) row._tok=j.token;
          }
          rfqRender();
        }
        setS('rf_status', j.ok?('hit busted \u2192 '+j.status+
          ' \u00b7 quote pulled')
          :(j.error||'reject failed'), j.ok?'ok':'err');
        rfqLoad(true);
      });
    ev.stopPropagation(); return;
  }
  const ab=ev.target.closest("span.rq-ab");
  if(ab){
    const tr=ab.closest("tr");
    const j=await (await fetch('/api/rfq/ack',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rfq_id:+tr.dataset.id,
        token:tr.dataset.tok, user:rfqUser()})})).json();
    if(j.ok){
      const row=rfqRows.find(x=>x.rfq_id===tr.dataset.id);
      if(row){ row.status="DONE"; row.ack_by=j.ack_by||rfqUser();
        row.flag="DONE"; row.flag_cls="fl-none"; }
      setS('rf_status','RFQ '+tr.dataset.id+' acknowledged by '+
        j.ack_by+'.','ok'); rfqRender(); rfqLoad(true);
    }
    else { setS('rf_status',j.error,'err'); rfqLoad(true); }
    ev.stopPropagation(); return;
  }
  const exb=ev.target.closest("span.rq-ex");
  if(exb){
    const tr=exb.closest("tr");
    const [ok,j]=await post('/api/rfq/expire',{rfq_id:tr.dataset.id,
      token:tr.dataset.tok});
    if(j&&j.ok){ setS('rf_status','RFQ '+tr.dataset.id+
        ' quote expired (off) - row stays open, Q re-quotes.','ok'); rfqLoad(true); }
    else { setS('rf_status',j.error,'err'); rfqLoad(true); }
    ev.stopPropagation(); return;
  }
  const xb=ev.target.closest("span.rq-x");
  if(xb){
    const tr=xb.closest("tr");
    const user=rfqUser();
    const j=await rfqEditSend(tr.dataset.id,"status","CANCELLED",
                              tr.dataset.tok);
    if(j.ok){ setS('rf_status','RFQ '+tr.dataset.id+
        ' cancelled - updates stopped.','ok'); rfqLoad(true); }
    else { setS('rf_status',j.error,'err'); rfqLoad(true); }
    ev.stopPropagation(); return;
  }
  const up=ev.target.closest("span.rq-up");
  if(up){
    const tr=up.closest("tr");
    const user=rfqUser();
    const j=await (await fetch('/api/rfq/upload',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rfq_id:+tr.dataset.id,user})})).json();
    if(j.ok) setS('rf_status','RFQ '+tr.dataset.id+' staged \u2192 '+
      JSON.stringify(j.blotter_row)+'  ['+(j.note||'uploaded')+']','ok');
    else setS('rf_status',j.error,'err');
    ev.stopPropagation(); return;
  }
});
$('rf_bcfg').addEventListener('click',ev=>{
  ev.stopPropagation();
  const on=$('rfq_bcfg').classList.toggle('hide');
  if(!on) bcLoad();
},true);
document.querySelectorAll('#rf_filt .rf-f').forEach(b=>{
  b.onclick=()=>{
    rfqFilt=b.dataset.f;
    document.querySelectorAll('#rf_filt .rf-f').forEach(x=>
      x.classList.toggle('on', x===b));
    rfqRender();
  };
});
$('btn_logout').onclick=async()=>{
  try{ await fetch('/api/auth/logout',{method:'POST'}); }catch(e){}
  location.href='/login';
};
function rfqCfgPaint(){
  document.querySelectorAll('input[name="cfg_den"]')
    .forEach(r=>r.checked=r.value===RFQ_CFG.density);
  document.querySelectorAll('input[name="cfg_fs"]')
    .forEach(r=>r.checked=r.value===RFQ_CFG.font);
  $('cfg_cols').innerHTML=RFQ_COL_TOGGLE.map(([k,lab])=>
    `<label><input type="checkbox" data-col="${k}"`+
    `${rfqColVis(k)?" checked":""}> ${lab}</label>`).join('');
}
$('rf_cfg').onclick=()=>{
  rfqCfgPaint(); $('rf_cfgp').classList.toggle('hide');
};
$('rf_cfg_x').onclick=()=>$('rf_cfgp').classList.add('hide');
$('rf_cfgp').addEventListener('change',ev=>{
  const t=ev.target;
  if(t.name==='cfg_den') RFQ_CFG.density=t.value;
  else if(t.name==='cfg_fs') RFQ_CFG.font=t.value;
  else if(t.dataset.col)
    RFQ_CFG.cols[t.dataset.col]=t.checked;
  rfqSaveCfg(); rfqApplyCfg(); rfqRender();
});
$('cfg_reset').onclick=()=>{
  RFQ_CFG=JSON.parse(JSON.stringify(RFQ_CFG_DEF));
  rfqSaveCfg(); rfqApplyCfg(); rfqCfgPaint(); rfqRender();
};
function rfqCfPaint(){
  const k=$('cf_col').value;
  const f=(RFQ_CFG.colfmt||{})[k]||{};
  $('cf_w').value=f.w||'';
  $('cf_fg').value=f.fg||'#1c1c1c';
  $('cf_bg').value=f.bg||'#ffffff';
  $('cf_bold').checked=!!f.bold;
}
function rfqCfSet(patch){
  const k=$('cf_col').value; if(!k) return;
  RFQ_CFG.colfmt=RFQ_CFG.colfmt||{};
  const f=Object.assign({},RFQ_CFG.colfmt[k]||{},patch);
  Object.keys(f).forEach(x=>{
    if(f[x]===null||f[x]===''||f[x]===false) delete f[x];});
  if(Object.keys(f).length) RFQ_CFG.colfmt[k]=f;
  else delete RFQ_CFG.colfmt[k];
  rfqSaveCfg(); rfqApplyColFmt();
}
$('cf_col').innerHTML=RFQ_COLS.map(([k,l])=>
  `<option value="${k}">${l||k}</option>`).join('');
$('cf_col').onchange=rfqCfPaint;
$('cf_w').onchange=()=>rfqCfSet({w:parseInt($('cf_w').value)||null});
$('cf_fg').oninput=()=>rfqCfSet({fg:$('cf_fg').value});
$('cf_bg').oninput=()=>rfqCfSet({bg:$('cf_bg').value});
$('cf_fgx').onclick=()=>{rfqCfSet({fg:null}); rfqCfPaint();};
$('cf_bgx').onclick=()=>{rfqCfSet({bg:null}); rfqCfPaint();};
$('cf_bold').onchange=()=>rfqCfSet({bold:$('cf_bold').checked});
$('cf_clear').onclick=()=>{
  const k=$('cf_col').value;
  if(RFQ_CFG.colfmt) delete RFQ_CFG.colfmt[k];
  rfqSaveCfg(); rfqApplyColFmt(); rfqCfPaint();
};
(function(){
  const TH=$('rfq_tbl').querySelector('thead');
  let dc=null;
  TH.addEventListener('mousemove',e=>{
    const th=e.target.closest('th'); if(!th||dc){ return; }
    const r=th.getBoundingClientRect();
    th.style.cursor=(r.width&&r.right-e.clientX<6)?'col-resize':'';
  });
  TH.addEventListener('mousedown',e=>{
    const th=e.target.closest('th');
    if(!th||th.classList.contains('rq-band')) return;
    const r=th.getBoundingClientRect();
    if(!r.width||r.right-e.clientX>=6) return;
    const idx=[...th.parentNode.children].indexOf(th);
    const vis=rfqVisCols(); if(!vis[idx]) return;
    dc={k:vis[idx][0], x:e.clientX, w:r.width};
    e.preventDefault();
  });
  window.addEventListener('mousemove',e=>{
    if(!dc) return;
    RFQ_CFG.colfmt=RFQ_CFG.colfmt||{};
    RFQ_CFG.colfmt[dc.k]=Object.assign({},
      RFQ_CFG.colfmt[dc.k]||{},
      {w:Math.max(24,Math.round(dc.w+e.clientX-dc.x))});
    rfqApplyColFmt();
  });
  window.addEventListener('mouseup',()=>{
    if(!dc) return; dc=null; rfqSaveCfg();
  });
})();
['rf_f_from','rf_f_to','rf_f_txt','rf_f_type'].forEach(id=>{
  const el=$(id); if(!el) return;
  el.addEventListener('input',()=>rfqRender());
  el.addEventListener('change',()=>rfqRender());
});
rfqCfPaint();
rfqApplyCfg();
async function bcLoad(){
  const j=await (await fetch('/api/rfq/bondcfg')).json();
  if(!j.ok) return;
  $('bc_body').innerHTML=(j.rows||[]).map(r=>
    `<tr><td>${blEsc(r.isin)}</td><td>${blEsc(r.short_name||'')}</td>`+
    `<td>${blEsc(r.tol||'')}</td>`+
    `<td>${r.autopilot?'ON':''}</td>`+
    `<td><span class="bc-e" data-i="${blEsc(r.isin)}" data-n="${blEsc(r.short_name||'')}" data-t="${blEsc(r.tol||'')}" data-a="${r.autopilot?1:0}" style="cursor:pointer" title="load into the editor">\u270e</span></td></tr>`).join('')||
    '<tr><td colspan="5" style="color:#8b919a">no per-bond defaults yet</td></tr>';
}
$('bc_body').addEventListener('click',ev=>{
  const e=ev.target.closest('.bc-e'); if(!e) return;
  $('bc_isin').value=e.dataset.i; $('bc_name').value=e.dataset.n;
  $('bc_tol').value=e.dataset.t; $('bc_ap').checked=e.dataset.a==='1';
});
$('bc_add').onclick=async()=>{
  const [ok,j]=await post('/api/rfq/bondcfg',{
    isin:$('bc_isin').value.trim(),
    short_name:$('bc_name').value.trim(),
    tol:$('bc_tol').value.trim(),
    autopilot:$('bc_ap').checked?'1':'0'});
  $('bc_status').textContent=j.ok?'saved':('ERR '+(j.error||''));
  if(j.ok) bcLoad();
};
async function twRun(){
  $('twrun').disabled=true;
  setS('twstatus','Pulling TWSE / TPEx / SFB feeds ...');
  try{
    const [ok,j]=await post('/api/twcb/run',
      {mark_seen:$('twmark').checked});
    if(!j.ok){ setS('twstatus',j.error||'failed','err'); return; }
    const rows=(j.events||[]).map(e=>
      `<tr><td>${e.is_new?'<b class="twnew">NEW</b>':''}</td>`+
      `<td>${blEsc(e.source||'')}</td>`+
      `<td>${blEsc(e.date||'')}</td>`+
      `<td>${e.link?`<a href="${blEsc(e.link)}" target="_blank">${blEsc(e.company||'')}</a>`
        :blEsc(e.company||'')}</td>`+
      `<td>${blEsc(e.text||'')}</td>`+
      `<td>${blEsc(e.text_en||'')}</td></tr>`).join('');
    $('tw_body').innerHTML=rows||
      '<tr><td colspan="6" style="color:#8b919a">No CB '+
      'issuance events in today\u2019s feeds.</td></tr>';
    const sh=(j.shelf_rows||[]);
    $('tw_shelf_wrap').classList.toggle('hide',!sh.length);
    $('tws_body').innerHTML=sh.map(v=>
      `<tr><td${v.days_left<=7?' style="color:#a8231b"':''}>${v.days_left}d</td>`+
      `<td>${blEsc(v.date||'')}</td>`+
      `<td>${blEsc(v.company||'')}</td>`+
      `<td>${blEsc(v.text||'')}</td>`+
      `<td>${blEsc(v.text_en||'')}</td></tr>`).join('');
    setS('twstatus',`${j.new} new \u00b7 shelf ${j.shelf_live} '+
      'live \u00b7 ${j.stats}`+(j.errors.length?' \u00b7 '+
      j.errors.length+' err':''),
      j.errors.length?'err':'ok');
    $('twmeta').textContent=j.errors.concat(j.warnings)
      .slice(0,2).join(' | ');
    $('twdraft').disabled=false; $('twsendnow').disabled=false;
  }catch(e){ setS('twstatus','ERROR: '+e,'err'); }
  finally{ $('twrun').disabled=false; }
}
async function twSend(sendNow){
  if(sendNow && !confirm('Send the TW CB pipeline email NOW?')) return;
  $('twdraft').disabled=true; $('twsendnow').disabled=true;
  setS('twstatus',sendNow?'Sending via Outlook ...'
    :'Opening Outlook draft ...');
  try{
    const [ok,j]=await post('/api/twcb/send',
      {to:$('twto').value, cc:$('twcc').value, send:sendNow});
    setS('twstatus',(j.ok?'Outlook: ':'Outlook ERROR: ')+
      (j.message||j.error), j.ok?'ok':'err');
    if(j.ok){ prefPush('lagrange.twto',$('twto').value);
      localStorage.setItem('lagrange.twto',$('twto').value);
      localStorage.setItem('lagrange.twcc',$('twcc').value); }
  }catch(e){ setS('twstatus','ERROR: '+e,'err'); }
  finally{ $('twdraft').disabled=false; $('twsendnow').disabled=false; }
}
async function twBackfill(){
  if(!confirm('Walk ~92 days of SFB daily pages to seed the '+
    'shelf?\nRuns in the background - a few minutes.')) return;
  $('twbf').disabled=true;
  try{
    const [ok,j]=await post('/api/twcb/backfill',{days:92});
    if(!j.ok){ setS('twstatus',j.error||'backfill failed','err');
      $('twbf').disabled=false; return; }
    setS('twstatus','Backfill running in the background '+
      '\u2014 walking SFB history ...');
    for(;;){
      const r=await fetch('/api/twcb/backfill_status');
      const s=await r.json();
      if(!s.running){
        if(s.error){ setS('twstatus','Backfill ERROR: '+
          s.error,'err'); }
        else { await twRun();
          setS('twstatus',s.msg||'backfill done','ok'); }
        break;
      }
      await new Promise(x=>setTimeout(x,3000));
    }
  }catch(e){ setS('twstatus','ERROR: '+e,'err'); }
  finally{ $('twbf').disabled=false; }
}
$('twbf').onclick=twBackfill;
$('twrun').onclick=twRun;
$('twdraft').onclick=()=>twSend(false);
$('twsendnow').onclick=()=>twSend(true);
$('twto').value=localStorage.getItem('lagrange.twto')||'';
$('twcc').value=localStorage.getItem('lagrange.twcc')||'';
function rfqBarReq(){
  const ord=($('rf_ord')&&$('rf_ord').value)||'';
  const vsty=['vs','versus'].includes($('rf_style').value);
  const R={rf_lvl:ord==='buy'||ord==='two',
    rf_lvl2:ord==='sell'||ord==='two',
    rf_vs:false,
    rf_fx:false, rf_delta:false, rf_qty:false, rf_client:false};
  Object.entries(R).forEach(([id,req])=>{
    const el=$(id); if(!el) return;
    el.classList.remove('miss');
    el.classList.toggle('req',req);
    el.classList.toggle('opt',!req);
  });
  return R;
}
function rfqOrdLab(){
  const o=$('rf_ord');
  $('rf_send').textContent=(o&&o.value)?'Send Order':'Send RFQ';
  rfqBarReq();
}
if($('rf_ord')) $('rf_ord').onchange=rfqOrdLab;
$('rf_style').addEventListener('change',rfqBarReq);
$('rf_sides').addEventListener('change',rfqBarReq);
rfqBarReq();
$('rf_send').onclick=rfqSend;
$('rf_reload').onclick=()=>rfqLoad();

/* ---- tab 4: Nuke Station embed ---- */
let nukeLoaded=false;
async function nukeConnect(){
  try{
    setS('nstatus','Checking / starting Nuke Station ...');
    let [ok,j]=await post('/api/nuke/start');
    $('nopen').href=j.url; $('nstat').textContent=j.url;
    for(let i=0; i<8 && !j.up; i++){          // give a fresh child ~8s to bind
      await new Promise(r=>setTimeout(r,1000));
      const r2=await fetch('/api/nuke/status'); j=await r2.json();
    }
    if(j.up){
      if(!nukeLoaded){ $('nframe').src=j.url; nukeLoaded=true; }
      setS('nstatus','Connected - live Nuke Station below.'+
        (j.child==='running'?' (auto-started by Lagrange; closes with it)':''),'ok');
    }else{
      nukeLoaded=false; $('nframe').src='about:blank';
      setS('nstatus','Nuke Station NOT running at '+j.url+'.\n'+
        'Autostart says: '+(j.note||'no info')+'\n'+
        'Fix the cause, then press Connect (it retries the start).','err');
    }
  }catch(e){ setS('nstatus','ERROR: '+e,'err'); }
}
$('nretry').onclick=()=>{ nukeLoaded=false; nukeConnect(); };
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


if __name__ == "__main__":
    import uvicorn
    if NUKE_EMBED:
        _embed_nuke()
        try:
            import socket
            print("[nuke] desk URL: http://%s:%d/nuke/"
                  % (socket.getfqdn(), PORT))
        except Exception:
            pass
    else:
        _maybe_start_nuke()
    print("LAGRANGE build %s | mode: %s | ONE port: %d (no 59999)"
          % (LAGRANGE_BUILD,
             "EMBEDDED /nuke/" if NUKE_EMBED else "external Nuke Station",
             PORT))
    import socket as _sock
    import webbrowser as _wb
    from threading import Timer as _Timer
    BIND_HOST = os.environ.get("LAGRANGE_HOST", "0.0.0.0")
    SERVER_FQDN = os.environ.get(
        "LAGRANGE_FQDN", "apachkgfiwx507.apac.nsroot.net")
    SERVER_SHORT = SERVER_FQDN.split(".")[0]

    def _running_on_server():
        try:
            th = _sock.gethostname().lower()
        except Exception:
            th = ""
        try:
            tf = _sock.getfqdn().lower()
        except Exception:
            tf = ""
        cand = {th, tf, th.split(".")[0]}
        return (SERVER_FQDN.lower() in cand
                or SERVER_SHORT.lower() in cand)

    if os.environ.get("BROWSER_URL"):
        BROWSER_URL = os.environ["BROWSER_URL"]
    elif _running_on_server():
        BROWSER_URL = "http://%s:%d/" % (SERVER_FQDN, PORT)
    else:
        BROWSER_URL = "http://localhost:%d/" % PORT
    print("CB Recon Web -> binding %s:%d" % (BIND_HOST, PORT))
    print("desk URL to share: http://%s:%d/  (login required)"
          % (SERVER_FQDN, PORT))
    print("NOTE: keep workers=1 - sessions + list snapshot "
          "are in-memory")
    _Timer(1.5, lambda: _wb.open_new(BROWSER_URL)).start()
    def _rfq_engine_loop():
        _hz = float(os.environ.get("RFQ_ENGINE_SEC", "0.3"))
        while True:
            try:
                if os.environ.get("LAGRANGE_TEST_LIVE") != "FILE":
                    api_rfq_list(_bg=1)
            except Exception:
                pass
            time.sleep(_hz)
    threading.Thread(target=_rfq_engine_loop,
                     daemon=True).start()
    print("=" * 62)
    print("  LAGRANGE  BUILD r107  ·  %s" % os.path.abspath(__file__))
    print("  port %s  ·  if this banner is missing, you are" % PORT)
    print("  running an OLD file — kill that process first.")
    print("=" * 62)
    _mount_dscan()
    uvicorn.run(app, host=BIND_HOST, port=PORT, log_level="warning")
