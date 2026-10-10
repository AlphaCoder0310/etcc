"""
app.py -- CB Nuke Station v10 (multi-user, WebSocket-shared)

Deploy (desk-wide):
  * Run on ONE machine: python app.py   (binds 0.0.0.0:59999, single worker).
  * Overrides via env vars: APP_PORT, APP_HOST, APP_FQDN, BROWSER_URL.
  * On the production box the browser auto-opens the FQDN link (the one to
    share); on a dev laptop it opens localhost instead.
  * Colleagues open   http://<machine-name>:59999   -- allow inbound TCP 59999
    in Windows Firewall the first time.
  * All users share one live table: security list, short names, und_fx,
    nGamma (manual, Δ-points per 1% und move) and overrides sync instantly;
    a client-computed theo · γ-adj section (m%, rollΔ, Δpnl, γpnl, theo,
    vs live) sits between override result and stock, re-priced on every
    Refinitiv tick / override / nuke; nukes are serialized on the server and the
    result is broadcast to everyone with the requester's name.
  * One server-side Refinitiv poller feeds all browsers (interval settable
    in Config, applies to everyone). A second poller re-fetches eqrms /
    cbanalytics reference data (ric, sec_fx, qty, usd, expiry, isin) every
    5 min by default -- also settable in Config -- so positions track fresh
    snaps without reloading. Latest-batch reads use a 10s tolerance window
    on loaded_at because the snap loader takes 1-2s to write a batch.
  * Durability: working state -> cba_app.cb_state (+ cb_state_meta for the
    book order), audit trail -> cba_app.app_events, rotating file log
    nuke_station.log next to this file. Tables are auto-created.
  * Do NOT run multiple workers/instances: state is in-process by design.
-------------------------------------------------------------
Minimal stack: FastAPI + Uvicorn backend, vanilla HTML/JS frontend
embedded in this single file. No build tools.

Install:
    pip install fastapi uvicorn requests pymysql refinitiv-data

Run:
    python app.py     (Refinitiv Workspace desktop running for live data)
    -> open http://127.0.0.1:59999

v9:
    * short_name and und_fx are now first-class grid columns with the
      same spreadsheet behaviour as the overrides: click selects,
      drag / Shift+arrows extend (within their own column), multi
      copy / cut / paste, drag-fill handle, Ctrl+D, Delete, Esc,
      double-click to edit. Arrow keys travel across all five
      editable columns; selections stay within a column zone so
      ranges never span the read-only columns in between.
    * und_fx takes an Eikon FX RIC (TWD=, KRW=, HKD=, TWDKRW=R, or
      1 for USD) and drives the fx group, which now also shows the
      FX close and close date (TR.PriceClose with CF_CLOSE fallback)
      alongside fx last / time / date.
    * cba_app.cb_nuke gains fx_close / fx_close_date (auto-migrated).

borrow.b61:
    * BORROW band (12 manual bw_* inputs, /api/borrow/ingest) replaced by TW SHORT QUOTA:
      a read-only mirror of the exchange's 信用額度總量管制餘額表 (TWSE TWT93U, TPEx
      margin/sbl) - next-session SBL quota, binding limit, listed shares (derived from
      the margin limit), 10% SBL sub-cap, balance, room, usage trend over ~10 sessions,
      USD capacity, sessions to fill. One background fetch per session (evening), disk
      cache twq_cache.json, /api/twq reads memory only. cb_state keeps its bw_* columns.
      Env: NUKE_TWQ_URL(_ALT), NUKE_TWQ_TPEX_URL, NUKE_TWQ_DIR (file drop), NUKE_TWQ_HISTORY,
      NUKE_TWQ_CEIL_PCT, NUKE_TWQ_SBL_PCT, NUKE_TWQ_CEIL_MODE, NUKE_TWQ_FX, NUKE_TWQ_PUBLISH,
      NUKE_TWQ_FETCH=0 to disable. Diagnostic: /api/twq/probe?date=YYYY-MM-DD&src=twse|tpex&code=2330
borrow.b62:
    * TW quota worker keeps going past a failed date (older sessions still load, the band shows
      the latest session it could read), per-date 10-min retry, back-off only after two
      consecutive failures; /api/twq carries the worker diary (log), plan and retry_in; header
      shows "no session yet" with the reason; page re-polls every 5 s until the first session lands.
borrow.b63:
    * Exchange requests go through one browser-like session (cookies from the report page), 3 s apart
      (NUKE_TWQ_PAUSE); an HTTP 307 security redirect, a non-200 or an empty answer carrying another
      report date is an error shown in the band (not a silent "holiday"). Header: "fetching MM-DD..."
      while the worker runs, "MM-DD no trading" for a holiday, diary in the tooltip, a refresh glyph
      (POST /api/twq/fetch) to fetch now. File drop defaults to <app dir>/twq_drop.
"""

import asyncio
import hashlib
import json
import math
import subprocess
import logging
import os
import sys
import time
import threading
from datetime import timedelta
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from collections import deque
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Set

import pymysql
import requests
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
HOST = os.environ.get("APP_HOST", "0.0.0.0")       # all interfaces for the desk
PORT = int(os.environ.get("APP_PORT", "59999"))    # override: SET APP_PORT=...

# Production box: when the app runs THERE, the browser opens the friendly
# FQDN (which is also the link to share); on a dev laptop it opens localhost.
SERVER_FQDN = os.environ.get(
    "APP_FQDN", "apachkgfiwx507.apac.nsroot.net")
SERVER_SHORT = SERVER_FQDN.split(".")[0]


def _running_on_server() -> bool:
    """True if the current host looks like the production server."""
    import socket
    try:
        this_host = socket.gethostname().lower()
    except Exception:
        this_host = ""
    try:
        this_fqdn = socket.getfqdn().lower()
    except Exception:
        this_fqdn = ""
    candidates = {this_host, this_fqdn, this_host.split(".")[0]}
    return SERVER_FQDN.lower() in candidates or SERVER_SHORT.lower() in candidates


def resolve_browser_url() -> str:
    """1. BROWSER_URL env override  2. FQDN on the real server  3. localhost."""
    if os.environ.get("BROWSER_URL"):
        return os.environ["BROWSER_URL"]
    if _running_on_server():
        return f"http://{SERVER_FQDN}:{PORT}/"
    return f"http://localhost:{PORT}/"
RFX_REFRESH_DEFAULT = 5   # server-side Refinitiv poll, seconds (shared by all)
REFDATA_REFRESH_DEFAULT = 300   # eqrms/cbanalytics refdata re-fetch, seconds

BASE_URL = "http://cbprice-on-demand.wlb4.apac.nsroot.net:65450"
ENDPOINT_NUKED = "/GetNukedCBPrice"
MAIN_LOOP = None
USERNAME = "jb33880"
OVERRIDES_KEY = "lstOverrides"
JSON_PAYLOAD_TYPE = (
    "Citi.Equity.CbService.Web.RestApi.JsonRequest_CBPricing_PriceCb_Overrides_list"
)
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
BATCH_SIZE = 50

DB_CONFIG = {
    "host": "localhost",
    "port": 33306,
    "user": "root",
    "password": "",
    "charset": "utf8mb4",
    "connect_timeout": 10,
}
CBA_DB = "cbanalytics"
REF_TABLE = "nuked_price"
EQRMS_DB = "eqrms"
RIC_TABLE = "risk_positions"
APP_DB = "cba_app"
APP_TABLE = "cb_nuke"

REFINITIV_FIELDS = ["CF_LAST", "CF_TIME", "CF_DATE", "CF_CLOSE",
                    "TR.PriceClose", "TR.PriceClose.date"]

DEFAULT_SEC_IDS = [
    42863076, 55995604, 30622018, 29647868, 43338089,
    52853490, 44497742, 28357967, 52940785,
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
)
logger = logging.getLogger("nuke_station")

LOG_RING: "deque[str]" = deque(maxlen=800)


class _RingHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_RING.append("%s [%s] %s" % (
                datetime.now().strftime("%H:%M:%S"),
                record.levelname[:4], self.format(record)))
        except Exception:
            pass


_rh = _RingHandler()
_rh.setFormatter(logging.Formatter("%(name)s: %(message)s"))
logging.getLogger().addHandler(_rh)
logging.getLogger().setLevel(logging.INFO)
for _noisy in ("httpx", "httpcore", "numexpr", "numexpr.utils"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
_fh = RotatingFileHandler("nuke_station.log", maxBytes=5_000_000,
                          backupCount=3, encoding="utf-8")
_fh.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s : %(message)s"))
logger.addHandler(_fh)
logging.getLogger("uvicorn.access").addHandler(_fh)

# ------------------------------------------------------------------
# Upstream HTTP session
# ------------------------------------------------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("POST",), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"Content-Type": "application/json"})
    return s


SESSION = _make_session()
if os.environ.get("NUKE_TRUST_ENV", "1") != "1":
    SESSION.trust_env = False       # bypass HTTP(S)_PROXY env for pricing


def call_nuked_api(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload = {
        "UserName": USERNAME,
        "JsonPayload": json.dumps({OVERRIDES_KEY: entries}),
        "JsonPayloadType": JSON_PAYLOAD_TYPE,
    }
    url = BASE_URL + ENDPOINT_NUKED
    try:
        resp = SESSION.post(url, json=payload,
                            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    except requests.RequestException as exc:
        logger.warning("pricing POST failed once (%s) - retrying: %s",
                       url, str(exc)[:160])
        import time as _t
        _t.sleep(0.4)
        resp = SESSION.post(url, json=payload,
                            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    env = resp.json()
    rows = json.loads(env.get("JsonPayload") or "[]")
    return {
        "host": env.get("HostName", "?"),
        "elapsed": resp.elapsed.total_seconds(),
        "rows": rows,
    }


# ------------------------------------------------------------------
# MariaDB
# ------------------------------------------------------------------
def _db(**kw):
    cfg = dict(DB_CONFIG); cfg.update(kw)
    return pymysql.connect(**cfg)


APP_DDL = f"""
CREATE TABLE IF NOT EXISTS {APP_DB}.{APP_TABLE} (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  saved_at DATETIME NOT NULL,
  sec_id BIGINT NOT NULL,
  short_name VARCHAR(64) NULL,
  company_name VARCHAR(128) NULL,
  ric VARCHAR(32) NULL,
  expiry_date VARCHAR(16) NULL,
  isin VARCHAR(24) NULL,
  und_fx VARCHAR(24) NULL,
  ovd_spot DOUBLE NULL, ovd_cbfx DOUBLE NULL, ovd_undfx DOUBLE NULL,
  n_bid DOUBLE NULL, n_delta DOUBLE NULL, n_spread DOUBLE NULL,
  n_spot DOUBLE NULL, n_spotfx DOUBLE NULL,
  live_bid DOUBLE NULL, live_ask DOUBLE NULL, live_spot DOUBLE NULL,
  live_cbfx DOUBLE NULL, live_undfx DOUBLE NULL,
  eod_bid DOUBLE NULL, eod_ask DOUBLE NULL, eod_spot DOUBLE NULL,
  eod_cbfx DOUBLE NULL, eod_undfx DOUBLE NULL,
  ovd_bid DOUBLE NULL, ovd_ask DOUBLE NULL,
  stk_last DOUBLE NULL, stk_time VARCHAR(16) NULL, stk_date VARCHAR(20) NULL,
  stk_close DOUBLE NULL, stk_close_date VARCHAR(20) NULL,
  fx_last DOUBLE NULL, fx_time VARCHAR(16) NULL, fx_date VARCHAR(20) NULL,
  fx_close DOUBLE NULL, fx_close_date VARCHAR(20) NULL,
  KEY idx_sec_saved (sec_id, saved_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SAVE_COLS = ["saved_at","sec_id","short_name","company_name","ric","expiry_date",
    "isin","und_fx","ovd_spot","ovd_cbfx","ovd_undfx",
    "n_bid","n_delta","n_spread","n_spot","n_spotfx",
    "live_bid","live_ask","live_spot","live_cbfx","live_undfx",
    "eod_bid","eod_ask","eod_spot","eod_cbfx","eod_undfx",
    "ovd_bid","ovd_ask","x_bid","x_ask","x_both","vol_flag","bond_type",
    "stk_last","stk_time","stk_date","stk_close","stk_close_date",
    "fx_last","fx_time","fx_date","fx_close","fx_close_date"]


def ensure_schema() -> None:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {APP_DB} "
                        f"DEFAULT CHARSET utf8mb4")
            cur.execute(APP_DDL)
            # migrate older tables that predate the fx close columns
            for ddl in (
                f"ALTER TABLE {APP_DB}.{APP_TABLE} "
                f"ADD COLUMN IF NOT EXISTS fx_close DOUBLE NULL",
                f"ALTER TABLE {APP_DB}.{APP_TABLE} "
                f"ADD COLUMN IF NOT EXISTS fx_close_date VARCHAR(20) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_dvb VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_dvs VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_brw VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_lo VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_hi VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_gap VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_util VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_d5 VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_htb VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_evt VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_src VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bw_tnr VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS or_bid_sprd VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS or_ask_sprd VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS x_bid VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS x_ask VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS x_both VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS bond_type VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS vol_flag VARCHAR(32) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS isin_manual VARCHAR(16) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS hedged VARCHAR(10) NULL",
                f"ALTER TABLE {APP_DB}.cb_state "
                f"ADD COLUMN IF NOT EXISTS iv_manual VARCHAR(16) NULL",
                f"CREATE INDEX IF NOT EXISTS ix_cbnuke_sec_id ON {APP_DB}.{APP_TABLE} (sec_id, id)",
                f"CREATE TABLE IF NOT EXISTS {APP_DB}.cb_state_hist ("
                "  id BIGINT AUTO_INCREMENT PRIMARY KEY, sec_id BIGINT NOT NULL, field VARCHAR(32) NOT NULL, "
                "  old_val VARCHAR(64) NULL, new_val VARCHAR(64) NULL, changed_by VARCHAR(64) NULL, "
                "  changed_at DATETIME NOT NULL, INDEX ix_sec_field (sec_id, field, id))",

            ):
                try:
                    cur.execute(ddl)
                except Exception:
                    pass
        conn.commit()
    finally:
        conn.close()


def save_rows(rows: List[Dict[str, Any]]) -> int:
    ensure_schema()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ph = ", ".join(["%s"] * len(SAVE_COLS))
    # arity guard: a tuple/SAVE_COLS mismatch surfaces as the
    # cryptic 'not enough arguments for format string'
    sql = (f"INSERT INTO {APP_DB}.{APP_TABLE} "
           f"({', '.join(SAVE_COLS)}) VALUES ({ph})")
    def num(v):
        try:
            return None if v in (None, "") else float(v)
        except (TypeError, ValueError):
            return None
    def txt(v):
        v = "" if v is None else str(v).strip()
        return v or None
    data = []
    for r in rows:
        data.append((
            now, int(r.get("sec_id", 0)),
            txt(r.get("short_name")), txt(r.get("company_name")),
            txt(r.get("ric")), txt(r.get("expiry_date")), txt(r.get("isin")),
            txt(r.get("und_fx")),
            num(r.get("ovd_spot")), num(r.get("ovd_cbfx")), num(r.get("ovd_undfx")),
            num(r.get("n_bid")), num(r.get("n_delta")), num(r.get("n_spread")),
            num(r.get("n_spot")), num(r.get("n_spotfx")),
            num(r.get("live_bid")), num(r.get("live_ask")), num(r.get("live_spot")),
            num(r.get("live_cbfx")), num(r.get("live_undfx")),
            num(r.get("eod_bid")), num(r.get("eod_ask")), num(r.get("eod_spot")),
            num(r.get("eod_cbfx")), num(r.get("eod_undfx")),
            num(r.get("ovd_bid")), num(r.get("ovd_ask")),
            num(r.get("stk_last")), txt(r.get("stk_time")), txt(r.get("stk_date")),
            num(r.get("stk_close")), txt(r.get("stk_close_date")),
            num(r.get("fx_last")), txt(r.get("fx_time")), txt(r.get("fx_date")),
            num(r.get("fx_close")), txt(r.get("fx_close_date")),
        ))
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()
    finally:
        conn.close()
    return len(data)


def fetch_saved_prefs(sec_ids: List[int]) -> Dict[int, Dict[str, str]]:
    if not sec_ids:
        return {}
    ph = ", ".join(["%s"] * len(sec_ids))
    out: Dict[int, Dict[str, str]] = {int(s): {"short_name": "", "und_fx": ""} for s in sec_ids}
    try:
        conn = _db()
    except Exception:
        return out
    try:
        with conn.cursor() as cur:
            for col in ("short_name", "und_fx"):
                # the latest NON-blank value per security - a snapshot taken while the state
                # was blank must never hide an older good value
                cur.execute(
                    f"SELECT t.sec_id, t.{col} FROM {APP_DB}.{APP_TABLE} t "
                    f"JOIN (SELECT sec_id, MAX(id) AS mid FROM {APP_DB}.{APP_TABLE} "
                    f"      WHERE sec_id IN ({ph}) AND {col} IS NOT NULL AND {col} <> '' GROUP BY sec_id) m "
                    f"ON m.mid = t.id", list(sec_ids))
                for sec_id, val in cur.fetchall():
                    out.setdefault(int(sec_id), {"short_name": "", "und_fx": ""})[col] = str(val or "")
    except Exception as exc:
        logger.warning("saved prefs read failed: %s", exc)
    finally:
        conn.close()
    return out


def fetch_startup_ids() -> tuple:
    """Unique sec_ids from the most recent save batch in cba_app.cb_nuke
    (latest saved_at), in their saved row order. Falls back to
    DEFAULT_SEC_IDS if the table is missing or empty."""
    query = f"""
        SELECT sec_id FROM {APP_DB}.{APP_TABLE}
        WHERE saved_at = (SELECT MAX(saved_at) FROM {APP_DB}.{APP_TABLE})
        ORDER BY id
    """
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        finally:
            conn.close()
        ids, seen = [], set()
        for (sec_id,) in rows:
            sid = int(sec_id)
            if sid not in seen:
                seen.add(sid)
                ids.append(sid)
        if ids:
            return ids, "last save"
    except Exception as exc:
        logger.warning("Startup id lookup failed, using defaults: %s", exc)
    return DEFAULT_SEC_IDS, "defaults"


def fetch_ref_rows(sec_ids: List[int]) -> List[Dict[str, Any]]:
    if not sec_ids:
        return []
    ph = ", ".join(["%s"] * len(sec_ids))
    query = f"""
        SELECT sec_id, company_name, expiry_date, isin, conversion_price
        FROM {CBA_DB}.{REF_TABLE}
        WHERE sec_id IN ({ph})
        ORDER BY snap_ts DESC
    """
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(query, sec_ids)
            rows = cur.fetchall()
    finally:
        conn.close()

    seen: Dict[int, Dict[str, Any]] = {}
    for sec_id, company, expiry, isin, conv_px in rows:
        sid = int(sec_id)
        if sid in seen:
            # newer snaps may leave conversion_price NULL; fall back to the
            # most recent snap that actually carries one
            if not seen[sid]["conversion_price"] and conv_px is not None:
                seen[sid]["conversion_price"] = str(conv_px)
            continue
        seen[sid] = {
            "secId": sid,
            "conversion_price": ("" if conv_px is None else str(conv_px)),
            "company_name": company or "",
            "expiry_date": expiry.isoformat() if hasattr(expiry, "isoformat")
                           else (str(expiry) if expiry else ""),
            "isin": isin or "",
        }
    return list(seen.values())


_QTY_DIAG_DONE = False
_RP_META = {"checked": False, "cols": [], "qty_cands": []}
_QTY_CANDS = ["quantity_live", "quantity_t1", "quantity_t2",
              "net_quantity", "quantity", "position_quantity",
              "pos_qty", "quantity_sod", "qty"]
_QTY_SRC = {"col": None}    # last chosen source, for change-logging


def fetch_ric_map(sec_ids: List[int]) -> Dict[int, Dict[str, str]]:
    if not sec_ids:
        return {}
    conn = _db()
    try:
        with conn.cursor() as cur:
            if not _RP_META["checked"]:
                cur.execute(f"SHOW COLUMNS FROM {EQRMS_DB}.{RIC_TABLE}")
                _RP_META["cols"] = [str(r[0]) for r in cur.fetchall()]
                low = {c.lower(): c for c in _RP_META["cols"]}
                _RP_META["qty_cands"] = [low[c] for c in _QTY_CANDS
                                         if c in low]
                _RP_META["checked"] = True
            cands = _RP_META["qty_cands"] or ["quantity_live"]
            sel_extra = ", ".join(f"`{c}`" for c in cands)
            ph = ", ".join(["%s"] * len(sec_ids))
            cur.execute(f"""
                SELECT security_id, component_ric, security_currency,
                       cb_notional_usd, {sel_extra}
                FROM {EQRMS_DB}.{RIC_TABLE}
                WHERE loaded_at >= (SELECT MAX(loaded_at)
                                    FROM {EQRMS_DB}.{RIC_TABLE})
                      - INTERVAL 10 SECOND
                  AND security_id IN ({ph})
            """, sec_ids)
            rows = cur.fetchall()
    finally:
        conn.close()

    def fnum(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    out: Dict[int, Dict[str, Any]] = {}
    acc: Dict[int, Dict[str, Dict[str, float]]] = {}
    raws: Dict[int, list] = {}
    for row in rows:
        sec_id, ric, ccy, usd = row[0], row[1], row[2], row[3]
        qvals = row[4:]
        sid = int(sec_id)
        if sid not in out:
            out[sid] = {"ric": str(ric).strip() if ric else "",
                        "ccy": str(ccy).strip().upper() if ccy else "",
                        "qty": 0.0, "usd": 0.0}
            acc[sid] = {c: {"all": 0.0, "usd": 0.0, "has_usd": 0.0}
                        for c in cands}
            raws[sid] = []
        u = fnum(usd) or 0.0
        out[sid]["usd"] += u
        for c, v in zip(cands, qvals):
            q = fnum(v) or 0.0
            acc[sid][c]["all"] += q
            if u:
                acc[sid][c]["usd"] += q
                acc[sid][c]["has_usd"] = 1.0
        if len(raws[sid]) < 6:
            raws[sid].append((str(ric or ""), u) + tuple(
                fnum(v) for v in qvals))

    # pick the quantity source PER BATCH: first candidate producing a
    # nonzero notional-row quantity. Weekend snaps carry quantities on
    # quantity_t1 (next business day) with quantity_live NULL - and on
    # Monday the choice flips back to quantity_live automatically.
    src = None
    for c in cands:
        if any(a[c]["usd"] for a in acc.values()):
            src = c
            break
    src = src or (cands[0] if cands else "quantity_live")
    if src != _QTY_SRC["col"]:
        logger.info("quantity source column: %s%s", src,
                    "" if src == "quantity_live"
                    else " (quantity_live empty on notional rows - "
                         "weekend/holiday snap)")
        _QTY_SRC["col"] = src

    global _QTY_DIAG_DONE
    for sid, rec in out.items():
        a = acc[sid].get(src) or {"all": 0.0, "usd": 0.0, "has_usd": 0.0}
        rec["qty"] = a["usd"] if a["has_usd"] else a["all"]
        if not _QTY_DIAG_DONE and rec["usd"] and not rec["qty"]:
            _QTY_DIAG_DONE = True
            logger.info("qty/usd mismatch: sec %s | risk_positions cols=%s "
                        "| sample rows (ric, usd, %s)=%s",
                        sid, _RP_META["cols"], ", ".join(cands), raws[sid])
    return out


def default_fx_ric(ccy: str) -> str:
    if not ccy:
        return ""
    return "1" if ccy == "USD" else f"{ccy}="


# ------------------------------------------------------------------
# Refinitiv (Workspace desktop session)
# ------------------------------------------------------------------
_RD = None


_RD_OPENING = False
_RD_LOCK = __import__("threading").Lock()


def _get_rd():
    """Open the Refinitiv session exactly once, even if a slow first open
    outlives the poller's timeout: concurrent open attempts wedge the
    library's session layer, so late-comers wait on the lock instead of
    starting a second handshake."""
    global _RD, _RD_OPENING
    if _RD is None:
        with _RD_LOCK:
            if _RD is None:
                import time as _t
                import refinitiv.data as rd
                _RD_OPENING = True
                t0 = _t.time()
                try:
                    _cfg_path = None
                    for _base in (os.getcwd(),
                                  os.path.dirname(os.path.abspath(__file__))):
                        _cand = os.path.join(_base,
                                             "refinitiv-data.config.json")
                        if os.path.exists(_cand):
                            _cfg_path = _cand
                            break
                    if _cfg_path:
                        logger.info("refinitiv config: %s", _cfg_path)
                        try:
                            rd.open_session(config_name=_cfg_path)
                        except TypeError:
                            rd.open_session()
                    else:
                        logger.warning("refinitiv-data.config.json not found "
                                       "in cwd or app dir - opening DEFAULT "
                                       "session (usually no entitlements)")
                        rd.open_session()
                finally:
                    _RD_OPENING = False
                cfg = os.path.join(os.getcwd(),
                                   "refinitiv-data.config.json")
                logger.info("session cwd=%s config_json=%s", os.getcwd(),
                            "FOUND" if os.path.exists(cfg) else "MISSING")
                globals()["_SESS_OPEN_TS"] = __import__("time").time()
                logger.info("Refinitiv session opened in %.1fs",
                            _t.time() - t0)
                _RD = rd
    return _RD


def _clean(v) -> Any:
    try:
        import pandas as pd
        if v is None or (hasattr(pd, "isna") and pd.isna(v)):
            return None
    except Exception:
        if v is None:
            return None
    if hasattr(v, "isoformat"):
        return str(v)
    if isinstance(v, (int, float, str, bool)):
        return v
    try:
        return float(v)
    except Exception:
        return str(v)


_RFX_LAST_RESET = {"t": 0.0}
_RFX_RESET_GAP = 600.0          # never recycle the session more than once / 10 min


def _rfx_may_reset() -> bool:
    import time as _t
    if _t.time() - _RFX_LAST_RESET["t"] < _RFX_RESET_GAP:
        return False
    _RFX_LAST_RESET["t"] = _t.time()
    return True


def fetch_refinitiv(rics: List[str]) -> List[Dict[str, Any]]:
    rd = _get_rd()
    try:
        df = rd.get_data(universe=rics, fields=REFINITIV_FIELDS)
    except Exception:
        global _RD
        if not _rfx_may_reset():
            raise                    # back off instead of a reopen storm
        try:
            rd.close_session()
        except Exception:
            pass
        _RD = None
        rd = _get_rd()
        df = rd.get_data(universe=rics, fields=REFINITIV_FIELDS)

    out: List[Dict[str, Any]] = []
    if df is None or getattr(df, "empty", True) or len(df.columns) == 0:
        # streaming warm-up right after session open, or a Workspace-side
        # gap: treat as "no data this cycle", never as a crash
        try:
            logger.info("empty rfx df: shape=%s cols=%s",
                        None if df is None else getattr(df, "shape", "?"),
                        [] if df is None else list(df.columns)[:8])
        except Exception:
            pass
        return out
    cols = {str(c).strip().lower(): c for c in df.columns}
    inst_col = cols.get("instrument") or df.columns[0]
    for _, row in df.iterrows():
        rec = {"ric": _clean(row[inst_col]),
               "last": None, "last_time": None, "last_date": None,
               "close": None, "close_date": None}
        cf_close = None
        for key, col in cols.items():
            v = _clean(row[col])
            if key == "cf_last":
                rec["last"] = v
            elif key == "cf_time":
                rec["last_time"] = v
            elif key == "cf_date":
                rec["last_date"] = v
            elif key == "cf_close":
                cf_close = v
            elif "close" in key and "date" not in key:
                rec["close"] = v
            elif key == "date" or ("close" in key and "date" in key):
                rec["close_date"] = v
        if rec["close"] is None:
            rec["close"] = cf_close          # FX RICs: CF_CLOSE fallback
        out.append(rec)
    return out


# ------------------------------------------------------------------
# Shared state (single-process; run with exactly one worker)
# ------------------------------------------------------------------
OVD_FIELDS = ("ovdSpot", "ovdCbFx", "ovdUndFx")
USER_FIELDS = ("short_name", "und_fx", "n_gamma", "isin_manual", "hedged", "iv_manual",
               "or_bid_sprd", "or_ask_sprd",
               "x_bid", "x_ask", "x_both", "vol_flag", "bond_type")
AUTOSAVE_DEFAULT = 300      # seconds; 0 = off

STATE: Dict[str, Any] = {
    "version": 0,
    "ids": [],                     # ordered sec_ids
    "rows": {},                    # sec_id -> {short_name, und_fx, ovdSpot,...}
    "stockRics": {}, "cbFxRics": {}, "cbFxCcy": {},               # sec_id -> ric (server-side, for the poller)
    "nuke": {},                    # sec_id -> last upstream row
    "nukeMeta": {},                # host / elapsed / by / ts / missing
    "rfx": {},                     # ric -> refinitiv rec
    "rfxTs": None,
    "rfxErr": None,
    "refreshSec": RFX_REFRESH_DEFAULT,
    "autosaveSec": AUTOSAVE_DEFAULT,
    "refdataSec": REFDATA_REFRESH_DEFAULT,
    "flagTh": {"staleSpot": 0.5, "staleFx": 0.25,
               "moveStk": 3.0, "moveFx": 30.0},
    "refErr": None,
}
CLIENTS: Set[WebSocket] = set()
CLIENT_NAMES: Dict[WebSocket, str] = {}
CLIENT_LOCKS: Dict[WebSocket, "asyncio.Lock"] = {}
NUKE_LOCK = None   # created lazily inside the running loop
# (py3.9: a module-level asyncio.Lock binds the import-time loop
#  and poisons wait_for from uvicorn's loop)
def _nlock():
    global NUKE_LOCK
    if NUKE_LOCK is None:
        NUKE_LOCK = asyncio.Lock()
    return NUKE_LOCK

RFX_WAKE: Optional[asyncio.Event] = None
REFDATA_WAKE: Optional[asyncio.Event] = None


PROTECTED_FIELDS = ("short_name", "und_fx", "n_gamma", "isin_manual", "bond_type", "iv_manual")


def _rows_apply(it, row, user=""):
    """Merge one rows-message item into a STATE row. Returns the touched fields.
    A blank value for a protected field is ignored unless the client lists the
    field in "_clear" (deliberate erase) - so a browser whose grid happened to be
    empty can never wipe names, FX rics or types for everyone."""
    clear = set(it.get("_clear") or [])
    touched = []
    for f in USER_FIELDS + OVD_FIELDS:
        if f not in it:
            continue
        v = str(it.get(f) or "")
        if f in PROTECTED_FIELDS and v == "" and (row.get(f) or "") != "" and f not in clear:
            logger.info("rows: ignored blank %s for %s from %s (kept %r)", f, it.get("secId"), user, row.get(f))
            continue
        row[f] = v
        touched.append(f)
    return touched


def _blank_row() -> Dict[str, Any]:
    return {"short_name": "", "und_fx": "", "n_gamma": "", "isin_manual": "", "hedged": "", "iv_manual": "",
            "or_bid_sprd": "", "or_ask_sprd": "",
            "x_bid": "", "x_ask": "", "x_both": "", "vol_flag": "",
            "bond_type": "",
            "ovdSpot": "", "ovdCbFx": "", "ovdUndFx": ""}


def _van_mirror(row) -> bool:
    """Vanilla bonds settle in the underlying ccy: ovdCbFx
    follows ovdUndFx. Idempotent; True when it changed."""
    try:
        bt = (row.get("bond_type") or "").strip().lower()
        uv = str(row.get("ovdUndFx") or "")
        if bt.startswith("vanil") and uv \
                and row.get("ovdCbFx") != uv:
            row["ovdCbFx"] = uv
            return True
    except Exception:
        pass
    return False


def snapshot() -> Dict[str, Any]:
    for _r in (STATE.get("rows") or {}).values():
        _van_mirror(_r)
    return {"version": STATE["version"], "ids": STATE["ids"],
            "rows": STATE["rows"], "nuke": STATE["nuke"],
            "nukeMeta": STATE["nukeMeta"], "rfx": STATE["rfx"],
            "rfxBanner": STATE.get("rfxBanner", ""),
            "cbFxRics": STATE.get("cbFxRics", {}),
            "cbFxCcy": STATE.get("cbFxCcy", {}),
            "rfxTs": STATE["rfxTs"], "rfxErr": STATE["rfxErr"],
            "refreshSec": STATE["refreshSec"],
            "autosaveSec": STATE["autosaveSec"],
            "userCfg": STATE.get("userCfg", {}),
            "refdataSec": STATE["refdataSec"],
            "flagTh": STATE["flagTh"]}


async def broadcast(msg: Dict[str, Any], skip: Optional[WebSocket] = None):
    """Concurrent fan-out with a per-peer timeout: one sleeping browser
    tab must never delay everyone else's nuke results."""
    data = json.dumps(msg)
    peers = [ws for ws in list(CLIENTS) if ws is not skip]
    if not peers:
        return

    async def _one(ws):
        try:
            lock = CLIENT_LOCKS.get(ws)
            if lock is None:
                lock = CLIENT_LOCKS.setdefault(ws, asyncio.Lock())
            async with lock:                 # one frame at a time per peer
                await asyncio.wait_for(ws.send_text(data), timeout=1.5)
            return None
        except Exception:
            return ws

    results = await asyncio.gather(*(_one(w) for w in peers),
                                   return_exceptions=False)
    dead = [w for w in results if w is not None]
    for ws in dead:
        CLIENTS.discard(ws)
        CLIENT_NAMES.pop(ws, None)
        CLIENT_LOCKS.pop(ws, None)
    if dead:
        logger.info("broadcast: dropped %d unresponsive client(s); "
                    "they will reconnect", len(dead))


# ---- durability: working state + audit log in cba_app ----
STATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {APP_DB}.cb_state (
  sec_id BIGINT PRIMARY KEY,
  short_name VARCHAR(64) NULL, und_fx VARCHAR(24) NULL,
  n_gamma VARCHAR(32) NULL,
  or_bid_sprd VARCHAR(32) NULL,
  or_ask_sprd VARCHAR(32) NULL,
  x_bid VARCHAR(32) NULL,
  x_ask VARCHAR(32) NULL,
  x_both VARCHAR(32) NULL,
  bond_type VARCHAR(32) NULL,
  vol_flag VARCHAR(32) NULL,
  ovd_spot VARCHAR(32) NULL, ovd_cbfx VARCHAR(32) NULL,
  bw_dvb VARCHAR(32) NULL,
  bw_dvs VARCHAR(32) NULL,
  bw_brw VARCHAR(32) NULL,
  bw_lo VARCHAR(32) NULL,
  bw_hi VARCHAR(32) NULL,
  bw_gap VARCHAR(32) NULL,
  bw_util VARCHAR(32) NULL,
  bw_d5 VARCHAR(32) NULL,
  bw_htb VARCHAR(32) NULL,
  bw_evt VARCHAR(32) NULL,
  bw_src VARCHAR(32) NULL,
  bw_tnr VARCHAR(32) NULL,

  ovd_undfx VARCHAR(32) NULL,
  updated_at DATETIME NULL, updated_by VARCHAR(32) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
META_DDL = f"""
CREATE TABLE IF NOT EXISTS {APP_DB}.cb_state_meta (
  k VARCHAR(32) PRIMARY KEY, v TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
EVENTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {APP_DB}.app_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ts DATETIME NOT NULL,
  user VARCHAR(32) NULL,
  action VARCHAR(32) NOT NULL,
  detail TEXT NULL,
  KEY idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def db_log(user: str, action: str, detail: Any = "") -> None:
    """Audit trail; never lets a DB hiccup break the app."""
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {APP_DB}.app_events (ts,user,action,detail) "
                    f"VALUES (%s,%s,%s,%s)",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user[:32],
                     action[:32], json.dumps(detail)[:4000]))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("event log skipped: %s", exc)


COLMAP = {"short_name": "short_name", "und_fx": "und_fx", "isin_manual": "isin_manual", "hedged": "hedged", "iv_manual": "iv_manual",
          "n_gamma": "n_gamma", "or_bid_sprd": "or_bid_sprd",
          "or_ask_sprd": "or_ask_sprd", "x_bid": "x_bid", "x_ask": "x_ask",
          "x_both": "x_both", "vol_flag": "vol_flag",
          "bond_type": "bond_type", "ovdSpot": "ovd_spot",
          "ovdCbFx": "ovd_cbfx", "ovdUndFx": "ovd_undfx"}


def load_state_rows(cur, into: Dict[int, Dict[str, Any]]) -> int:
    """Restore STATE rows from cb_state using whatever persisted columns
    exist (information_schema), so a missing column can never silently
    drop the extended fields (X, spreads, bond_type, vol_flag)."""
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name='cb_state'", (APP_DB,))
    have = {str(r[0]).lower() for r in cur.fetchall()}
    fields = [f for f, c in COLMAP.items() if c.lower() in have]
    if not fields:
        return 0
    cols = ", ".join(COLMAP[f] for f in fields)
    cur.execute(f"SELECT sec_id, {cols} FROM {APP_DB}.cb_state")
    n = 0
    for r in cur.fetchall():
        sid = int(r[0])
        row = into.setdefault(sid, _blank_row())
        for f, v in zip(fields, r[1:]):
            v = "" if v is None else str(v)
            if f == "vol_flag" and v == "Expensive":
                v = "Rich"
            row[f] = v
        n += 1
    return n


def db_upsert_state_fields(sec_id: int, row: Dict[str, Any], user: str,
                           fields) -> None:
    """Write ONLY the given row fields. A one-field edit must never blank
    the other columns - full-row writes wiped short_name/und_fx once when
    an edit landed on an empty in-memory row."""
    cols = [COLMAP[f] for f in fields if f in COLMAP]
    if not cols:
        return
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                # append-only history of every change to a user field: the live table can be
                # overwritten by a bad write, the history cannot - recovery reads it back
                try:
                    ufs = [f for f in fields if f in COLMAP and f in USER_FIELDS]
                    if ufs:
                        cur.execute(f"SELECT {', '.join(COLMAP[f] for f in ufs)} FROM {APP_DB}.cb_state WHERE sec_id=%s", (sec_id,))
                        prev = cur.fetchone() or tuple(None for _ in ufs)
                        hist = []
                        for f, old in zip(ufs, prev):
                            new = str(row.get(f) or "") or None
                            old = (str(old) if old is not None else None) or None
                            if new != old:
                                hist.append((sec_id, f, old, new, user, datetime.now()))
                        if hist:
                            cur.executemany(f"INSERT INTO {APP_DB}.cb_state_hist (sec_id, field, old_val, new_val, changed_by, changed_at) "
                                            "VALUES (%s,%s,%s,%s,%s,%s)", hist)
                except Exception as exc:
                    logger.warning("state history write failed: %s", exc)
                collist = ", ".join(cols)
                ph = ", ".join(["%s"] * len(cols))
                upd = ", ".join(f"{c}=VALUES({c})" for c in cols)
                cur.execute(
                    f"INSERT INTO {APP_DB}.cb_state (sec_id, {collist}, "
                    f"updated_at, updated_by) VALUES (%s, {ph}, %s, %s) "
                    f"ON DUPLICATE KEY UPDATE {upd}, "
                    "updated_at=VALUES(updated_at), "
                    "updated_by=VALUES(updated_by)",
                    [sec_id] + [str(row.get(f) or "") or None
                                for f in fields if f in COLMAP]
                    + [datetime.now(), user])
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("state upsert (partial) failed: %s", exc)


def db_upsert_state(sec_id: int, row: Dict[str, Any], user: str) -> None:
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {APP_DB}.cb_state
                        (sec_id, short_name, und_fx, n_gamma,
                         or_bid_sprd, or_ask_sprd, x_bid, x_ask, x_both,
                         vol_flag, bond_type, ovd_spot, ovd_cbfx, ovd_undfx,
                         updated_at, updated_by)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          short_name=VALUES(short_name), und_fx=VALUES(und_fx),
                          n_gamma=VALUES(n_gamma),
                          or_bid_sprd=VALUES(or_bid_sprd),
                          or_ask_sprd=VALUES(or_ask_sprd),
                          x_bid=VALUES(x_bid), x_ask=VALUES(x_ask),
                          x_both=VALUES(x_both),
                          vol_flag=VALUES(vol_flag),
                          bond_type=VALUES(bond_type),
                          ovd_spot=VALUES(ovd_spot), ovd_cbfx=VALUES(ovd_cbfx),
                          ovd_undfx=VALUES(ovd_undfx),
                          updated_at=VALUES(updated_at),
                          updated_by=VALUES(updated_by)""",
                    (sec_id, row.get("short_name") or None,
                     row.get("und_fx") or None,
                     str(row.get("n_gamma") or "") or None,
                     str(row.get("or_bid_sprd") or "") or None,
                     str(row.get("or_ask_sprd") or "") or None,
                     str(row.get("x_bid") or "") or None,
                     str(row.get("x_ask") or "") or None,
                     str(row.get("x_both") or "") or None,
                     str(row.get("vol_flag") or "") or None,
                     str(row.get("bond_type") or "") or None,
                     str(row.get("ovdSpot") or "") or None,
                     str(row.get("ovdCbFx") or "") or None,
                     str(row.get("ovdUndFx") or "") or None,
                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user[:32]))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("state upsert skipped: %s", exc)


def db_meta_set(k: str, v: Any) -> None:
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {APP_DB}.cb_state_meta (k,v) VALUES (%s,%s) "
                    f"ON DUPLICATE KEY UPDATE v=VALUES(v)",
                    (k, json.dumps(v)))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("meta set skipped: %s", exc)


def db_meta_get(k: str) -> Any:
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT v FROM {APP_DB}.cb_state_meta WHERE k=%s", (k,))
                r = cur.fetchone()
        finally:
            conn.close()
        return json.loads(r[0]) if r and r[0] else None
    except Exception:
        return None


def load_persisted_state() -> None:
    """Startup: restore id order and per-bond working state."""
    try:
        ensure_schema_state()
    except Exception as exc:
        logger.warning("state schema skipped: %s", exc)
    for ddl in (
        f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
        "EXISTS x_bid VARCHAR(32) NULL",
        f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
        "EXISTS x_ask VARCHAR(32) NULL",
        f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
        "EXISTS x_both VARCHAR(32) NULL",
        f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
        "EXISTS vol_flag VARCHAR(12) NULL",
        f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
        "EXISTS bond_type VARCHAR(32) NULL",
        f"ALTER TABLE {APP_DB}.{APP_TABLE} ADD COLUMN IF NOT "
        "EXISTS x_bid VARCHAR(32) NULL",
        f"ALTER TABLE {APP_DB}.{APP_TABLE} ADD COLUMN IF NOT "
        "EXISTS x_ask VARCHAR(32) NULL",
        f"ALTER TABLE {APP_DB}.{APP_TABLE} ADD COLUMN IF NOT "
        "EXISTS x_both VARCHAR(32) NULL",
        f"ALTER TABLE {APP_DB}.{APP_TABLE} ADD COLUMN IF NOT "
        "EXISTS vol_flag VARCHAR(12) NULL",
        f"ALTER TABLE {APP_DB}.{APP_TABLE} ADD COLUMN IF NOT "
        "EXISTS bond_type VARCHAR(32) NULL"):
        try:
            conn = _db()
            try:
                with conn.cursor() as cur:
                    cur.execute(ddl)
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("column ensure failed [%s...]: %s",
                           ddl[:60], exc)
    ids = db_meta_get("sec_ids")
    src = "shared state"
    if not ids:
        ids, src = fetch_startup_ids()
    STATE["ids"] = [int(i) for i in ids]
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                _n = load_state_rows(cur, STATE["rows"])
            logger.info("state restored from cb_state: %d rows", _n)
        finally:
            conn.close()
    except Exception as exc:
        logger.error("state restore from cb_state FAILED: %s", exc)
    try:                                             # fill still-blank names / fx from cb_state + the autosave snapshots
        _fx = recover_names(STATE["ids"])
        if _fx:
            logger.info("startup: filled blank names/fx for %d securities from cb_state/autosave", len(_fx))
    except Exception as exc:
        logger.warning("startup recover failed: %s", exc)
    for sid in STATE["ids"]:
        STATE["rows"].setdefault(sid, _blank_row())
    try:
        a = db_meta_get("autosaveSec")
        if a is not None:
            STATE["autosaveSec"] = max(0, int(a))
    except Exception:
        pass
    for _k, _lo in (("refreshSec", 2), ("refdataSec", 30)):
        try:
            v = db_meta_get(_k)
            if v is not None:
                STATE[_k] = max(_lo, int(v))
        except Exception:
            pass
    try:
        th = db_meta_get("flagTh")
        if isinstance(th, dict):
            STATE["flagTh"].update({k: float(v) for k, v in th.items()
                                    if k in STATE["flagTh"]})
    except Exception:
        pass
    refresh_stock_rics()
    logger.info("startup: %d ids (%s), %d state rows",
                len(STATE["ids"]), src, len(STATE["rows"]))


def ensure_schema_state() -> None:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {APP_DB} "
                        f"DEFAULT CHARSET utf8mb4")
            cur.execute(STATE_DDL)
            cur.execute(f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
                        f"EXISTS n_gamma VARCHAR(32) NULL AFTER und_fx")
            cur.execute(f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
                        f"EXISTS or_bid_sprd VARCHAR(16) NULL AFTER n_gamma")
            cur.execute(f"ALTER TABLE {APP_DB}.cb_state ADD COLUMN IF NOT "
                        f"EXISTS or_ask_sprd VARCHAR(16) NULL "
                        f"AFTER or_bid_sprd")
            cur.execute(META_DDL)
            cur.execute(EVENTS_DDL)
        conn.commit()
    finally:
        conn.close()


def cb_fx_ric(sec_fx) -> str:
    """Bond currency -> Refinitiv fx ric: USD -> '' (constant 1),
    CNH -> 'CNH=', EUR -> 'EUR=', ... (the ric's own quoting convention)."""
    s = str(sec_fx or "").strip().upper()
    return "" if s in ("", "USD") else s + "="


def refresh_stock_rics() -> None:
    try:
        m = fetch_ric_map(STATE["ids"])
        STATE["stockRics"] = {sid: d.get("ric", "") for sid, d in m.items()}
        STATE["cbFxRics"] = {sid: cb_fx_ric(d.get("ccy", "")) for sid, d in m.items()}
        STATE["cbFxCcy"] = {sid: str(d.get("ccy", "") or "").strip().upper() for sid, d in m.items()}
    except Exception as exc:
        logger.warning("stock ric refresh failed: %s", exc)


def poll_rics() -> List[str]:
    out = set()
    for sid in STATE["ids"]:
        r = STATE["stockRics"].get(sid, "")
        if r:
            out.add(r)
        cf = (STATE.get("cbFxRics") or {}).get(sid, "")
        if cf:
            out.add(cf)
        fx = (STATE["rows"].get(sid) or {}).get("und_fx", "").strip()
        if fx:
            try:
                float(fx)
            except ValueError:
                out.add(fx)
    return sorted(out)


async def rfx_poller():
    global RFX_WAKE, _RD, _RD
    RFX_WAKE = asyncio.Event()
    logger.info("Refinitiv poller started (every %ss)", STATE["refreshSec"])
    cooldown_until = 0.0
    while True:
        try:
            rics = poll_rics()
            if not rics:
                err = ("no RICs to poll yet - set und_fx on rows and/or "
                       "wait for EQRMS stock RICs")
                if STATE["rfxErr"] != err:
                    STATE["rfxErr"] = err
                    logger.warning(err)
                    await broadcast({"type": "rfxErr", "error": err})
            elif _RD_OPENING:
                err = ("opening Refinitiv session - first connection can "
                       "take 1-2 minutes ...")
                if STATE["rfxErr"] != err:
                    STATE["rfxErr"] = err
                    logger.info(err)
                    await broadcast({"type": "rfxErr", "error": err})
            elif (asyncio.get_event_loop().time() < cooldown_until
                  and not RFX_WAKE.is_set()):
                pass                       # backing off after a hang/timeout
            else:
                try:
                    rows = await asyncio.wait_for(
                        run_in_threadpool(fetch_refinitiv, rics),
                        timeout=(120 if _RD is None
                                 else max(30, STATE["refreshSec"] * 4)))
                    if not rows:
                        STATE["_rfxEmpty"] = STATE.get("_rfxEmpty", 0) + 1
                        n_e = STATE["_rfxEmpty"]
                        err = ("Refinitiv returned no data "
                               f"({n_e}x) - Workspace warming "
                               "up or logged out on the server; retrying")
                        if STATE["rfxErr"] != err:
                            STATE["rfxErr"] = err
                            await broadcast({"type": "rfxErr", "error": err})
                        if n_e >= 3:
                            # exponential back-off: 60s, 120s, 240s ... max 300s
                            hold = min(300, 60 * (2 ** min(n_e - 3, 3)))
                            cooldown_until = asyncio.get_event_loop().time() + hold
                            ban = ("Workspace not serving data (%d empty responses) "
                                   "- restart Workspace: tray icon > Exit, check Task "
                                   "Manager, relaunch, sign in, then click \u21bb. "
                                   "Retrying in %ds." % (n_e, hold))
                            if STATE.get("rfxBanner") != ban:
                                STATE["rfxBanner"] = ban
                                logger.warning(ban)
                                await broadcast({"type": "rfxBanner", "text": ban})
                        if n_e == 5 and _rfx_may_reset():
                            logger.warning("5 empty Refinitiv responses - "
                                           "one session reset (max once / 10 min)")
                            try:
                                _RD.close_session()
                            except Exception:
                                pass
                            _RD = None
                    else:
                        was_down = STATE.get("_rfxEmpty", 0) >= 3 or bool(STATE.get("rfxBanner"))
                        STATE["_rfxEmpty"] = 0
                        cooldown_until = 0.0
                        if STATE.get("rfxBanner"):
                            STATE["rfxBanner"] = ""
                            await broadcast({"type": "rfxBanner", "text": ""})
                        if was_down and VOL_WAKE is not None:
                            VOL_WAKE.set()          # vol child retries now, not next hour
                        for r in rows:
                            if r.get("ric"):
                                STATE["rfx"][r["ric"]] = r
                        STATE["rfxTs"] = datetime.now().strftime("%H:%M:%S")
                        if STATE["rfxErr"]:
                            STATE["rfxErr"] = None
                        await broadcast({"type": "rfx", "rows": rows,
                                        "ts": STATE["rfxTs"]})
                except asyncio.TimeoutError:
                    cooldown_until = asyncio.get_event_loop().time() + 60
                    try:
                        if _RD is not None:
                            _RD.close_session()
                    except Exception:
                        pass
                    _RD = None      # reopen fresh on the next attempt
                    err = ("Refinitiv timed out - session discarded, will "
                           "reopen fresh. Is Workspace running and logged "
                           "in on the server machine? (retrying every 60s; "
                           "click the stock band's \u21bb to retry now)")
                    if STATE["rfxErr"] != err:
                        STATE["rfxErr"] = err
                        logger.error(err)
                        await broadcast({"type": "rfxErr", "error": err})
                except ImportError:
                    err = "refinitiv-data not installed on the server"
                    if STATE["rfxErr"] != err:
                        STATE["rfxErr"] = err
                        logger.error(err)
                        await broadcast({"type": "rfxErr", "error": err})
                except Exception as exc:
                    err = f"Refinitiv fetch failed: {exc}"
                    if "502" in str(exc) or "Bad Gateway" in str(exc):
                        STATE["_rfxEmpty"] = STATE.get("_rfxEmpty", 0) + 1
                        hold = min(300, 60 * (2 ** min(max(STATE["_rfxEmpty"] - 1, 0), 3)))
                        cooldown_until = asyncio.get_event_loop().time() + hold
                        ban = ("Workspace 502 (Bad Gateway) - the local API proxy cannot "
                               "reach LSEG. Restart Workspace: tray icon > Exit, check Task "
                               "Manager, relaunch, sign in, confirm a quote loads, then click "
                               "\u21bb. Retrying in %ds." % hold)
                        if STATE.get("rfxBanner") != ban:
                            STATE["rfxBanner"] = ban
                            logger.warning(ban)
                            await broadcast({"type": "rfxBanner", "text": ban})
                    if STATE["rfxErr"] != err:
                        STATE["rfxErr"] = err
                        logger.error(err)
                        await broadcast({"type": "rfxErr", "error": err})
        except Exception as exc:
            logger.error("poller loop error: %s", exc)
        try:
            await asyncio.wait_for(RFX_WAKE.wait(),
                                   timeout=max(2, STATE["refreshSec"]))
            cooldown_until = 0.0           # manual wake overrides backoff
        except asyncio.TimeoutError:
            pass
        RFX_WAKE.clear()


# ------------------------------------------------------------------
# API models
# ------------------------------------------------------------------
class OverrideEntry(BaseModel):
    secId: int
    ovdSpot: float = 0.0
    ovdCbFx: float = 0.0
    ovdUndFx: float = 0.0


class NukeRequest(BaseModel):
    entries: List[OverrideEntry]


class RefRequest(BaseModel):
    sec_ids: List[int]


class RicRequest(BaseModel):
    rics: List[str]


class SaveRequest(BaseModel):
    rows: List[Dict[str, Any]]
    user: str = "anon"


AUTOSAVE_WAKE: Optional[asyncio.Event] = None


def server_rows_for_save() -> List[Dict[str, Any]]:
    """Autosave snapshot from server-held state: user fields, overrides,
    spreads, and the last-nuke block. Client-only columns (company/ric/
    live/eod/stk/fx and theo) stay NULL - the manual Save to DB button
    remains the full-fidelity snapshot."""
    out = []
    for sid in STATE["ids"]:
        row = STATE["rows"].get(sid, {})
        nk = STATE["nuke"].get(sid, {}) or {}
        out.append({
            "sec_id": sid,
            "short_name": row.get("short_name"),
            "und_fx": row.get("und_fx"),
            "ovd_spot": row.get("ovdSpot"),
            "ovd_cbfx": row.get("ovdCbFx"),
            "ovd_undfx": row.get("ovdUndFx"),
            "n_bid": nk.get("nBid"), "n_delta": nk.get("nDelta"),
            "n_spread": nk.get("nSpread"), "n_spot": nk.get("nSpot"),
            "n_spotfx": nk.get("nSpotFx"),
            "ovd_bid": nk.get("ovdMktBid"), "ovd_ask": nk.get("ovdMktAsk"),
            "x_bid": row.get("x_bid"), "x_ask": row.get("x_ask"),
            "x_both": row.get("x_both"), "vol_flag": row.get("vol_flag"),
            "bond_type": row.get("bond_type"),
        })
    return out


def _fnum0(v):
    try:
        n = float(str(v).replace(",", ""))
        return n
    except (TypeError, ValueError):
        return None


def ensure_snap8():
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {APP_DB}.quote_snap8 (
              snap_date DATE NOT NULL, sec_id BIGINT NOT NULL,
              short_name VARCHAR(64) NULL,
              quote_bid DECIMAL(18,6) NULL, quote_ask DECIMAL(18,6) NULL,
              mid DECIMAL(18,6) NULL, snapped_at DATETIME NULL,
              PRIMARY KEY (snap_date, sec_id))""")
            for ddl in (
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS isin VARCHAR(12) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS ovd_bid DECIMAL(18,6) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS ovd_ask DECIMAL(18,6) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS indic_ask DECIMAL(18,6) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS vs_ref DECIMAL(18,6) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS fx_ref DECIMAL(18,6) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS vs_usd DECIMAL(18,6) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS delta_pct DECIMAL(10,2) NULL",
                f"ALTER TABLE {APP_DB}.quote_snap8 ADD COLUMN IF NOT "
                "EXISTS qty DECIMAL(20,2) NULL"):
                cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def compute_snap8() -> int:
    """Store today's QuoteBid/QuoteAsk (= override result +/- X adj) per
    security; replace-by-date so a rerun overwrites the day cleanly."""
    ensure_snap8()
    today = date.today()
    try:
        rd_rows, _ = build_refdata(list(STATE["ids"]))
        ref = {r["secId"]: r for r in rd_rows}
    except Exception:
        ref = {}
    recs = []
    for sid in STATE["ids"]:
        nk = STATE["nuke"].get(sid, {}) or {}
        row = STATE["rows"].get(sid, {}) or {}
        rf = ref.get(sid, {}) or {}
        b = _fnum0(nk.get("ovdMktBid"))
        a = _fnum0(nk.get("ovdMktAsk"))
        if b is None and a is None:
            continue
        x2 = _fnum0(row.get("x_both")) or 0.0
        qb = None if b is None else b + (_fnum0(row.get("x_bid")) or 0.0) + x2
        qa = None if a is None else a + (_fnum0(row.get("x_ask")) or 0.0) + x2
        mid = None
        if qb is not None and qa is not None:
            mid = (qb + qa) / 2.0
        nsprd = _fnum0(nk.get("nSpread"))
        indic = qa if qa is not None else (
            None if qb is None else qb + (nsprd if nsprd is not None
                                          else 1.0))
        vs = _fnum0(row.get("ovdSpot"))
        fx = _fnum0(row.get("ovdUndFx"))
        vs_usd = (vs / fx) if (vs is not None and fx) else None
        nd = _fnum0(nk.get("nDelta"))
        recs.append((today, sid, (row.get("short_name") or "")[:64],
                     qb, qa, mid,
                     ((rf.get("isin") or row.get("isin_manual") or "")[:12]) or None,
                     qb, qa, indic, vs, fx, vs_usd,
                     None if nd is None else round(nd * 100.0, 1),
                     _fnum0(rf.get("quantity_live"))))
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {APP_DB}.quote_snap8 "
                        "WHERE snap_date=%s", (today,))
            if recs:
                cur.executemany(
                    f"INSERT INTO {APP_DB}.quote_snap8 (snap_date, sec_id, "
                    "short_name, quote_bid, quote_ask, mid, isin, ovd_bid, "
                    "ovd_ask, indic_ask, vs_ref, fx_ref, vs_usd, delta_pct, "
                    "qty, snapped_at) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                    recs)
        conn.commit()
    finally:
        conn.close()
    logger.info("8am snapshot: %d securities stored for %s",
                len(recs), today.isoformat())
    return len(recs)


def fetch_snap8_mids(sec_ids: List[int]) -> Dict[int, float]:
    if not sec_ids:
        return {}
    try:
        ensure_snap8()
        conn = _db()
        try:
            with conn.cursor() as cur:
                ph = ", ".join(["%s"] * len(sec_ids))
                cur.execute(f"SELECT sec_id, mid FROM {APP_DB}.quote_snap8 "
                            f"WHERE snap_date=%s AND sec_id IN ({ph})",
                            [date.today()] + list(sec_ids))
                return {int(r[0]): float(r[1]) for r in cur.fetchall()
                        if r[1] is not None}
        finally:
            conn.close()
    except Exception:
        return {}


CBFX_INVERT_SRV = ("EUR", "GBP", "AUD", "NZD")


def _cbfx_close_for(sid):
    """cb fx close as USD/CCY from the Refinitiv cache: USD -> 1.0,
    CCY= close (inverted for the majors quoted as CCY/USD); None if absent."""
    ccy = str((STATE.get("cbFxCcy") or {}).get(sid, "") or "").strip().upper()
    if ccy in ("", "USD"):
        return 1.0
    ric = (STATE.get("cbFxRics") or {}).get(sid) or (ccy + "=")
    v = _fnum0((STATE["rfx"].get(ric) or {}).get("close"))
    if v is None or v == 0:
        return None
    return round(1.0 / v, 6) if ccy in CBFX_INVERT_SRV else v


def snap8_close_to_ovd(user="snap8"):
    """Step 1 of the morning snapshot: Close -> ovd for every security
    (exactly what the Close -> ovd button does), persisted to cb_state.
    Rows with no stock close keep their overrides. Returns (set, skipped)."""
    n_set, skipped = 0, []
    for sid in STATE["ids"]:
        row = STATE["rows"].setdefault(sid, _blank_row())
        stk = STATE["stockRics"].get(sid, "")
        sc = _fnum0((STATE["rfx"].get(stk) or {}).get("close")) if stk else None
        if sc is None:
            skipped.append(sid)
            continue
        fxr = str(row.get("und_fx") or "").strip()
        fxc = None
        if fxr:
            try:
                fxc = float(fxr)                      # numeric constant stays
            except ValueError:
                fxc = _fnum0((STATE["rfx"].get(fxr) or {}).get("close"))
        touched = ["ovdSpot"]
        row["ovdSpot"] = str(sc)
        if fxc is not None:
            row["ovdUndFx"] = str(fxc); touched.append("ovdUndFx")
        van = str(row.get("bond_type") or "").lower().startswith("vanil")
        if van:
            if fxc is not None:
                row["ovdCbFx"] = str(fxc); touched.append("ovdCbFx")
        else:
            cb = _cbfx_close_for(sid)
            if cb is not None:
                row["ovdCbFx"] = str(cb); touched.append("ovdCbFx")
        if _van_mirror(row) and "ovdCbFx" not in touched:
            touched.append("ovdCbFx")
        try:
            db_upsert_state_fields(sid, row, user, touched)
        except Exception as exc:
            logger.warning("snap8 close->ovd persist failed for %s: %s", sid, exc)
        n_set += 1
    return n_set, skipped


def snap8_saved_today():
    try:
        ensure_snap8()
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {APP_DB}.quote_snap8 WHERE snap_date=%s",
                            (date.today(),))
                return int((cur.fetchone() or [0])[0]) > 0
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("snap8 saved-today check failed: %s", exc)
        return False


async def snap8_close_run(reason="manual", user="snap8"):
    """The morning snapshot procedure: Close -> ovd, re-nuke all rows at
    those overrides, store QuoteBid/QuoteAsk. A failed re-nuke stores
    nothing (so the 08:55 retry, or the button, can try again)."""
    n_set, skipped = await run_in_threadpool(snap8_close_to_ovd, user)
    STATE["version"] += 1
    await broadcast({"type": "rows", "by": user, "v": STATE["version"],
                     "list": [{"secId": sid, **STATE["rows"][sid]} for sid in STATE["ids"]
                              if sid in STATE["rows"]]})
    entries = []
    for sid in STATE["ids"]:
        row = STATE["rows"].get(sid, _blank_row())
        def num(v):
            try:
                return float(str(v).replace(",", "") or 0)
            except ValueError:
                return 0.0
        entries.append({"secId": sid, "ovdSpot": num(row.get("ovdSpot")),
                        "ovdCbFx": num(row.get("ovdCbFx")), "ovdUndFx": num(row.get("ovdUndFx"))})
    if not entries:
        return {"ok": False, "error": "no securities loaded", "set": n_set}
    try:
        await asyncio.wait_for(_nlock().acquire(), timeout=30)
    except asyncio.TimeoutError:
        return {"ok": False, "error": "pricing engine busy - not snapped", "set": n_set}
    try:
        data = await asyncio.wait_for(run_in_threadpool(run_nuke_batches, entries),
                                      timeout=int(os.environ.get("NUKE_PRICE_TIMEOUT", "45")))
    except Exception as exc:
        logger.error("snap8 re-nuke failed (%s): %s", reason, exc)
        return {"ok": False, "error": "re-nuke failed: %s" % str(exc)[:200], "set": n_set}
    finally:
        if _nlock().locked():
            _nlock().release()
    for r in data.get("rows", []):
        if r.get("secId") is not None:
            STATE["nuke"][int(r["secId"])] = r
    STATE["nukeMeta"] = {"host": data.get("host"), "elapsed": data.get("elapsed"),
                         "by": user + " (" + reason + ")",
                         "ts": datetime.now().strftime("%H:%M:%S"),
                         "missing": data.get("missing", [])}
    STATE["version"] += 1
    data["by"] = user
    await broadcast({"type": "nuke", "data": data, "v": STATE["version"]})
    n = await run_in_threadpool(compute_snap8)
    STATE["snap8Last"] = {"date": date.today().isoformat(), "time": datetime.now().strftime("%H:%M:%S"),
                          "reason": reason, "stored": n, "set": n_set, "skipped": len(skipped)}
    try:
        await run_in_threadpool(db_meta_set, "snap8_last", STATE["snap8Last"])
        await run_in_threadpool(db_log, user, "snap8", STATE["snap8Last"])
    except Exception:
        pass
    if REFDATA_WAKE:
        REFDATA_WAKE.set()
    logger.info("morning snapshot (%s): close->ovd %d rows (%d without close), re-nuked, stored %d",
                reason, n_set, len(skipped), n)
    return {"ok": True, "stored": n, "set": n_set, "skipped": skipped, "reason": reason}


SNAP8_TIMES = ((8, 0), (8, 55))      # 08:55 runs only if 08:00 did not save


async def snap8_poller():
    """Daily morning snapshot: Close -> ovd, re-nuke, store - at 08:00,
    and at 08:55 if today's snapshot is still missing."""
    logger.info("morning snapshot poller started (08:00, retry 08:55)")
    while True:
        now = datetime.now()
        nxt = None
        for h, m in SNAP8_TIMES:
            t = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if t <= now:
                t += timedelta(days=1)
            if nxt is None or t < nxt:
                nxt = t
        await asyncio.sleep((nxt - now).total_seconds())
        try:
            is_retry = (nxt.hour, nxt.minute) == SNAP8_TIMES[1]
            if is_retry and await run_in_threadpool(snap8_saved_today):
                logger.info("08:55 snapshot skipped - 08:00 already saved today")
                continue
            res = await snap8_close_run("08:00" if not is_retry else "08:55")
            if not res.get("ok"):
                logger.warning("morning snapshot %s not stored: %s", res.get("reason", ""), res.get("error"))
        except Exception as exc:
            logger.warning("morning snapshot failed: %s", exc)


async def autosave_poller():
    global AUTOSAVE_WAKE
    AUTOSAVE_WAKE = asyncio.Event()
    logger.info("autosave poller started (every %ss, 0=off)",
                STATE["autosaveSec"])
    while True:
        sec = STATE["autosaveSec"]
        try:
            await asyncio.wait_for(AUTOSAVE_WAKE.wait(),
                                   timeout=(sec if sec > 0 else 60))
        except asyncio.TimeoutError:
            pass
        AUTOSAVE_WAKE.clear()
        if STATE["autosaveSec"] <= 0:
            continue
        try:
            rows = server_rows_for_save()
            if rows:
                n = await run_in_threadpool(save_rows, rows)
                logger.info("autosave: %d rows -> %s.%s",
                            n, APP_DB, APP_TABLE)
        except Exception as exc:
            logger.error("autosave failed: %s", exc)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    await run_in_threadpool(load_persisted_state)
    asyncio.create_task(rfx_poller())
    asyncio.create_task(refdata_poller())
    asyncio.create_task(autosave_poller())
    asyncio.create_task(div_poller())
    asyncio.create_task(snap8_poller())
    asyncio.create_task(vol_poller())
    asyncio.create_task(twq_poller())
    if RFX_WAKE:
        RFX_WAKE.set()   # restored rows carry und_fx: first poll now
    logger.info("CB Nuke Station serving on http://%s:%s  (share with the desk)",
                HOST, PORT)
    yield


app = FastAPI(title="CB Nuke Station", lifespan=_lifespan)


_LP_META = {"checked": False, "cols": set(), "key": None, "ts": None}
_DIV_CACHE = {"ts": 0.0, "map": {}}


def fetch_lp_rows(sec_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """years_to_call / credit_spread_used (+ years_to_put when the table
    has it) from cbanalytics.lp_model_output, newest row per security.
    Schema introspected once - key and time columns differ across builds."""
    if not sec_ids:
        return {}
    conn = _db()
    try:
        with conn.cursor() as cur:
            if not _LP_META["checked"]:
                cur.execute("SHOW COLUMNS FROM cbanalytics.lp_model_output")
                _LP_META["cols"] = {str(r[0]).lower() for r in cur.fetchall()}
                _LP_META["key"] = ("sec_id" if "sec_id" in _LP_META["cols"]
                                   else "security_id"
                                   if "security_id" in _LP_META["cols"]
                                   else None)
                for t in ("snap_ts", "run_ts", "asof_ts", "asof",
                          "created_at", "loaded_at", "id"):
                    if t in _LP_META["cols"]:
                        _LP_META["ts"] = t
                        break
                _LP_META["checked"] = True
            key = _LP_META["key"]
            if not key:
                return {}
            want = ["years_to_call", "credit_spread_used"]
            for opt in ("years_to_put", "delta", "implied_vol",
                        "conversion_price", "conversion_fixed_fx", "vega"):
                if opt in _LP_META["cols"]:
                    want.append(opt)
            sel = ", ".join([key] + want)
            order = f" ORDER BY {_LP_META['ts']} DESC" if _LP_META["ts"] else ""
            ph = ", ".join(["%s"] * len(sec_ids))
            cur.execute(f"SELECT {sel} FROM cbanalytics.lp_model_output "
                        f"WHERE {key} IN ({ph}){order}", sec_ids)
            out: Dict[int, Dict[str, Any]] = {}
            for row in cur.fetchall():
                sid = int(row[0])
                if sid in out:
                    continue
                out[sid] = {w: ("" if v is None else str(v))
                            for w, v in zip(want, row[1:])}
            return out
    finally:
        conn.close()


_DIV_CHILD = r"""
import json, sys, os
try:
    import refinitiv.data as rd
    rd.open_session()
    rics = json.loads(sys.argv[1])
    df = rd.get_data(universe=rics, fields=["TR.ExDividendDate"])
    cols = {str(c).strip().lower(): c for c in df.columns}
    inst = cols.get("instrument") or df.columns[0]
    dcol = next((c for k, c in cols.items() if "dividend" in k), None)
    if dcol is None:
        dcol = next((c for c in df.columns if c != inst), None)
    out = {}
    if dcol is not None:
        for _, r in df.iterrows():
            v = r[dcol]
            sv = "" if v is None else str(v)[:10]
            if sv and sv.lower() not in ("nan", "nat", "<na>", "none"):
                out[str(r[inst]).strip()] = sv
    print("DIVJSON:" + json.dumps(out))
except Exception as exc:
    print("DIVJSON:{}")
    print("child error: %s" % exc, file=sys.stderr)
"""


def fetch_div_dates_now(rics: List[str]) -> Dict[str, str]:
    """Ex-dividend dates in a SEPARATE PROCESS with its own Refinitiv
    session. The parent's realtime session is never touched, and a hang
    is hard-killed by the subprocess timeout - worst case is blank DIV
    flags, never a broken stock/fx feed."""
    import subprocess, sys as _sys
    try:
        p = subprocess.run(
            [_sys.executable, "-c", _DIV_CHILD,
             json.dumps([r for r in rics if r])],
            capture_output=True, text=True, timeout=90,
            cwd=os.getcwd(), env=os.environ.copy())
        for line in (p.stdout or "").splitlines():
            if line.startswith("DIVJSON:"):
                return json.loads(line[len("DIVJSON:"):])
        if p.stderr:
            logger.info("div child stderr: %s", p.stderr.strip()[:300])
    except subprocess.TimeoutExpired:
        logger.info("div child killed after 90s timeout")
    except Exception as exc:
        logger.info("div child failed: %s", exc)
    return {}


_VOL_CACHE = {"day": "", "map": {}}
_VOL_CHILD = r"""
import json, sys, os, time
try:
    import refinitiv.data as rd
    rd.open_session()
    rics = json.loads(sys.argv[1])
    COUNT = int(os.environ.get("NUKE_VOL_COUNT", "280"))
    print("child up: rd=%s py=%s rics=%d count=%d" % (
        getattr(rd, "__version__", "?"), sys.version.split()[0],
        len(rics), COUNT), file=sys.stderr)
    T0 = time.monotonic()
    out = {}
    PREF = ["trdprc_1", "close", "off_close", "off_cl", "last"]

    def px_of(col):
        try:
            vals = [float(v) for v in col.tolist()
                    if v is not None and str(v).lower() not in
                    ("nan", "nat", "<na>", "none")]
            return vals if len(vals) >= 11 else None
        except Exception:
            return None

    def eat_batched(df):
        for c in list(df.columns):
            ric = str(c[0]).strip() if isinstance(c, tuple) else str(c).strip()
            px = px_of(df[c])
            if ric and px:
                out[ric] = px

    def eat_single(df, ric):
        cols = list(df.columns)
        def nm(c):
            return (str(c[-1]) if isinstance(c, tuple) else str(c)).lower()
        ranked = sorted(cols, key=lambda c: PREF.index(nm(c))
                        if nm(c) in PREF else 99)
        for c in ranked:
            if nm(c) not in PREF and len(cols) > 1:
                continue          # never fall through to volume-like cols
            px = px_of(df[c])
            if px:
                out[ric] = px
                return

    for cnt in (COUNT, 130):
        try:
            df = rd.get_history(universe=rics, fields=["TRDPRC_1"],
                                interval="daily", count=cnt)
            print("batched count=%d shape=%s" % (cnt,
                  getattr(df, "shape", "?")), file=sys.stderr)
            eat_batched(df)
            if out:
                break
        except Exception as exc:
            print("batched get_history count=%d failed: %s" % (cnt, exc),
                  file=sys.stderr)
    for ric in [r for r in rics if r not in out]:
        if time.monotonic() - T0 > 150:
            print("time budget reached, %d rics left" %
                  len([r for r in rics if r not in out]), file=sys.stderr)
            break
        for cnt in (COUNT, 130):
            try:                   # no field filter: some venues expose
                eat_single(rd.get_history(universe=ric, interval="daily",
                                          count=cnt), ric)  # CLOSE instead
                if ric in out:
                    break
            except Exception as exc:
                print("single %s count=%d failed: %s" % (ric, cnt, exc),
                      file=sys.stderr)
    print("VOLJSON:" + json.dumps(out))
except Exception as exc:
    print("VOLJSON:{}")
    print("vol child error: %s" % exc, file=sys.stderr)
"""


VOL_WAKE = None


def fetch_vol_history_now(rics: List[str]) -> Dict[str, list]:
    """Daily close history in a SEPARATE PROCESS with its own session -
    identical isolation to the div fetch; a hang is hard-killed and the
    realtime feed can never be touched."""
    import subprocess, sys as _sys
    try:
        if (__import__("time").time()
                - globals().get("_SESS_OPEN_TS", 0)) < 90:
            logger.info("vol child deferred: main session "
                        "reopened <90s ago - retrying next hour")
            return {}
        p = subprocess.run(
            [_sys.executable, "-c", _VOL_CHILD,
             json.dumps([r for r in rics if r])],
            capture_output=True, text=True, timeout=210,
            cwd=os.getcwd(), env=os.environ.copy())
        out = None
        for line in (p.stdout or "").splitlines():
            if line.startswith("VOLJSON:"):
                out = json.loads(line[len("VOLJSON:"):])
                break
        if p.stderr and (out is None or len(out) < len(rics)):
            for ln in p.stderr.strip().splitlines()[-12:]:
                logger.info("vol child | %s", ln.strip()[:220])
        if out is not None:
            logger.info("vol child: %d/%d rics with history",
                        len(out), len(rics))
            return out
    except subprocess.TimeoutExpired:
        logger.info("vol child killed after 120s timeout")
    except Exception as exc:
        logger.info("vol child failed: %s", exc)
    return {}


async def vol_poller():
    """Daily-close history for realised vol. Opt-out via NUKE_VOL_FETCH=0.
    One fetch per day per ric set; vols themselves are computed client-side
    from this cached series, so the page never waits on Refinitiv."""
    if os.environ.get("NUKE_VOL_FETCH", "1") != "1":
        logger.info("vol poller disabled (NUKE_VOL_FETCH=0)")
        return
    delay = int(os.environ.get("NUKE_VOL_DELAY", "120"))
    logger.info("vol poller started (first fetch in %ss, then hourly "
                "check, once per day)", delay)
    global VOL_WAKE
    VOL_WAKE = asyncio.Event()
    await asyncio.sleep(delay)
    while True:
        VOL_WAKE.clear()
        try:
            rics = sorted({r for r in STATE["stockRics"].values() if r})
            today = date.today().isoformat()
            if rics and (_VOL_CACHE["day"] != today or
                         any(r not in _VOL_CACHE["map"] for r in rics)):
                out = await run_in_threadpool(fetch_vol_history_now, rics)
                if not out:
                    healthy = (STATE.get("_rfxEmpty", 0) == 0 and not STATE.get("rfxErr")
                               and not STATE.get("rfxBanner"))
                    if healthy:
                        logger.warning("vol child returned no data for %d rics "
                                       "- Workspace get_history entitlement? "
                                       "retrying next hour", len(rics))
                    else:
                        logger.info("vol child: no data while pricing is down - "
                                    "will retry as soon as Refinitiv recovers")
                if out:
                    _VOL_CACHE["map"].update(out)
                    _VOL_CACHE["day"] = today
                    logger.info("vol history refreshed for %d rics",
                                len(out))
                    if REFDATA_WAKE:
                        REFDATA_WAKE.set()
        except Exception as exc:
            logger.info("vol poll skipped: %s", exc)
        try:                         # hourly, or immediately when RFX recovers
            await asyncio.wait_for(VOL_WAKE.wait(), timeout=3600)
        except asyncio.TimeoutError:
            pass


async def div_poller():
    """Opt-in (NUKE_DIV_FETCH=1). Runs every 30 min with a hard timeout,
    updates the cache, then nudges refdata so DIV flags repaint."""
    if os.environ.get("NUKE_DIV_FETCH", "0") != "1":
        logger.info("div poller disabled (NUKE_DIV_FETCH!=1) - DIV flags "
                    "stay blank")
        return
    await asyncio.sleep(
        int(os.environ.get("NUKE_DIV_DELAY", "90")))   # realtime first
    while True:
        try:
            rics = sorted({r for r in STATE["stockRics"].values() if r})
            if rics:
                out = await run_in_threadpool(fetch_div_dates_now, rics)
                _DIV_CACHE["map"] = out
                _DIV_CACHE["ts"] = asyncio.get_event_loop().time()
                logger.info("div dates refreshed for %d rics", len(out))
                if REFDATA_WAKE:
                    REFDATA_WAKE.set()
        except Exception as exc:
            logger.info("div poll skipped: %s", exc)
        await asyncio.sleep(1800)



def build_refdata(sec_ids: List[int]):
    """Compose one refdata row per security from cbanalytics + eqrms + prefs."""
    sec_ids = sorted(set(sec_ids))
    errors = []
    try:
        rows = fetch_ref_rows(sec_ids)
    except Exception as exc:
        logger.error("cbanalytics lookup failed: %s", exc)
        rows, errors = [], [f"cbanalytics lookup failed: {exc}"]
    try:
        rics = fetch_ric_map(sec_ids)
    except Exception as exc:
        logger.error("eqrms RIC lookup failed: %s", exc)
        rics = {}
        errors.append(f"RIC lookup failed: {exc}")
    prefs = fetch_saved_prefs(sec_ids)
    try:
        lp = fetch_lp_rows(sec_ids)
    except Exception as exc:
        lp = {}
        logger.info("lp_model_output lookup skipped: %s", exc)
    _snap_mids = fetch_snap8_mids(sec_ids)

    by_id = {r["secId"]: r for r in rows}
    for sid in sec_ids:
        rec = by_id.setdefault(sid, {"secId": sid, "company_name": "",
                                     "expiry_date": "", "isin": ""})
        extra = rics.get(sid, {})
        pref = prefs.get(sid, {})
        rec["ric"] = extra.get("ric", "")
        rec["sec_fx"] = extra.get("ccy", "")
        rec["quantity_live"] = extra.get("qty", "")
        rec["usd_qty_live"] = extra.get("usd", "")
        rec["short_name"] = pref.get("short_name", "")
        rec["und_fx"] = pref.get("und_fx") or default_fx_ric(extra.get("ccy", ""))
        lrow = lp.get(sid, {})
        rec["snap8_mid"] = _snap_mids.get(sid, "")
        rec["lp_delta"] = lrow.get("delta", "")
        rec["implied_vol"] = lrow.get("implied_vol", "")
        rec["lp_conversion_price"] = lrow.get("conversion_price", "")
        rec["conversion_fixed_fx"] = lrow.get("conversion_fixed_fx", "")
        rec["vega"] = lrow.get("vega", "")
        rec["px_hist"] = _VOL_CACHE["map"].get(rec.get("ric", ""), [])
        rec["years_to_call"] = lrow.get("years_to_call", "")
        rec["credit_spread_used"] = lrow.get("credit_spread_used", "")
        rec["years_to_put"] = lrow.get("years_to_put", "")
    divs = _DIV_CACHE["map"]           # background-filled; empty when off
    for r in by_id.values():
        r["next_div_date"] = divs.get(r.get("ric", ""), "")
    return list(by_id.values()), ("; ".join(errors) if errors else None)


@app.post("/api/refdata")
def api_refdata(req: RefRequest):
    rows, error = build_refdata(req.sec_ids)
    return {"rows": rows, "error": error}


async def refdata_poller():
    """Re-fetch eqrms/cbanalytics reference data periodically and broadcast,
    so qty/usd/ric/sec_fx track fresh snaps without anyone reloading."""
    global REFDATA_WAKE
    REFDATA_WAKE = asyncio.Event()
    logger.info("refdata poller started (every %ss)", STATE["refdataSec"])
    while True:
        try:
            ids = list(STATE["ids"])
            if ids:
                rows, error = await run_in_threadpool(build_refdata, ids)
                if error:
                    if STATE["refErr"] != error:
                        STATE["refErr"] = error
                        logger.error("refdata refresh: %s", error)
                        await broadcast({"type": "refErr", "error": error})
                else:
                    if STATE["refErr"]:
                        STATE["refErr"] = None
                    prev = {v for v in STATE["stockRics"].values() if v}
                    STATE["refRows"] = {int(r["secId"]): r for r in rows}
                    STATE["stockRics"] = {r["secId"]: r.get("ric", "")
                                          for r in rows}
                    now = {v for v in STATE["stockRics"].values() if v}
                    if now - prev and RFX_WAKE:
                        RFX_WAKE.set()   # new rics: retry realtime now
                    await broadcast({"type": "refdata", "rows": rows,
                                     "ts": datetime.now().strftime("%H:%M:%S")})
                    if RFX_WAKE: RFX_WAKE.set()   # ric set may have changed
        except Exception as exc:
            logger.error("refdata poller loop error: %s", exc)
        try:
            await asyncio.wait_for(REFDATA_WAKE.wait(),
                                   timeout=max(30, STATE["refdataSec"]))
        except asyncio.TimeoutError:
            pass
        REFDATA_WAKE.clear()


@app.get("/api/snap8")
def api_snap8(d: str = ""):
    try:
        ensure_snap8()
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT snap_date FROM "
                            f"{APP_DB}.quote_snap8 ORDER BY snap_date DESC "
                            "LIMIT 15")
                dates = [r[0].isoformat() for r in cur.fetchall()]
                pick = d if d in dates else (dates[0] if dates else "")
                rows = []
                if pick:
                    cur.execute(
                        f"SELECT sec_id, short_name, quote_bid, quote_ask, "
                        f"mid, snapped_at, isin, ovd_bid, ovd_ask, "
                        f"indic_ask, vs_ref, fx_ref, vs_usd, delta_pct, qty "
                        f"FROM {APP_DB}.quote_snap8 "
                        "WHERE snap_date=%s ORDER BY short_name", (pick,))
                    def _f(v):
                        return None if v is None else float(v)
                    rows = [{"sec_id": r[0], "short_name": r[1] or "",
                             "quote_bid": _f(r[2]), "quote_ask": _f(r[3]),
                             "mid": _f(r[4]), "snapped_at": str(r[5] or ""),
                             "isin": r[6] or "",
                             "ovd_bid": _f(r[7]), "ovd_ask": _f(r[8]),
                             "indic_ask": _f(r[9]), "vs_ref": _f(r[10]),
                             "fx_ref": _f(r[11]), "vs_usd": _f(r[12]),
                             "delta_pct": _f(r[13]), "qty": _f(r[14])}
                            for r in cur.fetchall()]
            return {"ok": True, "dates": dates, "date": pick, "rows": rows}
        finally:
            conn.close()
    except Exception as exc:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(exc)})


@app.post("/api/vol/run")
async def api_vol_run():
    try:
        rics = sorted({r for r in STATE["stockRics"].values() if r})
        if not rics:
            return {"ok": False, "error": "no stock rics yet"}
        out = await run_in_threadpool(fetch_vol_history_now, rics)
        if out:
            _VOL_CACHE["map"].update(out)
            _VOL_CACHE["day"] = date.today().isoformat()
            if REFDATA_WAKE:
                REFDATA_WAKE.set()
        return {"ok": True, "rics": len(out)}
    except Exception as exc:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(exc)})


@app.get("/api/logs")
def api_logs(n: int = 300):
    lines = list(LOG_RING)[-max(1, min(n, 800)):]
    return {"ok": True, "lines": lines}


# ---- CB NUKE: TWAP / VWAP of the stock over a window, via Bloomberg ----
RIC_TO_BBG = {   # Refinitiv suffix -> Bloomberg exchange code (Korea: KOSPI .KS -> KP, KOSDAQ .KQ -> KQ)
    "KS": "KP", "KQ": "KQ", "HK": "HK", "T": "JT", "OS": "JO", "TW": "TT", "TWO": "TT",
    "SS": "CG", "SZ": "CS", "N": "UN", "O": "UW", "OQ": "UW", "A": "UA", "P": "UP",
    "L": "LN", "SI": "SP", "AX": "AT", "KL": "MK", "BK": "TB", "JK": "IJ", "NS": "IS",
    "BO": "IB", "PA": "FP", "DE": "GY", "F": "GF", "MI": "IM", "AS": "NA", "SW": "SW",
    "S": "SE", "ST": "SS", "HE": "FH", "CO": "DC", "OL": "NO", "TO": "CN", "V": "CV",
    "MC": "SM", "BR": "BB", "VI": "AV", "WA": "PW", "IS": "TI", "SA": "BZ", "MX": "MM",
    "J": "SJ", "NZ": "NZ", "PS": "PM", "TA": "IT", "QA": "QD", "AD": "UH", "DU": "DB",
    "SR": "AB", "LS": "PL", "AT": "GA", "IR": "ID", "PR": "CP", "BD": "HB",
}
RIC_TO_BBG.update({k.strip().upper(): v.strip().upper() for k, v in
                   (p.split("=") for p in os.environ.get("RIC_BBG_EXTRA", "").split(",") if "=" in p)})


def bbg_ticker(ric):
    """'329180.KS' -> '329180 KP Equity'; '6886.HK' -> '6886 HK Equity'.
    Unknown suffix -> None (shown as 'no bbg code for .XX')."""
    s = str(ric or "").strip().upper()
    if "." not in s:
        return None
    sym, suf = s.rsplit(".", 1)
    code = RIC_TO_BBG.get(suf)
    if not code or not sym:
        return None
    sym = sym.lstrip("0") if code in ("HK",) else sym           # 0700.HK -> 700 HK Equity
    return "%s %s Equity" % (sym, code)


def twap_vwap(bars, start, end):
    """bars: list of dicts with time 'HH:MM' (exchange local), close,
    volume, optional value (turnover). Window [start, end).
    VWAP = sum(value)/sum(volume) (fallback sum(close*volume)/sum(volume));
    TWAP = mean(close). Returns dict (None values when not computable)."""
    def _hm(t):
        h, m = str(t).strip().split(":")[:2]
        return int(h) * 60 + int(m)
    try:
        s, e = _hm(start), _hm(end)
    except Exception:
        return {"twap": None, "vwap": None, "volume": None, "bars": 0, "status": "bad time"}
    if e <= s:
        return {"twap": None, "vwap": None, "volume": None, "bars": 0, "status": "end must be after start"}
    closes, vol, val, fallback = [], 0.0, 0.0, 0.0
    for b in bars or []:
        try:
            t = _hm(b.get("time"))
        except Exception:
            continue
        if t < s or t >= e:
            continue
        c = _fnum0(b.get("close")); v = _fnum0(b.get("volume")); x = _fnum0(b.get("value"))
        if c is None or c != c:                   # None / NaN close
            continue
        closes.append(c)
        if v is not None and v == v and v > 0:
            vol += v
            fallback += c * v
            if x is not None and x == x and x > 0:
                val += x
    if not closes:
        return {"twap": None, "vwap": None, "volume": 0.0, "bars": 0, "status": "no bars in window"}
    twap = sum(closes) / len(closes)
    vwap = (val / vol) if (vol > 0 and val > 0) else ((fallback / vol) if vol > 0 else None)
    return {"twap": round(twap, 4), "vwap": (None if vwap is None else round(vwap, 4)),
            "volume": vol, "bars": len(closes), "status": "ok" if vwap is not None else "no volume (twap only)"}


_BBG_CACHE: Dict[str, Any] = {}


def bbg_bars(ticker, day):
    """1-minute TRADE bars for the exchange day, exchange-local 'HH:MM'.
    xbbg first, blpapi second; raises with a readable message otherwise."""
    try:
        from xbbg import blp
        df = blp.bdib(ticker=ticker, dt=day, typ="TRADE")
        if df is None or len(df) == 0:
            return []
        cols = {str(c[-1] if isinstance(c, tuple) else c).lower(): c for c in df.columns}
        out = []
        for ts, row in df.iterrows():
            out.append({"time": ts.strftime("%H:%M"),
                        "close": row[cols["close"]] if "close" in cols else None,
                        "volume": row[cols["volume"]] if "volume" in cols else None,
                        "value": row[cols["value"]] if "value" in cols else None})
        return out
    except ImportError:
        pass
    try:
        import blpapi
    except ImportError:
        raise RuntimeError("Bloomberg API not available on this PC (install xbbg or blpapi)")
    sess = blpapi.Session()
    if not sess.start() or not sess.openService("//blp/refdata"):
        raise RuntimeError("Bloomberg session could not start - is the Terminal running?")
    try:
        svc = sess.getService("//blp/refdata")
        req = svc.createRequest("IntradayBarRequest")
        req.set("security", ticker); req.set("eventType", "TRADE"); req.set("interval", 1)
        d = datetime.strptime(day, "%Y-%m-%d")
        req.set("startDateTime", blpapi.datetime.Datetime(d.year, d.month, d.day, 0, 0, 0))
        req.set("endDateTime", blpapi.datetime.Datetime(d.year, d.month, d.day, 23, 59, 59))
        sess.sendRequest(req)
        out = []
        while True:
            ev = sess.nextEvent(5000)
            for msg in ev:
                if msg.hasElement("barData"):
                    for bar in msg.getElement("barData").getElement("barTickData").values():
                        ts = bar.getElementAsDatetime("time")        # UTC
                        out.append({"time": "%02d:%02d" % (ts.hour, ts.minute), "close": bar.getElementAsFloat("close"),
                                    "volume": bar.getElementAsFloat("volume"), "value": bar.getElementAsFloat("value"),
                                    "utc": True})
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return out
    finally:
        sess.stop()


def parse_time_smart(t):
    """Forgiving time entry -> (h, m, s). Accepts 9, 09, 930, 0930, 9:3,
    9.30, 9h30, 9am, 930a, 2pm, 14, 14:30:15, now."""
    import re as _re
    s = str(t or "").strip().lower().replace(" ", "")
    if not s:
        raise ValueError("time required")
    if s == "now":
        n = datetime.now(); return n.hour, n.minute, n.second
    ap = None
    m = _re.match(r"^(.*?)(am|pm|a|p)$", s)
    if m and m.group(1):
        s, ap = m.group(1), m.group(2)[0]
    s = s.replace(".", ":").replace("h", ":")
    if ":" in s:
        p = [x for x in s.split(":") if x != ""]
        if not p or not all(x.isdigit() for x in p):
            raise ValueError("time must be HH:MM")
        h = int(p[0]); mi = int(p[1]) if len(p) > 1 else 0; sec = int(p[2]) if len(p) > 2 else 0
    elif s.isdigit():
        if len(s) <= 2:
            h, mi, sec = int(s), 0, 0
        elif len(s) == 3:
            h, mi, sec = int(s[0]), int(s[1:]), 0
        elif len(s) == 4:
            h, mi, sec = int(s[:2]), int(s[2:]), 0
        elif len(s) == 6:
            h, mi, sec = int(s[:2]), int(s[2:4]), int(s[4:])
        else:
            raise ValueError("time must be HH:MM")
    else:
        raise ValueError("time must be HH:MM")
    if ap == "p" and h < 12:
        h += 12
    if ap == "a" and h == 12:
        h = 0
    if not (0 <= h < 24 and 0 <= mi < 60 and 0 <= sec < 60):
        raise ValueError("time out of range")
    return h, mi, sec


def _hms(t):
    """Any accepted time entry -> 'HH:MM:SS' for the Bloomberg overrides."""
    h, mi, s = parse_time_smart(t)
    return "%02d:%02d:%02d" % (h, mi, s)


# Bloomberg TWAP: field + override names are configurable because the interval
# override name should be confirmed on the desk (GET /api/twap/fields?q=TWAP).
BBG_TWAP_FIELD = os.environ.get("BBG_TWAP_FIELD", "TWAP")
BBG_TWAP_OV = {"start": os.environ.get("BBG_TWAP_OV_START", "TWAP_START_TIME"),
               "end": os.environ.get("BBG_TWAP_OV_END", "TWAP_END_TIME"),
               "date": os.environ.get("BBG_TWAP_OV_DATE", "TWAP_DT"),
               "interval": os.environ.get("BBG_TWAP_OV_INTERVAL", "TWAP_INTERVAL")}
BBG_TWAP_INTERVAL = os.environ.get("BBG_TWAP_INTERVAL", "1S")     # 1-second benchmark by default


BBG_TZ_OFFSET = {  # exchange code -> UTC offset hours (fallback when zoneinfo/tzdata is unavailable)
    "KP": 9, "KQ": 9, "JT": 9, "JO": 9, "HK": 8, "TT": 8, "CG": 8, "CS": 8, "SP": 8, "MK": 8, "PM": 8,
    "TB": 7, "IJ": 7, "IS": 5.5, "IB": 5.5, "AT": 10, "NZ": 12, "UN": -4, "UW": -4, "UA": -4, "UP": -4,
    "CN": -4, "CV": -4, "LN": 1, "FP": 2, "GY": 2, "IM": 2, "NA": 2, "SW": 2, "SS": 2, "SM": 2, "BB": 2}
BBG_TZ_NAME = {"KP": "Asia/Seoul", "KQ": "Asia/Seoul", "JT": "Asia/Tokyo", "HK": "Asia/Hong_Kong", "TT": "Asia/Taipei",
               "CG": "Asia/Shanghai", "CS": "Asia/Shanghai", "SP": "Asia/Singapore", "MK": "Asia/Kuala_Lumpur",
               "TB": "Asia/Bangkok", "IJ": "Asia/Jakarta", "IS": "Asia/Kolkata", "IB": "Asia/Kolkata", "AT": "Australia/Sydney",
               "NZ": "Pacific/Auckland", "UN": "America/New_York", "UW": "America/New_York", "UA": "America/New_York",
               "CN": "America/Toronto", "LN": "Europe/London", "FP": "Europe/Paris", "GY": "Europe/Berlin", "IM": "Europe/Rome",
               "NA": "Europe/Amsterdam", "SW": "Europe/Zurich", "SS": "Europe/Stockholm", "SM": "Europe/Madrid"}


def _bbg_code(ticker):
    p = str(ticker or "").split()
    return p[1].upper() if len(p) >= 2 else ""


def _utc_to_local(dt_utc, code):
    """datetime (naive UTC) -> exchange-local datetime; zoneinfo when available, fixed offset otherwise."""
    name = BBG_TZ_NAME.get(code)
    if name:
        try:
            from zoneinfo import ZoneInfo
            from datetime import timezone
            return dt_utc.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(name)).replace(tzinfo=None)
        except Exception:
            pass
    return dt_utc + timedelta(hours=BBG_TZ_OFFSET.get(code, 0))


def _snap_edges(ticks, start_hms, end_hms):
    """ticks: iterable of exchange-local 'HH:MM:SS' trade times (any order).
    Returns (first trade >= start, last trade <= end) or (None, None)."""
    def sec(t):
        h, m, s = [int(x) for x in str(t).split(":")[:3]]
        return h * 3600 + m * 60 + s
    s0, e0 = sec(start_hms), sec(end_hms)
    inside = sorted(sec(t) for t in ticks if s0 <= sec(t) <= e0)
    if not inside:
        return None, None
    f = lambda v: "%02d:%02d:%02d" % (v // 3600, (v % 3600) // 60, v % 60)
    return f(inside[0]), f(inside[-1])


def bbg_ticks(ticker, day, start_hms, end_hms):
    """TRADE ticks in [start, end] as [(exchange-local 'HH:MM:SS', price), ...], sorted.
    xbbg bdtick first, raw blpapi IntradayTickRequest second."""
    code = _bbg_code(ticker)
    out = []
    try:
        from xbbg import blp
        df = blp.bdtick(ticker, dt=day, time_range=(start_hms, end_hms), types=["TRADE"])
        if df is not None and len(df):
            cols = {str(c[-1] if isinstance(c, tuple) else c).lower(): c for c in df.columns}
            pc = cols.get("value") or cols.get("price") or cols.get("px") or list(df.columns)[0]
            for ts, row in df.iterrows():
                v = _fnum0(row[pc])
                if v is not None and v == v:
                    out.append((ts.strftime("%H:%M:%S"), v))
        return sorted(out)
    except ImportError:
        pass
    import blpapi
    sess = blpapi.Session()
    if not sess.start() or not sess.openService("//blp/refdata"):
        raise RuntimeError("Bloomberg session could not start")
    try:
        svc = sess.getService("//blp/refdata")
        d = datetime.strptime(day, "%Y-%m-%d")
        off = timedelta(hours=BBG_TZ_OFFSET.get(code, 0))
        a = datetime.combine(d.date(), datetime.strptime(start_hms, "%H:%M:%S").time()) - off
        b = datetime.combine(d.date(), datetime.strptime(end_hms, "%H:%M:%S").time()) - off
        req = svc.createRequest("IntradayTickRequest")
        req.set("security", ticker); req.getElement("eventTypes").appendValue("TRADE")
        req.set("startDateTime", blpapi.datetime.Datetime(a.year, a.month, a.day, a.hour, a.minute, a.second))
        req.set("endDateTime", blpapi.datetime.Datetime(b.year, b.month, b.day, b.hour, b.minute, b.second))
        sess.sendRequest(req)
        while True:
            ev = sess.nextEvent(5000)
            for msg in ev:
                if msg.hasElement("tickData"):
                    for tk in msg.getElement("tickData").getElement("tickData").values():
                        t = tk.getElementAsDatetime("time")
                        lt = _utc_to_local(datetime(t.year, t.month, t.day, t.hour, t.minute, t.second), code)
                        out.append((lt.strftime("%H:%M:%S"), tk.getElementAsFloat("value")))
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return sorted(out)
    finally:
        sess.stop()


def twap_1s(ticks, start_hms, end_hms):
    """Bloomberg's 1-second benchmark TWAP: sample the last trade price at
    every second of [start, end] (from the first trade onward) and average."""
    def sec(t):
        h, m, s = [int(x) for x in str(t).split(":")[:3]]
        return h * 3600 + m * 60 + s
    s0, e0 = sec(start_hms), sec(end_hms)
    pts = sorted((sec(t), p) for t, p in (ticks or []) if p is not None and p == p and s0 <= sec(t) <= e0)
    if not pts:
        return None, 0
    total, n, i, last = 0.0, 0, 0, None
    for t in range(s0, e0 + 1):
        while i < len(pts) and pts[i][0] <= t:
            last = pts[i][1]; i += 1
        if last is None:
            continue
        total += last; n += 1
    return (round(total / n, 4) if n else None), n


def bbg_window_edges(ticker, day, start_hms, end_hms):
    """What the Bloomberg VWAP/TWAP screen does with a typed window: move the
    start to the first trade at/after it and the end to the last trade at/before
    it. Uses TRADE ticks around each edge (xbbg bdtick, then raw blpapi)."""
    code = _bbg_code(ticker)
    d = datetime.strptime(day, "%Y-%m-%d")
    s_dt = datetime.combine(d.date(), datetime.strptime(start_hms, "%H:%M:%S").time())
    e_dt = datetime.combine(d.date(), datetime.strptime(end_hms, "%H:%M:%S").time())
    pad = timedelta(minutes=10)
    ticks = []
    try:
        from xbbg import blp
        for a, b in ((s_dt, min(s_dt + pad, e_dt)), (max(e_dt - pad, s_dt), e_dt)):
            df = blp.bdtick(ticker, dt=day, time_range=(a.strftime("%H:%M:%S"), b.strftime("%H:%M:%S")), types=["TRADE"])
            if df is not None and len(df):
                ticks += [ts.strftime("%H:%M:%S") for ts in df.index]
        return _snap_edges(ticks, start_hms, end_hms)
    except ImportError:
        pass
    import blpapi
    sess = blpapi.Session()
    if not sess.start() or not sess.openService("//blp/refdata"):
        raise RuntimeError("Bloomberg session could not start")
    try:
        svc = sess.getService("//blp/refdata")
        off = timedelta(hours=BBG_TZ_OFFSET.get(code, 0))
        for a, b in ((s_dt, min(s_dt + pad, e_dt)), (max(e_dt - pad, s_dt), e_dt)):
            req = svc.createRequest("IntradayTickRequest")
            req.set("security", ticker); req.getElement("eventTypes").appendValue("TRADE")
            ua, ub = a - off, b - off                                        # local -> UTC for the request
            req.set("startDateTime", blpapi.datetime.Datetime(ua.year, ua.month, ua.day, ua.hour, ua.minute, ua.second))
            req.set("endDateTime", blpapi.datetime.Datetime(ub.year, ub.month, ub.day, ub.hour, ub.minute, ub.second))
            sess.sendRequest(req)
            while True:
                ev = sess.nextEvent(5000)
                for msg in ev:
                    if msg.hasElement("tickData"):
                        for tk in msg.getElement("tickData").getElement("tickData").values():
                            t = tk.getElementAsDatetime("time")
                            lt = _utc_to_local(datetime(t.year, t.month, t.day, t.hour, t.minute, t.second), code)
                            ticks.append(lt.strftime("%H:%M:%S"))
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
        return _snap_edges(ticks, start_hms, end_hms)
    finally:
        sess.stop()


# ---- 3. TWAP: configured field first, then known candidates; the winner is remembered ----
BBG_TWAP_CANDIDATES = [
    ("TWAP", {"start": "TWAP_START_TIME", "end": "TWAP_END_TIME", "date": "TWAP_DT", "interval": "TWAP_INTERVAL"}),
    ("TWAP", {"start": "VWAP_START_TIME", "end": "VWAP_END_TIME", "date": "VWAP_DT", "interval": ""}),
    ("TIME_WEIGHTED_AVG_PX", {"start": "VWAP_START_TIME", "end": "VWAP_END_TIME", "date": "VWAP_DT", "interval": ""}),
    ("EQY_TWAP", {"start": "VWAP_START_TIME", "end": "VWAP_END_TIME", "date": "VWAP_DT", "interval": ""}),
]
_BBG_TWAP_WINNER = {}


def bbg_twap_field(ticker, day, start, end):
    """Bloomberg-computed TWAP: the configured field/overrides first, then the
    candidate list; the first combination that returns a number is cached.
    Bloomberg's own error text is kept when nothing returns a value."""
    combos = [(BBG_TWAP_FIELD, dict(BBG_TWAP_OV))]
    combos += [c for c in BBG_TWAP_CANDIDATES if (c[0], c[1].get("start")) != (BBG_TWAP_FIELD, BBG_TWAP_OV.get("start"))]
    if _BBG_TWAP_WINNER:
        combos.insert(0, (_BBG_TWAP_WINNER["field"], _BBG_TWAP_WINNER["ov"]))
    last_status, tried = "no value returned", []
    for field, names in combos:
        ov = {names["start"]: _hms(start), names["end"]: _hms(end), names["date"]: day.replace("-", "")}
        if names.get("interval") and BBG_TWAP_INTERVAL:
            ov[names["interval"]] = BBG_TWAP_INTERVAL
        try:
            r = _bbg_ref(ticker, [field], ov)
        except Exception as exc:
            raise
        v = r.get("values", {}).get(field)
        tried.append(field + "/" + names["start"])
        if v is not None and v == v:
            _BBG_TWAP_WINNER.update({"field": field, "ov": names})
            return {"twap": v, "status": "ok", "field": field, "overrides": ov}
        last_status = r.get("status") or "no value returned"
    return {"twap": None, "status": last_status + " (tried " + ", ".join(tried) + " - run /api/twap/fields?q=TWAP to pin the names)",
            "field": None, "overrides": {}}


def _bbg_twap_field_legacy(ticker, day, start, end):
    """Bloomberg-computed TWAP (1-second benchmark by default) via a
    ReferenceDataRequest with start/end/date/interval overrides."""
    ov = {BBG_TWAP_OV["start"]: _hms(start), BBG_TWAP_OV["end"]: _hms(end),
          BBG_TWAP_OV["date"]: day.replace("-", "")}
    if BBG_TWAP_OV.get("interval") and BBG_TWAP_INTERVAL:
        ov[BBG_TWAP_OV["interval"]] = BBG_TWAP_INTERVAL
    r = _bbg_ref(ticker, [BBG_TWAP_FIELD], ov)
    v = r.get("values", {}).get(BBG_TWAP_FIELD)
    return {"twap": v, "status": ("ok" if v is not None else (r.get("status") or "no value returned")),
            "field": BBG_TWAP_FIELD, "overrides": ov}


def _bbg_ref(ticker, flds, ov):
    """One ReferenceDataRequest with overrides -> {"values": {fld: float}, "status": msg}.
    xbbg first, raw blpapi second; field/security errors come back verbatim."""
    try:
        from xbbg import blp
        df = blp.bdp(tickers=ticker, flds=flds, **ov)
        if df is None or len(df) == 0:
            return {"values": {}, "status": "no value returned"}
        row = df.iloc[0]
        cols = {str(c).lower(): c for c in df.columns}
        vals = {f: _fnum0(row[cols[f.lower()]]) for f in flds if f.lower() in cols}
        return {"values": vals, "status": "ok" if vals else "no value returned"}
    except ImportError:
        pass
    try:
        import blpapi
    except ImportError:
        raise RuntimeError("Bloomberg API not available on this PC (install xbbg or blpapi)")
    sess = blpapi.Session()
    if not sess.start() or not sess.openService("//blp/refdata"):
        raise RuntimeError("Bloomberg session could not start - is the Terminal running?")
    try:
        svc = sess.getService("//blp/refdata")
        req = svc.createRequest("ReferenceDataRequest")
        req.getElement("securities").appendValue(ticker)
        for f in flds:
            req.getElement("fields").appendValue(f)
        ovs = req.getElement("overrides")
        for k, val in ov.items():
            o = ovs.appendElement(); o.setElement("fieldId", k); o.setElement("value", val)
        sess.sendRequest(req)
        out = {"values": {}, "status": "no value returned"}
        while True:
            ev = sess.nextEvent(5000)
            for msg in ev:
                if not msg.hasElement("securityData"):
                    continue
                for sd in msg.getElement("securityData").values():
                    if sd.hasElement("securityError"):
                        out["status"] = "security error: " + sd.getElement("securityError").getElementAsString("message")
                        continue
                    fd = sd.getElement("fieldData")
                    for f in flds:
                        if fd.hasElement(f):
                            out["values"][f] = fd.getElementAsFloat(f); out["status"] = "ok"
                    if sd.hasElement("fieldExceptions") and sd.getElement("fieldExceptions").numValues():
                        fe = sd.getElement("fieldExceptions").getValueAsElement(0)
                        out["status"] = "field error: " + fe.getElementAsString("fieldId") + " - " + fe.getElement("errorInfo").getElementAsString("message")
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return out
    finally:
        sess.stop()


@app.get("/api/twap/fields")
def api_twap_fields(q: str = "TWAP"):
    """Discovery: Bloomberg FieldSearchRequest for q -> field ids, mnemonics,
    descriptions and override field ids, so BBG_TWAP_* can be pinned."""
    try:
        import blpapi
    except ImportError:
        return {"ok": False, "error": "blpapi not installed on this PC"}
    try:
        sess = blpapi.Session()
        if not sess.start() or not sess.openService("//blp/apiflds"):
            return {"ok": False, "error": "Bloomberg session could not start"}
        try:
            svc = sess.getService("//blp/apiflds")
            req = svc.createRequest("FieldSearchRequest")
            req.set("searchSpec", q)
            req.getElement("returnFieldDocumentation").setValue(True)
            sess.sendRequest(req)
            rows = []
            while True:
                ev = sess.nextEvent(5000)
                for msg in ev:
                    if not msg.hasElement("fieldData"):
                        continue
                    for fd in msg.getElement("fieldData").values():
                        info = fd.getElement("fieldInfo") if fd.hasElement("fieldInfo") else None
                        if info is None:
                            continue
                        ovs = []
                        if info.hasElement("overrides"):
                            ovs = [str(x) for x in info.getElement("overrides").values()]
                        rows.append({"id": fd.getElementAsString("id"),
                                     "mnemonic": info.getElementAsString("mnemonic"),
                                     "description": info.getElementAsString("description"),
                                     "overrides": ovs})
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
            return {"ok": True, "query": q, "fields": rows[:60],
                    "configured": {"field": BBG_TWAP_FIELD, "overrides": BBG_TWAP_OV, "interval": BBG_TWAP_INTERVAL}}
        finally:
            sess.stop()
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:300]}


def bbg_vwap_field(ticker, day, start, end):
    """Bloomberg-computed window VWAP: EQY_WEIGHTED_AVG_PX with the
    VWAP_START_TIME / VWAP_END_TIME (exchange local) / VWAP_DT overrides,
    plus VWAP_VOLUME. xbbg first, raw blpapi ReferenceDataRequest second."""
    ov = {"VWAP_START_TIME": _hms(start), "VWAP_END_TIME": _hms(end),
          "VWAP_DT": day.replace("-", "")}
    flds = ["EQY_WEIGHTED_AVG_PX", "VWAP_VOLUME"]
    try:
        from xbbg import blp
        df = blp.bdp(tickers=ticker, flds=flds, **ov)
        if df is None or len(df) == 0:
            return {"vwap": None, "volume": None, "status": "no value returned"}
        row = df.iloc[0]
        cols = {str(c).lower(): c for c in df.columns}
        v = _fnum0(row[cols["eqy_weighted_avg_px"]]) if "eqy_weighted_avg_px" in cols else None
        q = _fnum0(row[cols["vwap_volume"]]) if "vwap_volume" in cols else None
        return {"vwap": v, "volume": q, "status": "ok" if v is not None else "no value returned"}
    except ImportError:
        pass
    try:
        import blpapi
    except ImportError:
        raise RuntimeError("Bloomberg API not available on this PC (install xbbg or blpapi)")
    sess = blpapi.Session()
    if not sess.start() or not sess.openService("//blp/refdata"):
        raise RuntimeError("Bloomberg session could not start - is the Terminal running?")
    try:
        svc = sess.getService("//blp/refdata")
        req = svc.createRequest("ReferenceDataRequest")
        req.getElement("securities").appendValue(ticker)
        for f in flds:
            req.getElement("fields").appendValue(f)
        ovs = req.getElement("overrides")
        for k, val in ov.items():
            o = ovs.appendElement(); o.setElement("fieldId", k); o.setElement("value", val)
        sess.sendRequest(req)
        out = {"vwap": None, "volume": None, "status": "no value returned"}
        while True:
            ev = sess.nextEvent(5000)
            for msg in ev:
                if not msg.hasElement("securityData"):
                    continue
                for sd in msg.getElement("securityData").values():
                    if sd.hasElement("securityError"):
                        out["status"] = "security error: " + sd.getElement("securityError").getElementAsString("message")
                        continue
                    fd = sd.getElement("fieldData")
                    if fd.hasElement("EQY_WEIGHTED_AVG_PX"):
                        out["vwap"] = fd.getElementAsFloat("EQY_WEIGHTED_AVG_PX"); out["status"] = "ok"
                    if fd.hasElement("VWAP_VOLUME"):
                        out["volume"] = fd.getElementAsFloat("VWAP_VOLUME")
                    if sd.hasElement("fieldExceptions") and sd.getElement("fieldExceptions").numValues():
                        fe = sd.getElement("fieldExceptions").getValueAsElement(0)
                        out["status"] = "field error: " + fe.getElement("errorInfo").getElementAsString("message")
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return out
    finally:
        sess.stop()


class TwapReq(BaseModel):
    sec_id: str = ""
    ric: str = ""
    start: str = ""
    end: str = ""
    day: str = ""


@app.post("/api/twap")
async def api_twap(req: TwapReq):
    """CB NUKE band: TWAP / VWAP of the stock between start and end (exchange local, [start,end))."""
    tk = bbg_ticker(req.ric)
    if not tk:
        suf = str(req.ric or "").rsplit(".", 1)[-1] if "." in str(req.ric or "") else str(req.ric or "")
        return {"ok": False, "ticker": None, "error": "no bbg code for .%s - add RIC_BBG_EXTRA=%s=XX" % (suf, suf)}
    day = (req.day or date.today().isoformat())[:10]
    try:
        s_, e_ = _hms(req.start), _hms(req.end)
    except ValueError as exc:
        return {"ok": False, "ticker": tk, "error": str(exc)}
    if e_ <= s_:
        return {"ok": False, "ticker": tk, "error": "end must be after start"}
    tmo = float(os.environ.get("BBG_TIMEOUT_S", "25"))
    now = datetime.now().timestamp()
    # 0) snap the typed window to actual trades, as the Bloomberg screen does (09:30 -> 09:30:06, 10:30 -> 10:29:47)
    snap_note = ""
    ekey = "E|%s|%s|%s|%s" % (tk, day, s_, e_)
    eent = _BBG_CACHE.get(ekey)
    ticks = None
    try:
        if eent and now - eent["ts"] < 60:
            ticks = eent["v"]
        else:
            ticks = await asyncio.wait_for(run_in_threadpool(bbg_ticks, tk, day, s_, e_), timeout=tmo)
            _BBG_CACHE[ekey] = {"ts": now, "v": ticks}
        fe, le = _snap_edges([t for t, _p in ticks], s_, e_)
        if fe is None:
            snap_note = "no ticks found in the window - using typed times"
        else:
            if (fe, le) != (s_, e_):
                snap_note = "window snapped to trades %s-%s" % (fe, le)
            s_, e_ = fe, le
    except asyncio.TimeoutError:
        snap_note = "tick request timed out - using typed times"
    except Exception as exc:
        snap_note = "ticks n/a (%s) - using typed times" % str(exc)[:60]
    # 1) Bloomberg-computed VWAP (the number of record)
    key = "V|%s|%s|%s|%s" % (tk, day, s_, e_)
    ent = _BBG_CACHE.get(key)
    try:
        if ent and now - ent["ts"] < 60:
            vw = ent["v"]
        else:
            vw = await asyncio.wait_for(run_in_threadpool(bbg_vwap_field, tk, day, s_, e_), timeout=tmo)
            _BBG_CACHE[key] = {"ts": now, "v": vw}
    except asyncio.TimeoutError:
        return {"ok": False, "ticker": tk, "error": "Bloomberg timed out"}
    except Exception as exc:
        return {"ok": False, "ticker": tk, "error": str(exc)[:200]}
    r = {"ok": vw.get("vwap") is not None, "ticker": tk, "day": day, "vwap": vw.get("vwap"),
         "volume": vw.get("volume"), "status": vw.get("status", ""), "vwap_src": "EQY_WEIGHTED_AVG_PX",
         "twap": None, "bars": 0, "start_eff": s_, "end_eff": e_, "snap_note": snap_note}
    if not r["ok"]:
        r["error"] = vw.get("status") or "no value returned"
    # 2) TWAP: Bloomberg-computed (1-second benchmark by default), cached per window; never blocks the VWAP
    tkey = "T|%s|%s|%s|%s" % (tk, day, s_, e_)
    tent = _BBG_CACHE.get(tkey)
    try:
        if tent and now - tent["ts"] < 60:
            tw = tent["v"]
        else:
            tw = await asyncio.wait_for(run_in_threadpool(bbg_twap_field, tk, day, s_, e_), timeout=tmo)
            _BBG_CACHE[tkey] = {"ts": now, "v": tw}
        r["twap"] = tw.get("twap"); r["twap_src"] = tw.get("field"); r["twap_interval"] = (tw.get("overrides") or {}).get(BBG_TWAP_OV.get("interval", ""), "") or BBG_TWAP_INTERVAL
        if r["twap"] is None and ticks:
            tv, n = twap_1s(ticks, s_, e_)                       # no Bloomberg TWAP field: 1-second benchmark from ticks
            if tv is not None:
                r["twap"], r["twap_src"], r["twap_interval"] = tv, "ticks", "1s (%d samples)" % n
        if r["twap"] is None:
            r["twap_note"] = "twap: " + (tw.get("status") or "no value returned")
    except asyncio.TimeoutError:
        r["twap_note"] = "twap: Bloomberg timed out"
    except Exception as exc:
        r["twap_note"] = "twap n/a: " + str(exc)[:100]
    if len(_BBG_CACHE) > 400:
        for k in sorted(_BBG_CACHE, key=lambda k: _BBG_CACHE[k]["ts"])[:100]:
            _BBG_CACHE.pop(k, None)
    return r


# ---- Bond dates from Bloomberg (static reference data): NXT_CALL_DT / MATURITY / NXT_PUT_DT ----
BOND_DATES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bond_dates_cache.json")
_BD = {"data": {}, "pending": set(), "lock": threading.Lock(), "busy": False, "err": "", "sched_err": ""}
BD_CALL_FIELDS = [f.strip() for f in os.environ.get("BBG_CALL_FIELDS", "NXT_CALL_DT,CV_NXT_CALL_DT,FIRST_CALL_DT").split(",") if f.strip()]
BD_PUT_FIELDS = [f.strip() for f in os.environ.get("BBG_PUT_FIELDS", "NXT_PUT_DT,CV_NXT_PUT_DT").split(",") if f.strip()]
BD_SOFT_FIELDS = [f.strip() for f in os.environ.get("BBG_SOFTCALL_FIELDS", "CV_SOFT_CALL_START_DT,CV_PROV_CALL_START_DT,PROVISIONAL_CALL_DT").split(",") if f.strip()]
BD_TRIG_FIELDS = [f.strip() for f in os.environ.get("BBG_TRIGGER_FIELDS", "SOFT_CALL_TRIGGER_PCT,CV_SOFT_CALL_TRIGGER,PROV_CALL_TRIGGER_PCT,CV_PROV_CALL_TRIGGER").split(",") if f.strip()]
BD_FIELDS = BD_CALL_FIELDS + ["MATURITY"] + BD_PUT_FIELDS + BD_SOFT_FIELDS + ["CALLABLE", "PUTABLE"] + BD_TRIG_FIELDS


def _bd_first_num(row_get, fields):
    for f in fields:
        v = _fnum0(row_get(f))
        if v is not None and v == v:
            return v
    return None


def _bd_sched_from_df(df):
    """xbbg bds frame -> {isin: [dates]} tolerant of flat / multi index and column naming."""
    out = {}
    if df is None or len(df) == 0:
        return out
    dcol = next((c for c in df.columns if "date" in str(c).lower()), None)
    if dcol is None:
        for c in df.columns:
            if str(df[c].dtype).startswith("datetime"):
                dcol = c; break
    if dcol is None:
        return out
    idx = df.index
    secs = [str(i[0] if isinstance(i, tuple) else i) for i in idx]
    for sec, v in zip(secs, df[dcol].tolist()):
        isin = sec.replace("/isin/", "").split()[0].upper()
        d = _bd_norm_date(v)
        if d:
            out.setdefault(isin, []).append(d)
    return out


def _bd_first(row_get, fields):
    """first non-empty date among candidate fields (unknown fields are simply empty)."""
    for f in fields:
        v = _bd_norm_date(row_get(f))
        if v:
            return v
    return ""


def _bd_sched_next(dates):
    """first schedule date on/after today (the next call in a provisional/soft-call schedule)."""
    today = date.today().isoformat()
    ds = sorted(x for x in dates if x)
    nxt = [x for x in ds if x >= today]
    return (ds[0] if ds else ""), (nxt[0] if nxt else "")
BD_BATCH = 50


def _bd_load():
    try:
        with open(BOND_DATES_FILE, "r", encoding="utf-8") as fh:
            d = json.load(fh)
        if isinstance(d, dict):
            _BD["data"] = d
    except Exception:
        pass


def _bd_save():
    try:
        tmp = BOND_DATES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(_BD["data"], fh)
        os.replace(tmp, BOND_DATES_FILE)
    except Exception as exc:
        logger.warning("bond dates cache save failed: %s", exc)


def _bd_norm_date(v):
    """Bloomberg date -> 'YYYY-MM-DD' or ''."""
    if v is None:
        return ""
    try:
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m-%d")
        s = str(v).strip()
        if not s or s.lower() in ("nan", "nat", "none"):
            return ""
        return s[:10]
    except Exception:
        return ""


def bbg_bond_dates(isins):
    """ReferenceDataRequest for a batch of ISINs -> {isin: {call, expiry, put, callable, putable}}.
    xbbg first, raw blpapi second."""
    secs = ["/isin/" + i for i in isins]
    out = {}
    try:
        from xbbg import blp
        starts = {}
        sched = {}
        try:                                   # call schedule: one batched bulk request ...
            sched = _bd_sched_from_df(blp.bds(secs, "CALL_SCHEDULE"))
        except Exception as exc:
            _BD["sched_err"] = "batch: " + str(exc)[:160]
            logger.warning("CALL_SCHEDULE batch failed (%s) - trying per security", exc)
            for s_ in secs:                    # ... else one by one, so a single bad security cannot blank the rest
                try:
                    sched.update(_bd_sched_from_df(blp.bds(s_, "CALL_SCHEDULE")))
                except Exception as exc2:
                    _BD["sched_err"] = str(exc2)[:160]
        if sched:
            _BD["sched_err"] = ""
        for isin, ds in sched.items():
            starts[isin] = _bd_sched_next(ds)
        df = blp.bdp(tickers=secs, flds=BD_FIELDS)
        if df is not None and len(df):
            cols = {str(c).lower(): c for c in df.columns}
            for sec, row in df.iterrows():
                isin = str(sec).replace("/isin/", "").split()[0].upper()
                g = lambda f: row[cols[f.lower()]] if f.lower() in cols else None
                cs, cn = starts.get(isin, ("", ""))
                call = _bd_first(g, BD_CALL_FIELDS) or cn            # no hard-call field -> next date in the (soft) call schedule
                soft = _bd_first(g, BD_SOFT_FIELDS) or cs            # soft-call start: scalar field if the Terminal has one, else the schedule start
                trig = _bd_first_num(g, BD_TRIG_FIELDS)
                out[isin] = {"call": call, "expiry": _bd_norm_date(g("MATURITY")),
                             "put": _bd_first(g, BD_PUT_FIELDS), "call_start": soft or cs, "soft_call": soft,
                             "trigger": trig, "hard_call": _bd_first(g, BD_CALL_FIELDS),
                             "call_src": ("field" if _bd_first(g, BD_CALL_FIELDS) else ("schedule" if cn else "")),
                             "callable": str(g("CALLABLE") or "").upper()[:1], "putable": str(g("PUTABLE") or "").upper()[:1]}
        return out
    except ImportError:
        pass
    import blpapi
    sess = blpapi.Session()
    if not sess.start() or not sess.openService("//blp/refdata"):
        raise RuntimeError("Bloomberg session could not start - is the Terminal running?")
    try:
        svc = sess.getService("//blp/refdata")
        req = svc.createRequest("ReferenceDataRequest")
        for s in secs:
            req.getElement("securities").appendValue(s)
        for f in BD_FIELDS + ["CALL_SCHEDULE"]:
            req.getElement("fields").appendValue(f)
        sess.sendRequest(req)
        while True:
            ev = sess.nextEvent(5000)
            for msg in ev:
                if not msg.hasElement("securityData"):
                    continue
                for sd in msg.getElement("securityData").values():
                    isin = sd.getElementAsString("security").replace("/isin/", "").split()[0].upper()
                    if sd.hasElement("securityError"):
                        out[isin] = {"error": sd.getElement("securityError").getElementAsString("message")}
                        continue
                    fd = sd.getElement("fieldData")
                    gv = lambda f: (fd.getElementAsString(f) if fd.hasElement(f) else None)
                    cs, cn = "", ""
                    try:
                        if fd.hasElement("CALL_SCHEDULE"):
                            ds = []
                            for row in fd.getElement("CALL_SCHEDULE").values():
                                for k in range(row.numElements()):
                                    el = row.getElement(k)
                                    if "date" in str(el.name()).lower():
                                        ds.append(_bd_norm_date(el.getValueAsString()))
                            cs, cn = _bd_sched_next(ds)
                    except Exception:
                        cs, cn = "", ""
                    call = _bd_first(gv, BD_CALL_FIELDS) or cn
                    soft = _bd_first(gv, BD_SOFT_FIELDS) or cs
                    trig = _bd_first_num(gv, BD_TRIG_FIELDS)
                    out[isin] = {"call": call, "expiry": _bd_norm_date(gv("MATURITY")),
                                 "put": _bd_first(gv, BD_PUT_FIELDS), "call_start": soft or cs, "soft_call": soft,
                                 "trigger": trig, "hard_call": _bd_first(gv, BD_CALL_FIELDS),
                                 "call_src": ("field" if _bd_first(gv, BD_CALL_FIELDS) else ("schedule" if cn else "")),
                                 "callable": str(gv("CALLABLE") or "").upper()[:1], "putable": str(gv("PUTABLE") or "").upper()[:1]}
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return out
    finally:
        sess.stop()


def _bd_worker(isins):
    """Background: fetch in batches, stamp with today's date, persist. Never on a request path."""
    today = date.today().isoformat()
    try:
        for i in range(0, len(isins), BD_BATCH):
            batch = isins[i:i + BD_BATCH]
            try:
                res = bbg_bond_dates(batch)
                with _BD["lock"]:
                    for isin in batch:
                        r = res.get(isin) or {"error": "no data"}
                        r = dict(r); r["asof"] = today
                        _BD["data"][isin] = r
                _BD["err"] = ""
            except Exception as exc:
                _BD["err"] = str(exc)[:200]
                logger.warning("bond dates fetch failed (%d isins): %s", len(batch), exc)
                with _BD["lock"]:
                    for isin in batch:                          # remember the failure for an hour, do not hammer
                        _BD["data"][isin] = {"error": str(exc)[:120], "asof": today, "retry_at": time.time() + 3600}
        _bd_save()
    finally:
        with _BD["lock"]:
            _BD["pending"].difference_update(isins)
            _BD["busy"] = False


def _bd_need(isin):
    r = _BD["data"].get(isin)
    if not r:
        return True
    if r.get("retry_at"):
        return time.time() >= r["retry_at"]
    return r.get("asof") != date.today().isoformat()        # a new day -> refresh once


def recover_names(sids=None):
    """Fill blank short_name / und_fx (and other user fields) from cb_state, then
    blank short_name / und_fx from the latest autosave snapshot. Persists and
    returns {sec_id: {field: value}} of what was recovered."""
    sids = [int(s) for s in (sids or STATE["ids"])]
    fixed = {}
    db_rows = {}
    try:
        conn = _db()
        try:
            with conn.cursor() as cur:
                load_state_rows(cur, db_rows)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("recover: cb_state read failed: %s", exc)
    for sid in sids:
        row = STATE["rows"].setdefault(sid, _blank_row())
        src_row = db_rows.get(sid) or {}
        for f in USER_FIELDS:
            if (row.get(f) or "") == "" and (src_row.get(f) or "") != "":
                row[f] = src_row[f]; fixed.setdefault(sid, {})[f] = src_row[f]
    still = [sid for sid in sids if (STATE["rows"][sid].get("short_name") or "") == "" or (STATE["rows"][sid].get("und_fx") or "") == ""]
    if still:
        try:
            prefs = fetch_saved_prefs(still)
        except Exception as exc:
            prefs = {}; logger.warning("recover: prefs read failed: %s", exc)
        for sid in still:
            p = prefs.get(sid) or {}
            row = STATE["rows"][sid]
            for f in ("short_name", "und_fx"):
                if (row.get(f) or "") == "" and (p.get(f) or "") != "":
                    row[f] = p[f]; fixed.setdefault(sid, {})[f] = p[f]
    # last resort for ANY still-blank user field: the last non-blank value ever written, from the history
    blanks = {sid: [f for f in USER_FIELDS if (STATE["rows"][sid].get(f) or "") == ""] for sid in sids}
    need = {sid: fs for sid, fs in blanks.items() if fs}
    if need:
        try:
            conn = _db()
            try:
                with conn.cursor() as cur:
                    ph = ", ".join(["%s"] * len(need))
                    cur.execute(f"SELECT sec_id, field, new_val FROM {APP_DB}.cb_state_hist "
                                f"WHERE sec_id IN ({ph}) AND new_val IS NOT NULL AND new_val<>'' ORDER BY id DESC", list(need.keys()))
                    seen = set()
                    for sec_id, field, val in cur.fetchall():
                        sec_id = int(sec_id)
                        if (sec_id, field) in seen or field not in need.get(sec_id, []):
                            continue
                        seen.add((sec_id, field))
                        STATE["rows"][sec_id][field] = str(val); fixed.setdefault(sec_id, {})[field] = str(val)
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("recover: history read failed: %s", exc)
    missing = [sid for sid in sids if (STATE["rows"][sid].get("short_name") or "") == "" or (STATE["rows"][sid].get("und_fx") or "") == ""]
    if missing:
        logger.warning("recover: still no short_name/und_fx anywhere (cb_state, history, autosave) for %s", missing)
    STATE["recoverMissing"] = missing
    for sid, fs in fixed.items():
        try:
            db_upsert_state_fields(sid, STATE["rows"][sid], "recover", list(fs.keys()))
        except Exception as exc:
            logger.warning("recover: persist failed for %s: %s", sid, exc)
    if fixed:
        STATE["version"] += 1
    return fixed


@app.post("/api/state/recover")
async def api_state_recover():
    fixed = await run_in_threadpool(recover_names)
    if fixed:
        await broadcast({"type": "rows", "by": "recover", "v": STATE["version"],
                         "list": [{"secId": sid, **STATE["rows"][sid]} for sid in fixed]})
    return {"ok": True, "recovered": {str(k): v for k, v in fixed.items()}, "n": len(fixed),
            "missing": STATE.get("recoverMissing", [])}


# =====================================================================================
# CLASS: CB Code v3.1 (cbcode_v3.py + cbcode_v31.py beside app.py). Inputs come from the live
# grid; the engine runs in a worker thread on request, memoised on a fingerprint of its inputs.
# =====================================================================================
# The two reference modules are embedded verbatim (spec section 12 and handover Appendix A) so the desk
# deploys one file; a cbcode_v3.py / cbcode_v31.py placed beside app.py takes precedence if present.
_CBCODE_V3_SRC = r"""<<<TQ>>>CB Code v3 reference implementation (Python 3.9, standard library only).

Mirrors the tested dashboard logic: code letters, category (profile + vol read),
flags, regime P&L by strategy lens, strategy classification and book summaries.
Inputs per bond (None when missing):
  name, delta (Delta%, 0-100), parity (Parity%, % of par), price (CB price, % of par),
  prem (Prem% if no price), csprd (bp), put / exp (datetime.date), iv (CB Imp Vol %),
  rv10 / rv30 / rv90 / rv250 (realised vol %).
<<<TQ>>>
import math
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

DEFAULTS = dict(d_lo=30.0, d_hi=70.0, p_lo=15.0, p_hi=40.0, s_lo=300.0, s_hi=800.0,
                r_cheap=0.75, r_rich=0.925, hurdle=5.0, trend=20.0, cpn=1.0, rf=4.0)
K_CONV = 50.0  # notional conversion price used to turn parity into a stock price

CELLS = {"13": ("Yield paper", "Far above its shares and sitting on its floor; credit and rates drive it."),
         "12": ("Floor-held", "Premium looks medium, but the price sits on the floor and barely follows the stock."),
         "11": ("Rare", "Floor and parity nearly meet with little time left."),
         "23": ("Expensive balanced", "Half the stock move for a high premium: long-dated, high-vol names paying for time value, or a high coupon."),
         "22": ("Core balanced", "The textbook middle."),
         "21": ("Thin balanced", "Half the stock move for a low premium: mostly lower-vol names, less cushion in a sell-off."),
         "33": ("Rare", "Very long-dated, very high-vol bonds that already follow the stock."),
         "32": ("Paid-up equity", "Follows the stock but still carries time value; ages toward D3P1."),
         "31": ("Stock substitute", "Trades like the stock with almost no cushion.")}
VRN = {"11": ("Quiet name, cheap vol", "The option is cheap, but the stock barely moves: the edge is implied vol repricing, not trading the swings."),
       "12": ("Quiet name, fair vol", "No vol edge: own it for credit, yield or direction."),
       "13": ("Quiet name, rich vol", "Paying more than CBs usually charge, for swings that aren't happening: the weakest vol value."),
       "21": ("Normal name, cheap vol", "Ordinary swings bought cheaply: good value for gamma and vega."),
       "22": ("Normal name, fair vol", "Priced about where CBs usually trade: no vol edge either way."),
       "23": ("Normal name, rich vol", "Paying more than CBs usually charge for ordinary swings."),
       "31": ("Wild name, cheap vol", "Big swings bought cheaply: the gamma sweet spot."),
       "32": ("Wild name, fair vol", "The big swings are priced fairly: gamma pays only if realized stays high."),
       "33": ("Wild name, rich vol", "The market already charges for the big swings: gamma pays only if realized rises further.")}
# name, stock move, vol move (abs), credit spread move (abs); rates flat
REGIMES = [("Steady grind higher", 0.10, -0.08, -0.005), ("Volatile rally", 0.10, 0.08, -0.005),
           ("Equity\u2013credit divergence", 0.10, -0.08, 0.01), ("Uneasy rally", 0.10, 0.08, 0.01),
           ("Gentle pullback", -0.15, -0.08, -0.005), ("Sell-off without credit stress", -0.15, 0.08, -0.005),
           ("Slow credit grind", -0.15, -0.08, 0.01), ("Crisis (2008-style)", -0.15, 0.08, 0.01)]
LENSNAME = {"out": "outright", "hed": "delta-hedged", "ow": "overwritten", "hc": "delta- and credit-hedged"}
FAMILY_ORDER = ["Convertible arbitrage", "Rich vol", "Credit and yield", "Directional", "Mixed"]
MINUS = "\u2212"


# ---------- formatting (matches JavaScript toFixed, so text matches the dashboard) ----------
def fx(x: float, d: int) -> str:
    q = Decimal(1).scaleb(-d)
    s = str(Decimal(x).quantize(q, rounding=ROUND_HALF_UP))
    return s if d > 0 or "." not in s else s.split(".")[0]


def t1(x: float) -> str:
    s = fx(x, 1)
    return s[:-2] if s.endswith(".0") else s


def sg(x: float, d: int) -> str:
    r = fx(abs(x), d)
    if float(r) == 0:
        return fx(0, d)
    return ("+" if x > 0 else MINUS) + r


def rd(v: float) -> float:  # JavaScript Math.round(v * 100) / 100
    return math.floor(v * 100 + 0.5) / 100


# ---------- simple CB model: bond floor + call (replace with the desk pricer where possible) ----------
def erf(x: float) -> float:  # Abramowitz-Stegun 7.1.26, same as the dashboard
    s = -1.0 if x < 0 else 1.0
    x = abs(x)
    t = 1 / (1 + 0.3275911 * x)
    y = 1 - ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * math.exp(-x * x)
    return s * y


def ncdf(x: float) -> float:
    return 0.5 * (1 + erf(x / math.sqrt(2)))


def bond_floor(T: float, c: float, y: float) -> float:
    pv = 100 / (1 + y) ** T
    t = T
    while t > 1e-9:
        pv += c * 100 / (1 + y) ** t
        t -= 1
    return pv


def bs_call(St: float, K: float, T: float, r: float, sig: float) -> Tuple[float, float]:
    if T <= 1e-9 or sig <= 1e-9:
        return max(St - K * math.exp(-r * max(T, 0)), 0.0), (1.0 if St > K else 0.0)
    st = sig * math.sqrt(T)
    d1 = (math.log(St / K) + (r + 0.5 * sig * sig) * T) / st
    d2 = d1 - st
    return St * ncdf(d1) - K * math.exp(-r * T) * ncdf(d2), ncdf(d1)


def val(par: float, T: float, vol: float, rf: float, sp: float, cpn: float) -> Dict[str, float]:
    St = par * K_CONV / 100
    c, nd1 = bs_call(St, K_CONV, T, rf, vol)
    m = bond_floor(T, cpn, rf + sp) + (100 / K_CONV) * c
    conv = par > m
    price = par if conv else m
    return {"price": price, "prem": price / par - 1, "delta": 1.0 if conv else nd1}


# ---------- code letters ----------
def v_cuts(rv90s: List[Optional[float]]) -> Tuple[float, float, str]:
    a = sorted(v for v in rv90s if v is not None)
    if len(a) < 3:
        return 31.0, 45.0, "fallback"

    def q(p: float) -> float:
        pos = p * (len(a) - 1)
        i = int(math.floor(pos))
        fr = pos - i
        return a[i] + fr * (a[i + 1] - a[i]) if i + 1 < len(a) else a[i]
    return q(1 / 3), q(2 / 3), "pasted"


def code_row(r: Dict[str, Any], today: date, P: Dict[str, float], cuts: Tuple[float, float, str]) -> Dict[str, Any]:
    o: Dict[str, Any] = {"near": [], "missing": []}
    ends = [d for d in (r.get("put"), r.get("exp")) if d is not None and today is not None and d > today]
    o["end"] = min(ends) if ends else None
    o["end_is_put"] = o["end"] is not None and o["end"] == r.get("put")
    o["days"] = (o["end"] - today).days if o["end"] is not None else None
    o["yrs"] = o["days"] / 365.25 if o["days"] is not None else None
    o["prem"], o["est"] = None, False
    if r.get("price") is not None and r.get("parity"):
        o["prem"] = r["price"] / r["parity"] - 1
    elif r.get("prem") is not None:
        o["prem"] = r["prem"] / 100
    elif r.get("parity") and r.get("iv") is not None and r.get("csprd") is not None and o["yrs"]:
        o["prem"] = val(r["parity"], o["yrs"], r["iv"] / 100, P["rf"] / 100, r["csprd"] / 10000, P["cpn"] / 100)["prem"]
        o["est"] = True
    dl = r.get("delta")
    o["d"] = 0 if dl is None else (1 if dl < P["d_lo"] else (3 if dl >= P["d_hi"] else 2))
    pp = None if o["prem"] is None else o["prem"] * 100
    o["p"] = 0 if pp is None else (1 if pp < P["p_lo"] else (3 if pp > P["p_hi"] else 2))
    y = o["yrs"]
    o["t"] = 0 if y is None else (1 if y < 1 else (3 if y > 3 else 2))
    cs = r.get("csprd")
    o["s"] = 0 if cs is None else (1 if cs < P["s_lo"] else (3 if cs >= P["s_hi"] else 2))
    rv90 = r.get("rv90")
    o["v"] = 0 if rv90 is None else (1 if rv90 < cuts[0] else (3 if rv90 > cuts[1] else 2))
    iv = r.get("iv")
    if iv is None or rv90 is None or not rv90 > 0:
        o["r"] = 0
    else:
        o["gap"], o["ivr"] = iv - rv90, iv / rv90
        o["r"] = 1 if o["ivr"] < P["r_cheap"] else (3 if o["ivr"] >= P["r_rich"] else 2)
    for lab, k in (("Delta%", "d"), ("premium", "p"), ("live date", "t"), ("RV 90D", "v"), ("CB Imp Vol / RV 90D", "r"), ("CSprd", "s")):
        if not o[k]:
            o["missing"].append(lab)
    o["sum"] = o["d"] + o["p"] if (o["d"] and o["p"]) else None
    o["trend"] = None
    if r.get("rv10") is not None and rv90:
        o["ratio"] = r["rv10"] / rv90
        o["trend"] = "rising" if o["ratio"] > 1 + P["trend"] / 100 else ("falling" if o["ratio"] < 1 - P["trend"] / 100 else "steady")
    o["conf"] = None
    if o["r"] in (1, 3) and r.get("rv250") is not None and r["rv250"] > 0:
        o["ivr250"] = iv / r["rv250"]
        o["conf"] = o["ivr250"] < P["r_cheap"] if o["r"] == 1 else o["ivr250"] >= P["r_rich"]
    o["low_vega"] = dl is not None and o["r"] > 0 and (dl < 15 or dl > 90)
    if dl is not None and (abs(dl - P["d_lo"]) <= 5 or abs(dl - P["d_hi"]) <= 5):
        o["near"].append("delta")
    if pp is not None and (abs(pp - P["p_lo"]) <= 3 or abs(pp - P["p_hi"]) <= 3):
        o["near"].append("premium")
    if y is not None and (abs(y - 1) <= 0.1 or abs(y - 3) <= 0.1):
        o["near"].append("tenor")
    if cs is not None and (abs(cs - P["s_lo"]) <= 50 or abs(cs - P["s_hi"]) <= 50):
        o["near"].append("spread")
    o["near"].sort()
    o["focus"] = (o["prem"] < 0.75 and 70 <= r["price"] <= 125) if (o["prem"] is not None and r.get("price") is not None) else None
    o["cell"] = "%d%d" % (o["d"], o["p"]) if (o["d"] and o["p"]) else ""
    o["code"] = "D%dP%dT%d-V%dR%d-S%d" % (o["d"], o["p"], o["t"], o["v"], o["r"], o["s"])
    return o


# ---------- words ----------
def reads_as(r: Dict[str, Any], c: Dict[str, Any]) -> str:
    L = ["", "low", "medium", "high"]
    parts = [L[c["d"]] + " delta " + t1(r["delta"]) + " (" + ["", "bond-like", "balanced", "equity-like"][c["d"]] + ")" if c["d"] else "delta missing",
             L[c["p"]] + " premium " + fx(c["prem"] * 100, 1) + "%" + (" (estimated)" if c["est"] else "") if c["p"] else "premium missing",
             ["", "short-dated", "medium-dated", "long-dated"][c["t"]] + " (" + fx(c["yrs"], 2) + " yrs to " + ("put" if c["end_is_put"] else "expiry") + ")" if c["t"] else "no live date",
             L[c["v"]] + " stock vol " + t1(r["rv90"]) + "%" if c["v"] else "stock vol missing",
             ["", "cheap", "fair", "rich"][c["r"]] + " CB vol (IV/RV " + fx(c["ivr"], 2) + ", " + fx(abs(1 - c["ivr"]) * 100, 1) + "% " + ("discount" if c["ivr"] < 1 else "premium") + ")" if c["r"] else "CB vol missing",
             L[c["s"]] + " spread " + fx(r["csprd"], 0) + " bp" + (", distressed" if c["s"] == 3 else "") if c["s"] else "spread missing"]
    x = ", ".join(parts)
    return x[0].upper() + x[1:] + "."


def vol_read(r: Dict[str, Any], c: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not c["v"] or not c["r"]:
        return None
    k = "%d%d" % (c["v"], c["r"])
    q, why = [], [VRN[k][1]]
    if c["low_vega"]:
        q.append("little vega")
        why.append("With little vega, the vol read barely moves the price.")
    if c["r"] == 3 and c["ivr"] < 1:
        q.append("still under realized")
        why.append("Rich only against the usual CB discount: implied is still " + fx((1 - c["ivr"]) * 100, 1) + "% under RV 90D, so a hedged long can still earn if the swings hold.")
    if c["trend"] in ("rising", "falling"):
        q.append("realized " + c["trend"])
        x = "RV 10D is \u00d7" + fx(c["ratio"], 2) + " RV 90D"
        if c["trend"] == "rising":
            why.append("Realized is rising (" + x + "), so the richness may fade by itself; don't sell vol into it." if c["r"] == 3 else
                       ("Realized is rising too (" + x + "), which backs the cheap read." if c["r"] == 1 else "Realized is rising (" + x + "), so fair vol may turn cheap."))
        else:
            why.append("Realized is falling (" + x + "), so the cheapness may fade." if c["r"] == 1 else
                       ("Realized is falling (" + x + "), so the richness may grow." if c["r"] == 3 else "Realized is falling (" + x + "), so fair vol may turn rich."))
    nm = VRN[k][0]
    return {"k": k, "name": nm, "q": q, "why": " ".join(why),
            "label": nm[0].lower() + nm[1:] + (" (" + "; ".join(q) + ")" if q else "")}


def category(r: Dict[str, Any], c: Dict[str, Any]) -> Dict[str, Any]:
    prof = CELLS[c["cell"]][0] if c["cell"] in CELLS else None
    vr = vol_read(r, c)
    return {"prof": prof, "vr": vr, "name": (prof or "Profile missing") + ", " + (vr["label"] if vr else "vol read missing")}


def plug_flags(c: Dict[str, Any]) -> List[Tuple[str, str]]:
    o = []
    if c["r"] == 1 and c["s"] >= 2:
        o.append(("cheap vol: check the credit input", "Cheap vol on a credit-sensitive name: if the market sees worse credit than your CSprd, the floor is lower and the true implied vol higher, so check the credit input before calling it cheap."))
    if c["r"] == 3 and c["t"] == 3:
        o.append(("rich and long-dated: check the inputs", "Rich vol on a long-dated bond: credit and rate inputs move implied vol most here, so check CSprd before calling it rich."))
    return o


def sum_text(s: Optional[int]) -> str:
    return "" if s is None else ("in line" if s == 4 else ("premium high for its delta" if s > 4 else "premium low for its delta"))


# ---------- regimes and lenses ----------
def lens_chg(r: Dict[str, Any], c: Dict[str, Any], P: Dict[str, float]) -> Optional[Dict[str, List[float]]]:
    if not r.get("parity") or not c["yrs"] or r.get("iv") is None or r.get("csprd") is None:
        return None
    rf, cp, vol, sp, par, T = P["rf"] / 100, P["cpn"] / 100, r["iv"] / 100, r["csprd"] / 10000, r["parity"], c["yrs"]
    b = val(par, T, vol, rf, sp, cp)
    out, hed, vo, cr = [], [], [], []
    for _, dS, dv, dc in REGIMES:
        a = val(par * (1 + dS), T, max(0.01, vol + dv), rf, max(0.0, sp + dc), cp)
        v = val(par, T, max(0.01, vol + dv), rf, sp, cp)
        k = val(par, T, vol, rf, max(0.0, sp + dc), cp)
        out.append((a["price"] / b["price"] - 1) * 100)
        hed.append((a["price"] - b["price"] - b["delta"] * par * dS) / b["price"] * 100)
        vo.append((v["price"] / b["price"] - 1) * 100)
        cr.append((k["price"] / b["price"] - 1) * 100)
    return {"out": out, "hed": hed, "vol": vo, "cr": cr}


def lens_of(L: Dict[str, List[float]], lens: str) -> List[float]:
    if lens == "hed":
        return L["hed"]
    if lens == "ow":
        return [x - v for x, v in zip(L["out"], L["vol"])]
    if lens == "hc":
        return [x - k for x, k in zip(L["hed"], L["cr"])]
    return L["out"]


def own_tiers(x: List[float]) -> Dict[str, int]:
    y = [rd(v) for v in x]
    s = sorted(y)
    med = (s[3] + s[4]) / 2
    best = worst = typ = 0
    for j in range(1, len(y)):
        if y[j] > y[best]:
            best = j
        if y[j] < y[worst]:
            worst = j
        if abs(y[j] - med) < abs(y[typ] - med) - 1e-9:
            typ = j
    return {"best": best, "worst": worst, "typ": typ}


def money_text(x: List[float]) -> str:
    idx = sorted([j for j in range(len(x)) if x[j] > 0], key=lambda j: (-rd(x[j]), j))
    return "%d of 8" % len(idx) + (": " + ", ".join(REGIMES[j][0] + " " + sg(x[j], 1) + "%" for j in idx) if idx else "")


def rel_tiers(chg: List[float], bench: Optional[List[float]]) -> Dict[str, Any]:
    ex = [x - bench[j] for j, x in enumerate(chg)] if bench else list(chg)
    best = worst = normal = 0
    for j in range(1, len(chg)):
        if ex[j] > ex[best]:
            best = j
        if ex[j] < ex[worst]:
            worst = j
        if abs(ex[j]) < abs(ex[normal]):
            normal = j
    return {"best": best, "worst": worst, "normal": normal, "ex": ex, "rel": bool(bench)}


# ---------- strategy ----------
def ytp(price: float, yrs: float, P: Dict[str, float]) -> float:
    return (P["cpn"] + (100 - price) / yrs) / ((100 + price) / 2) * 100


def strategy(r: Dict[str, Any], c: Dict[str, Any], P: Dict[str, float]) -> Dict[str, str]:
    d, p, t, v, R, s, px = c["d"], c["p"], c["t"], c["v"], c["r"], c["s"], r.get("price")

    def mk(name, family, lens, why, exit_):
        return {"name": name, "family": family, "lens": lens, "why": why, "exit": exit_}
    if not d or not p or px is None:
        return mk("Not enough data", "Mixed", "out", "Needs Delta%, premium and the CB price.", "")
    if s == 3:
        return mk("Credit: distressed (capital structure)", "Credit and yield", "out", "S3: spread " + fx(r["csprd"], 0) + " bp, so credit drives the price. Pair with a short stock position as a default hedge.", "Exit on your recovery view; resize the stock hedge as the bond falls.")
    if d == 1 and s == 2 and px < 90:
        return mk("Credit: busted", "Credit and yield", "out", "D1 at " + t1(px) + ": a cheap credit with a free call on the stock.", "Exit if spread reaches S3 or the price recovers toward 100.")
    if d == 1 and px <= 100 and t in (1, 2):
        y, lab = ytp(px, c["yrs"], P), ("put" if c["end_is_put"] else "maturity")
        if y >= P["hurdle"]:
            return mk("Yield to " + lab, "Credit and yield", "out", "D1 at " + t1(px) + " with " + fx(c["yrs"], 2) + " years to " + lab + ": about " + fx(y, 1) + "% a year, above the " + t1(P["hurdle"]) + "% hurdle.", "Exit if spread reaches S3 or the yield drops below the hurdle.")
        return mk("Carry only (yield below hurdle)", "Credit and yield", "out", "D1 at " + t1(px) + ": about " + fx(y, 1) + "% a year to " + lab + ", below the " + t1(P["hurdle"]) + "% hurdle.", "Review if spread widens or the stock rallies.")
    if d == 1:
        return mk("Carry only", "Credit and yield", "out", "D1 above par or long-dated: little edge beyond the coupon.", "Review if spread widens or the stock rallies.")
    if d == 2 and R == 3:
        return mk("Rich vol: avoid, trim or switch", "Rich vol", "out", "D2 with rich CB vol (R3): don't pay for vol. Avoid it, trim into demand, or switch to a cheaper line; overwrite only where listed options exist.", "Re-enter when CB vol cheapens to R1 or R2.")
    if d == 2 and s == 2 and R in (1, 2):
        return mk("Convertible arbitrage: credit-hedged", "Convertible arbitrage", "hc", "D2 with a medium spread (S2) and vol not rich: buy the CB, short delta, and buy CDS or asset-swap the bond floor to keep only the option.", "Exit when vol turns rich (R3) or the CDS basis moves against you.")
    if d == 2 and R == 1 and t == 3:
        return mk("Convertible arbitrage: vol", "Convertible arbitrage", "hed", "D2 with cheap CB vol (R1) and over 3 years left: buy the CB, short delta, own the vol.", "Take profit at R3; cut if spread reaches S3.")
    if d == 2 and R in (1, 2) and v >= 2 and t in (1, 2):
        return mk("Convertible arbitrage: gamma", "Convertible arbitrage", "hed", "D2, CB vol not rich, a stock that moves (V%d), 3 years or less: re-hedge to harvest the swings." % v, "Exit at R3, if delta leaves 30\u201370, or with under 6 months left.")
    if d == 3 and p == 1:
        return mk("Outright: stock substitute", "Directional", "out", "D3P1: moves almost like the stock with a floor underneath.", "Switch to the stock if premium rises (P2) or a soft call nears.")
    if d in (2, 3) and p <= 2:
        return mk("Outright: asymmetric long", "Directional", "out", "D%dP%d: stock upside with a bond floor." % (d, p), "Re-check when it becomes D3P1 or slips to D1.")
    return mk("Mixed: no clear edge", "Mixed", "out", "Delta and premium point different ways and vol gives no edge.", "")


# ---------- book ----------
def run_book(rows: List[Dict[str, Any]], today: date, P: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    P = dict(DEFAULTS, **(P or {}))
    cuts = v_cuts([r.get("rv90") for r in rows])
    bk = []
    for r in rows:
        c = code_row(r, today, P, cuts)
        L = lens_chg(r, c, P)
        bk.append({"name": r["name"], "r": r, "c": c, "L": L, "chg": L["out"] if L else None})
    with_chg = [x for x in bk if x["chg"]]
    book = [sum(x["chg"][j] for x in with_chg) / len(with_chg) for j in range(8)] if len(with_chg) >= 3 else None
    bk.sort(key=lambda x: (x["c"]["d"], -x["c"]["p"], x["c"]["t"], x["name"]))
    out_rows, fam, allx = [], {}, []
    for x in bk:
        r, c = x["r"], x["c"]
        flags = []
        if c["sum"] is not None and c["sum"] != 4:
            flags.append(sum_text(c["sum"]))
        if c["near"]:
            flags.append("near: " + ", ".join(c["near"]))
        if c["conf"] is not None:
            flags.append(("cheap" if c["r"] == 1 else "rich") + (" confirmed by RV 250D" if c["conf"] else ", not confirmed by RV 250D"))
        if c["low_vega"]:
            flags.append("R low confidence (little vega)")
        flags += [f[0] for f in plug_flags(c)]
        if c["trend"] and c["trend"] != "steady":
            flags.append("vol " + c["trend"])
        if c["est"]:
            flags.append("premium estimated")
        if c["missing"]:
            flags.append("missing: " + ", ".join(c["missing"]))
        st = strategy(r, c, P)
        xs = lens_of(x["L"], st["lens"]) if x["L"] else None
        own = own_tiers(xs) if xs else None
        t = rel_tiers(x["chg"], book) if x["chg"] else None
        if t and t["rel"] and max(abs(e) for e in t["ex"]) < 1:
            flags.append("close to the book in every regime")
        ot = (lambda j: REGIMES[j][0] + ": " + sg(xs[j], 1) + "%")
        vsb = ("Beats most in " + REGIMES[t["best"]][0].lower() + " (" + sg(x["chg"][t["best"]], 1) + "% vs " + sg(book[t["best"]], 1) + "%); lags most in "
               + REGIMES[t["worst"]][0].lower() + " (" + sg(x["chg"][t["worst"]], 1) + "% vs " + sg(book[t["worst"]], 1) + "%).") if (t and t["rel"]) else "\u2014"
        cat = category(r, c)
        out_rows.append({"name": x["name"], "code": c["code"], "category": cat["name"], "cat_prof": cat["prof"] or "Profile missing",
                         "cat_vr": (cat["vr"]["name"] + (" (" + "; ".join(cat["vr"]["q"]) + ")" if cat["vr"]["q"] else "")) if cat["vr"] else "Vol read missing",
                         "cat_meaning": VRN[cat["vr"]["k"]][1] if cat["vr"] else "", "strategy": st["name"], "family": st["family"],
                         "strategy_text": st["why"] + (" Exit: " + st["exit"] if st["exit"] else ""),
                         "money": money_text(xs) + " (" + LENSNAME[st["lens"]] + ")" if xs else "\u2014",
                         "best": ot(own["best"]) if own else "\u2014", "typical": ot(own["typ"]) if own else "\u2014", "worst": ot(own["worst"]) if own else "\u2014",
                         "vs_book": vsb, "reads_as": reads_as(r, c), "flags": "; ".join(flags)})
        if xs:
            fam.setdefault(st["family"], []).append(xs)
            allx.append(xs)

    def avg(lst):
        return [sum(v[j] for v in lst) / len(lst) for j in range(8)]

    def money_list(a):
        return [REGIMES[j][0] + " " + sg(a[j], 1) + "%" for j in sorted([j for j in range(8) if a[j] > 0], key=lambda j: (-rd(a[j]), j))]

    def worst_of(a):
        w = 0
        for j in range(1, len(a)):
            if rd(a[j]) < rd(a[w]):
                w = j
        return w
    fam_rows = []
    for k in FAMILY_ORDER:
        if k in fam:
            a = avg(fam[k])
            m, w = money_list(a), worst_of(a)
            fam_rows.append([k, str(len(fam[k])), ", ".join(m) if m else "none", REGIMES[w][0] + " " + sg(a[w], 1) + "%"])
    net = ""
    if allx:
        a2 = avg(allx)
        m2, w2 = money_list(a2), worst_of(a2)
        arb = avg(fam["Convertible arbitrage"]) if "Convertible arbitrage" in fam else None
        net = ("Your book as traded makes money in " + (", ".join(m2) if m2 else "no regime") + "; it loses most in " + REGIMES[w2][0].lower()
               + " (" + sg(a2[w2], 1) + "%)." + (" The convertible arbitrage bucket makes " + sg(arb[w2], 1) + "% there: it is the book's hedge." if arb and arb[w2] > 0 else ""))
    return {"rows": out_rows, "families": fam_rows, "net": net, "cuts": cuts, "book_avg": book}


# ---------- next step, not yet in the dashboard: peer consistency check on implied vol ----------
def peer_input_flags(items: List[Dict[str, Any]], prem_tol: float = 3.0, iv_gap: float = 5.0, min_peers: int = 3) -> Dict[str, str]:
    <<<TQ>>>items: dicts with name, d, t, s (code digits), prem_pct (premium in %), iv (CB Imp Vol in %).
    Peers are the other bonds with the same D, T and S digits. If a bond's premium is within prem_tol
    points of the peers' median premium but its implied vol is at least iv_gap points from the peers'
    median implied vol, the model inputs (credit, borrow, dividends) are the likely cause.<<<TQ>>>
    from statistics import median
    out = {}
    for x in items:
        if not (x.get("d") and x.get("t") and x.get("s")) or x.get("prem_pct") is None or x.get("iv") is None:
            continue
        peers = [y for y in items if y is not x and (y.get("d"), y.get("t"), y.get("s")) == (x["d"], x["t"], x["s"])
                 and y.get("prem_pct") is not None and y.get("iv") is not None]
        if len(peers) < min_peers:
            continue
        pg = x["prem_pct"] - median([y["prem_pct"] for y in peers])
        ig = x["iv"] - median([y["iv"] for y in peers])
        if abs(pg) <= prem_tol and abs(ig) >= iv_gap:
            out[x["name"]] = ("premium in line with peers (%s pts) but CB vol %s pts off: check credit and borrow inputs" % (sg(pg, 1), sg(ig, 1)))
    return out
"""
_CBCODE_V31_SRC = r"""<<<TQ>>>CB Code v3.1 reference implementation (Python 3.9, standard library only).

Builds on cbcode_v3 (spec section 12) and changes none of its outputs: cbcode_v3.run_book() is untouched,
and book_v31() returns the new columns, one dict per bond, in the same order as run_book()["rows"].

What v3.1 adds
  tags            one trade tag per bond (GAMMA, VOL, PARITY, CONVERT, CREDIT, YIELD, COUPON, DELTA, AVOID, NONE)
  strategy v3.1   rule 6 needs T3; D3 is hedged by default (Conversion trade, Parity carry, Hedged vol)
  R margin        vol pts IV must move to change R, against realized-vol sampling noise
  vs peers        cheap or rich against similar bonds (vol pts and % of price)
  input check     flags bonds whose implied vol is probably an input artefact
  category        where each bond is best or worst of its D x P category in the 8 regimes, as held
  economics       5 vol pts, gamma rent, 1 pt of price, lost if called, one month of carry
                  (time and coupon, funding, borrow) at per-bond funding and borrow rates
  what-if         any view (stock, vol, credit, rates, horizon) on every bond, P&L split by factor

The pricing model is the spec's simple model (section 7). With the desk pricer, keep the same shifts,
lenses, thresholds and text. All thresholds are in EXT_DEFAULTS.
<<<TQ>>>
import math
from datetime import date
from statistics import median
from typing import Any, Dict, List, Optional, Sequence, Tuple

from cbcode_v3 import (DEFAULTS, K_CONV, REGIMES, bond_floor, bs_call, code_row, fx, lens_chg, lens_of,
                       money_text, ncdf, own_tiers, run_book, sg, strategy, t1, v_cuts, val, ytp)

EXT_DEFAULTS = dict(
    min_peers=3,          # peers needed before falling back to a wider peer set
    default_ratio=0.8375, # IV/RV 90D used when no peer set has min_peers (middle of the R2 band)
    lo_delta=15.0,        # below this Delta%, implied vol is unreliable: no peer verdict, no ratio check
    hi_delta=90.0,        # above this Delta%, same
    peer_line_pct=1.0,    # cheap / rich vs peers when the gap is worth at least this % of price
    peer_iv_gap=6.0,      # vol pts from the peer-fair IV that triggers "check inputs first"
    ivr_lo=0.60,          # IV/RV 90D below this: check inputs first
    ivr_hi=1.15,          # IV/RV 90D above this: check inputs first
    bump_spread_bp=100.0, # credit shift for the IV sensitivity
    bump_borrow=0.01,     # borrow shift (decimal) for the IV sensitivity
    horizon=1.0 / 12.0,   # years: the carry window
    conv_line=1.0,        # D3 premium (% over parity) at or below which the trade is a conversion
    rv_noise=0.075,       # RV 90D sampling noise as a share of RV (about 1/sqrt(180))
    cat_min=3,            # bonds needed in a D x P category before falling back to the same D
    cat_edge=0.5,         # pts: smaller leads or lags against the category are shown as 'never ahead/behind'
    borrow_default=0.5,   # % a year, until a borrow column exists
    funding_default=4.0,  # % a year
    directional=False,    # True only for a book that wants D3 held outright as stock substitutes
)
NUM_KEYS = ("delta", "parity", "price", "prem", "csprd", "iv", "rv10", "rv30", "rv90", "rv250")


# ------------------------------------------------------------------ input cleaning
def num(x: Any) -> Optional[float]:
    <<<TQ>>>float or None. None, NaN, inf, bools and non-numeric strings -> None; '42.5' -> 42.5.<<<TQ>>>
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def clean_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    <<<TQ>>>Copy of rows that cbcode_v3.run_book can take safely: numeric fields through num(), dates kept only
    if they are datetime.date. The reference code treats NaN as a number and silently assigns digits
    (NaN Delta% becomes D2), so pass pandas/Excel data through this first.<<<TQ>>>
    out = []
    for i, r in enumerate(rows or []):
        r = dict(r) if isinstance(r, dict) else {}
        c = {k: num(r.get(k)) for k in NUM_KEYS}
        for k in ("put", "exp"):
            v = r.get(k)
            c[k] = v if isinstance(v, date) else None
        c["name"] = str(r.get("name")) if r.get("name") is not None else "row %d" % (i + 1)
        out.append(c)
    return out


# ------------------------------------------------------------------ model with borrow / dividend yield
def val_q(par: float, T: float, vol: float, rf: float, sp: float, cpn: float, q: float = 0.0) -> Dict[str, float]:
    <<<TQ>>>cbcode_v3.val plus a continuous borrow/dividend yield q on the option leg; q = 0 is identical to val.<<<TQ>>>
    if q == 0.0:
        return val(par, T, vol, rf, sp, cpn)
    St = par * K_CONV / 100
    if T <= 1e-9 or vol <= 1e-9:
        c, dl = bs_call(St * math.exp(-q * max(T, 0.0)), K_CONV, T, rf, vol)
    else:
        st = vol * math.sqrt(T)
        d1 = (math.log(St / K_CONV) + (rf - q + 0.5 * vol * vol) * T) / st
        dq = math.exp(-q * T)
        c, dl = St * dq * ncdf(d1) - K_CONV * math.exp(-rf * T) * ncdf(d1 - st), dq * ncdf(d1)
    m = bond_floor(T, cpn, rf + sp) + (100 / K_CONV) * c
    conv = par > m
    price = par if conv else m
    return {"price": price, "prem": price / par - 1, "delta": 1.0 if conv else dl}


def _bisect(f, lo: float, hi: float, rising: bool, n: int = 70) -> Optional[float]:
    flo, fhi = f(lo), f(hi)
    if (rising and (flo > 0 or fhi < 0)) or (not rising and (flo < 0 or fhi > 0)):
        return None
    for _ in range(n):
        mid = 0.5 * (lo + hi)
        if (f(mid) > 0) == rising:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)


def solve_iv(price: float, par: float, T: float, rf: float, sp: float, cpn: float, q: float = 0.0) -> Optional[float]:
    <<<TQ>>>Implied vol (decimal) that reprices price; None if no vol in [0.5%, 300%] can.<<<TQ>>>
    if None in (num(price), num(par), num(T)) or par <= 0 or T <= 0:
        return None
    return _bisect(lambda v: val_q(par, T, v, rf, sp, cpn, q)["price"] - price, 0.005, 3.0, True)


def implied_spread(price: float, par: float, T: float, vol: float, rf: float, cpn: float, q: float = 0.0) -> Optional[float]:
    <<<TQ>>>Credit spread (decimal) that reprices price at the given vol; None if outside 0-60%.<<<TQ>>>
    if None in (num(price), num(par), num(T), num(vol)) or par <= 0 or T <= 0 or vol <= 0:
        return None
    return _bisect(lambda s: val_q(par, T, vol, rf, s, cpn, q)["price"] - price, 0.0, 0.60, False)


def implied_borrow(price: float, par: float, T: float, vol: float, rf: float, sp: float, cpn: float) -> Optional[float]:
    <<<TQ>>>Borrow/dividend yield (decimal) that reprices price at the given vol and spread; None if outside -5% to 30%.<<<TQ>>>
    if None in (num(price), num(par), num(T), num(vol)) or par <= 0 or T <= 0 or vol <= 0:
        return None
    return _bisect(lambda q: val_q(par, T, vol, rf, sp, cpn, q)["price"] - price, -0.05, 0.30, False)


def greeks(par: float, T: float, vol: float, rf: float, sp: float, cpn: float, q: float = 0.0) -> Dict[str, float]:
    <<<TQ>>>Reference-model sensitivities, % of price unless stated.<<<TQ>>>
    b = val_q(par, T, vol, rf, sp, cpn, q)
    p0 = b["price"]
    up, dn = val_q(par * 1.01, T, vol, rf, sp, cpn, q), val_q(par * 0.99, T, vol, rf, sp, cpn, q)
    vu = val_q(par, T, vol + 0.01, rf, sp, cpn, q)["price"]
    vd = val_q(par, T, max(vol - 0.01, 0.005), rf, sp, cpn, q)["price"]
    return {"price": p0,
            "delta": b["delta"] * 100,                                             # Delta%
            "gamma": (up["delta"] - dn["delta"]) / 2 * 100,                        # Delta% pts per 1% stock move
            "vega": (vu - vd) / 2 / p0 * 100,                                      # % per vol pt
            "cs01": (val_q(par, T, vol, rf, sp + 0.01, cpn, q)["price"] - p0) / p0 * 100,  # % per +100 bp spread
            "rho": (val_q(par, T, vol, rf + 0.01, sp, cpn, q)["price"] - p0) / p0 * 100,   # % per +100 bp rates
            "elast": b["delta"] * par / p0 * 100}                                  # equity share, %: price % move per 100% stock move


def gamma_rent(par: float, T: float, vol: float, rf: float, sp: float, cpn: float, real_vol: float,
               h: float = 1.0 / 12.0, q: float = 0.0) -> float:
    <<<TQ>>>Delta-hedged P&L (% of price) over h from gamma net of theta, if the stock realizes real_vol.<<<TQ>>>
    g = greeks(par, T, vol, rf, sp, cpn, q)
    gamma_par = g["gamma"] / 100 / (0.01 * par)
    return 0.5 * gamma_par * par * par * (real_vol ** 2 - vol ** 2) * h / g["price"] * 100


# ------------------------------------------------------------------ horizon lenses
def coupons_paid(T: float, h: float) -> int:
    <<<TQ>>>Coupons (annual, counted back from the live date as in bond_floor) paid within the next h years.<<<TQ>>>
    n, t = 0, T
    while t > 1e-9:
        if t <= h + 1e-12:
            n += 1
        t -= 1
    return n


def lens_h(par: float, T: float, vol: float, sp: float, shocks: Sequence[Tuple[float, float, float]],
           rf: float = 0.04, cpn: float = 0.01, h: float = 1.0 / 12.0, q: float = 0.0,
           drate: Optional[Sequence[float]] = None) -> Dict[str, List[float]]:
    <<<TQ>>>P&L in % of starting price per shock (stock move, abs vol move, abs spread move) over h years.

    out: long CB funded at rf.  hed: plus static short delta, short proceeds earn rf, borrow q paid.
    hc : hed plus CDS sized to the CB's spread risk, premium paid over h.
    h = 0 with no rate moves reproduces cbcode_v3.lens_chg (out, hed) and lens_of(..., "hc") exactly.
    <<<TQ>>>
    b = val_q(par, T, vol, rf, sp, cpn, q)
    p0, d0 = b["price"], b["delta"]
    T2 = T - h
    alive = T2 > 1e-6
    cpn_pts = coupons_paid(T, h) * cpn * 100 if h > 0 else 0.0
    base2 = val_q(par, T2, vol, rf, sp, cpn, q)["price"] if alive else 0.0
    cs1 = val_q(par, T, vol, rf, sp + 0.0001, cpn, q)["price"] - p0
    ra = (1 - math.exp(-(rf + sp) * T)) / (rf + sp) if rf + sp > 0 else T
    cds_prem = sp * max(-cs1 / 0.0001 / ra, 0.0) * h
    out, hed, hc = [], [], []
    for j, (dS, dv, dc) in enumerate(shocks):
        r2 = rf + (drate[j] if drate else 0.0)
        par1, vol1, sp1 = par * (1 + dS), max(0.01, vol + dv), max(0.0, sp + dc)
        if alive:
            p1 = val_q(par1, T2, vol1, r2, sp1, cpn, q)["price"]
            cr = val_q(par, T2, vol, rf, sp1, cpn, q)["price"] - base2
        else:
            p1, cr = max(par1, 100.0), 0.0
        o = p1 + cpn_pts - p0 - rf * h * p0
        hd = p1 + cpn_pts - p0 - d0 * par * dS - rf * h * (p0 - d0 * par) - q * d0 * par * h
        out.append(o / p0 * 100)
        hed.append(hd / p0 * 100)
        hc.append((hd - cr - cds_prem) / p0 * 100)
    return {"out": out, "hed": hed, "hc": hc}


# ------------------------------------------------------------------ cheap or rich against peers
def peer_value(items: List[Dict[str, Any]], X: Optional[Dict[str, float]] = None) -> List[Dict[str, Any]]:
    <<<TQ>>>items: dicts with name, d, t (code digits), delta (Delta%), iv, rv90 (%), vega (% per vol pt).

    Fair IV = RV 90D x the peers' median IV/RV 90D. Peers: same D and T digits; with fewer than min_peers,
    same D; then the whole paste; then default_ratio. Bonds outside lo_delta-hi_delta are never peers
    and get no verdict. gap_vol = fair IV - IV (+ = cheap); gap_px = gap_vol x vega (% of price).
    <<<TQ>>>
    X = dict(EXT_DEFAULTS, **(X or {}))
    rows = []
    for x in items or []:
        dl, iv, rv, vg = num(x.get("delta")), num(x.get("iv")), num(x.get("rv90")), num(x.get("vega"))
        ok = (dl is not None and iv is not None and rv is not None and rv > 0 and vg is not None
              and X["lo_delta"] <= dl <= X["hi_delta"] and bool(x.get("d")) and bool(x.get("t")))
        rows.append({"name": x.get("name"), "d": x.get("d"), "t": x.get("t"), "iv": iv, "rv90": rv, "vega": vg,
                     "ok": ok, "ratio": iv / rv if ok else None})
    out = []
    for i, x in enumerate(rows):
        res = {"name": x["name"], "peer_ratio": None, "peers": 0, "peer_set": "", "fair_iv": None,
               "gap_vol": None, "gap_px": None, "verdict": ""}
        if x["ok"]:
            pool = [j for j, y in enumerate(rows) if j != i and y["ok"]]
            g, label = [], "default ratio"
            for lab, keep in (("same D and T", lambda y: (y["d"], y["t"]) == (x["d"], x["t"])),
                              ("same D", lambda y: y["d"] == x["d"]),
                              ("whole paste", lambda y: True)):
                cand = [j for j in pool if keep(rows[j])]
                if len(cand) >= X["min_peers"]:
                    g, label = cand, lab
                    break
            pr = median([rows[j]["ratio"] for j in g]) if g else X["default_ratio"]
            fair = pr * x["rv90"]
            gv = fair - x["iv"]
            gp = gv * x["vega"]
            res.update(peer_ratio=pr, peers=len(g), peer_set=label, fair_iv=fair, gap_vol=gv, gap_px=gp,
                       verdict="cheap vs peers" if gp >= X["peer_line_pct"] else
                       ("rich vs peers" if gp <= -X["peer_line_pct"] else "in line with peers"))
        out.append(res)
    return out


# ------------------------------------------------------------------ input check
def input_screen(price: Optional[float], par: Optional[float], yrs: Optional[float], csprd_bp: Optional[float],
                 iv: Optional[float], rv90: Optional[float], delta: Optional[float], pv: Dict[str, Any],
                 P: Dict[str, float], X: Dict[str, float]) -> Dict[str, Any]:
    <<<TQ>>>Flag bonds whose CB Imp Vol is probably an input artefact, say which input moves it most, and what
    input value would bring the IV to the peer-fair level. Reasons (any one fires the flag):
      1. price below the bond floor at this CSprd (the credit input is too tight for this price);
      2. Delta% within lo_delta-hi_delta and IV/RV 90D outside ivr_lo-ivr_hi;
      3. Delta% within lo_delta-hi_delta and |peer-fair IV - IV| >= peer_iv_gap.
    <<<TQ>>>
    rf, cpn = P["rf"] / 100, P["cpn"] / 100
    o: Dict[str, Any] = {"flag": False, "reasons": [], "iv_spread": None, "iv_borrow": None, "first": "", "text": "", "sens_text": ""}
    have_px = None not in (price, par, yrs, csprd_bp) and par > 0 and yrs > 0 and csprd_bp >= 0
    if have_px:
        sp = csprd_bp / 10000
        base = solve_iv(price, par, yrs, rf, sp, cpn)
        if base is not None:
            w = solve_iv(price, par, yrs, rf, sp + X["bump_spread_bp"] / 10000, cpn)
            b = solve_iv(price, par, yrs, rf, sp, cpn, X["bump_borrow"])
            o["iv_spread"] = None if w is None else (w - base) * 100
            o["iv_borrow"] = None if b is None else (b - base) * 100
            if o["iv_spread"] is not None and o["iv_borrow"] is not None:
                o["sens_text"] = "IV %s pts per +%s bp CSprd; %s pts per +%s%% borrow" % (
                    sg(o["iv_spread"], 1), fx(X["bump_spread_bp"], 0), sg(o["iv_borrow"], 1), fx(X["bump_borrow"] * 100, 0))
        if price < bond_floor(yrs, cpn, rf + sp):
            o["reasons"].append("price below the bond floor at this CSprd")
    mid = delta is not None and X["lo_delta"] <= delta <= X["hi_delta"]
    if mid and iv is not None and rv90 is not None and rv90 > 0:
        r = iv / rv90
        if r < X["ivr_lo"] or r > X["ivr_hi"]:
            o["reasons"].append("IV/RV 90D %s outside %s-%s" % (fx(r, 2), fx(X["ivr_lo"], 2), fx(X["ivr_hi"], 2)))
    gv = pv.get("gap_vol")
    if mid and gv is not None and abs(gv) >= X["peer_iv_gap"]:
        o["reasons"].append("CB vol %s pts %s the peer-fair IV" % (fx(abs(gv), 1), "under" if gv > 0 else "over"))
    if o["reasons"]:
        o["flag"] = True
        sb = abs(o["iv_borrow"] or 0.0) / max(X["bump_borrow"] * 100, 1e-9)       # pts per 1% borrow
        ss = abs(o["iv_spread"] or 0.0) / max(X["bump_spread_bp"] / 100, 1e-9)    # pts per 100 bp
        o["first"] = "borrow" if sb > ss else "credit"
        expl = ""
        fair = pv.get("fair_iv")
        if have_px and fair:
            s_star = implied_spread(price, par, yrs, fair / 100, rf, cpn)
            q_star = implied_borrow(price, par, yrs, fair / 100, rf, csprd_bp / 10000, cpn)
            bits = []
            if s_star is not None:
                bits.append("CSprd %s bp (now %s)" % (fx(s_star * 10000, 0), fx(csprd_bp, 0)))
            if q_star is not None:
                bits.append("borrow %s%% (now 0%%)" % fx(q_star * 100, 1))
            if bits:
                expl = "; the peer-fair IV needs " + " or ".join(bits)
        o["text"] = "check inputs first: " + "; ".join(o["reasons"]) + "; check " + o["first"] + " first" + expl
    return o



# ------------------------------------------------------------------ v3.1 strategy, tags, exit triggers
FAMILY_ORDER_V31 = ["Convertible arbitrage", "Conversion and parity", "Rich vol", "Credit and yield", "Directional", "Mixed"]
LENS_OF = {"Convertible arbitrage: gamma": "hed", "Convertible arbitrage: vol": "hed",
           "Convertible arbitrage: credit-hedged": "hc", "Hedged vol": "hed",
           "Parity carry, hedged": "hed", "Conversion trade": "hed"}
TAG_OF = {
    "Convertible arbitrage: gamma": "GAMMA", "Convertible arbitrage: vol": "VOL",
    "Convertible arbitrage: credit-hedged": "VOL", "Hedged vol": "VOL",
    "Parity carry, hedged": "PARITY", "Conversion trade": "CONVERT",
    "Credit: distressed (capital structure)": "CREDIT", "Credit: busted": "CREDIT",
    "Yield to put": "YIELD", "Yield to maturity": "YIELD",
    "Carry only (yield below hurdle)": "COUPON", "Carry only": "COUPON",
    "Outright: stock substitute": "DELTA", "Outright: asymmetric long": "DELTA",
    "Rich vol: avoid, trim or switch": "AVOID", "Mixed: no clear edge": "NONE", "Not enough data": "NONE"}
TAG_GROUP = {"GAMMA": "hedged", "VOL": "hedged", "PARITY": "hedged", "CONVERT": "hedged",
             "CREDIT": "outright", "YIELD": "outright", "COUPON": "outright", "DELTA": "outright",
             "AVOID": "no trade", "NONE": "no trade"}
EXIT_TAGS = {
    "Convertible arbitrage: vol": ["PROFIT AT R3", "CUT AT S3"],
    "Convertible arbitrage: gamma": ["EXIT AT R3", "\u0394 OUTSIDE 30\u201370", "UNDER 6M"],
    "Convertible arbitrage: credit-hedged": ["EXIT AT R3", "CDS BASIS"],
    "Hedged vol": ["PROFIT AT R3", "CALL"],
    "Parity carry, hedged": ["CONVERT AT \u22641%", "CALL NOTICE"],
    "Conversion trade": ["CONVERT"],
    "Credit: distressed (capital structure)": ["RECOVERY VIEW"],
    "Credit: busted": ["CUT AT S3", "PRICE NEAR 100"],
    "Yield to put": ["CUT AT S3", "YIELD < HURDLE"], "Yield to maturity": ["CUT AT S3", "YIELD < HURDLE"],
    "Carry only (yield below hurdle)": ["SPREAD WIDENS", "STOCK RALLIES"], "Carry only": ["SPREAD WIDENS", "STOCK RALLIES"],
    "Rich vol: avoid, trim or switch": ["RE-ENTER AT R1\u2013R2"],
    "Outright: stock substitute": ["SWITCH AT P2", "SOFT CALL"],
    "Outright: asymmetric long": ["BECOMES D3P1", "SLIPS TO D1"],
    "Mixed: no clear edge": [], "Not enough data": []}
REGIME_TAGS = ["\u2191 STEADY UP", "\u2191 VOL RALLY", "\u2191 DIVERGE", "\u2191 UNEASY",
               "\u2193 PULLBACK", "\u2193 SELL-OFF", "\u2193 CREDIT GRIND", "\u2193 CRISIS"]


def _mk(name: str, family: str, lens: str, why: str, exit_: str) -> Dict[str, Any]:
    return {"name": name, "family": family, "lens": lens, "why": why, "exit": exit_}


def strategy_v31(r: Dict[str, Any], c: Dict[str, Any], P: Dict[str, float], X: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    <<<TQ>>>Spec strategy with the two v3.1 changes, plus its tag and exit-trigger tags.

    Rule 6 (credit-hedged arbitrage) needs T3; a shorter D2 S2 bond falls through to rules 7-11.
    D3 is hedged by default (unless X['directional']): premium up to conv_line -> Conversion trade;
    rest of P1 -> Parity carry, hedged; D3P2 with R1-R2 -> Hedged vol; D3P2 with R3 -> Rich vol.
    S3 keeps rule 1 (distressed credit) and missing data keeps rule 0.
    <<<TQ>>>
    X = dict(EXT_DEFAULTS, **(X or {}))
    st = dict(strategy(r, c, P))
    if st["name"] == "Convertible arbitrage: credit-hedged" and c["t"] != 3:
        c2 = dict(c)
        c2["s"] = 1                       # skips rule 6 only: for a D2 bond, rules 1-5 and 7-11 don't read S
        st = dict(strategy(r, c2, P))
        st["why"] += " Add CDS only for default protection; for S2 bonds of 3 years or less it doesn't cut day-to-day risk."
    price, parity = num(r.get("price")), num(r.get("parity"))
    if (c["d"] == 3 and c["p"] and c["s"] != 3 and price is not None and parity and not X["directional"]
            and st["name"] != "Not enough data"):
        prem = c["prem"] * 100
        lost = (price - parity) / price * 100
        if prem <= X["conv_line"]:
            st = _mk("Conversion trade", "Conversion and parity", "hed",
                     "D3 at %s%% over parity: bid parity \u2212 X, short the stock, convert; the edge is the discount "
                     "against accrued lost, borrow over the conversion lag, dividends and costs." % fx(prem, 1),
                     "Convert. Risk: a borrow recall or a dividend inside the lag.")
        elif c["p"] == 1:
            st = _mk("Parity carry, hedged", "Conversion and parity", "hed",
                     "D3P1 at %s%% premium (%s pts): hold delta-hedged; carry and the premium shrinking are the edge, "
                     "vol and gamma are small." % (fx(prem, 1), fx(price - parity, 1)),
                     "Convert at the conversion line (premium %s%% or less) or on a call notice; a forced conversion "
                     "costs %s%% of price." % (t1(X["conv_line"]), fx(lost, 1)))
        elif c["p"] == 2 and c["r"] in (1, 2):
            st = _mk("Hedged vol", "Convertible arbitrage", "hed",
                     "D3P2: with 15\u201340% premium, vol counts about as much as on D2; hold delta-hedged while CB vol isn't rich.",
                     "Take profit at R3. Risk: a call puts the whole premium at stake.")
        elif c["p"] == 2 and c["r"] == 3:
            st = _mk("Rich vol: avoid, trim or switch", "Rich vol", "out",
                     "D3P2 with rich CB vol (R3): don't pay for vol.", "Re-enter when CB vol cheapens to R1 or R2.")
    st["tag"] = TAG_OF.get(st["name"], "NONE")
    st["exit_tags"] = list(EXIT_TAGS.get(st["name"], []))
    return st


# ------------------------------------------------------------------ R margin and the read tags
def r_margin(r: Dict[str, Any], c: Dict[str, Any], P: Dict[str, float], X: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    <<<TQ>>>Vol pts IV must move to change R, and the RV 90D sampling noise of that line (line ratio x rv_noise x RV).<<<TQ>>>
    iv, rv = num(r.get("iv")), num(r.get("rv90"))
    if not c["r"] or iv is None or rv is None or rv <= 0:
        return None
    cheap, rich = P["r_cheap"] * rv, P["r_rich"] * rv
    if c["r"] == 1:
        m, line = cheap - iv, "cheap"
    elif c["r"] == 3:
        m, line = iv - rich, "rich"
    else:
        a, b = iv - cheap, rich - iv
        m, line = (a, "cheap") if a < b else (b, "rich")
    noise = (P["r_cheap"] if line == "cheap" else P["r_rich"]) * X["rv_noise"] * rv
    return {"margin": m, "line": line, "noise": noise, "inside": m < noise}


def read_tags(r: Dict[str, Any], c: Dict[str, Any], rm: Optional[Dict[str, Any]], P: Dict[str, float]) -> Dict[str, str]:
    <<<TQ>>>R tag (CHEAP/FAIR/RICH, '?' inside noise), 250D tag (verdict on RV 250D, check or cross vs R), RV-trend tag.<<<TQ>>>
    out = {"r": "", "r250": "", "trend": ""}
    if c["r"]:
        out["r"] = ["", "CHEAP", "FAIR", "RICH"][c["r"]] + ("?" if rm and rm["inside"] else "")
        iv, r250 = num(r.get("iv")), num(r.get("rv250"))
        if iv is not None and r250 is not None and r250 > 0:
            q = iv / r250
            v = 1 if q < P["r_cheap"] else (3 if q >= P["r_rich"] else 2)
            out["r250"] = "250D " + ["", "CHEAP", "FAIR", "RICH"][v] + (" \u2713" if v == c["r"] else " \u2717")
    if c.get("trend") in ("rising", "falling", "steady"):
        out["trend"] = {"rising": "RV \u2191", "falling": "RV \u2193", "steady": "RV \u2192"}[c["trend"]]
    return out


def trust_tags(c: Dict[str, Any], spec_flags: str, input_flag: bool, tag: str, rm: Optional[Dict[str, Any]],
               delta: Optional[float]) -> List[str]:
    t = []
    if input_flag:
        t.append("CHECK INPUTS")
    if tag in ("GAMMA", "VOL", "AVOID") and rm and rm["inside"]:
        t.append("R INSIDE NOISE")
    if c["near"]:
        t.append("NEAR " + ", ".join(c["near"]).upper())
    if "not confirmed by RV 250D" in spec_flags:
        t.append("250D DISAGREES")
    if delta is not None and (delta < 15 or delta > 90):
        t.append("LOW VEGA")
    if c["missing"]:
        t.append("MISSING DATA")
    return t or ["OK"]


# ------------------------------------------------------------------ funding and borrow per bond
def rates_for(name: str, assumptions: Optional[Dict[str, Any]], X: Dict[str, Any]) -> Tuple[float, float]:
    <<<TQ>>>(funding %, borrow %) for one bond: its own valid rate, else the book default, else EXT_DEFAULTS.
    assumptions = {"defaults": {"funding": f, "borrow": b}, "bonds": {name: {"funding": f, "borrow": b}}}.
    Valid: borrow 0-100, funding -10-50; anything else is ignored.<<<TQ>>>
    a = assumptions if isinstance(assumptions, dict) else {}
    d = a.get("defaults") if isinstance(a.get("defaults"), dict) else {}
    f = num(d.get("funding"))
    b = num(d.get("borrow"))
    f = f if f is not None and -10 <= f <= 50 else X["funding_default"]
    b = b if b is not None and 0 <= b <= 100 else X["borrow_default"]
    own = (a.get("bonds") or {}).get(name) if isinstance(a.get("bonds"), dict) else None
    if isinstance(own, dict):
        of, ob = num(own.get("funding")), num(own.get("borrow"))
        if of is not None and -10 <= of <= 50:
            f = of
        if ob is not None and 0 <= ob <= 100:
            b = ob
    return f, b


# ------------------------------------------------------------------ what-if: any view on one bond
VIEW_LIMITS = {"dS": (-0.95, 3.0), "dv": (-50.0, 50.0), "dc": (-1000.0, 3000.0), "dr": (-500.0, 500.0), "h": (0.0, 5.0)}


def clean_view(view: Dict[str, Any]) -> Dict[str, float]:
    <<<TQ>>>dS: stock move as a fraction (-0.20 = -20%); dv: implied vol move, vol pts; dc: credit move, bp;
    dr: rates move, bp; h: horizon in years (0 = instant). Missing or bad values are 0; values are clamped.<<<TQ>>>
    v = {}
    for k, (lo, hi) in VIEW_LIMITS.items():
        x = num((view or {}).get(k))
        v[k] = 0.0 if x is None else min(max(x, lo), hi)
    return v


def what_if_bond(r: Dict[str, Any], c: Dict[str, Any], view: Dict[str, Any], lens: str, funding_pct: float,
                 borrow_pct: float, P: Dict[str, float]) -> Optional[Dict[str, float]]:
    <<<TQ>>>P&L in % of the starting (model) price, split by factor, for one view.

    Repricing is sequential at the horizon: time (no moves) -> stock -> vol -> credit -> rates, so the parts add up
    to the total. lens 'out' is long the CB funded at funding_pct; 'hed' adds a static short of delta x stock,
    the short proceeds earn funding_pct and the short pays borrow_pct; 'hc' also removes the credit move and pays
    the CDS premium. Past the live date the bond is redeemed at 100 or converted, whichever is higher.
    With h = 0 and no funding or borrow it reproduces cbcode_v3.lens_chg exactly.<<<TQ>>>
    par, T, iv, cs = num(r.get("parity")), c.get("yrs"), num(r.get("iv")), num(r.get("csprd"))
    if None in (par, T, iv, cs) or par <= 0 or T <= 0 or iv <= 0 or cs < 0 or lens not in ("out", "hed", "hc"):
        return None
    v = clean_view(view)
    rf, cp = P["rf"] / 100, P["cpn"] / 100
    vol, sp, h = iv / 100, cs / 10000, v["h"]
    b0 = val(par, T, vol, rf, sp, cp)
    p0, d0 = b0["price"], b0["delta"]
    hedged = lens != "out"
    T2 = T - h
    alive = T2 > 1e-6
    par1, vol1 = par * (1 + v["dS"]), max(0.01, vol + v["dv"] / 100)
    sp1, r1 = max(0.0, sp + v["dc"] / 10000), rf + v["dr"] / 10000
    cpn_pts = coupons_paid(T, h) * cp * 100 if h > 0 else 0.0
    cr = 0.0
    if alive:
        pT = val(par, T2, vol, rf, sp, cp)["price"]
        pS = val(par1, T2, vol, rf, sp, cp)["price"]
        pV = val(par1, T2, vol1, rf, sp, cp)["price"]
        pC = val(par1, T2, vol1, rf, sp1, cp)["price"]
        pR = val(par1, T2, vol1, r1, sp1, cp)["price"]
        if lens == "hc":
            cr = val(par, T2, vol, rf, sp1, cp)["price"] - pT
    else:
        pT = max(par, 100.0)
        pS = pV = pC = pR = max(par1, 100.0)
    cds = 0.0
    if lens == "hc":
        cs1 = val(par, T, vol, rf, sp + 0.0001, cp)["price"] - p0
        ra = (1 - math.exp(-(rf + sp) * T)) / (rf + sp) if rf + sp > 0 else T
        cds = sp * max(-cs1 / 0.0001 / ra, 0.0) * h

    def pct(pts: float) -> float:
        return pts / p0 * 100
    out = {"stock": pct(pS - pT - (d0 * par * v["dS"] if hedged else 0.0)),
           "vol": pct(pV - pS),
           "credit": pct(pC - pV - cr),
           "rates": pct(pR - pC),
           "time": pct(pT + cpn_pts - p0 - cds),
           "funding": -pct(funding_pct / 100 * h * ((p0 - d0 * par) if hedged else p0)),
           "borrow": -pct(borrow_pct / 100 * h * d0 * par) if hedged else 0.0}
    out["total"] = sum(out[k] for k in ("stock", "vol", "credit", "rates", "time", "funding", "borrow"))
    return out


# ------------------------------------------------------------------ book: every new column
SECTIONS = [("Hedged vol", ("GAMMA", "VOL")), ("Parity", ("PARITY", "CONVERT")),
            ("Credit and yield", ("CREDIT", "YIELD", "COUPON")), ("Directional", ("DELTA",)),
            ("Rich vol", ("AVOID",)), ("No clear edge", ("NONE",))]


def _prepare(rows: Sequence[Dict[str, Any]], today: date, P: Dict[str, float], X: Dict[str, Any]) -> List[Dict[str, Any]]:
    rs = clean_rows(rows)
    cuts = v_cuts([r["rv90"] for r in rs])
    bk = []
    for i, r in enumerate(rs):
        c = code_row(r, today, P, cuts)
        st = strategy_v31(r, c, P, X)
        bk.append({"i": i, "r": r, "c": c, "st": st, "lens": st["lens"], "L": lens_chg(r, c, P)})
    bk.sort(key=lambda x: (x["c"]["d"], -x["c"]["p"], x["c"]["t"], x["r"]["name"]))
    return bk


def _category(bk: List[Dict[str, Any]], X: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    <<<TQ>>>Rank each bond in the 8 regimes against its D x P category (same D when the category has fewer than
    cat_min bonds), every member valued in THIS bond's lens. Top: no member strictly better; bottom: none worse.<<<TQ>>>
    out = {}
    for k, e in enumerate(bk):
        if e["L"] is None or not e["c"]["d"] or not e["c"]["p"]:
            continue
        same = [j for j, f in enumerate(bk) if f["L"] is not None and (f["c"]["d"], f["c"]["p"]) == (e["c"]["d"], e["c"]["p"])]
        if len(same) >= X["cat_min"]:
            grp, label = same, "D%dP%d" % (e["c"]["d"], e["c"]["p"])
        else:
            grp, label = [j for j, f in enumerate(bk) if f["L"] is not None and f["c"]["d"] == e["c"]["d"]], "D%d" % e["c"]["d"]
        if len(grp) < 2:
            continue
        lens = e["lens"]
        vals = {j: lens_of(bk[j]["L"], lens) for j in grp}
        mine = vals[k]
        avg = [sum(vals[j][m] for j in grp) / len(grp) for m in range(8)]
        edge = [mine[m] - avg[m] for m in range(8)]
        beat = max(range(8), key=lambda m: (edge[m], -m))
        lag = min(range(8), key=lambda m: (edge[m], m))
        out[k] = {"group": label, "n": len(grp),
                  "top": [m for m in range(8) if not any(vals[j][m] > mine[m] + 1e-12 for j in grp)],
                  "bottom": [m for m in range(8) if not any(vals[j][m] < mine[m] - 1e-12 for j in grp)],
                  "rank": [1 + sum(1 for j in grp if vals[j][m] > mine[m] + 1e-12) for m in range(8)],
                  "beat": beat, "beat_x": mine[beat], "beat_avg": avg[beat],
                  "lag": lag, "lag_x": mine[lag], "lag_avg": avg[lag]}
    return out


def book_v31(rows: Sequence[Dict[str, Any]], today: date, P: Optional[Dict[str, float]] = None,
             X: Optional[Dict[str, Any]] = None, assumptions: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    <<<TQ>>>All v3.1 columns, one dict per bond, ordered like cbcode_v3.run_book(clean_rows(rows), today)['rows'].<<<TQ>>>
    P = dict(DEFAULTS, **(P or {}))
    X = dict(EXT_DEFAULTS, **(X or {}))
    if not rows or today is None:
        return []
    bk = _prepare(rows, today, P, X)
    rf, cp = P["rf"] / 100, P["cpn"] / 100
    for e in bk:
        r, c = e["r"], e["c"]
        e["g"] = None
        if r["parity"] and r["parity"] > 0 and c["yrs"] and r["iv"] is not None and r["iv"] > 0 and r["csprd"] is not None and r["csprd"] >= 0:
            e["g"] = greeks(r["parity"], c["yrs"], r["iv"] / 100, rf, r["csprd"] / 10000, cp)
    pv = peer_value([{"name": e["r"]["name"], "d": e["c"]["d"], "t": e["c"]["t"], "delta": e["r"]["delta"],
                      "iv": e["r"]["iv"], "rv90": e["r"]["rv90"], "vega": e["g"]["vega"] if e["g"] else None} for e in bk], X)
    cat = _category(bk, X)
    spec_flags = _spec_flags(rows, today, P)
    out = []
    for k, e in enumerate(bk):
        r, c, st, g, v = e["r"], e["c"], e["st"], e["g"], pv[k]
        ic = input_screen(r["price"], r["parity"], c["yrs"], r["csprd"], r["iv"], r["rv90"], r["delta"], v, P, X)
        rm = r_margin(r, c, P, X)
        flags = spec_flags[k] if k < len(spec_flags) else ""
        f, b = rates_for(r["name"], assumptions, X)
        lens = e["lens"]
        held = lens_of(e["L"], lens) if e["L"] else None
        carry = what_if_bond(r, c, {"h": X["horizon"]}, lens, f, b, P)
        price, parity = r["price"], r["parity"]
        econ = None
        if g is not None:
            econ = {"vol_5pts": 5 * g["vega"],
                    "gamma_rent_month": gamma_rent(r["parity"], c["yrs"], r["iv"] / 100, rf, r["csprd"] / 10000, cp, r["iv"] / 100 + 0.05),
                    "one_pt": 100 / price if price else None,
                    "lost_if_called": (price - parity) / price * 100 if (c["d"] == 3 and price and parity) else None}
        if econ is not None and carry is not None:
            econ.update(time_coupon=carry["time"], funding=carry["funding"], borrow=carry["borrow"], carry_net=carry["total"],
                        funding_rate=f, borrow_rate=b if lens != "out" else None)
        peer_tag = "N/A" if not v["verdict"] else ("CHEAP" if v["gap_px"] >= X["peer_line_pct"] else ("RICH" if v["gap_px"] <= -X["peer_line_pct"] else "IN LINE"))
        out.append({
            "index": e["i"], "name": r["name"], "code": c["code"],
            "tag": st["tag"], "tag_group": TAG_GROUP[st["tag"]], "section": next(s for s, ts in SECTIONS if st["tag"] in ts),
            "strategy_v3": strategy(r, c, P)["name"], "strategy": st["name"], "family": st["family"], "lens": lens,
            "held_as": {"out": "OUTRIGHT", "hed": "HEDGED", "hc": "HEDGED+CDS"}[lens],
            "why": st["why"], "exit": st["exit"], "exit_tags": st["exit_tags"],
            "r_margin": rm, "read_tags": read_tags(r, c, rm, P),
            "vs_peers": v, "peer_tag": peer_tag,
            "input_check": ic["text"], "input_first": ic["first"], "iv_per_100bp": ic["iv_spread"], "iv_per_1pct_borrow": ic["iv_borrow"],
            "trust": trust_tags(c, flags, ic["flag"], st["tag"], rm, r["delta"]),
            "regimes_as_held": held,
            "worst_as_held": own_tiers(held)["worst"] if held else None,
            "makes_money_in": money_text(held) if held else "",
            "category": cat.get(k),
            "economics": econ,
            "premium_pts": (price - parity) if (price is not None and parity) else None,
            "premium_pct": c["prem"] * 100 if c["prem"] is not None else None,
            "yield": ytp(price, c["yrs"], P) if (c["d"] == 1 and price and c["yrs"]) else None,
            "hurdle_tag": (("\u2265 HURDLE" if ytp(price, c["yrs"], P) >= P["hurdle"] else "< HURDLE") if (c["d"] == 1 and price and c["yrs"]) else ""),
            "equity_share": g["elast"] if g else None,
        })
    return out


def _spec_flags(rows: Sequence[Dict[str, Any]], today: date, P: Dict[str, float]) -> List[str]:
    return [x["flags"] for x in run_book(clean_rows(rows), today, P)["rows"]]


def workspace(book: List[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    <<<TQ>>>The tag workspace: sections in a fixed order, rows ranked by the section's edge.<<<TQ>>>
    keys = {"Hedged vol": lambda x: -(x["vs_peers"]["gap_px"] if x["vs_peers"]["gap_px"] is not None else -1e9),
            "Parity": lambda x: x["premium_pct"] if x["premium_pct"] is not None else 1e9,
            "Credit and yield": lambda x: -(x["yield"] if x["yield"] is not None else -1e9),
            "Directional": lambda x: -(x["equity_share"] or 0),
            "Rich vol": lambda x: -(x["r_margin"]["margin"] if x["r_margin"] else -1e9),
            "No clear edge": lambda x: 0}
    return [(s, sorted([x for x in book if x["section"] == s], key=keys[s])) for s, _ in SECTIONS]


# ------------------------------------------------------------------ what-if: the whole book
def what_if(rows: Sequence[Dict[str, Any]], today: date, view: Dict[str, Any], lens_mode: str = "held",
            assumptions: Optional[Dict[str, Any]] = None, P: Optional[Dict[str, float]] = None,
            X: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    <<<TQ>>>Every bond under one view. lens_mode: 'held' (each bond's v3.1 lens), 'out' or 'hed'.
    Returns ranked rows (best first) with the P&L split and the bond's rank in its category for this view,
    the equal-weight book average, the count of bonds that gain, and averages by tag (best tag first).<<<TQ>>>
    P = dict(DEFAULTS, **(P or {}))
    X = dict(EXT_DEFAULTS, **(X or {}))
    if lens_mode not in ("held", "out", "hed"):
        raise ValueError("lens_mode must be 'held', 'out' or 'hed'")
    empty = {"rows": [], "skipped": [], "book_avg": None, "n_up": 0, "by_tag": []}
    if not rows or today is None:
        return empty
    bk = _prepare(rows, today, P, X)
    cat = _category(bk, X)
    res, skipped = [], []
    for k, e in enumerate(bk):
        lens = e["lens"] if lens_mode == "held" else lens_mode
        f, b = rates_for(e["r"]["name"], assumptions, X)
        w = what_if_bond(e["r"], e["c"], view, lens, f, b, P)
        if w is None:
            skipped.append(e["r"]["name"])
            continue
        res.append({"name": e["r"]["name"], "tag": e["st"]["tag"], "lens": lens, "group": cat[k]["group"] if k in cat else "D%d" % e["c"]["d"], **w})
    res.sort(key=lambda x: (-x["total"], x["name"]))
    groups = {}
    for x in res:
        groups.setdefault(x["group"], []).append(x)
    for g in groups.values():
        for i, x in enumerate(g):
            x["cat_rank"], x["cat_n"] = i + 1, len(g)
    tags = {}
    for x in res:
        tags.setdefault(x["tag"], []).append(x)
    by_tag = sorted([{"tag": t, "n": len(a), "avg": sum(y["total"] for y in a) / len(a), "best": a[0]["name"], "worst": a[-1]["name"]}
                     for t, a in tags.items()], key=lambda d: -d["avg"])
    return {"rows": res, "skipped": skipped, "book_avg": (sum(x["total"] for x in res) / len(res)) if res else None,
            "n_up": sum(1 for x in res if x["total"] > 0.005), "by_tag": by_tag}
"""


def _load_cbcode():
    import types as _types
    try:
        import cbcode_v3 as m3                     # external files, if the desk keeps them
        import cbcode_v31 as m31
        return m3, m31, ""
    except ImportError:
        pass
    try:
        m3 = _types.ModuleType("cbcode_v3")
        exec(compile(_CBCODE_V3_SRC.replace("<<<TQ>>>", '"' * 3), "cbcode_v3.py", "exec"), m3.__dict__)
        sys.modules.setdefault("cbcode_v3", m3)
        m31 = _types.ModuleType("cbcode_v31")
        exec(compile(_CBCODE_V31_SRC.replace("<<<TQ>>>", '"' * 3), "cbcode_v31.py", "exec"), m31.__dict__)
        sys.modules.setdefault("cbcode_v31", m31)
        return m3, m31, ""
    except Exception as exc:  # pragma: no cover - the band stays blank with the reason
        return None, None, "cbcode engine failed to load: %s" % exc


_CBV3, _CBV31, CLASS_ERR = _load_cbcode()
_CLASS = {"key": None, "res": None, "ts": 0.0, "lock": threading.Lock()}
CLASS_MEMO_SEC = 10.0


def _rv_series(px, n):
    """Realised vol (annualised, %) from the last n+1 closes - same formula as the VOL band."""
    try:
        px = [float(x) for x in (px or [])]
    except Exception:
        return None
    if len(px) < n + 1:
        return None
    tail = px[-(n + 1):]
    rets = []
    for i in range(1, len(tail)):
        a, b = tail[i - 1], tail[i]
        if a > 0 and b > 0:
            rets.append(math.log(b / a))
    if len(rets) < 2:
        return None
    m = sum(rets) / len(rets)
    v = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
    return math.sqrt(v) * math.sqrt(252) * 100


_RV_CACHE = {}


def _rv_cached(key, px, n):
    """RV for one (security, series) - the close history changes once a day, so the Python loop runs once per
    day per security and the class engine's input build costs only dict lookups afterwards."""
    try:
        if isinstance(px, dict):                       # test fixture path: RVs supplied directly
            return px.get(n)
        k = (key, n, len(px), float(px[-1]) if px else None)
    except Exception:
        return _rv_series(px, n)
    v = _RV_CACHE.get(k)
    if v is None and k not in _RV_CACHE:
        v = _rv_series(px, n)
        if len(_RV_CACHE) > 20000:
            _RV_CACHE.clear()
        _RV_CACHE[k] = v
    return v


def _class_date(s):
    try:
        return date.fromisoformat(str(s)[:10]) if s else None
    except Exception:
        return None


def _class_parity(ref, row, nk):
    """PARITY% as the grid computes it: vanilla spot/CP; else (spot/fx)/(CP/fixedFX)."""
    cp = _fnum0(ref.get("lp_conversion_price")) or _fnum0(ref.get("conversion_price"))
    ff = _fnum0(ref.get("conversion_fixed_fx")) or 1.0
    sp = _fnum0(row.get("ovdSpot"))
    if sp is None:
        sp = _fnum0(nk.get("nSpot"))
    fx = _fnum0(row.get("ovdUndFx"))
    if fx is None:
        fx = _fnum0(nk.get("nSpotFx"))
    if not cp or cp <= 0 or sp is None:
        return None
    if str(row.get("bond_type") or "").strip().lower().startswith("vanil"):
        return sp / cp * 100
    if fx is None or fx <= 0 or ff <= 0:
        return None
    return (sp / fx) / (cp / ff) * 100


def _class_price(nk):
    """CB price: mid of the override result (QuoteBid/QuoteAsk basis), else the live mid."""
    for lo, hi in (("ovdMktBid", "ovdMktAsk"), ("nBid", "nAsk")):
        a, b = _fnum0(nk.get(lo)), _fnum0(nk.get(hi))
        if a is not None and b is not None and a > 0 and b > 0:
            return (a + b) / 2
        if a is not None and a > 0 and b is None:
            return a
    return None


def class_inputs(today=None):
    """One spec row per loaded security, from the live grid. Returns (rows, sec_ids) aligned by index."""
    today = today or date.today()
    rows, sids = [], []
    refrows = STATE.get("refRows") or {}
    for sid in STATE["ids"]:
        row = STATE["rows"].get(sid, {}) or {}
        nk = STATE["nuke"].get(sid, {}) or {}
        ref = refrows.get(int(sid), {}) or {}
        nd = _fnum0(nk.get("nDelta"))
        if nd is not None and abs(nd) <= 1.5:
            nd = nd * 100                                   # fraction -> %
        iv = _fnum0(ref.get("implied_vol"))
        if iv is not None and iv <= 1.5:
            iv = iv * 100
        if iv is None or iv <= 0:
            iv = _fnum0(row.get("iv_manual"))              # manual vol (already in %)
        cs = _fnum0(ref.get("credit_spread_used"))
        if cs is not None and abs(cs) <= 30:
            cs = cs * 10000                                 # decimal -> bp
        isin = str(ref.get("isin") or row.get("isin_manual") or "").strip().upper()
        bd = (_BD["data"].get(isin) or {}) if isin else {}
        put = _class_date(bd.get("put"))
        exp = _class_date(bd.get("expiry")) or _class_date(ref.get("expiry_date"))
        if put is None and _fnum0(ref.get("years_to_put")):
            put = today + timedelta(days=int(round(_fnum0(ref.get("years_to_put")) * 365.25)))
        px = ref.get("px_hist") or []
        rows.append({"name": str(row.get("short_name") or ref.get("short_name") or sid), "delta": nd,
                     "parity": _class_parity(ref, row, nk), "price": _class_price(nk), "prem": None, "csprd": cs,
                     "put": put, "exp": exp, "iv": iv,
                     "rv10": _rv_cached(sid, px, 10), "rv30": _rv_cached(sid, px, 30), "rv90": _rv_cached(sid, px, 90), "rv250": _rv_cached(sid, px, 250)})
        sids.append(int(sid))
    return rows, sids


def _class_fingerprint(assumptions):
    bits = []
    for sid in STATE["ids"]:
        nk = STATE["nuke"].get(sid, {}) or {}
        row = STATE["rows"].get(sid, {}) or {}
        bits.append((sid, nk.get("nBid"), nk.get("nDelta"), nk.get("ovdMktBid"), nk.get("ovdMktAsk"), nk.get("nSpot"),
                     row.get("ovdSpot"), row.get("ovdUndFx"), row.get("iv_manual"), row.get("bond_type")))
    return json.dumps([bits, len(STATE.get("refRows") or {}), len(_BD["data"]), assumptions or {}, date.today().isoformat()],
                      sort_keys=True, default=str)


_CLASS_CHILD = r"""
import sys, json, types, datetime
def main():
    m31 = None
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        job = json.loads(line)
        try:
            if job["op"] == "init":
                m3 = types.ModuleType("cbcode_v3"); exec(compile(job["v3"], "cbcode_v3.py", "exec"), m3.__dict__); sys.modules["cbcode_v3"] = m3
                m31 = types.ModuleType("cbcode_v31"); exec(compile(job["v31"], "cbcode_v31.py", "exec"), m31.__dict__); sys.modules["cbcode_v31"] = m31
                out = {"ok": True}
            else:
                D = lambda s: datetime.date.fromisoformat(s) if s else None
                rows = [dict(r, put=D(r.get("put")), exp=D(r.get("exp"))) for r in job["rows"]]
                today = D(job["today"])
                if job["op"] == "book":
                    book = m31.book_v31(rows, today, None, None, job.get("assumptions"))
                    out = {"ok": True, "book": book, "ws": [[s, [x["index"] for x in rs]] for s, rs in m31.workspace(book)]}
                else:
                    out = {"ok": True, "w": m31.what_if(rows, today, job.get("view") or {}, job.get("lens") or "held", job.get("assumptions"))}
        except Exception as exc:
            out = {"ok": False, "error": str(exc)[:300]}
        sys.stdout.write(json.dumps(out, default=str) + "\n"); sys.stdout.flush()
main()
"""
_CC = {"proc": None, "lock": threading.Lock(), "fails": 0}


def _class_child_start():
    p = subprocess.Popen([sys.executable, "-u", "-c", _CLASS_CHILD], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, text=True, encoding="utf-8", bufsize=1)
    p.stdin.write(json.dumps({"op": "init", "v3": _CBCODE_V3_SRC.replace("<<<TQ>>>", '"' * 3),
                              "v31": _CBCODE_V31_SRC.replace("<<<TQ>>>", '"' * 3)}) + "\n")
    p.stdin.flush()
    ack = json.loads(p.stdout.readline() or "{}")
    if not ack.get("ok"):
        p.kill()
        raise RuntimeError("class child init failed")
    return p


def _class_child(op, rows, today, **kw):
    """Run the engine in one long-lived child interpreter (started on first use, restarted if it dies) so the
    engine's CPU never touches this process's GIL or event loop. Returns the result, or None when the child is
    unavailable (the caller falls back to in-process)."""
    if os.environ.get("NUKE_CLASS_INPROC") == "1" or _CC["fails"] >= 3:
        return None
    job = {"op": op, "rows": [{k: (v.isoformat() if isinstance(v, date) else v) for k, v in r.items()} for r in rows],
           "today": today.isoformat()}
    job.update(kw)
    with _CC["lock"]:
        try:
            p = _CC["proc"]
            if p is None or p.poll() is not None:
                p = _CC["proc"] = _class_child_start()
            p.stdin.write(json.dumps(job, default=str) + "\n")
            p.stdin.flush()
            out = json.loads(p.stdout.readline() or "{}")
            if not out.get("ok"):
                logger.warning("class child error: %s", out.get("error"))
                return None
            return out
        except Exception as exc:
            _CC["fails"] += 1
            logger.warning("class child unavailable (%d): %s", _CC["fails"], exc)
            try:
                if _CC["proc"]:
                    _CC["proc"].kill()
            except Exception:
                pass
            _CC["proc"] = None
            return None


def class_book(assumptions=None, today=None):
    """Per-security v3.1 columns + the workspace order. Memoised for CLASS_MEMO_SEC on the input fingerprint."""
    if _CBV31 is None:
        return {"ok": False, "error": CLASS_ERR, "rows": {}, "sections": [], "names": {}}
    key = _class_fingerprint(assumptions)
    with _CLASS["lock"]:
        if _CLASS["key"] == key and time.time() - _CLASS["ts"] < CLASS_MEMO_SEC:
            return _CLASS["res"]
    today = today or date.today()
    rows, sids = class_inputs(today)
    t0 = time.perf_counter()
    child = _class_child("book", rows, today, assumptions=assumptions or {}) if rows else {"book": [], "ws": []}
    if child is None:                                   # fallback: in-process (worker thread)
        book = _CBV31.book_v31(rows, today, None, None, assumptions) if rows else []
        ws_idx = [[s, [x["index"] for x in rs]] for s, rs in _CBV31.workspace(book)]
        engine = "thread"
    else:
        book, ws_idx, engine = child["book"], child["ws"], "child"
    by_sid = {}
    for x in book:
        sid = sids[x["index"]]
        rm = x.get("r_margin") or {}
        econ = x.get("economics") or {}
        cat = x.get("category") or {}
        wrst = x.get("worst_as_held")
        held = x.get("regimes_as_held") or []
        by_sid[sid] = dict(x, sec_id=sid,
                           r_margin_txt=("%s pts %s the %s line; noise %s" % (_CBV3.fx(rm["margin"], 2), "past" if rm["margin"] >= 0 else "from", rm["line"], _CBV3.fx(rm["noise"], 1))) if rm else "",
                           worst_txt=("%s %s%%" % (_CBV31.REGIME_TAGS[wrst], _CBV3.sg(held[wrst], 1))) if (wrst is not None and held) else "",
                           cat_txt=("%s (%d)" % (cat["group"], cat["n"])) if cat else "",
                           carry_txt=("%s%%" % _CBV3.sg(econ["carry_net"], 2)) if econ.get("carry_net") is not None else "")
    sections = [(s, [sids[i] for i in idxs]) for s, idxs in ws_idx] if book else [(s, []) for s, _ in _CBV31.SECTIONS]
    res = {"ok": True, "today": today.isoformat(), "n": len(book), "ms": round((time.perf_counter() - t0) * 1000, 1), "engine": engine,
           "rows": by_sid, "sections": sections, "names": {sids[i]: r["name"] for i, r in enumerate(rows)},
           "inputs": {sids[i]: {k: (v.isoformat() if isinstance(v, date) else v) for k, v in r.items()} for i, r in enumerate(rows)},
           "stamp": hashlib.md5(key.encode("utf-8")).hexdigest()[:12]}
    with _CLASS["lock"]:
        _CLASS.update(key=key, res=res, ts=time.time())
    return res


def _class_assump(a):
    try:
        d = json.loads(a) if a else {}
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


CLASS_PAGE = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nuke Station · CLASS workspace (CB Code v3.1)</title>
<style>
:root{--mono:Consolas,"Courier New",monospace;--ui:"Segoe UI",system-ui,sans-serif;--bg:#fff;--text:#111;--muted:#6b7280;--border:#e3e6ea;--panel:#f8f8f8;--hed:#2340B8;--out:#7A5F12;--gain:#13786C;--loss:#B23A62}
@media (prefers-color-scheme: dark){body.auto{--bg:#15171c;--text:#e6e6e6;--muted:#9aa3b2;--border:#2b3038;--panel:#1d2026;--hed:#A9BBFF;--out:#E5C873;--gain:#4CC3B1;--loss:#F0789D}}
body{margin:0;background:var(--bg);color:var(--text);font:12px var(--ui)}
.hdr{display:flex;align-items:center;gap:12px;padding:6px 12px;background:#0b1f3a;color:#fff;position:sticky;top:0;z-index:5}.hdr b{font-size:13px}.hdr .sm{color:#9fb3c8;font-size:11px}.hdr .tabs{margin-left:auto;display:flex;gap:4px}
.hdr button{font:12px var(--ui);padding:3px 10px;border:1px solid #395a86;background:#143259;color:#fff;border-radius:3px;cursor:pointer}.hdr button.on{background:#e8eefc;color:#0b1f3a;font-weight:700}
.bar{display:flex;flex-wrap:wrap;gap:14px;align-items:center;padding:6px 12px;border-bottom:1px solid var(--border);background:var(--panel)}.bar label{color:var(--muted)}.bar input{width:56px;font:12px var(--mono);padding:2px 4px}
.key{padding:4px 12px;color:var(--muted);font-size:11px;border-bottom:1px solid var(--border)}
.sec{margin:10px 12px 16px}.sec h2{font:700 13px var(--ui);margin:0 0 2px}.sec .q{color:var(--muted);font-size:11px;margin-bottom:4px}.sec .sumline{font-size:11.5px;margin:0 0 6px;color:var(--text)}
table{border-collapse:collapse;width:100%;font:11px var(--ui)}th{text-align:left;font-size:9.5px;letter-spacing:.4px;text-transform:uppercase;color:var(--muted);padding:3px 6px;border-bottom:2px solid #888;white-space:nowrap}
td{padding:4px 6px;border-bottom:1px solid var(--border);vertical-align:top;max-width:280px}tr:hover td{background:rgba(127,127,127,.06)}td.mono{font-family:var(--mono)}
.ct{display:inline-block;padding:0 6px;border-radius:3px;font:700 9.5px var(--ui);letter-spacing:.3px;line-height:15px;margin:1px 3px 1px 0;white-space:nowrap}
.ct.h{background:var(--hed);color:#fff}.ct.o{background:var(--out);color:#fff}.ct.n{border:1px dashed #9ca3af;color:var(--muted)}.ct.tr{background:rgba(127,127,127,.18)}.ct.bad{background:rgba(178,58,98,.18);color:var(--loss)}.ct.ok{background:rgba(19,120,108,.18);color:var(--gain)}
.ct.chp{background:rgba(19,120,108,.16);color:var(--gain)}.ct.rch{background:rgba(178,58,98,.16);color:var(--loss)}.ct.q{border:1px dashed currentColor;background:transparent}.ct.rg{background:rgba(19,120,108,.14);color:var(--gain)}.ct.rr{background:rgba(178,58,98,.14);color:var(--loss)}.ct.ex{border:1px solid #cbd5e1;background:transparent;color:var(--muted);font-weight:600}.ct.nd{border:1px dashed #9ca3af;color:var(--muted);font-weight:500}
.code{font:700 10px var(--mono);letter-spacing:.5px}.code b{display:inline-block;min-width:11px;text-align:center}.strike{text-decoration:line-through;color:var(--muted);font-size:10px}
.gain{color:var(--gain)}.loss{color:var(--loss)}.muted{color:var(--muted)}.carry b{font-size:12px}.carry input{width:44px;font:11px var(--mono);padding:1px 3px;margin:0 2px}.carry input.ov{color:var(--hed);font-weight:700}.carry input.bad{border-color:var(--loss);background:rgba(178,58,98,.12)}.carry input:disabled{opacity:.45}
.sub{color:var(--muted);font-size:10.5px}.none{color:var(--muted);font-style:italic;padding:6px}
#wi{display:none;padding:10px 12px}#wi.on{display:block}#ws.off{display:none}
.ctrl{display:grid;grid-template-columns:120px 1fr 70px;gap:6px 10px;align-items:center;max-width:620px}.ctrl input[type=range]{width:100%}.ctrl input[type=number]{width:64px;font:12px var(--mono)}
.presets{display:flex;flex-wrap:wrap;gap:4px;margin:8px 0}.presets button,.lens button{font:11px var(--ui);padding:2px 8px;border:1px solid #9ca3af;background:var(--panel);color:var(--text);border-radius:3px;cursor:pointer}.lens button.on{background:var(--hed);color:#fff;border-color:var(--hed)}
.head{font-size:13px;margin:8px 0}.bar2{display:inline-block;height:10px;background:var(--gain)}.bar2.neg{background:var(--loss)}.rk{font:700 10px var(--mono);color:var(--muted)}.top3 td:first-child{border-left:3px solid var(--gain)}.bot3 td:first-child{border-left:3px solid var(--loss)}
.note{color:var(--muted);font-size:11px;max-width:900px;margin-top:10px}
.drawer{position:fixed;right:0;top:34px;bottom:0;width:min(520px,95vw);background:var(--bg);border-left:1px solid var(--border);box-shadow:-4px 0 14px rgba(0,0,0,.15);overflow:auto;padding:10px 14px;display:none;z-index:6;font-size:11.5px}.drawer.on{display:block}.drawer h3{margin:8px 0 2px;font-size:12px}.drawer .x{float:right;cursor:pointer;font-weight:700}
</style></head><body class="auto">
<div class="hdr"><b>NUKE STATION · CLASS</b><span class="sm" id="hsub">CB Code v3.1 workspace</span><div class="tabs"><button id="tabws" class="on" onclick="showTab('ws')">Workspace</button><button id="tabwi" onclick="showTab('wi')">What-if</button><button onclick="loadBook(true)" title="recompute from the live grid now">&#8635; refresh</button><button onclick="exportCsv()" title="every column, CSV">export CSV</button></div></div>
<div class="bar"><label>Default borrow <input id="defb" type="number" step="0.1" min="0" max="100"> %</label><label>Default funding <input id="deff" type="number" step="0.1" min="-10" max="50"> %</label><button onclick="clearOverrides()">Clear bond overrides</button><span id="savest" class="muted">Saved in this browser.</span><span class="muted" style="margin-left:auto" id="meta"></span></div>
<div class="key">Chips: <span class="ct h">HEDGED GROUP</span> <span class="ct o">OUTRIGHT GROUP</span> <span class="ct n">NO TRADE</span> &nbsp; <span class="ct chp">CHEAP</span> <span class="ct rch">RICH</span> <span class="ct tr q">? = inside RV noise</span> &nbsp; <span class="ct rg">&#8593; best in category</span> <span class="ct rr">&#8595; worst in category</span> &nbsp; <span class="ct ex">EXIT TRIGGER</span> <span class="ct nd">needs data</span> &nbsp; code digits: grey 0 · slate 1 · blue 2 · red 3 (R: green 1 · blue 2 · red 3)</div>
<div id="ws"></div>
<div id="wi">
  <div class="ctrl">
    <label>Stock (%)</label><input type="range" id="r_dS" min="-50" max="50" step="1" value="0" oninput="syncN('dS',this.value)"><input type="number" id="n_dS" min="-50" max="50" step="1" value="0" onchange="syncR('dS',this.value)">
    <label>Implied vol (pts)</label><input type="range" id="r_dv" min="-20" max="20" step="1" value="0" oninput="syncN('dv',this.value)"><input type="number" id="n_dv" min="-20" max="20" step="1" value="0" onchange="syncR('dv',this.value)">
    <label>Credit spread (bp)</label><input type="range" id="r_dc" min="-300" max="600" step="25" value="0" oninput="syncN('dc',this.value)"><input type="number" id="n_dc" min="-300" max="600" step="25" value="0" onchange="syncR('dc',this.value)">
    <label>Rates (bp)</label><input type="range" id="r_dr" min="-200" max="200" step="25" value="0" oninput="syncN('dr',this.value)"><input type="number" id="n_dr" min="-200" max="200" step="25" value="0" onchange="syncR('dr',this.value)">
    <label>Horizon (months)</label><input type="range" id="r_h" min="0" max="12" step="1" value="0" oninput="syncN('h',this.value)"><input type="number" id="n_h" min="0" max="12" step="1" value="0" onchange="syncR('h',this.value)">
  </div>
  <div class="presets" id="presets"></div>
  <div class="lens"><span class="muted">Lens:</span> <button id="l_held" class="on" onclick="setLens('held')">As its strategy holds it</button> <button id="l_out" onclick="setLens('out')">All outright</button> <button id="l_hed" onclick="setLens('hed')">All delta-hedged</button></div>
  <div id="wires"></div>
  <p class="note">Same model as the spec (section 7): no calls, puts, dividends or default, so a call or a default never shows up here. The stock move is the total move over the horizon; vol, credit and rates moves are where they stand at the horizon. The hedge is set once at the start. Funding and borrow use each bond's own rates and don't move with the rates shock. Equal weights, since positions aren't loaded.</p>
</div>
<div class="drawer" id="drawer"><span class="x" onclick="document.getElementById('drawer').classList.remove('on')">&#10005;</span><div id="dbody"></div></div>
<script>
const BASE=location.pathname.replace(/\/class\/?$/,"");
const REG=["\u2191 STEADY UP","\u2191 VOL RALLY","\u2191 DIVERGE","\u2191 UNEASY","\u2193 PULLBACK","\u2193 SELL-OFF","\u2193 CREDIT GRIND","\u2193 CRISIS"];
const PRESETS=[["Steady grind higher",10,-8,-50],["Volatile rally",10,8,-50],["Equity\u2013credit divergence",10,-8,100],["Uneasy rally",10,8,100],["Gentle pullback",-15,-8,-50],["Sell-off without credit stress",-15,8,-50],["Slow credit grind",-15,-8,100],["Crisis (2008-style)",-15,8,100]];
const SECTIONS=[["Hedged vol","GAMMA, VOL","peer gap in % of price, cheapest first","Is the vol really cheap, and how will it pay?"],["Parity","PARITY, CONVERT","premium %, closest to conversion first","Hold it hedged, convert it, or watch the call?"],["Credit and yield","CREDIT, YIELD, COUPON","yield, highest first","Is the credit paid enough?"],["Directional","DELTA","equity share, highest first","Direction, with what cushion?"],["Rich vol","AVOID","R margin, most clearly rich first","Trim now, or is the rich call marginal?"],["No clear edge","NONE","book order","Why is there nothing to do?"]];
let BOOK=null, ROWS={}, LENS="held", VIEW={dS:0,dv:0,dc:0,dr:0,h:0}, saveT=null, pending=false;
const esc=s=>String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const sg=(x,d)=>{ if(x==null||!isFinite(x)) return ""; const r=Math.abs(x).toFixed(d); return (Number(r)===0?"":(x>0?"+":"\u2212"))+r; };
const pct=(x,d)=>sg(x,d==null?2:d)+"%";
function assump(){ try{ const j=JSON.parse(localStorage.getItem("nukestation.class.assump")||"{}"); return (j&&typeof j==="object")?j:{}; }catch(e){ return {}; } }
function setAssump(a){ localStorage.setItem("nukestation.class.assump", JSON.stringify(a)); const s=document.getElementById("savest"); if(s) s.textContent="Saved in this browser."; }
function defaults(){ const a=assump(); const d=a.defaults||{}; const b=Number(d.borrow), f=Number(d.funding); return {borrow:(isFinite(b)&&b>=0&&b<=100)?b:0.5, funding:(isFinite(f)&&f>=-10&&f<=50)?f:4.0}; }
function tagChip(t){ const g=["GAMMA","VOL","PARITY","CONVERT"].includes(t)?"h":(["CREDIT","YIELD","COUPON","DELTA"].includes(t)?"o":"n"); return '<span class="ct '+g+'">'+t+'</span>'; }
function codeHtml(code){ let o=""; for(let i=0;i<code.length;i++){ const ch=code[i]; if(/\d/.test(ch)){ const lvl=+ch; const col=(code[i-1]==="R")?{0:"#9ca3af",1:"#15803d",2:"#1d4ed8",3:"#b91c1c"}[lvl]:{0:"#9ca3af",1:"#64748b",2:"#1d4ed8",3:"#b91c1c"}[lvl]; o+='<b style="color:'+col+'">'+ch+'</b>'; } else o+=ch; } return '<span class="code">'+o+'</span>'; }
function regChips(list,cls){ if(!list||!list.length) return '<span class="muted">none</span>'; const s=list.slice(0,3).map(i=>'<span class="ct '+cls+'">'+REG[i]+'</span>').join(""); return s+(list.length>3?' <span class="muted">+'+(list.length-3)+'</span>':""); }
function catTxt(c,edge){ if(!c) return ""; const ahead=(c.beat_x-c.beat_avg)>=edge?("most ahead: "+REG[c.beat]+" "+sg(c.beat_x-c.beat_avg,1)+" pts"):("never "+edge+" pts ahead"); const behind=(c.lag_avg-c.lag_x)>=edge?("most behind: "+REG[c.lag]+" "+sg(c.lag_x-c.lag_avg,1)+" pts"):("never "+edge+" pts behind"); return [ahead,behind]; }
function volSig(x){ const rt=x.read_tags||{}; const low=x.trust.includes("LOW VEGA"); const chips=[rt.r?'<span class="ct '+(rt.r.startsWith("CHEAP")?"chp":rt.r.startsWith("RICH")?"rch":"tr")+(rt.r.endsWith("?")?" q":"")+'">'+rt.r+'</span>':"", rt.r250?'<span class="ct tr">'+rt.r250+'</span>':"", rt.trend?'<span class="ct tr">'+rt.trend+'</span>':""].join(""); return '<div'+(low?' style="opacity:.55"':'')+'>'+chips+'</div><div class="sub">'+(low?"little vega: not decisive":esc(x.r_margin_txt))+'</div>'; }
function peers(x){ const v=x.vs_peers||{}; if(x.peer_tag==="N/A"||!x.peer_tag) return '<span class="ct n">N/A</span><div class="sub">no verdict</div>'; const c=x.peer_tag==="CHEAP"?"chp":x.peer_tag==="RICH"?"rch":"tr"; return '<span class="ct '+c+'">'+x.peer_tag+'</span><div class="sub">'+sg(v.gap_vol,1)+' pts, '+sg(v.gap_px,1)+'%</div>'; }
function trust(x){ return (x.trust||[]).map(t=>'<span class="ct tr'+(t==="OK"?" ok":t==="CHECK INPUTS"?" bad":"")+'">'+t+'</span>').join(""); }
function volEdge(e){ if(!e) return '<span class="ct nd">needs data</span>'; return "5 vol pts "+pct(e.vol_5pts,2)+"; gamma "+pct(e.gamma_rent_month,3)+" a month"; }
function carryCell(x){ const e=x.economics; const a=assump(); const own=(a.bonds||{})[x.name]||{}; const d=defaults(); const hedged=x.lens!=="out";
  const box=(k,v,dis)=>'<input type="number" step="0.1" data-k="'+k+'" data-n="'+esc(x.name)+'" value="'+(v==null?"":v)+'" placeholder="'+(k==="borrow"?d.borrow:d.funding)+'" class="'+(v==null?"":"ov")+'"'+(dis?' disabled title="Outright: no short stock, so no borrow"':'')+' oninput="rateTyped(this)">';
  if(!e||e.carry_net==null) return '<span class="ct nd">needs data</span>';
  return '<div class="carry"><b class="'+(e.carry_net>0?"gain":e.carry_net<0?"loss":"")+'">'+pct(e.carry_net,3)+'</b> <span class="sub">borrow'+box("borrow",own.borrow,!hedged)+' funding'+box("funding",own.funding,false)+'</span><div class="sub">time and coupon '+sg(e.time_coupon,3)+', funding '+sg(e.funding,3)+', borrow '+sg(e.borrow,3)+'</div></div>'; }
function secCols(sec,x){ const e=x.economics||{}; const yrs=x.yrs; const toP=(x.end_is_put?"put":"expiry");
  if(sec==="Hedged vol") return ["<td>"+volEdge(e.vol_5pts!=null?e:null)+"</td>"];
  if(sec==="Parity") return ["<td>"+(x.premium_pts!=null?sg(x.premium_pts,1)+" pts over parity; "+sg(x.premium_pct,1)+"%":"")+"</td><td>"+(e.one_pt!=null?"1 pt "+pct(e.one_pt,2)+(e.lost_if_called!=null?"; if called "+pct(-Math.abs(e.lost_if_called),1):"; <span class='ct nd'>needs call terms</span>"):"")+"</td><td>"+volEdge(e.vol_5pts!=null?e:null)+"</td>"];
  if(sec==="Credit and yield") return ["<td>"+(x.hurdle_tag?'<span class="ct '+(x.hurdle_tag.startsWith("\u2265")?"ok":"tr")+'">'+x.hurdle_tag+'</span> ':"")+(x["yield"]!=null?x["yield"].toFixed(1)+"% to put/maturity":"")+(x.csprd!=null?", CSprd "+Math.round(x.csprd):"")+"</td><td>"+(e.one_pt!=null?"1 pt "+pct(e.one_pt,2):"")+"</td>"];
  if(sec==="Directional") return ["<td>"+(x.equity_share!=null?x.equity_share.toFixed(0)+"%":"")+"</td><td>"+(e.one_pt!=null?"1 pt "+pct(e.one_pt,2)+(e.lost_if_called!=null?"; if called "+pct(-Math.abs(e.lost_if_called),1):""):"")+"</td>"];
  if(sec==="Rich vol") return ["<td>IV below 0.925 \u00d7 RV90</td><td>"+(x.cheaper_peer||'<span class="muted">none clean</span>')+"</td><td>"+volEdge(e.vol_5pts!=null?e:null)+"</td>"];
  return ["<td class='sub'>"+esc(x.why)+"</td>"]; }
const SECHDR={"Hedged vol":["Vol edge"],"Parity":["Premium","Entry and call","Vol edge"],"Credit and yield":["Yield","Price, 1 pt"],"Directional":["Equity share","Entry and call"],"Rich vol":["Back to R2","Cheaper peer","Paying for vol"],"No clear edge":["Reason"]};
function summary(sec,rows){ if(!rows.length) return ""; const n=rows.length;
  if(sec==="Rich vol"){ const k=rows.filter(x=>x.r_margin&&x.r_margin.inside).length; const clear=rows.filter(x=>!(x.r_margin&&x.r_margin.inside)).map(x=>x.name); return k+" of "+n+" are rich by less than RV noise"+(clear.length?"; only "+clear.join(", ")+(clear.length>1?" are":" is")+" clearly rich.":"."); }
  if(sec==="Parity"){ const p=rows.map(x=>x.premium_pct).filter(v=>v!=null); const lc=rows.map(x=>(x.economics||{}).lost_if_called).filter(v=>v!=null); const conv=rows.filter(x=>x.tag==="CONVERT").length; return "All "+n+" sit "+(p.length?Math.min(...p).toFixed(1)+"\u2013"+Math.max(...p).toFixed(1)+"% over parity":"")+": "+(conv?conv+" ready to convert":"none is a conversion yet")+(lc.length?"; a call would cost "+Math.min(...lc.map(Math.abs)).toFixed(1)+"\u2013"+Math.max(...lc.map(Math.abs)).toFixed(1)+"% of price.":"."); }
  if(sec==="Credit and yield"){ const y=rows.map(x=>x["yield"]).filter(v=>v!=null); const ok=rows.filter(x=>(x.hurdle_tag||"").startsWith("\u2265")).length; return (y.length?"Yields "+Math.min(...y).toFixed(1)+"\u2013"+Math.max(...y).toFixed(1)+"% to put or maturity; ":"")+ok+" of "+n+" clear the hurdle."; }
  if(sec==="Hedged vol") return rows.map(x=>x.name+" ("+x.tag+") is "+(x.read_tags||{}).r+(x.trust.includes("CHECK INPUTS")?", input check first":"")).join("; ")+".";
  if(sec==="Directional") return n+" held outright for direction.";
  return n+" with no clear edge."; }
function renderWS(){ const el=document.getElementById("ws"); if(!BOOK){ el.innerHTML='<div class="none">loading\u2026</div>'; return; }
  const bySec={}; (BOOK.sections||[]).forEach(([s,ids])=>{ bySec[s]=ids.map(i=>ROWS[i]).filter(Boolean); });
  let h="";
  for(const [sec,tags,rule,q] of SECTIONS){ const rows=bySec[sec]||[]; const cols=SECHDR[sec];
    h+='<div class="sec"><h2>'+sec+' <span class="muted" style="font-weight:500">\u00b7 '+tags+' \u00b7 '+rows.length+' \u00b7 ranked by '+rule+'</span></h2><div class="q">'+q+'</div>'+(rows.length?'<div class="sumline">'+esc(summary(sec,rows))+'</div>':"");
    h+='<table><tr><th>Bond and code</th><th>Strategy and exit</th><th>Trust</th><th>Vol signal</th><th>Vs peers</th>'+cols.map(c=>"<th>"+c+"</th>").join("")+'<th>Best in its category</th><th>Worst in its category</th><th>Worst, as held</th><th>Carry, 1 month</th></tr>';
    if(!rows.length) h+='<tr><td colspan="'+(10+cols.length)+'" class="none">None in this paste</td></tr>';
    rows.forEach(x=>{ const c=x.category; const ct=catTxt(c,0.5); const held=x.held_as;
      h+='<tr data-id="'+x.sec_id+'"><td><b style="cursor:pointer" onclick="openDrawer('+x.sec_id+')">'+esc(x.name)+'</b> '+tagChip(x.tag)+'<div>'+codeHtml(x.code)+(c?' <span class="sub">vs '+c.group+' ('+c.n+')</span>':"")+'</div></td>';
      h+='<td>'+esc(x.strategy)+(x.strategy!==x.strategy_v3?' <span class="strike">'+esc(x.strategy_v3)+'</span> <span class="ct tr">V3.1</span>':"")+'<div>'+(x.exit_tags||[]).map(t=>'<span class="ct ex">'+esc(t)+'</span>').join("")+'</div></td>';
      h+='<td>'+trust(x)+'</td><td>'+volSig(x)+'</td><td>'+peers(x)+'</td>'+secCols(sec,x).join("");
      h+='<td>'+regChips(c?c.top:null,"rg")+(ct?'<div class="sub">'+ct[0]+'</div>':"")+'</td><td>'+regChips(c?c.bottom:null,"rr")+(ct?'<div class="sub">'+ct[1]+'</div>':"")+'</td>';
      h+='<td>'+(x.worst_as_held!=null?'<span class="ct rr">'+REG[x.worst_as_held]+'</span> <span class="'+(x.regimes_as_held[x.worst_as_held]<0?"loss":"gain")+'">'+pct(x.regimes_as_held[x.worst_as_held],1)+'</span> <span class="ct '+(held==="OUTRIGHT"?"o":"h")+'">'+held+'</span>':"")+'</td><td>'+carryCell(x)+'</td></tr>'; });
    h+='</table></div>'; }
  el.innerHTML=h; }
function rateTyped(inp){ const k=inp.dataset.k, name=inp.dataset.n; const raw=inp.value.trim(); const a=assump(); a.bonds=a.bonds||{}; a.bonds[name]=a.bonds[name]||{};
  if(raw===""){ delete a.bonds[name][k]; inp.classList.remove("ov","bad"); if(!Object.keys(a.bonds[name]).length) delete a.bonds[name]; }
  else { const v=Number(raw); const ok=isFinite(v)&&((k==="borrow"&&v>=0&&v<=100)||(k==="funding"&&v>=-10&&v<=50)); if(!ok){ inp.classList.add("bad"); return; } inp.classList.remove("bad"); inp.classList.add("ov"); a.bonds[name][k]=v; }
  pending=true; clearTimeout(saveT); saveT=setTimeout(()=>{ setAssump(a); pending=false; loadBook(true); if(document.getElementById("wi").classList.contains("on")) runWhatIf(); }, 600); }
function clearOverrides(){ const a=assump(); delete a.bonds; setAssump(a); loadBook(true); }
function defTyped(){ const a=assump(); a.defaults=a.defaults||{}; const b=Number(document.getElementById("defb").value), f=Number(document.getElementById("deff").value); if(isFinite(b)&&b>=0&&b<=100) a.defaults.borrow=b; if(isFinite(f)&&f>=-10&&f<=50) a.defaults.funding=f; clearTimeout(saveT); saveT=setTimeout(()=>{ setAssump(a); loadBook(true); },600); }
async function loadBook(force){ try{ const j=await (await fetch(BASE+"/api/class?a="+encodeURIComponent(JSON.stringify(assump())))).json(); if(!j.ok){ document.getElementById("ws").innerHTML='<div class="none">'+esc(j.error||"class engine unavailable")+'</div>'; return; }
  ROWS={}; Object.keys(j.rows||{}).forEach(k=>{ const x=j.rows[k]; const inp=(j.inputs||{})[k]||{}; x.csprd=inp.csprd; ROWS[Number(k)]=x; });
  BOOK=j; document.getElementById("meta").textContent=j.n+" bonds \u00b7 "+j.today+" \u00b7 "+j.ms+" ms"; const d=defaults(); document.getElementById("defb").value=d.borrow; document.getElementById("deff").value=d.funding;
  // cheaper peer for Rich vol rows: lowest IV/RV90 bond with the same D, a peer verdict and no input flag
  const all=Object.values(ROWS); all.forEach(x=>{ if(x.tag!=="AVOID") return; const D=x.code[1]; const cands=all.filter(y=>y!==x && y.code[1]===D && y.peer_tag && y.peer_tag!=="N/A" && !y.trust.includes("CHECK INPUTS") && y.vs_peers && y.vs_peers.iv!=null); const iv=(y)=>(y.vs_peers.iv/y.vs_peers.rv90); const best=cands.length?cands.sort((p,q)=>iv(p)-iv(q))[0]:null; x.cheaper_peer=best?(esc(best.name)+" (IV/RV "+iv(best).toFixed(2)+")"):""; });
  if(!pending) renderWS(); }catch(e){ document.getElementById("ws").innerHTML='<div class="none">'+esc(e)+'</div>'; } }
function openDrawer(sid){ const x=ROWS[sid]; if(!x) return; const e=x.economics||{}; const v=x.vs_peers||{}; const c=x.category;
  const bars=(x.regimes_as_held||[]).map((p,i)=>'<div style="display:flex;align-items:center;gap:6px"><span style="width:120px;font-size:10px">'+REG[i]+'</span><span class="bar2'+(p<0?" neg":"")+'" style="width:'+Math.min(200,Math.abs(p)*10)+'px"></span><span class="'+(p<0?"loss":"gain")+'">'+pct(p,1)+'</span>'+(c?' <span class="rk">'+c.rank[i]+' of '+c.n+'</span>':"")+'</div>').join("");
  document.getElementById("dbody").innerHTML='<h3>'+esc(x.name)+' '+tagChip(x.tag)+' '+codeHtml(x.code)+'</h3><div class="sub">'+esc(x.cat_txt)+'</div><h3>Why and exit</h3><div>'+esc(x.why)+'</div><div class="sub">'+esc(x.exit)+'</div><h3>8 regimes as held ('+x.held_as+')</h3>'+bars+'<div class="sub">makes money in '+esc(x.makes_money_in)+'</div><h3>Economics</h3><div>'+volEdge(e.vol_5pts!=null?e:null)+(e.one_pt!=null?'; 1 pt '+pct(e.one_pt,2):"")+(e.lost_if_called!=null?'; lost if called '+pct(-Math.abs(e.lost_if_called),1):"")+'</div><h3>Vol read</h3><div>'+volSig(x)+'</div><h3>Peers</h3><div>'+(v.fair_iv!=null?("fair IV "+v.fair_iv.toFixed(1)+"% from "+v.peer_set+" ("+v.peers+" peers, ratio "+v.peer_ratio.toFixed(3)+"); gap "+sg(v.gap_vol,1)+" pts = "+sg(v.gap_px,1)+"%"):"no verdict")+'</div><h3>Input check</h3><div>'+(x.input_check?esc(x.input_check):"none")+(x.iv_per_100bp!=null?'<div class="sub">IV '+sg(x.iv_per_100bp,1)+' pts per +100 bp CSprd; '+sg(x.iv_per_1pct_borrow,1)+' pts per +1% borrow</div>':"")+'</div><h3>Needs data</h3><div class="sub">dividends; call trigger and dates; coupon and put price; listed implied vol; CDS or asset-swap spread; positions; daily history; liquidity; FX for cross-currency bonds</div>';
  document.getElementById("drawer").classList.add("on"); }
function showTab(t){ document.getElementById("ws").classList.toggle("off",t!=="ws"); document.getElementById("wi").classList.toggle("on",t==="wi"); document.getElementById("tabws").classList.toggle("on",t==="ws"); document.getElementById("tabwi").classList.toggle("on",t==="wi"); if(t==="wi") runWhatIf(); }
function syncN(k,v){ document.getElementById("n_"+k).value=v; VIEW[k]=Number(v); saveView(); runWhatIf(); }
function syncR(k,v){ const r=document.getElementById("r_"+k); if(v===""||!isFinite(Number(v))){ document.getElementById("n_"+k).value=VIEW[k]; return; } const c=Math.min(Number(r.max),Math.max(Number(r.min),Number(v))); r.value=c; document.getElementById("n_"+k).value=c; VIEW[k]=c; saveView(); runWhatIf(); }
function setView(o){ for(const k of ["dS","dv","dc","dr","h"]){ VIEW[k]=o[k]||0; const r=document.getElementById("r_"+k); r.value=VIEW[k]; document.getElementById("n_"+k).value=VIEW[k]; } saveView(); runWhatIf(); }
function setLens(l){ LENS=l; ["held","out","hed"].forEach(x=>document.getElementById("l_"+x).classList.toggle("on",x===l)); saveView(); runWhatIf(); }
function saveView(){ localStorage.setItem("nukestation.class.view", JSON.stringify({v:VIEW,lens:LENS})); }
function loadView(){ try{ const j=JSON.parse(localStorage.getItem("nukestation.class.view")||"null"); if(j&&j.v){ VIEW=Object.assign({dS:0,dv:0,dc:0,dr:0,h:0},j.v); LENS=["held","out","hed"].includes(j.lens)?j.lens:"held"; } }catch(e){} for(const k of ["dS","dv","dc","dr","h"]){ const r=document.getElementById("r_"+k); if(r){ r.value=VIEW[k]; document.getElementById("n_"+k).value=VIEW[k]; } } ["held","out","hed"].forEach(x=>document.getElementById("l_"+x).classList.toggle("on",x===LENS)); }
let wiT=null, wiReq=0;
function runWhatIf(){ clearTimeout(wiT); wiT=setTimeout(async()=>{ const my=++wiReq; try{ const body={view:{dS:VIEW.dS/100,dv:VIEW.dv,dc:VIEW.dc,dr:VIEW.dr,h:VIEW.h/12},lens:LENS,assumptions:assump()};
    const j=await (await fetch(BASE+"/api/class/whatif",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)})).json(); if(my!==wiReq) return; renderWI(j); }catch(e){ document.getElementById("wires").innerHTML='<div class="none">'+esc(e)+'</div>'; } },150); }
function viewWords(){ const p=[]; if(VIEW.dS) p.push("stock "+sg(VIEW.dS,0)+"%"); if(VIEW.dv) p.push("vol "+sg(VIEW.dv,0)+" pts"); if(VIEW.dc) p.push("credit "+sg(VIEW.dc,0)+" bp"); if(VIEW.dr) p.push("rates "+sg(VIEW.dr,0)+" bp"); p.push(VIEW.h?VIEW.h+" month"+(VIEW.h>1?"s":""):"instant"); return p.join(", "); }
function renderWI(j){ const el=document.getElementById("wires"); if(!j||!j.ok){ el.innerHTML='<div class="none">'+esc((j&&j.error)||"what-if unavailable")+'</div>'; return; } const rows=j.rows||[]; if(!rows.length){ el.innerHTML='<div class="none">No bonds with enough data.</div>'; return; }
  const best=rows[0], worst=rows[rows.length-1]; const tagOf=n=>(ROWS[n]||{}).tag||""; const byName={}; Object.values(ROWS).forEach(x=>byName[x.name]=x);
  let h='<div class="head"><b>'+esc(viewWords())+'</b> \u00b7 book '+pct(j.book_avg,2)+' (equal weight) \u00b7 '+j.n_up+' of '+rows.length+' gain \u00b7 best '+esc(best.name)+' '+tagChip(best.tag)+' '+pct(best.total,2)+' \u00b7 worst '+esc(worst.name)+' '+tagChip(worst.tag)+' '+pct(worst.total,2)+'</div>';
  h+='<table style="max-width:720px"><tr><th>Tag</th><th>Bonds</th><th>Average</th><th>Best of the tag</th><th>Worst of the tag</th></tr>'+(j.by_tag||[]).map(t=>'<tr><td>'+tagChip(t.tag)+'</td><td>'+t.n+'</td><td class="'+(t.avg<0?"loss":"gain")+'">'+pct(t.avg,2)+'</td><td>'+esc(t.best)+'</td><td>'+esc(t.worst)+'</td></tr>').join("")+'</table>';
  const mx=Math.max(...rows.map(r=>Math.abs(r.total)),0.01);
  h+='<table style="margin-top:8px"><tr><th>#</th><th>Bond and code</th><th>Held</th><th>Total</th><th></th><th>Stock</th><th>Vol</th><th>Credit</th><th>Rates</th><th>Time and coupon</th><th>Funding</th><th>Borrow</th><th>In its category</th></tr>';
  rows.forEach((r,i)=>{ const x=byName[r.name]||{}; const cls=i<3?"top3":(i>=rows.length-3?"bot3":""); const rk=r.cat_rank===1?"BEST OF "+r.cat_n:(r.cat_rank===r.cat_n?"WORST OF "+r.cat_n:r.cat_rank+" OF "+r.cat_n);
    h+='<tr class="'+cls+'"><td class="rk">'+(i+1)+'</td><td><b>'+esc(r.name)+'</b> '+tagChip(r.tag)+' '+(x.code?codeHtml(x.code):"")+'</td><td><span class="ct '+(r.lens==="out"?"o":"h")+'">'+({out:"OUTRIGHT",hed:"HEDGED",hc:"HEDGED+CDS"}[r.lens])+'</span></td><td class="'+(r.total<0?"loss":"gain")+'"><b>'+pct(r.total,2)+'</b></td><td style="width:120px"><span class="bar2'+(r.total<0?" neg":"")+'" style="width:'+Math.round(Math.abs(r.total)/mx*110)+'px"></span></td>'+["stock","vol","credit","rates","time","funding","borrow"].map(k=>'<td class="mono '+(r[k]<0?"loss":r[k]>0?"gain":"muted")+'">'+sg(r[k],2)+'</td>').join("")+'<td><span class="ct '+(r.cat_rank===1?"rg":r.cat_rank===r.cat_n?"rr":"tr")+'">'+rk+'</span> <span class="sub">'+r.group+'</span></td></tr>'; });
  el.innerHTML=h+'</table>'; }
function exportCsv(){ const cols=["name","code","tag","strategy","strategy_v3","held_as","trust","read_tags","peer_tag","vs_peers","input_check","category","worst_txt","makes_money_in","economics","premium_pct","yield","equity_share"]; const rows=Object.values(ROWS); const esc2=v=>'"'+String(v==null?"":(typeof v==="object"?JSON.stringify(v):v)).replace(/"/g,'""')+'"';
  const csv=[cols.join(",")].concat(rows.map(r=>cols.map(c=>esc2(r[c])).join(","))).join("\n"); const a=document.createElement("a"); a.href="data:text/csv;charset=utf-8,"+encodeURIComponent(csv); a.download="nuke_class_"+(BOOK&&BOOK.today||"book")+".csv"; document.body.appendChild(a); a.click(); a.remove(); }
(function(){ const p=document.getElementById("presets"); p.innerHTML=PRESETS.map((x,i)=>'<button onclick="setView({dS:'+x[1]+',dv:'+x[2]+',dc:'+x[3]+',dr:0,h:0})" title="'+REG[i]+'">'+x[0]+'</button>').join("")+'<button onclick="setView({h:1})">No move, 1 month</button><button onclick="setView({})">Reset</button>';
  document.getElementById("defb").addEventListener("input",defTyped); document.getElementById("deff").addEventListener("input",defTyped); loadView(); loadBook(true); setInterval(()=>{ if(!document.hidden && !pending) loadBook(false); }, 60000); })();
</script></body></html>
"""


@app.get("/class")
async def class_page():
    return HTMLResponse(CLASS_PAGE)


@app.get("/api/class")
async def api_class(a: str = ""):
    """CB Code v3.1 columns for the loaded book. a = JSON assumptions {defaults:{borrow,funding}, bonds:{name:{...}}}."""
    res = await run_in_threadpool(class_book, _class_assump(a))
    return {k: (v if k != "rows" else {str(s): r for s, r in v.items()}) for k, v in res.items()}


@app.post("/api/class/whatif")
async def api_class_whatif(req: Request):
    """One view on every bond: body {view:{dS,dv,dc,dr,h}, lens:'held'|'out'|'hed', assumptions:{...}}."""
    if _CBV31 is None:
        return {"ok": False, "error": CLASS_ERR}
    try:
        body = await req.json()
    except Exception:
        body = {}
    view = body.get("view") if isinstance(body.get("view"), dict) else {}
    lens = body.get("lens") if body.get("lens") in ("held", "out", "hed") else "held"
    assumptions = body.get("assumptions") if isinstance(body.get("assumptions"), dict) else {}

    def _run():
        today = _class_date(body.get("today")) or date.today()      # optional ISO date (reproducible checks)
        rows, sids = class_inputs(today)
        child = _class_child("whatif", rows, today, view=view, lens=lens, assumptions=assumptions) if rows else None
        w = child["w"] if child else _CBV31.what_if(rows, today, view, lens, assumptions)
        names = {r["name"]: sids[i] for i, r in enumerate(rows)}
        for x in w["rows"]:
            x["sec_id"] = names.get(x["name"])
        return dict(w, ok=True, view=_CBV31.clean_view(view), lens=lens)
    return await run_in_threadpool(_run)


@app.get("/api/bond_dates/probe")
def api_bond_dates_probe(isin: str = ""):
    """Diagnostic for one ISIN: every candidate call/put/maturity field with the value the
    Terminal returns (field exceptions verbatim) plus the CALL_SCHEDULE / PUT_SCHEDULE rows."""
    isin = isin.strip().upper()
    if not isin:
        return {"ok": False, "error": "isin required"}
    try:
        import blpapi
    except ImportError:
        return {"ok": False, "error": "blpapi not installed on this PC"}
    flds = ["NXT_CALL_DT", "NXT_CALL_PX", "CALLABLE", "CALL_SCHEDULE", "CV_NXT_CALL_DT", "FIRST_CALL_DT",
            "NXT_PUT_DT", "NXT_PUT_PX", "PUTABLE", "PUT_SCHEDULE", "CV_NXT_PUT_DT", "MATURITY", "FINAL_MATURITY",
            "CV_CNVS_START_DT", "CV_CNVS_END_DT"] + BD_SOFT_FIELDS + BD_TRIG_FIELDS
    out = {"ok": True, "isin": isin, "values": {}, "errors": {}, "schedules": {}}
    try:
        sess = blpapi.Session()
        if not sess.start() or not sess.openService("//blp/refdata"):
            return {"ok": False, "error": "Bloomberg session could not start"}
        try:
            svc = sess.getService("//blp/refdata")
            req = svc.createRequest("ReferenceDataRequest")
            req.getElement("securities").appendValue("/isin/" + isin)
            for f in flds:
                req.getElement("fields").appendValue(f)
            sess.sendRequest(req)
            while True:
                ev = sess.nextEvent(5000)
                for msg in ev:
                    if not msg.hasElement("securityData"):
                        continue
                    for sd in msg.getElement("securityData").values():
                        if sd.hasElement("securityError"):
                            out["errors"]["security"] = sd.getElement("securityError").getElementAsString("message")
                            continue
                        fd = sd.getElement("fieldData")
                        for f in flds:
                            if not fd.hasElement(f):
                                continue
                            el = fd.getElement(f)
                            if el.isArray():
                                rows = []
                                for row in el.values():
                                    rows.append({str(row.getElement(k).name()): row.getElement(k).getValueAsString() for k in range(row.numElements())})
                                out["schedules"][f] = rows
                            else:
                                out["values"][f] = el.getValueAsString()
                        if sd.hasElement("fieldExceptions"):
                            for fe in sd.getElement("fieldExceptions").values():
                                out["errors"][fe.getElementAsString("fieldId")] = fe.getElement("errorInfo").getElementAsString("message")
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
        finally:
            sess.stop()
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:300]}
    out["configured"] = {"call_fields": BD_CALL_FIELDS, "put_fields": BD_PUT_FIELDS}
    return out


# ---- Next ex-dividend date per stock (Bloomberg DVD_EX_DT on the equity ticker derived from the RIC) ----
EXDIV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exdiv_cache.json")
_DV = {"data": {}, "pending": set(), "lock": threading.Lock(), "busy": False, "err": ""}
DV_FIELDS = ["DVD_EX_DT", "DVD_SH_LAST"]


def _dv_load():
    try:
        with open(EXDIV_FILE, "r", encoding="utf-8") as fh:
            d = json.load(fh)
        if isinstance(d, dict):
            _DV["data"] = d
    except Exception:
        pass


def _dv_save():
    try:
        tmp = EXDIV_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(_DV["data"], fh)
        os.replace(tmp, EXDIV_FILE)
    except Exception as exc:
        logger.warning("exdiv cache save failed: %s", exc)


def bbg_exdiv(rics):
    """ReferenceDataRequest DVD_EX_DT / DVD_SH_LAST for a batch of RICs -> {ric: {exdiv, amount, ticker}}.
    RICs without a Bloomberg code map to an error entry. xbbg first, raw blpapi second."""
    tk = {}
    for r in rics:
        t = bbg_ticker(r)
        if t:
            tk[t] = r
    out = {r: {"error": "no bbg code for ." + (str(r).rsplit(".", 1)[-1] if "." in str(r) else str(r))} for r in rics if r not in tk.values()}
    if not tk:
        return out
    secs = list(tk.keys())
    try:
        from xbbg import blp
        df = blp.bdp(tickers=secs, flds=DV_FIELDS)
        if df is not None and len(df):
            cols = {str(c).lower(): c for c in df.columns}
            for sec, row in df.iterrows():
                ric = tk.get(str(sec))
                if not ric:
                    continue
                g = lambda f: row[cols[f.lower()]] if f.lower() in cols else None
                out[ric] = {"exdiv": _bd_norm_date(g("DVD_EX_DT")), "amount": _fnum0(g("DVD_SH_LAST")), "ticker": str(sec)}
        for sec, ric in tk.items():
            out.setdefault(ric, {"exdiv": "", "amount": None, "ticker": sec})
        return out
    except ImportError:
        pass
    import blpapi
    sess = blpapi.Session()
    if not sess.start() or not sess.openService("//blp/refdata"):
        raise RuntimeError("Bloomberg session could not start - is the Terminal running?")
    try:
        svc = sess.getService("//blp/refdata")
        req = svc.createRequest("ReferenceDataRequest")
        for s in secs:
            req.getElement("securities").appendValue(s)
        for f in DV_FIELDS:
            req.getElement("fields").appendValue(f)
        sess.sendRequest(req)
        while True:
            ev = sess.nextEvent(5000)
            for msg in ev:
                if not msg.hasElement("securityData"):
                    continue
                for sd in msg.getElement("securityData").values():
                    sec = sd.getElementAsString("security"); ric = tk.get(sec)
                    if not ric:
                        continue
                    if sd.hasElement("securityError"):
                        out[ric] = {"error": sd.getElement("securityError").getElementAsString("message"), "ticker": sec}
                        continue
                    fd = sd.getElement("fieldData")
                    gv = lambda f: (fd.getElementAsString(f) if fd.hasElement(f) else None)
                    out[ric] = {"exdiv": _bd_norm_date(gv("DVD_EX_DT")), "amount": _fnum0(gv("DVD_SH_LAST")), "ticker": sec}
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        for sec, ric in tk.items():
            out.setdefault(ric, {"exdiv": "", "amount": None, "ticker": sec})
        return out
    finally:
        sess.stop()


def _dv_worker(rics):
    today = date.today().isoformat()
    try:
        for i in range(0, len(rics), BD_BATCH):
            batch = rics[i:i + BD_BATCH]
            try:
                res = bbg_exdiv(batch)
                with _DV["lock"]:
                    for r in batch:
                        e = dict(res.get(r) or {"error": "no data"}); e["asof"] = today
                        _DV["data"][r] = e
                _DV["err"] = ""
            except Exception as exc:
                _DV["err"] = str(exc)[:200]
                logger.warning("exdiv fetch failed (%d rics): %s", len(batch), exc)
                with _DV["lock"]:
                    for r in batch:
                        _DV["data"][r] = {"error": str(exc)[:120], "asof": today, "retry_at": time.time() + 3600}
        _dv_save()
    finally:
        with _DV["lock"]:
            _DV["pending"].difference_update(rics)
            _DV["busy"] = False


def _dv_need(ric):
    r = _DV["data"].get(ric)
    if not r:
        return True
    if r.get("retry_at"):
        return time.time() >= r["retry_at"]
    return r.get("asof") != date.today().isoformat()


@app.get("/api/exdiv")
def api_exdiv(rics: str = ""):
    """Cached next ex-div dates for the given RICs; missing or stale ones are queued for
    one background fetch (batched). The request itself never calls Bloomberg."""
    if not _DV["data"] and os.path.exists(EXDIV_FILE):
        _dv_load()
    want = [r.strip().upper() for r in rics.split(",") if r.strip()]
    out, need = {}, []
    with _DV["lock"]:
        for r in want:
            e = _DV["data"].get(r)
            if e and not _dv_need(r):
                out[r] = e
            elif e:
                out[r] = dict(e, stale=True)
            if _dv_need(r) and r not in _DV["pending"]:
                need.append(r); _DV["pending"].add(r)
        if need and not _DV["busy"]:
            _DV["busy"] = True
            threading.Thread(target=_dv_worker, args=(need,), name="exdiv", daemon=True).start()
        elif need:
            _DV["pending"].difference_update(need)
    return {"ok": True, "exdiv": out, "pending": len(_DV["pending"]), "busy": _DV["busy"], "err": _DV["err"]}


# ---- TW SHORT QUOTA: TWSE TWT93U (信用額度總量管制餘額表) + TPEx margin/sbl, one fetch per session, cached ----
# The band is a read-only mirror of the exchange's daily report: nothing here runs on the pricing path.
# A daemon thread fetches each missing session once (evening publication, retried every 30 min until
# 23:30 Taiwan time), parses it off the event loop and swaps it in under a lock; /api/twq only reads the
# cache and does a few hundred dict lookups. The table layout is asserted on every parse, so an upstream
# change gives the band an error text, never a plausible wrong number.
TWQ_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "twq_cache.json")
TWQ_URLS = [u for u in (os.environ.get("NUKE_TWQ_URL", "https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U"),
                        os.environ.get("NUKE_TWQ_URL_ALT", "https://www.twse.com.tw/exchangeReport/TWT93U")) if u]
TWQ_TPEX_URL = os.environ.get("NUKE_TWQ_TPEX_URL", "https://www.tpex.org.tw/www/zh-tw/margin/sbl")
TWQ_DIR = os.environ.get("NUKE_TWQ_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "twq_drop"))   # file drop: TWT93U_YYYYMMDD.json|.csv, TPEX_SBL_YYYYMMDD.json|.csv
TWQ_PAUSE = float(os.environ.get("NUKE_TWQ_PAUSE", "3"))       # seconds between exchange requests - TWSE's security filter blocks bursts
TWQ_HISTORY = max(2, int(os.environ.get("NUKE_TWQ_HISTORY", "10")))
TWQ_CEIL_PCT = float(os.environ.get("NUKE_TWQ_CEIL_PCT", "25"))   # 融券+借券賣出 combined ceiling, % of listed shares
TWQ_SBL_PCT = float(os.environ.get("NUKE_TWQ_SBL_PCT", "10"))     # 借券賣出餘額 sub-cap, % of listed shares
TWQ_CEIL_MODE = os.environ.get("NUKE_TWQ_CEIL_MODE", "flat").lower()   # flat: 融券限額 = ceil% x issued; net: it already nets the balances
TWQ_FX_DEFAULT = float(os.environ.get("NUKE_TWQ_FX", "30.5"))     # TWD per USD when TWD= is not live
TWQ_PUBLISH = os.environ.get("NUKE_TWQ_PUBLISH", "16:00")        # Taiwan time: first try for today's report
TWQ_TIMEOUT = float(os.environ.get("NUKE_TWQ_TIMEOUT", "20"))
TWQ_UA = os.environ.get("NUKE_TWQ_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
_TWQ = {"sessions": {}, "nodata": {}, "failed": {}, "lock": threading.Lock(), "busy": False, "err": "", "layout": "",
        "fetched_at": "", "retry_at": 0.0, "loaded": False, "last_try": "", "log": deque(maxlen=24), "stat": "", "cur": ""}


def _twq_log(msg):
    """Short worker diary, served by /api/twq so 'why is the band empty' is answered without the log file."""
    _TWQ["log"].append("%s %s" % (datetime.now().strftime("%H:%M:%S"), msg))
# per-ticker record: [name, margin_bal(6), margin_limit(7), sbl_bal(12), sbl_next(13), note(14), source]
TQ_N, TQ_MB, TQ_CE, TQ_SB, TQ_NX, TQ_NOTE, TQ_SRC = range(7)


def _twq_tw_now():
    from datetime import timezone as _tz
    return datetime.now(_tz.utc).replace(tzinfo=None) + timedelta(hours=8)   # Taiwan has no DST; host timezone irrelevant


def _twq_prev_weekday(d):
    d = d - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _twq_publish_minutes():
    try:
        hh, mm = TWQ_PUBLISH.split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return 16 * 60


def _twq_expected_session(now_tw=None):
    """The session whose report should be out by now: today once it is a weekday past 18:00 Taiwan time,
    otherwise the previous weekday. Holidays show as 'waiting' until the next report arrives."""
    now_tw = now_tw or _twq_tw_now()
    d = now_tw.date()
    if d.weekday() >= 5 or now_tw.hour < 18:
        d = _twq_prev_weekday(d)
    return d


def _twq_candidates(now_tw=None):
    """ISO dates worth holding, most recent first: the expected session and the weekdays before it."""
    now_tw = now_tw or _twq_tw_now()
    d = now_tw.date()
    if d.weekday() >= 5 or (now_tw.hour * 60 + now_tw.minute) < _twq_publish_minutes():
        d = _twq_prev_weekday(d)                           # today's report is not even due yet
    out = []
    while len(out) < TWQ_HISTORY + 3:
        out.append(d.isoformat())
        d = _twq_prev_weekday(d)
    return out


def _twq_num(s):
    """'1,129,455' -> 1129455; '', '-', '--', 'X' -> None (unknown, never silently 0)."""
    if s is None:
        return None
    t = str(s).strip().replace(",", "")
    if t in ("", "-", "--", "---", "X", "x", "N/A", "n/a"):
        return None
    try:
        return int(t)
    except ValueError:
        try:
            return int(float(t))
        except ValueError:
            return None


def _twq_is_code(s):
    s = str(s or "").strip()
    return bool(s) and s.isascii() and s.isalnum()


def _twq_layout_check(fields):
    """Positional layout both exchanges publish (checked 2026-09 by two independent parsers):
    0 代號 1 名稱 | 融券: 2 前日餘額 3 賣出 4 買進 5 現券 6 今日餘額 7 次一營業日限額 |
    借券賣出: 8 前日餘額 9 當日賣出 10 當日還券 11 當日調整 12 當日餘額 13 次一營業日可限額 | 14 備註."""
    f = [str(x or "").replace(" ", "") for x in (fields or [])]
    if len(f) < 14:
        raise ValueError("expected 14-15 columns, got %d: %s" % (len(f), f))
    want = {0: "代號", 1: "名稱", 6: "餘額", 7: "限額", 9: "賣出", 10: "還券", 12: "餘額", 13: "限額"}
    bad = ["[%d] %r (want %s)" % (i, f[i], w) for i, w in want.items() if w not in f[i]]
    if bad:
        raise ValueError("unexpected table layout: " + "; ".join(bad))
    return f


def _twq_parse_table(fields, data, src):
    _twq_layout_check(fields)
    rows = {}
    for r in data or []:
        if not isinstance(r, (list, tuple)) or len(r) < 14 or not _twq_is_code(r[0]):
            continue                                       # totals / notes / blank lines
        code = str(r[0]).strip()
        rows[code] = [str(r[1] or "").strip(), _twq_num(r[6]), _twq_num(r[7]), _twq_num(r[12]), _twq_num(r[13]),
                      (str(r[14]).strip() if len(r) > 14 and r[14] is not None else ""), src]
    return rows


def _twq_parse_json(j, src, want=None):
    """TWSE: {stat,date,fields,data}; TPEx: {date,tables:[{fields,data}]}. -> (rows|None, report_date|None).
    None rows = the exchange says there is no report for that date (holiday / not yet published); the reason
    is kept in _TWQ["stat"] for the diary. An empty answer carrying a DIFFERENT report date than the one asked
    (want) is a redirected / filtered request, raised as an error so it never passes as a holiday."""
    if not isinstance(j, dict):
        raise ValueError("response is not a JSON object")
    tbl = j
    if isinstance(j.get("tables"), list):
        tbl = next((t for t in j["tables"] if isinstance(t, dict) and t.get("data")), j["tables"][0] if j["tables"] else {})
    data = tbl.get("data") if isinstance(tbl, dict) else None
    rd = str(j.get("date") or (tbl.get("date") if isinstance(tbl, dict) else "") or "").replace("/", "").replace("-", "").strip()
    rdate = None
    if len(rd) == 8 and rd.isdigit():
        rdate = "%s-%s-%s" % (rd[:4], rd[4:6], rd[6:])
    if not data:
        stat = str(j.get("stat") or (tbl.get("stat") if isinstance(tbl, dict) else "") or "").strip()
        note = "%s: stat=%s date=%s keys=%s" % (src, stat[:60] or "-", rd or "-", ",".join(list(j.keys())[:6]))
        _TWQ["stat"] = ((_TWQ["stat"] + " | ") if _TWQ["stat"] else "") + note      # both exchanges' answers for the diary
        if want and rdate and rdate != want and (not stat or stat.upper() == "OK"):
            raise ValueError("answered for %s instead of %s with no rows - request redirected or filtered (%s)" % (rdate, want, _TWQ["stat"][:80]))
        return None, None
    rows = _twq_parse_table(tbl.get("fields"), data, src)
    return rows, rdate


def _twq_parse_csv(text, src):
    """The 'CSV 下載' file of the same page: title line(s), group header, column header, rows, notes."""
    import csv
    import io
    rdr = list(csv.reader(io.StringIO(text)))
    hdr = None
    for i, r in enumerate(rdr):
        if len(r) >= 14 and any("代號" in str(c) for c in r[:2]) and any("名稱" in str(c) for c in r[:3]):
            hdr = i
            break
    if hdr is None:
        raise ValueError("csv: no header row with 代號/名稱 and 14+ columns")
    data = [r for r in rdr[hdr + 1:] if len(r) >= 14 and _twq_is_code(r[0])]
    return _twq_parse_table(rdr[hdr], data, src)


_TWQ_HTTP = {"sess": None, "warm": 0.0, "last": ""}


def _twq_session():
    """One requests.Session shared by the worker: browser-like headers and the site's cookies (picked up from the
    report page like a browser would, refreshed every 30 min). Proxy settings come from the environment."""
    s = _TWQ_HTTP["sess"]
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": TWQ_UA, "Accept": "application/json, text/plain, */*",
                          "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8", "Referer": "https://www.twse.com.tw/zh/trading/margin/twt93u.html"})
        _TWQ_HTTP["sess"] = s
    if time.time() - _TWQ_HTTP["warm"] > 1800:
        _TWQ_HTTP["warm"] = time.time()
        try:
            s.get("https://www.twse.com.tw/zh/trading/margin/twt93u.html", timeout=TWQ_TIMEOUT)
        except Exception:
            pass
    return s


def _twq_http_json(url, params):
    """One GET -> parsed JSON. A redirect (TWSE's security filter answers automated bursts with HTTP 307
    'FOR SECURITY REASONS'), a non-200 or a non-JSON body are errors - never silently 'no data'. Mocked in tests."""
    r = _twq_session().get(url, params=params, timeout=TWQ_TIMEOUT, allow_redirects=False)
    head = (r.text or "")[:160].replace("\n", " ").replace("\r", " ")
    _TWQ_HTTP["last"] = "HTTP %d %s | %s" % (r.status_code, r.headers.get("Content-Type", ""), head)
    if 300 <= r.status_code < 400:
        raise ValueError("HTTP %d redirect to %s - the exchange's security filter blocked the request (automated / too frequent); retry later or use the file drop"
                         % (r.status_code, (r.headers.get("Location") or "?")[:100]))
    if r.status_code != 200:
        raise ValueError("HTTP %d: %s" % (r.status_code, head[:80]))
    try:
        return r.json()
    except ValueError:
        raise ValueError("non-JSON reply (%s...)" % head[:80])


def _twq_read_drop(iso, stem, src):
    """File drop for hosts that cannot reach the exchange: <TWQ_DIR>/<stem>_YYYYMMDD.json or .csv."""
    if not TWQ_DIR:
        return None
    ymd = iso.replace("-", "")
    for ext in (".json", ".csv"):
        p = os.path.join(TWQ_DIR, "%s_%s%s" % (stem, ymd, ext))
        if not os.path.exists(p):
            continue
        if ext == ".json":
            with open(p, "r", encoding="utf-8-sig") as fh:
                rows, _ = _twq_parse_json(json.load(fh), src)
            return rows or {}
        raw = open(p, "rb").read()
        for enc in ("utf-8-sig", "cp950", "big5"):
            try:
                return _twq_parse_csv(raw.decode(enc), src)
            except UnicodeDecodeError:
                continue
        raise ValueError("csv %s: undecodable" % p)
    return None


def _twq_fetch_date(iso):
    """Both exchanges for one session -> (rows, sources, errors). rows empty + no errors = no report that day."""
    rows, srcs, errs = {}, [], []
    ymd = iso.replace("-", "")
    # TWSE (上市): primary endpoint, then the legacy one
    try:
        got = _twq_read_drop(iso, "TWT93U", "TWSE")
        if got is None:
            last = None
            for u in TWQ_URLS:
                try:
                    got, rdate = _twq_parse_json(_twq_http_json(u, {"date": ymd, "response": "json"}), "TWSE", want=iso)
                    if got and rdate and rdate != iso:
                        logger.info("twq TWSE %s answered with the %s table - treated as no report", iso, rdate)
                        got = None
                    last = None
                    break
                except ValueError as exc:                  # layout / bad body: do not try to 'fix' it
                    if "layout" in str(exc) or "columns" in str(exc):
                        raise
                    last = exc
                except Exception as exc:
                    last = exc
                time.sleep(TWQ_PAUSE)
            if last is not None:
                raise last
        if got:
            rows.update(got); srcs.append("TWSE")
    except Exception as exc:
        errs.append("TWSE %s: %s" % (iso, str(exc)[:160]))
        if "layout" in str(exc) or "columns" in str(exc):
            _TWQ["layout"] = "TWSE " + str(exc)[:200]
    # TPEx (上櫃): same table, same layout, tables[0]
    if TWQ_TPEX_URL or TWQ_DIR:
        try:
            got = _twq_read_drop(iso, "TPEX_SBL", "TPEX")
            if got is None and TWQ_TPEX_URL:
                time.sleep(TWQ_PAUSE)
                got, rdate = _twq_parse_json(_twq_http_json(TWQ_TPEX_URL, {"date": "%s/%s/%s" % (ymd[:4], ymd[4:6], ymd[6:]), "response": "json"}), "TPEX", want=iso)
                if got and rdate and rdate != iso:
                    logger.info("twq TPEx %s answered with the %s table - treated as no report", iso, rdate)
                    got = None
            if got:
                for k, v in got.items():
                    rows.setdefault(k, v)                  # a code listed on both: TWSE wins
                srcs.append("TPEX")
        except Exception as exc:
            errs.append("TPEx %s: %s" % (iso, str(exc)[:160]))
            if "layout" in str(exc) or "columns" in str(exc):
                _TWQ["layout"] = "TPEx " + str(exc)[:200]
    return rows, srcs, errs


def _twq_load():
    try:
        with open(TWQ_FILE, "r", encoding="utf-8") as fh:
            d = json.load(fh)
        if isinstance(d, dict) and isinstance(d.get("sessions"), dict):
            with _TWQ["lock"]:
                _TWQ["sessions"] = d["sessions"]
                _TWQ["nodata"] = {k: 0.0 for k in (d.get("nodata") or [])}   # known holidays: permanent
                _TWQ["fetched_at"] = str(d.get("fetched_at") or "")
            logger.info("twq cache loaded: %d sessions", len(d["sessions"]))
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.warning("twq cache load failed: %s", exc)
    _TWQ["loaded"] = True


def _twq_save():
    try:
        today = _twq_tw_now().date().isoformat()
        with _TWQ["lock"]:
            keep = sorted(_TWQ["sessions"].keys())[-(TWQ_HISTORY + 5):]
            for k in list(_TWQ["sessions"].keys()):
                if k not in keep:
                    del _TWQ["sessions"][k]
            sessions = dict(_TWQ["sessions"])
            meta = {"fetched_at": _TWQ["fetched_at"],
                    "nodata": sorted(k for k in _TWQ["nodata"] if k < today and k >= keep[0]) if keep else []}
        # one json.dumps per session (~100 KB each) instead of one 1 MB dump: C-level serialisation holds the
        # GIL, and short holds keep the event loop's pricing ticks smooth while this runs in the thread
        parts = []
        for k in sorted(sessions):
            parts.append(json.dumps(k) + ":" + json.dumps(sessions[k], ensure_ascii=False, separators=(",", ":")))
            time.sleep(0)
        txt = '{"sessions":{' + ",".join(parts) + '},"fetched_at":' + json.dumps(meta["fetched_at"]) + ',"nodata":' + json.dumps(meta["nodata"]) + "}"
        tmp = TWQ_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(txt)
        os.replace(tmp, TWQ_FILE)
    except Exception as exc:
        logger.warning("twq cache save failed: %s", exc)


def _twq_worker(dates):
    """Daemon thread: fetch + parse the missing sessions (most recent first), swap in under the lock.
    A date that fails is skipped (retried in 10 min) and the older dates are still fetched, so the band
    always shows the latest session that could be read; two consecutive failures = the exchange is
    unreachable -> stop and back off 10 min."""
    try:
        today = _twq_tw_now().date().isoformat()
        got_any, fails, problems = False, 0, []
        for iso in dates:
            _TWQ["last_try"] = datetime.now().strftime("%H:%M:%S")
            _TWQ["cur"] = iso
            _TWQ["stat"] = ""
            rows, srcs, errs = _twq_fetch_date(iso)
            now = time.time()
            if rows:
                with _TWQ["lock"]:
                    _TWQ["sessions"][iso] = {"rows": rows, "src": "+".join(srcs), "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                    _TWQ["nodata"].pop(iso, None)
                    _TWQ["failed"].pop(iso, None)
                    _TWQ["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fails = 0
                _twq_log("%s: %d codes (%s)%s" % (iso, len(rows), "+".join(srcs), (" - partial: " + "; ".join(errs)[:120]) if errs else ""))
                logger.info("twq %s: %d codes (%s)%s", iso, len(rows), "+".join(srcs), (" partial: " + "; ".join(errs)) if errs else "")
                if errs:
                    problems.append("partial: " + "; ".join(errs))
                if not got_any:
                    got_any = True
                    _twq_save()                            # the first session lands on disk at once
            elif errs:
                fails += 1
                _TWQ["failed"][iso] = now + 600            # this date again in 10 min
                problems.append("; ".join(errs))
                _twq_log("%s FAILED: %s" % (iso, "; ".join(errs)[:160]))
                logger.warning("twq %s failed: %s", iso, "; ".join(errs))
                if fails >= 2:
                    _TWQ["retry_at"] = now + 600            # exchange unreachable: back off 10 min
                    _twq_log("2 consecutive failures - backing off 10 min")
                    break
            else:
                _TWQ["nodata"][iso] = (now + 1800) if iso >= today else 0.0   # today: retry 30 min; past: holiday
                _twq_log("%s: no report (holiday or not published yet)%s" % (iso, (" - " + _TWQ["stat"][:220]) if _TWQ["stat"] else ""))
                logger.info("twq %s: no report (holiday or not published yet)", iso)
            time.sleep(0.5)
        _TWQ["err"] = "; ".join(problems)[:300]              # newest problems stay visible next to the data that did load
        if got_any:
            _twq_save()
    except Exception as exc:
        _TWQ["err"] = str(exc)[:300]
        _twq_log("worker crashed: %s" % str(exc)[:160])
        logger.warning("twq worker: %s", exc)
    finally:
        _TWQ["busy"] = False
        _TWQ["cur"] = ""


def _twq_plan(now_tw=None):
    """Sessions still worth fetching, most recent first."""
    now = time.time()
    if now < _TWQ["retry_at"]:
        return []
    now_tw = now_tw or _twq_tw_now()
    today = now_tw.date().isoformat()
    late = (now_tw.hour * 60 + now_tw.minute) >= 23 * 60 + 30
    out = []
    with _TWQ["lock"]:
        for iso in _twq_candidates(now_tw):
            if iso in _TWQ["sessions"]:
                continue
            if _TWQ["failed"].get(iso, 0) > now:
                continue                                   # this date failed a moment ago: wait for its retry window
            nd = _TWQ["nodata"].get(iso)
            if nd is not None:
                if iso < today or late or now < nd:
                    continue                               # holiday (permanent) or not due for a retry yet
            out.append(iso)
    return out


def _twq_kick(force=False):
    """Start one worker for the plan if none is running. Returns the plan."""
    plan = _twq_plan()
    if plan and not _TWQ["busy"]:
        _TWQ["busy"] = True
        threading.Thread(target=_twq_worker, args=(plan,), name="twq", daemon=True).start()
    return plan


async def twq_poller():
    """Loads the disk cache, backfills the last sessions, then checks each minute whether a session is
    missing (today's from NUKE_TWQ_PUBLISH Taiwan time, retried every 30 min). Opt-out NUKE_TWQ_FETCH=0."""
    if os.environ.get("NUKE_TWQ_FETCH", "1") != "1":
        logger.info("twq poller disabled (NUKE_TWQ_FETCH=0) - tw short quota band stays blank")
        await run_in_threadpool(_twq_load)
        return
    await run_in_threadpool(_twq_load)
    await asyncio.sleep(int(os.environ.get("NUKE_TWQ_DELAY", "15")))
    logger.info("twq poller started (TWSE TWT93U%s, %d sessions, publish %s TW)",
                " + TPEx" if TWQ_TPEX_URL else "", TWQ_HISTORY, TWQ_PUBLISH)
    _twq_log("poller started - %d sessions cached on disk" % len(_TWQ["sessions"]))
    while True:
        try:
            _twq_kick()
        except Exception as exc:
            logger.info("twq poll skipped: %s", exc)
        await asyncio.sleep(60)


def _twq_ticker(ric):
    """'2382.TW' -> ('2382', 'TWSE'); '6669.TWO' -> ('6669', 'TPEX'); anything else -> (None, None)."""
    s = str(ric or "").strip().upper()
    i = s.rfind(".")
    if i <= 0:
        return None, None
    code, sfx = s[:i], s[i + 1:]
    if not _twq_is_code(code):
        return None, None
    if sfx == "TW":
        return code, "TWSE"
    if sfx == "TWO":
        return code, "TPEX"
    return None, None


def _twq_rate(rec):
    if not rec:
        return None, ""
    for k in ("last", "close"):
        v = _fnum0(rec.get(k))
        if v is not None and v > 0:
            return v, k
    return None, ""


def _twq_fx(sid):
    """TWD per USD for a Taiwan listing: the row's und_fx RIC when it is a TWD rate (e.g. TWD=TP), else TWD=,
    else the NUKE_TWQ_FX default. A row's numeric und_fx constant is ignored - TW stocks trade in TWD."""
    fxr = str((STATE["rows"].get(sid) or {}).get("und_fx") or "").strip().upper()
    for ric in ([fxr] if ("TWD" in fxr and fxr != "TWD=") else []) + ["TWD="]:
        v, k = _twq_rate(STATE["rfx"].get(ric))
        if v:
            return (v if v >= 1 else 1.0 / v), "%s %s" % (ric, k)
    return TWQ_FX_DEFAULT, "default NUKE_TWQ_FX"


def _twq_calc(rec, price, fx):
    """One row of the band from the exchange record. All shares; USD = shares x price / (TWD per USD)."""
    mb, ce, sb, nx = rec[TQ_MB], rec[TQ_CE], rec[TQ_SB], rec[TQ_NX]
    out = {"n": rec[TQ_N], "x": rec[TQ_SRC], "mb": mb, "ce": ce, "bal": sb, "lim": nx, "note": rec[TQ_NOTE] or ""}
    issued = None
    if ce is not None and ce > 0 and TWQ_CEIL_PCT > 0:
        issued = (ce + (mb or 0) + (sb or 0)) / (TWQ_CEIL_PCT / 100.0) if TWQ_CEIL_MODE == "net" else ce / (TWQ_CEIL_PCT / 100.0)
    subcap = issued * TWQ_SBL_PCT / 100.0 if issued else None
    room = max(subcap - sb, 0.0) if (subcap is not None and sb is not None) else None
    used = (sb / subcap * 100.0) if (subcap and sb is not None) else None
    comb = ((mb or 0) + (sb or 0)) / (issued * TWQ_CEIL_PCT / 100.0) * 100.0 if issued else None
    bind = ""
    if nx is not None and nx <= 0:
        bind = "restricted" if (room is None or room > 2) else "sub-cap"   # zero quota with cap room left: exchange-side restriction
    elif nx is not None and room is not None:
        if abs(nx - room) <= 2:
            bind = "sub-cap"
        elif nx > room + 2:
            bind = "check"                                 # quota above the derived room: listed-share basis is off
        else:
            bind = "throttle"
    out.update({"iss": issued, "sub": subcap, "room": room, "used": used, "comb": comb, "bind": bind,
                "thr": nx if bind == "throttle" else None,
                "sess": (room / nx) if (room is not None and nx and nx > 0) else None,
                "px": price, "fx": fx,
                "cap_usd": (nx * price / fx) if (nx is not None and price and fx) else None,
                "room_usd": (room * price / fx) if (room is not None and price and fx) else None})
    return out


@app.get("/api/twq")
def api_twq(sids: str = ""):
    """TW short-sale quota rows for the book (TWSE/TPEx codes derived from the stock RIC), computed from the
    cached sessions plus the live/close price already in memory. Never calls the exchange."""
    want = None
    if sids.strip():
        want = set()
        for s in sids.split(","):
            try:
                want.add(int(s))
            except ValueError:
                pass
    now_tw = _twq_tw_now()
    with _TWQ["lock"]:
        dates = sorted(_TWQ["sessions"].keys())[-TWQ_HISTORY:]
        sess = [_TWQ["sessions"][d] for d in dates]
        nodata = sorted(k for k in _TWQ["nodata"].keys())
    latest = dates[-1] if dates else ""
    tables = [s["rows"] for s in sess]
    expected = _twq_expected_session(now_tw).isoformat()
    tkey = "|".join(dates) + "|" + _TWQ["fetched_at"]
    if _TWQ.get("tc_key") != tkey:                         # usage trend depends on the sessions only: memoised per code
        _TWQ["tc"] = {}
        _TWQ["tc_key"] = tkey
    tc = _TWQ["tc"]
    rows = {}
    for sid, ric in list(STATE["stockRics"].items()):
        if want is not None and sid not in want:
            continue
        code, exch = _twq_ticker(ric)
        if not code:
            continue                                       # not a Taiwan listing: the band stays blank
        if not tables:
            rows[str(sid)] = {"tk": code, "x": exch, "na": "no TWT93U session cached yet"}
            continue
        rec = tables[-1].get(code)
        if rec is None:
            if exch not in (sess[-1].get("src") or ""):
                rows[str(sid)] = {"tk": code, "x": exch, "na": "%s report for %s not fetched%s" % ("TPEx" if exch == "TPEX" else "TWSE", latest, (" - " + _TWQ["err"]) if _TWQ["err"] else "")}
            else:
                rows[str(sid)] = {"tk": code, "x": exch, "na": "%s not in the %s report for %s (not margin/SBL eligible?)" % (code, "TPEx" if exch == "TPEX" else "TWSE", latest)}
            continue
        price, psrc = _twq_rate(STATE["rfx"].get(ric))
        fx, fsrc = _twq_fx(sid)
        row = _twq_calc(rec, price, fx)
        row.update({"tk": code, "px_src": psrc, "fx_src": fsrc, "dt": latest})
        cached = tc.get(code)
        if cached is None:
            trend = []
            for t in tables:
                r = t.get(code)
                u = None
                if r is not None and r[TQ_CE] and r[TQ_SB] is not None:
                    iss = (r[TQ_CE] + (r[TQ_MB] or 0) + r[TQ_SB]) / (TWQ_CEIL_PCT / 100.0) if TWQ_CEIL_MODE == "net" else r[TQ_CE] / (TWQ_CEIL_PCT / 100.0)
                    sub = iss * TWQ_SBL_PCT / 100.0
                    u = round(r[TQ_SB] / sub * 100.0, 2) if sub else None
                trend.append(u)
            pts = [u for u in trend if u is not None]
            chg = (pts[-1] - pts[0]) if len(pts) >= 2 else None
            cached = tc[code] = (trend, chg, "" if chg is None else ("up" if chg >= 1.5 else ("down" if chg <= -1.5 else "flat")))
        trend, chg = cached[0], cached[1]
        row["trend"] = trend
        row["tchg"] = chg
        row["tdir"] = cached[2]
        row["watch"] = bool(row["bind"] == "sub-cap" or (row["used"] is not None and row["used"] >= 70 and chg is not None and chg >= 3))
        rows[str(sid)] = row
    exp_state = "waiting"
    if expected in _TWQ["sessions"]:
        exp_state = "have"
    elif _TWQ["nodata"].get(expected) == 0.0 or (expected in _TWQ["nodata"] and expected < now_tw.date().isoformat()):
        exp_state = "holiday"                              # the exchange published nothing for that day
    n_calc = sum(1 for r in rows.values() if r.get("bind"))
    n_check = sum(1 for r in rows.values() if r.get("bind") == "check")
    hint = ""
    if n_check >= 3 and n_check * 10 >= n_calc * 3:
        hint = ("%d of %d names show a quota above the derived cap room: the margin limit column may already net the "
                "balances - try NUKE_TWQ_CEIL_MODE=net (currently %s)" % (n_check, n_calc, TWQ_CEIL_MODE))
    return {"ok": True, "rows": rows, "tdates": dates, "session": latest, "expected": expected,
            "pending": bool(latest < expected), "nodata": nodata[-5:], "fetched_at": _TWQ["fetched_at"],
            "busy": _TWQ["busy"], "err": _TWQ["err"], "layout": _TWQ["layout"], "last_try": _TWQ["last_try"], "hint": hint,
            "log": list(_TWQ["log"]), "plan": _twq_plan(now_tw), "retry_in": max(0, int(_TWQ["retry_at"] - time.time())),
            "cur": _TWQ["cur"], "exp_state": exp_state, "drop_dir": TWQ_DIR, "http_last": _TWQ_HTTP["last"],
            "mode": TWQ_CEIL_MODE, "ceil_pct": TWQ_CEIL_PCT, "sbl_pct": TWQ_SBL_PCT,
            "stamp": "%s|%s|%s|%s|%s|%s" % (latest, _TWQ["fetched_at"], STATE.get("rfxTs") or "", _TWQ["err"] or _TWQ["layout"] or "", expected, hint[:8])}


@app.post("/api/twq/fetch")
def api_twq_fetch(date: str = ""):
    """Manual 'fetch now' (the band header's refresh glyph): forgets the back-offs - for one date or all - and
    starts the worker at once. Returns the plan it will work through."""
    with _TWQ["lock"]:
        if date.strip():
            _TWQ["nodata"].pop(date.strip(), None)
            _TWQ["failed"].pop(date.strip(), None)
        else:
            _TWQ["failed"].clear()
            today = _twq_tw_now().date().isoformat()
            for k in [k for k, v in _TWQ["nodata"].items() if v or k >= today]:
                _TWQ["nodata"].pop(k, None)
    _TWQ["retry_at"] = 0.0
    _twq_log("manual fetch requested%s" % ((" for " + date.strip()) if date.strip() else ""))
    plan = _twq_kick()
    return {"ok": True, "plan": plan, "busy": _TWQ["busy"], "log": list(_TWQ["log"])[-6:]}


@app.get("/api/twq/probe")
def api_twq_probe(date: str = "", src: str = "twse", code: str = ""):
    """Diagnostic: fetch one session live (TWSE or TPEx) and show the raw header, the first rows, the parse
    verdict and one code's parsed record. date=YYYY-MM-DD (default: expected session)."""
    iso = date.strip() or _twq_expected_session().isoformat()
    ymd = iso.replace("-", "")
    try:
        if src.lower() == "tpex":
            j = _twq_http_json(TWQ_TPEX_URL, {"date": "%s/%s/%s" % (ymd[:4], ymd[4:6], ymd[6:]), "response": "json"})
        else:
            j = _twq_http_json(TWQ_URLS[0], {"date": ymd, "response": "json"})
    except Exception as exc:
        return {"ok": False, "date": iso, "src": src, "error": str(exc)[:300], "http": _TWQ_HTTP["last"]}
    tbl = j
    if isinstance(j, dict) and isinstance(j.get("tables"), list) and j["tables"]:
        tbl = j["tables"][0]
    fields = tbl.get("fields") if isinstance(tbl, dict) else None
    data = tbl.get("data") if isinstance(tbl, dict) else None
    out = {"ok": True, "date": iso, "src": src, "stat": (j.get("stat") if isinstance(j, dict) else None),
           "report_date": (j.get("date") if isinstance(j, dict) else None), "fields": fields,
           "n_rows": len(data or []), "first_rows": (data or [])[:3]}
    try:
        rows, rdate = _twq_parse_json(j, src.upper())
        out["parsed"] = len(rows or {})
        out["layout"] = "ok" if rows is not None else "no data for this date"
        if code and rows:
            out["code"] = code
            out["record"] = rows.get(code)
            if rows.get(code):
                out["calc"] = _twq_calc(rows[code], None, TWQ_FX_DEFAULT)   # shares only: no price in a probe
    except Exception as exc:
        out["layout"] = "FAILED: " + str(exc)[:300]
    return out


@app.get("/api/bond_dates")
def api_bond_dates(isins: str = ""):
    """Cached bond dates for the given ISINs; missing or stale ones are queued for one
    background fetch (batched). The request itself never calls Bloomberg."""
    if not _BD["data"] and os.path.exists(BOND_DATES_FILE):
        _bd_load()
    want = [i.strip().upper() for i in isins.split(",") if i.strip()]
    out, need = {}, []
    with _BD["lock"]:
        for isin in want:
            r = _BD["data"].get(isin)
            if r and not _bd_need(isin):
                out[isin] = r
            elif r:
                out[isin] = dict(r, stale=True)
            if _bd_need(isin) and isin not in _BD["pending"]:
                need.append(isin); _BD["pending"].add(isin)
        if need and not _BD["busy"]:
            _BD["busy"] = True
            threading.Thread(target=_bd_worker, args=(need,), name="bond-dates", daemon=True).start()
        elif need:
            _BD["pending"].difference_update(need)   # let the next poll queue them once the worker is free
    return {"ok": True, "dates": out, "pending": len(_BD["pending"]), "busy": _BD["busy"], "err": _BD["err"], "sched_err": _BD.get("sched_err", "")}


@app.post("/api/snap8/run")
async def api_snap8_run():
    """Manual morning snapshot: Close -> ovd, re-nuke, store (replaces today)."""
    try:
        res = await snap8_close_run("manual")
        if not res.get("ok"):
            return JSONResponse(status_code=500, content=res)
        return res
    except Exception as exc:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": str(exc)})


@app.post("/api/save")

@app.post("/api/save")
def api_save(req: SaveRequest):
    if not req.rows:
        return JSONResponse({"error": "Nothing to save."}, status_code=400)
    try:
        n = save_rows(req.rows)
    except Exception as exc:
        logger.error("Save failed: %s", exc)
        return JSONResponse({"error": f"Save failed: {exc}"}, status_code=500)
    logger.info("save by %s: %d rows -> %s.%s", req.user, n, APP_DB, APP_TABLE)
    db_log(req.user, "save", {"n": n})
    return {"saved": n, "table": f"{APP_DB}.{APP_TABLE}"}


def run_nuke_batches(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    all_rows: List[Dict[str, Any]] = []
    hosts, total_elapsed = [], 0.0
    for i in range(0, len(entries), BATCH_SIZE):
        out = call_nuked_api(entries[i:i + BATCH_SIZE])
        hosts.append(out["host"])
        total_elapsed += out["elapsed"]
        all_rows.extend(out["rows"])
    requested = {e["secId"] for e in entries}
    returned = {r.get("secId") for r in all_rows}
    return {"rows": all_rows,
            "host": ", ".join(sorted(set(hosts))),
            "elapsed": round(total_elapsed, 3),
            "requested": len(requested), "returned": len(all_rows),
            "missing": sorted(requested - returned),
            "secIds": sorted(requested)}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    global MAIN_LOOP
    import asyncio as _aio
    MAIN_LOOP = _aio.get_running_loop()
    await ws.accept()
    CLIENTS.add(ws)
    CLIENT_LOCKS[ws] = asyncio.Lock()
    user = "anon"
    try:
        while True:
            try:
                msg = json.loads(await ws.receive_text())
            except json.JSONDecodeError:
                continue
            t = msg.get("type")

            if t == "cfg":
                u2 = (str(msg.get("user") or user or "anon"))[:32]
                k2 = str(msg.get("key") or "")[:48]
                if k2:
                    STATE.setdefault("userCfg", {}).setdefault(
                        u2, {})[k2] = msg.get("val")
                    await broadcast({"type": "cfg", "by": u2,
                        "key": k2, "val": msg.get("val")},
                        skip=ws)
                continue
            if t == "hello":
                user = (str(msg.get("user") or "anon"))[:32]
                CLIENT_NAMES[ws] = user
                await ws.send_text(json.dumps(
                    {"type": "snapshot", "state": snapshot(),
                     "you": user, "online": len(CLIENTS)}))
                await broadcast({"type": "online", "n": len(CLIENTS),
                                 "note": f"{user} joined"}, skip=ws)
                logger.info("ws hello: %s (online=%d)", user, len(CLIENTS))
                await run_in_threadpool(db_log, user, "connect", {})

            elif t == "ping":
                await ws.send_text('{"type":"pong"}')

            elif t == "ids":
                ids = [int(i) for i in msg.get("ids", []) if str(i).isdigit()]
                seen, uniq = set(), []
                for i in ids:
                    if i not in seen:
                        seen.add(i); uniq.append(i)
                STATE["ids"] = uniq
                for sid in uniq:
                    STATE["rows"].setdefault(sid, _blank_row())
                STATE["version"] += 1
                await run_in_threadpool(refresh_stock_rics)
                if REFDATA_WAKE: REFDATA_WAKE.set()
                await run_in_threadpool(db_meta_set, "sec_ids", uniq)
                await run_in_threadpool(db_log, user, "ids", {"n": len(uniq)})
                await broadcast({"type": "ids", "ids": uniq,
                                 "rows": STATE["rows"], "by": user,
                                 "v": STATE["version"]})
                if RFX_WAKE: RFX_WAKE.set()
                logger.info("ids set by %s: %d securities", user, len(uniq))

            elif t in ("row", "rows"):
                items = msg.get("list") if t == "rows" else [msg]
                changed = []
                for it in items or []:
                    try:
                        sid = int(it.get("secId"))
                    except (TypeError, ValueError):
                        continue
                    row = STATE["rows"].setdefault(sid, _blank_row())
                    touched = _rows_apply(it, row, user)
                    if _van_mirror(row) and \
                            "ovdCbFx" not in touched:
                        touched.append("ovdCbFx")
                    changed.append(sid)
                    asyncio.create_task(          # persist off hot path
                        run_in_threadpool(db_upsert_state_fields,
                                          sid, row, user, touched))
                if changed:
                    if any("und_fx" in it for it in items or []) \
                            and RFX_WAKE:
                        RFX_WAKE.set()   # fx rics changed: poll now
                    STATE["version"] += 1
                    await broadcast({"type": "rows", "by": user,
                                     "v": STATE["version"],
                                     "list": [{"secId": sid, **STATE["rows"][sid]}
                                              for sid in changed]}, skip=ws)
                    if RFX_WAKE: RFX_WAKE.set()   # und_fx may have changed
                    logger.info("rows updated by %s: %s", user, changed[:20])
                    _ovd_sids = sorted({int(it.get("secId"))
                        for it in (items or [])
                        if str(it.get("secId") or "").isdigit()
                        and any(f in it for f in OVD_FIELDS)})
                    if _ovd_sids:
                        async def _auto_nuke(sids=_ovd_sids, u=user):
                            try:
                                await asyncio.wait_for(
                                    _nlock().acquire(), timeout=15)
                            except Exception:
                                return   # lock unavailable -
                                # skip this auto-nuke silently
                            try:
                                ents = []
                                for s2 in sids:
                                    rw = STATE["rows"].get(s2, {})
                                    def nm(v):
                                        try:
                                            return float(str(v)
                                                .replace(",", "") or 0)
                                        except ValueError:
                                            return 0.0
                                    ents.append({"secId": s2,
                                        "ovdSpot": nm(rw.get("ovdSpot")),
                                        "ovdCbFx": nm(rw.get("ovdCbFx")),
                                        "ovdUndFx": nm(rw.get(
                                            "ovdUndFx"))})
                                data = await run_in_threadpool(
                                    run_nuke_batches, ents)
                                for r2 in data.get("rows", []):
                                    s3 = r2.get("secId")
                                    if s3 is not None:
                                        STATE["nuke"][int(s3)] = r2
                                STATE["nukeMeta"] = {
                                    "host": data.get("host", ""),
                                    "elapsed": data.get("elapsed",
                                                        0),
                                    "by": u + " (auto)",
                                    "ts": datetime.now().strftime(
                                        "%H:%M:%S"),
                                    "missing": data.get("missing",
                                                        [])}
                                STATE["version"] += 1
                                data["by"] = u + " (auto)"
                                await broadcast({"type": "nuke",
                                    "data": data,
                                    "v": STATE["version"]})
                                logger.info("auto-nuke by %s: %d "
                                    "rics on ovd edit", u, len(ents))
                            except Exception as exc:
                                logger.info("auto-nuke failed: %s",
                                            exc)
                            finally:
                                if _nlock().locked():
                                    _nlock().release()
                        asyncio.create_task(_auto_nuke())

            elif t == "nuke":
                sec_ids = [int(i) for i in msg.get("secIds", [])
                           if str(i).isdigit()]
                if not sec_ids:
                    continue
                entries = []
                for sid in sec_ids:
                    row = STATE["rows"].get(sid, _blank_row())
                    def num(v):
                        try:
                            return float(str(v).replace(",", "") or 0)
                        except ValueError:
                            return 0.0
                    entries.append({"secId": sid,
                                    "ovdSpot": num(row.get("ovdSpot")),
                                    "ovdCbFx": num(row.get("ovdCbFx")),
                                    "ovdUndFx": num(row.get("ovdUndFx"))})
                await broadcast({"type": "nukeStart", "by": user,
                                 "n": len(entries)})
                try:
                    await asyncio.wait_for(_nlock().acquire(), timeout=15)
                except asyncio.TimeoutError:
                    await broadcast({"type": "nukeErr", "by": user,
                                     "error": "Pricing engine busy for 15s "
                                     "- previous nuke still running; retry"})
                    continue
                try:
                    try:
                        data = await asyncio.wait_for(
                            run_in_threadpool(run_nuke_batches, entries),
                            timeout=int(os.environ.get(
                                "NUKE_PRICE_TIMEOUT", "45")))
                    except asyncio.TimeoutError as exc:
                        logger.error("nuke by %s timed out", user)
                        await broadcast({"type": "nukeErr", "by": user,
                                         "error": "Pricing timed out - "
                                         "engine unresponsive"})
                        continue
                    except requests.RequestException as exc:
                        logger.error("nuke by %s failed: %s", user, exc)
                        await broadcast({"type": "nukeErr",
                                         "error": f"Upstream call failed: {exc}",
                                         "by": user})
                        await run_in_threadpool(db_log, user, "nuke_error",
                                                {"error": str(exc)[:500]})
                        continue
                finally:
                    if _nlock().locked():
                        _nlock().release()
                for r in data["rows"]:
                    if r.get("secId") is not None:
                        STATE["nuke"][int(r["secId"])] = r
                STATE["nukeMeta"] = {"host": data["host"],
                                     "elapsed": data["elapsed"],
                                     "by": user,
                                     "ts": datetime.now().strftime("%H:%M:%S"),
                                     "missing": data["missing"]}
                STATE["version"] += 1
                data["by"] = user
                await broadcast({"type": "nuke", "data": data,
                                 "v": STATE["version"]})
                await run_in_threadpool(db_log, user, "nuke",
                                        {"n": len(entries),
                                         "returned": data["returned"],
                                         "elapsed": data["elapsed"],
                                         "host": data["host"]})
                logger.info("nuke by %s: %d req, %d returned, %.2fs on %s",
                            user, len(entries), data["returned"],
                            data["elapsed"], data["host"])

            elif t == "refresh":
                sec = max(2, int(msg.get("sec") or RFX_REFRESH_DEFAULT))
                STATE["refreshSec"] = sec
                db_meta_set("refreshSec", sec)
                await broadcast({"type": "cfg", "refreshSec": sec, "by": user})
                if RFX_WAKE: RFX_WAKE.set()
                logger.info("refresh interval set to %ss by %s", sec, user)

            elif t == "autosave":
                sec = max(0, int(msg.get("sec") or 0))
                STATE["autosaveSec"] = sec
                db_meta_set("autosaveSec", sec)
                await broadcast({"type": "cfg", "autosaveSec": sec,
                                 "by": user})
                if AUTOSAVE_WAKE: AUTOSAVE_WAKE.set()
                logger.info("autosave interval set to %ss by %s", sec, user)

            elif t == "refInt":
                sec = max(30, int(msg.get("sec") or REFDATA_REFRESH_DEFAULT))
                STATE["refdataSec"] = sec
                db_meta_set("refdataSec", sec)
                await broadcast({"type": "cfg", "refdataSec": sec, "by": user})
                if REFDATA_WAKE: REFDATA_WAKE.set()
                logger.info("refdata interval set to %ss by %s", sec, user)

            elif t == "flagTh":
                th = dict(STATE["flagTh"])
                for k, lo in (("staleSpot", 0.01), ("staleFx", 0.01),
                              ("moveStk", 0.1), ("moveFx", 1.0)):
                    if msg.get(k) is not None:
                        try:
                            th[k] = max(lo, float(msg[k]))
                        except (TypeError, ValueError):
                            pass
                STATE["flagTh"] = th
                db_meta_set("flagTh", th)
                await broadcast({"type": "cfg", "flagTh": th, "by": user})
                logger.info("flag thresholds set by %s: %s", user, th)

            elif t == "rfxNow":
                if RFX_WAKE: RFX_WAKE.set()

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("ws loop ended for %s: %s", user, exc)
    finally:
        CLIENTS.discard(ws)
        CLIENT_NAMES.pop(ws, None)
        await broadcast({"type": "online", "n": len(CLIENTS),
                         "note": f"{user} left"})
        logger.info("ws bye: %s (online=%d)", user, len(CLIENTS))
        try:
            await run_in_threadpool(db_log, user, "disconnect", {})
        except Exception:
            pass


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>CB nuke station</title>
<style>
td[data-c="m_delta"],td[data-c="x_bid"]{min-width:52px}
:root{
  --bg:#ffffff; --panel:#f2f3f5; --panel2:#f7f8f9; --row:#f7f8fa; --hover:#eef0f3;
  --rowsel:#dbe7f7; --border:#e3e6ea; --border2:#c9ced4;
  --text:#16181d; --muted:#5a6068; --faint:#8b919a;
  --amber:#8a5b00; --amber-dim:#f7f1e2; --green:#106b3f; --red:#a8231b;
  --blue:#274f8f; --teal:#0b6e66; --teal-dim:rgba(11,110,102,.08);
  --selbg:rgba(39,79,143,.10); --selbg2:rgba(39,79,143,.18);
  --mono:'Consolas','JetBrains Mono',monospace;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
  font:12px/1.35 var(--mono)}
header{display:flex;align-items:center;gap:10px;
  padding:5px 12px;border-bottom:1px solid var(--border2);
  background:var(--panel)}
header h1{font-size:12px;font-weight:700;margin:0;letter-spacing:1.5px;
  text-transform:uppercase}
header span.sub{color:var(--faint);font-size:10px;font-family:var(--mono)}
.conn{font-size:10px;line-height:1}
td.gwarn{color:var(--amber)}
.conn.ok{color:var(--green)} .conn.warn{color:var(--amber)} .conn.err{color:var(--red)}
.tabs{margin-left:auto;display:flex;gap:0}
.tabs button{padding:3px 11px;border-radius:0;font-size:10.5px;
  letter-spacing:.6px;text-transform:uppercase}
.tabs button.active{background:var(--text);border-color:var(--text);
  color:#ffffff;font-weight:700}
.layout{display:grid;grid-template-columns:172px 1fr;min-height:calc(100vh - 29px)}
.side{border-right:1px solid var(--border);padding:8px;background:var(--panel2);
  position:relative}
#siderz{position:absolute;top:0;right:-3px;width:6px;height:100%;
  cursor:col-resize;z-index:7}
#siderz:hover{background:var(--amber);opacity:.4}
.side.collapsed{padding:0}
.side.collapsed label,.side.collapsed textarea,
.side.collapsed button:not(#sidecol),.side.collapsed #siderz{display:none}
#sidecol{position:absolute;top:1px;right:3px;width:auto;margin:0;
  padding:0 3px;border:none;background:transparent;color:var(--faint);
  font:11px var(--mono);cursor:pointer;z-index:8}
#sidecol:hover{color:var(--amber);border:none;background:transparent}
.side label{display:block;font-size:9px;color:var(--muted);margin-bottom:4px;
  text-transform:uppercase;letter-spacing:.6px}
.side textarea{width:100%;height:300px;resize:vertical;background:var(--bg);
  color:var(--text);border:1px solid var(--border);border-radius:0;
  padding:5px;font:11px var(--mono)}
.side button{width:100%;margin-top:6px}
main{padding:6px 8px;overflow-x:auto}
button{background:var(--bg);color:var(--text);border:1px solid var(--border2);
  border-radius:0;padding:3px 9px;font:11px var(--mono);cursor:pointer}
button:hover{border-color:var(--text)}
button:disabled{opacity:.4;cursor:default}
button.primary{background:var(--text);border-color:var(--text);color:#ffffff;font-weight:700}
button.primary:disabled{opacity:.5;cursor:wait}
button.b-a{border-color:var(--border2);color:var(--amber)}
button.b-a:hover{border-color:var(--amber);background:var(--amber-dim);color:var(--amber)}
button.b-e{border-color:var(--border2);color:var(--blue);
  box-shadow:inset 0 -2px 0 var(--teal)}
button.b-e:hover{border-color:var(--blue);background:var(--rowsel);color:var(--blue)}
button.b-t{border-color:var(--border2);color:var(--teal)}
button.b-t:hover{border-color:var(--teal);background:var(--teal-dim);color:var(--teal)}
button.b-g{border-color:var(--border2);color:var(--green)}
button.b-g:hover{border-color:var(--green);background:#e9f5ee;color:var(--green)}
button.b-r{border-color:var(--border2);color:var(--red)}
button.b-r:hover{border-color:var(--red);background:#f9ebe9;color:var(--red)}
.toprow{display:flex;align-items:baseline;gap:10px;margin:0 0 4px}
.namebox{margin-left:auto;font:10px var(--mono);color:var(--muted);
  background:var(--panel2);border:1px solid var(--border);border-radius:0;
  padding:1px 8px;min-width:120px;text-align:center}
.tablewrap{position:relative;border:1px solid var(--border2);border-radius:0;
  overflow:auto;max-height:calc(100vh - 130px)}
table{border-collapse:separate;border-spacing:0;font-family:var(--mono);
  font-size:11.5px;white-space:nowrap;width:max-content}
th,td{padding:2px 7px;border-bottom:1px solid var(--border);text-align:right;
  background:var(--bg)}
tr:nth-child(even) td{background:var(--row)}
tr td.gM{background:#eef6f0} tr:nth-child(even) td.gM{background:#e8f1ea}
tr td.gO{background:#f8f3e6} tr:nth-child(even) td.gO{background:#f3edda}
tr td.gL{background:#edf3fb} tr:nth-child(even) td.gL{background:#e7eef8}
tr td.gE{background:#f0f1f4} tr:nth-child(even) td.gE{background:#eaecf0}
tr.rowsel td{background:var(--rowsel);
  border-top:1px solid #0b6e66 !important;
  border-bottom:1px solid #0b6e66 !important}
tr.rowsel.rowfocus td{background-image:linear-gradient(rgba(11,110,102,.16),rgba(11,110,102,.16))}
tr.rowsel td:first-child{border-left:4px solid #0b6e66 !important}
tr.rowsel td.stick1{font-weight:700;color:#0b6e66}
tbody tr:hover td{background:var(--hover)}
th{color:var(--muted);font-weight:700;font-size:9.5px;letter-spacing:.4px;
  text-transform:uppercase;position:sticky;top:0;z-index:3;
  background:var(--panel)!important;border-bottom:1px solid var(--text)}
tr.band td{position:sticky;top:0;z-index:3;background:var(--panel)!important;
  border-bottom:none;padding:3px 7px 0;color:var(--faint);
  font-size:9px;text-transform:uppercase;letter-spacing:.7px;text-align:left}
tr.band ~ tr th{top:16px}
tr.band td.in{color:var(--amber)}
tr.band td.gm{color:var(--green)}
tr.band td.go{color:var(--amber)}
tr.band td.gl{color:var(--blue)}
tr.band td.ge{color:var(--muted);cursor:pointer}
tr.band td.uin{color:var(--teal)}
tr.band td.rfx{color:var(--blue);cursor:pointer}
tr.band td.fxx{color:var(--teal);cursor:pointer}
td.ref,th.ref{text-align:left;color:var(--muted)}
td.co{max-width:180px;overflow:hidden;text-overflow:ellipsis}
td.rf,th.rf{color:var(--blue)}
td.fx,th.fx{color:var(--teal)}
.stick0{position:sticky;left:0;z-index:2;min-width:28px;max-width:28px;
  text-align:center!important;padding:2px 5px}
.stick1{position:sticky;left:28px;z-index:2;min-width:97px;max-width:97px;
  text-align:left}
.stick2{position:sticky;left:125px;z-index:2;min-width:180px;max-width:180px}
.stick3{position:sticky;left:305px;z-index:2;border-right:1px solid var(--border2)}
th.stick0,th.stick1,th.stick2,th.stick3,tr.band td.stick0,tr.band td.stick3{z-index:4}
.stick0 input[type=checkbox]{accent-color:var(--text);width:12px;height:12px;
  cursor:pointer}
td.rowclick{cursor:pointer}
.grp{border-left:1px solid var(--border2)}
td.gc{padding:0;position:relative}
td.gc input{width:100%;height:100%;background:transparent;border:1px solid transparent;
  border-radius:0;padding:2px 7px;font:11.5px var(--mono);outline:none;
  cursor:cell;user-select:none}
td.gc input.editing{cursor:text;user-select:text}
tbody tr:hover td.gc input{border-color:var(--border2)}
td.inp input{color:var(--amber);text-align:right}
td.inp input:not(:placeholder-shown){background:var(--amber-dim)}
td.uinp input{color:var(--teal);text-align:left}
td.uinp input:not(:placeholder-shown){background:var(--teal-dim)}
td.gc.sel input{background:var(--selbg)!important}
td.gc.sel input:not(:placeholder-shown){background:var(--selbg2)!important}
td.gc.active input{box-shadow:inset 0 0 0 1px var(--amber)}
td.uinp.active input{box-shadow:inset 0 0 0 1px var(--teal)}
td.gc.fillprev input{box-shadow:inset 0 0 0 1px var(--amber);
  background:rgba(138,91,0,.07)!important}
td.gc input.justset{box-shadow:inset 0 0 0 1px var(--green)}
.chips{position:absolute;left:1px;top:50%;transform:translateY(-50%);
  display:none;gap:1px;z-index:1}
td.inp:hover .chips,td.inp.active .chips{display:flex}
.chips b{font:8px/1 var(--mono);font-weight:700;padding:1px 2px;
  border:1px solid var(--border2);border-radius:0;color:var(--muted);
  cursor:pointer;background:var(--bg)}
.chips b:hover{color:#ffffff;background:var(--amber);border-color:var(--amber)}
.chips b.rfs{color:var(--blue)}
.chips b.rfs:hover{background:var(--blue);border-color:var(--blue);color:#ffffff}
.chips b.rff{color:var(--teal)}
.chips b.rff:hover{background:var(--teal);border-color:var(--teal);color:#ffffff}
.qgrp{display:inline-flex;align-items:center;gap:3px;
  border:1px solid var(--border2);padding:2px 5px 2px 4px;
  background:var(--panel2)}
.qgrp>b{font:8px var(--mono);font-weight:700;color:var(--faint);
  letter-spacing:1px;margin-right:2px}
.qgrp.qvs button{border-color:#b9c6dd;color:var(--blue)}
.qgrp.qvs button:hover{background:var(--blue);border-color:var(--blue);color:#ffffff}
.qgrp.qor button{border-color:#d9c9a3;color:var(--amber)}
.qgrp.qor button:hover{background:var(--amber);border-color:var(--amber);color:#ffffff}
td.gc input.sprd{cursor:text;user-select:text}
td.gc input.sprd:focus{box-shadow:inset 0 0 0 1px var(--amber)}
#tbl.hb-model th[data-band="model"]:not(.bfirst),#tbl.hb-model td[data-band="model"]:not(.bfirst){display:none}
#tbl.hb-twq th[data-band="twq"]:not(.bfirst),#tbl.hb-twq td[data-band="twq"]:not(.bfirst){display:none}
#tbl.hb-twq .bfirst[data-band="twq"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f1ecf6;border-left:1px solid #d8d4cc}
.tq{background:#efe9f5;color:#4a2c6b}
td[data-band="twq"].tqc{background:#f7f4fa;font-variant-numeric:tabular-nums;white-space:nowrap;text-align:right}
td[data-c="twq_bind"].tqc,td[data-c="twq_trend"].tqc,td[data-c="twq_dt"].tqc{text-align:center}
td[data-c="twq_lim"].tqc{background:#fff7ed;font-weight:700}td[data-c="twq_sess"].tqc{background:#f0fdf4;font-weight:700}
td[data-band="twq"].tqc.tqw{background:#fffbeb}
.tqp{display:inline-block;padding:0 5px;border-radius:3px;font:700 9px 'Segoe UI',system-ui,sans-serif;line-height:14px;letter-spacing:.3px}
.tqp.sc{background:#b91c1c;color:#fff}.tqp.th{background:#dbeafe;color:#1e3a8a}.tqp.rs{background:#e5e7eb;color:#374151}.tqp.ck{background:#fde68a;color:#78350f}
td[data-c="twq_used"].u1{color:#15803d}td[data-c="twq_used"].u2{color:#1d4ed8}td[data-c="twq_used"].u3{color:#c2410c;font-weight:700}td[data-c="twq_used"].u4{background:#b91c1c !important;color:#fff;font-weight:700}
.tqt{display:inline-block;font-style:normal;font-size:8px;font-weight:700;padding:0 3px;border-radius:2px;margin-left:3px;vertical-align:middle;line-height:11px}.tqt.tS{background:#dcfce7;color:#14532d}.tqt.tD{background:#fef3c7;color:#92400e}
.tqb{display:inline-block;width:4px;height:2px;margin-right:1px;background:#8b5cf6;vertical-align:bottom}.tqb.tq0{background:#e5e7eb}
.tqa{font-weight:700}.tqa.up{color:#c2410c}.tqa.down{color:#15803d}.tqa.flat{color:#6b7280}
.tqhd{font-weight:600;font-size:9.5px;padding:0 4px;border-radius:3px;background:rgba(255,255,255,.6);margin-left:4px}.tqhd.pend{color:#92400e;background:#fef3c7}.tqhd.bad{color:#fff;background:#b91c1c}
#tbl.hb-res th[data-band="res"]:not(.bfirst),#tbl.hb-res td[data-band="res"]:not(.bfirst){display:none}
#tbl.hb-cbnuke th[data-band="cbnuke"]:not(.bfirst),#tbl.hb-cbnuke td[data-band="cbnuke"]:not(.bfirst){display:none}
#tbl.hb-cbnuke .bfirst[data-band="cbnuke"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-cbnuke .bfirst[data-band="cbnuke"] input{display:none}
.cn{background:#e3eef9;color:#1e3a6e}td[data-band="cbnuke"].cnc{background:#eef3fb;font-weight:600}td[data-band="cbnuke"].cnt{background:#f4f7fb;font-size:9.5px;color:#555;text-align:left}
input.cnv{width:62px;text-align:center}td[data-c="cn_tk"].cn-err{color:#b91c1c}input.cnv.cn-bad{background:#fee2e2;color:#7f1d1d}
td.ivcell{white-space:nowrap}td.ivcell input.ivv{width:52px;text-align:right}td.ivcell input.ivv:not(:placeholder-shown){background:#fff3cd}td.ivcell span.iv-db{display:inline-block;padding:2px 6px;font:11.5px var(--mono);color:#1e3a6e;font-weight:600}
td.hdg{text-align:center}td.hdg input.hdgcb{width:14px;height:14px;cursor:pointer;accent-color:#0b6e66;vertical-align:middle}td.hdg.on{background:#dcfce7}
td.isincell{white-space:nowrap;min-width:110px}td.isincell input.isinv{text-align:left;text-transform:uppercase;color:#7a5a00}td.isincell input.isinv:not(:placeholder-shown){background:#fff3cd}
td.isincell span.isin-db{display:inline-block;padding:2px 7px;color:var(--muted);font:11.5px var(--mono)}td[data-fl="f_trig"]{color:#6b21a8;font-weight:600}
input.hnv{width:60px;text-align:right}input.hnv.hn-live{color:#1e3a6e;font-style:italic}input.hnv:not(.hn-live):not(:placeholder-shown){color:#111;font-style:normal}td[data-band="cbnuke"].hnc{background:#dcfce7;font-weight:700}
#tbl.hb-idb th[data-band="idb"]:not(.bfirst),#tbl.hb-idb td[data-band="idb"]:not(.bfirst){display:none}
#tbl.hb-idb .bfirst[data-band="idb"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
.ib{background:#dcd3f0;color:#3b2a6e}td[data-band="idb"].ichk{background:#f6f6f6}
/* IDB quotes: colour by SIDE (blue = bid, orange = offer; colour-blind safe), broker light / mine strong */
:root{--idb-ibBid-bg:#dbeafe;--idb-ibBid-fg:#111827;--idb-myBid-bg:#93c5fd;--idb-myBid-fg:#111827;--idb-ibOfr-bg:#ffedd5;--idb-ibOfr-fg:#111827;--idb-myOfr-bg:#fdba74;--idb-myOfr-fg:#111827;
      --idb-ibBidq-bg:#eaf2ff;--idb-ibBidq-fg:#1e3a8a;--idb-ibOfrq-bg:#fff4e8;--idb-ibOfrq-fg:#7c2d12}
td[data-band="idb"].ib-bid{background:var(--idb-ibBid-bg);color:var(--idb-ibBid-fg)}
td[data-band="idb"].my-bid{background:var(--idb-myBid-bg);color:var(--idb-myBid-fg);font-weight:700}
td[data-band="idb"].ib-ofr{background:var(--idb-ibOfr-bg);color:var(--idb-ibOfr-fg)}
td[data-band="idb"].my-ofr{background:var(--idb-myOfr-bg);color:var(--idb-myOfr-fg);font-weight:700}
td[data-band="idb"].ib-bidq{background:var(--idb-ibBidq-bg);color:var(--idb-ibBidq-fg);font-weight:700}
td[data-band="idb"].ib-ofrq{background:var(--idb-ibOfrq-bg);color:var(--idb-ibOfrq-fg);font-weight:700}
td[data-band="idb"].ib-refu{background:rgb(229,231,235);color:rgb(55,65,81);font-weight:700;border-left:2px solid rgb(156,163,175)}
td[data-c="idb_flag"]{text-align:left;white-space:nowrap}
.ip{display:inline-block;padding:0 5px;border-radius:9px;font-size:9px;font-weight:700;letter-spacing:.3px;line-height:14px;vertical-align:middle;font-family:'Segoe UI',system-ui,sans-serif}
.ip-x{background:#b91c1c;color:#fff}.ip-g1{background:#fde68a;color:#78350f}.ip-g2{background:#f59e0b;color:#3b1f00}.ip-g3{background:#c2410c;color:#fff}
.ip-b1{background:#dbeafe;color:#1e3a8a}.ip-b2{background:#60a5fa;color:#0b1b4d}.ip-b3{background:#1d4ed8;color:#fff}
.ip-o1{background:#ffedd5;color:#7c2d12}.ip-o2{background:#fb923c;color:#3b1104}.ip-o3{background:#c2410c;color:#fff}
.ip-grey{background:#e5e7eb;color:#374151}.ip-out{background:transparent;color:#6b7280;border:1px solid #cbd5e1;line-height:12px}.ip-blue{background:transparent;color:#1d4ed8;border:1px solid #93c5fd;line-height:12px}.ip-purple{background:#ede9fe;color:#5b21b6}
#tbl.hb-lrisk th[data-band="lrisk"]:not(.bfirst),#tbl.hb-lrisk td[data-band="lrisk"]:not(.bfirst){display:none}
#tbl.hb-lrisk .bfirst[data-band="lrisk"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
.lr{background:#e6f1ee;color:#0b4a44}td[data-band="lrisk"].lrc{background:#f1f8f6;font-variant-numeric:tabular-nums}td[data-band="lrisk"].lrc.pos{color:#15803d}td[data-band="lrisk"].lrc.neg{color:#b91c1c}td[data-band="lrisk"].lrc.lr-na{color:#9ca3af}
#tbl.hb-cls th[data-band="cls"]:not(.bfirst),#tbl.hb-cls td[data-band="cls"]:not(.bfirst){display:none}
#tbl.hb-cls .bfirst[data-band="cls"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
.cl{background:#e3e8f6;color:#1e3a6e}.cl a.wslink{color:#1e3a6e;text-decoration:underline;font-weight:600;margin-left:6px}td[data-band="cls"].clc{background:#f3f5fb;white-space:nowrap;max-width:230px;overflow:hidden;text-overflow:ellipsis}
.ct{display:inline-block;padding:0 6px;border-radius:3px;font:700 9px 'Segoe UI',system-ui,sans-serif;letter-spacing:.3px;line-height:14px}.ct.h{background:#2340B8;color:#fff}.ct.o{background:#7A5F12;color:#fff}.ct.n{border:1px dashed #9ca3af;color:#6b7280;background:transparent}
.ct.tr{background:#e5e7eb;color:#374151;margin-right:2px}.ct.tr.bad{background:#fde2e2;color:#7f1d1d}.ct.ok{background:#dcfce7;color:#14532d}.ct.chp{background:#d7f0ec;color:#0f5f55}.ct.rch{background:#f7dbe4;color:#8a2a4d}.ct.q{border:1px dashed currentColor;background:transparent}
#tbl.hb-dcalc th[data-band="dcalc"]:not(.bfirst),#tbl.hb-dcalc td[data-band="dcalc"]:not(.bfirst){display:none}
#tbl.hb-dcalc .bfirst[data-band="dcalc"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-dcalc .bfirst[data-band="dcalc"] input{display:none}
input.dcv{width:100%;box-sizing:border-box}
.dc{background:#f6f3ea}
td[data-c="dc_shares"]{background:#f6f3ea;font-weight:600;text-align:right}td[data-c="dc_usd"] input.dcv{text-align:right}td[data-c="dc_usd"] input.dcv.derived{color:#6b7280;font-style:italic}
#tbl.hb-inp th[data-band="inp"]:not(.bfirst),#tbl.hb-inp td[data-band="inp"]:not(.bfirst){display:none}
#tbl.hb-live th[data-band="live"]:not(.bfirst),#tbl.hb-live td[data-band="live"]:not(.bfirst){display:none}
#tbl.hb-eod th[data-band="eod"]:not(.bfirst),#tbl.hb-eod td[data-band="eod"]:not(.bfirst){display:none}
#tbl.hb-theo th[data-band="theo"]:not(.bfirst),#tbl.hb-theo td[data-band="theo"]:not(.bfirst){display:none}
#tbl.hb-stk th[data-band="stk"]:not(.bfirst),#tbl.hb-stk td[data-band="stk"]:not(.bfirst){display:none}
#tbl.hb-cbfx th[data-band="cbfx"]:not(.bfirst),#tbl.hb-cbfx td[data-band="cbfx"]:not(.bfirst){display:none}
#tbl.hb-cbfx .bfirst[data-band="cbfx"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
.cfx{background:#efe4f3}td.cf{background:#f6eef9}
#tbl.hb-fx th[data-band="fx"]:not(.bfirst),#tbl.hb-fx td[data-band="fx"]:not(.bfirst){display:none}
#tbl.hb-flags th[data-band="flags"]:not(.bfirst),#tbl.hb-flags td[data-band="flags"]:not(.bfirst){display:none}
#tbl.hb-vol th[data-band="vol"]:not(.bfirst),#tbl.hb-vol td[data-band="vol"]:not(.bfirst){display:none}
#tbl.hb-model .bfirst[data-band="model"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-model .bfirst[data-band="model"] input,#tbl.hb-model .bfirst[data-band="model"] .rz{display:none}
#tbl.hb-res .bfirst[data-band="res"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-res .bfirst[data-band="res"] input,#tbl.hb-res .bfirst[data-band="res"] .rz{display:none}
#tbl.hb-inp .bfirst[data-band="inp"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-inp .bfirst[data-band="inp"] input,#tbl.hb-inp .bfirst[data-band="inp"] .rz{display:none}
#tbl.hb-live .bfirst[data-band="live"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-live .bfirst[data-band="live"] input,#tbl.hb-live .bfirst[data-band="live"] .rz{display:none}
#tbl.hb-eod .bfirst[data-band="eod"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-eod .bfirst[data-band="eod"] input,#tbl.hb-eod .bfirst[data-band="eod"] .rz{display:none}
#tbl.hb-theo .bfirst[data-band="theo"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-theo .bfirst[data-band="theo"] input,#tbl.hb-theo .bfirst[data-band="theo"] .rz{display:none}
#tbl.hb-stk .bfirst[data-band="stk"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-stk .bfirst[data-band="stk"] input,#tbl.hb-stk .bfirst[data-band="stk"] .rz{display:none}
#tbl.hb-fx .bfirst[data-band="fx"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-fx .bfirst[data-band="fx"] input,#tbl.hb-fx .bfirst[data-band="fx"] .rz{display:none}
#tbl.hb-flags .bfirst[data-band="flags"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-flags .bfirst[data-band="flags"] input,#tbl.hb-flags .bfirst[data-band="flags"] .rz{display:none}
#tbl.hb-vol .bfirst[data-band="vol"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-vol .bfirst[data-band="vol"] input,#tbl.hb-vol .bfirst[data-band="vol"] .rz{display:none}
.fb{background:#f0e9f7}
td[data-fl]{text-align:center;font-size:10.5px}
td[data-fl^="f_"].fl-red{color:#c62828;font-weight:700}
td[data-fl^="f_"].fl-amb{color:#b26a00;font-weight:600}
td[data-fl^="f_"].fl-dim{color:#b6b1a8}
td[data-fl="stk_move"],td[data-fl="fx_move"]{text-align:right}
td.bemv{color:#334155;font-variant-numeric:tabular-nums}td[data-fl="f_move"]{white-space:nowrap;text-align:left}td[data-fl="f_move"].fl-dim{color:#8a8a8a}
td.mv-p0{color:#15803d;font-weight:600}td.mv-n0{color:#b91c1c;font-weight:600}
td.mv-p1{background:#dcfce7 !important;color:#14532d;font-weight:600}td.mv-n1{background:#fee2e2 !important;color:#7f1d1d;font-weight:600}
td.mv-p2{background:#a7f3c4 !important;color:#14532d;font-weight:700}td.mv-n2{background:#fecaca !important;color:#7f1d1d;font-weight:700}
td.mv-p3{background:#4ade80 !important;color:#052e16;font-weight:700}td.mv-n3{background:#f87171 !important;color:#450a0a;font-weight:700}
td.mv-p4{background:#15803d !important;color:#fff;font-weight:700}td.mv-n4{background:#b91c1c !important;color:#fff;font-weight:700}
tr.rowfocus td{background-image:linear-gradient(rgba(11,110,102,.10),rgba(11,110,102,.10))}
tr.rowfocus td:first-child{border-left:4px solid #0b6e66 !important}
tr.rowfocus td.stick1{font-weight:700;color:#0b6e66}
td.bgpos{background:#d7f2dd !important;color:#14532d;font-weight:600}
td.bgneg{background:#fbdcda !important;color:#7f1d1d;font-weight:600}
td.qcell{color:#0b3d91;font-weight:600;text-align:right}
td[data-q="mid_drift"]{text-align:right;font-weight:600}
.volsel{font:inherit;font-size:10.5px;border:1px solid #cfcabf;
  background:#fffbe8;border-radius:3px}
.volsel.vol-cheap{background:#d7f2dd;color:#14532d;border-color:#14532d;
  font-weight:700}
.volsel.vol-rich{background:#fbdcda;color:#7f1d1d;border-color:#7f1d1d;
  font-weight:700}
.vb{background:#e8ddf2;color:#4a2d6b}
td[data-v]{text-align:right;color:#4a2d6b}
th[data-band="vol"] input{width:26px;font:inherit;font-size:10px;
  border:1px solid #b9a8cf;border-radius:2px;background:#f6f0fc}
td.gc.stick3{position:sticky}
td.cpy{cursor:pointer}
td.cpy:hover{color:var(--amber)}
#fh{position:absolute;width:7px;height:7px;background:var(--text);
  border:1px solid #ffffff;cursor:crosshair;z-index:5;display:none}
.rz{position:absolute;right:-3px;top:0;width:6px;height:100%;cursor:col-resize;
  z-index:6}
.rz:hover{background:var(--amber);opacity:.4}
th{position:sticky}
.bar{display:flex;align-items:center;gap:6px;margin-top:8px;flex-wrap:wrap}
.status{margin-left:auto;font:11px var(--mono);color:var(--muted)}
.status .ok{color:var(--green)} .status .warn{color:var(--amber)} .status .err{color:var(--red)}
.ovd-on{background:var(--amber-dim)!important}
.pos{color:var(--green)} .neg{color:var(--red)}
.hint{color:var(--faint);font-size:10px;margin:0}
.filters{display:flex;align-items:center;gap:10px;font-size:11px;color:var(--text)}.filters .fl-lbl{color:var(--muted);font-weight:700;letter-spacing:.3px;text-transform:uppercase;font-size:9.5px}
.filters label{display:inline-flex;align-items:center;gap:4px;cursor:pointer;padding:1px 6px;border:1px solid var(--border2);border-radius:3px;background:var(--panel)}.filters label:has(input:checked){background:#0b6e66;color:#fff;border-color:#0b6e66}
.filters .fl-cnt{color:var(--muted);font-variant-numeric:tabular-nums}.filters .fl-sep{width:1px;height:14px;background:var(--border2)}.filters .fl-grp{color:var(--muted);font-size:9.5px;font-weight:700;text-transform:uppercase}
#tbl tr.flt-hide{display:none}
kbd{background:var(--panel2);border:1px solid var(--border2);border-radius:0;
  padding:0 4px;font:10px var(--mono);color:var(--muted)}
h2{font-size:10.5px;font-weight:700;color:var(--muted);margin:0;
  text-transform:uppercase;letter-spacing:.7px}
.cfg{max-width:520px;background:var(--panel2);border:1px solid var(--border);
  border-radius:0;padding:12px 14px}
.cfg h2{margin-bottom:10px}
.cfg .fieldrow{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.cfg .fieldrow label{flex:1;font-size:11px;color:var(--muted)}
.cfg input[type=number]{width:100px;background:var(--bg);color:var(--text);
  border:1px solid var(--border2);border-radius:0;padding:3px 8px;
  font:11.5px var(--mono);text-align:right}
.cfg input[type=checkbox]{accent-color:var(--text);width:13px;height:13px}
.cfg .note{font-size:10px;color:var(--faint);margin:8px 0 10px}
</style>
<style id="colstyle"></style>
</head>
<body>
<header>
  <h1>CB nuke station</h1>
  <span class="sub">/GetNukedCBPrice &middot; wlb4 &middot; cbanalytics &middot; eqrms &middot; refinitiv &middot; cba_app &middot; <b style="color:#6b4b8a">borrow.b63</b></span>
  <span id="conn" class="conn warn" title="Connection">&#9679;</span>
  <span id="online" class="sub"></span>
  <div class="tabs">
    <button id="tabMain" class="active" onclick="showTab('main')">Workstation</button>
    <button id="tabRuns" onclick="showTab('runs')">Runs</button>
    <button id="tabSnap" onclick="showTab('snap')">Morning Snap</button>
    <button id="tabLog" onclick="showTab('log')">Log</button>
    <button id="tabCfg" onclick="showTab('cfg')">Config</button>
  </div>
</header>
<div class="layout" id="viewMain">
  <aside class="side">
    <span id="siderz" title="drag: sidebar width &middot; double-click: reset &middot; Save layout to keep"></span>
    <button id="sidecol" onclick="sideToggle()" title="collapse security IDs panel">&#9666;</button>
    <label for="ids">Security IDs (one per line)
      <span style="color:var(--faint)">&middot; __IDS_SRC__</span></label>
    <textarea id="ids"></textarea>
    <button class="b-a" onclick="sendIds()">Load into table (all users)</button>
  </aside>
  <main>
    <div class="toprow">
      <h2>Securities</h2>
      <div class="filters" id="filters" title="Filters show only the rows that match; hidden rows are still priced and refreshed. (Tip: und_fx takes an Eikon FX RIC - TWD=, KRW=, TWDKRW=R, or 1 for USD.)">
        <span class="fl-lbl">Filter</span>
        <label><input type="checkbox" id="fltMove" onchange="filtersChanged()"> MOVE flag</label>
        <span class="fl-sep"></span><span class="fl-grp">VOL</span>
        <label><input type="checkbox" data-flt="vol" value="Rich" onchange="filtersChanged()"> Rich</label>
        <label><input type="checkbox" data-flt="vol" value="Cheap" onchange="filtersChanged()"> Cheap</label>
        <label><input type="checkbox" data-flt="vol" value="" onchange="filtersChanged()"> Neutral</label>
        <span class="fl-sep"></span><span class="fl-grp">IDB gap</span>
        <label title="bid gap pill (G.B)"><input type="checkbox" data-flt="idb" value="gb" onchange="filtersChanged()"> G.B</label>
        <label title="offer gap pill (G.O)"><input type="checkbox" data-flt="idb" value="go" onchange="filtersChanged()"> G.O</label>
        <label title="mid gap pill (G)"><input type="checkbox" data-flt="idb" value="g" onchange="filtersChanged()"> G</label>
        <span class="fl-sep"></span><span class="fl-grp">Opp</span>
        <label title="no position (qty_live 0 or blank) and the IDB bid is above my bid (gap bid > 0)"><input type="checkbox" data-flt="opp" value="flat_gb" onchange="filtersChanged()"> Flat &amp; G.B &gt; 0</label>
        <label title="long (qty_live > 0) and the IDB offer is below my offer (gap ofr < 0)"><input type="checkbox" data-flt="opp" value="long_go" onchange="filtersChanged()"> Long &amp; G.O &lt; 0</label>
        <span class="fl-sep"></span><span class="fl-grp">Dates</span>
        <label title="call period has started (Bloomberg call schedule start <= today) and the bond has not matured"><input type="checkbox" data-flt="dt" value="in_call" onchange="filtersChanged()"> In call period</label>
        <label title="call period starts within the next 3 months"><input type="checkbox" data-flt="dt" value="call_3m" onchange="filtersChanged()"> Call starts &le; 3m</label>
        <label title="maturity within the next 6 months"><input type="checkbox" data-flt="dt" value="exp_6m" onchange="filtersChanged()"> Expiry &le; 6m</label>
        <label title="next put date within the next 6 months"><input type="checkbox" data-flt="dt" value="put_6m" onchange="filtersChanged()"> Put &le; 6m</label>
        <span id="fltCount" class="fl-cnt"></span>
      </div>
      <span class="namebox" id="namebox">&mdash;</span>
    </div>
    <div class="tablewrap" id="wrap"><table id="tbl"></table><div id="fh"></div></div>
    <div class="bar">
      <button class="primary" id="go" onclick="nuke()">Nuke prices</button>
      <button id="cplive" class="b-a" onclick="bulkCopy('live')">Live &rarr; ovd</button>
      <button id="cpeod" class="b-a" onclick="bulkCopy('eod')">EOD &rarr; ovd</button>
      <button class="b-e" onclick="bulkEikon('last')"
        title="Selected rows only: stock last &rarr; ovdSpot, fx last &rarr; ovdUndFx, live &rarr; ovdCbFx">Last &rarr; ovd</button>
      <button id="autoLastBtn" class="b-e" onclick="toggleAutoLast()"
        title="While ON: every Refinitiv tick copies stock last &rarr; ovdSpot and fx last &rarr; ovdUndFx on ALL rows, then auto-renukes changed rows">AUTO last: OFF</button>
      <button class="b-e" onclick="bulkEikon('close')"
        title="Selected rows only: stock close &rarr; ovdSpot, fx close &rarr; ovdUndFx, live &rarr; ovdCbFx">Close &rarr; ovd</button>
      <button class="b-r" onclick="clearOverrides()">Clear overrides</button>
      <button class="b-a" onclick="generateRuns()"
        title="Selected rows only: build a runs table in the Runs tab">Generate runs</button>
      <button class="b-g" onclick="saveToDb()">Save to DB</button>
      <button class="b-t" onclick="saveLayout()">Save layout</button>
      <button onclick="copyTable()">Copy table (TSV)</button>
      <span class="qgrp qvs"><b>VS</b><button onclick="quoteRows('vs')">copy vs</button><button onclick="quoteRows('bid')">vs bid</button><button onclick="quoteRows('ask')">vs ask</button></span>
      <span class="qgrp qor"><b>O/R</b><button onclick="quoteRows('or')">copy o/r</button><button onclick="quoteRows('orbid')">o/r bid</button><button onclick="quoteRows('orask')">o/r ask</button></span>
      <button id="dl" onclick="download()" disabled>Download JSON</button>
      <div class="status" id="status">Ready.</div>
    </div>
  </main>
</div>
<div id="viewLog" style="display:none;padding:14px 18px">
  <h3 style="margin:0 0 8px">Server log
    <button onclick="loadLogs()" style="margin-left:10px">&#8635;</button>
    <label style="margin-left:10px;font-weight:400;font-size:12px">
      <input type="checkbox" id="logAuto" checked> auto-refresh 5s</label>
    <button onclick="copyLogs()" style="margin-left:10px">Copy</button>
    <span id="logMeta" style="margin-left:12px;color:#6e6a63"></span>
  </h3>
  <pre id="logpre" style="background:#14161a;color:#d7dbe0;padding:10px 12px;
    border-radius:4px;max-height:70vh;overflow:auto;font-size:11.5px;
    line-height:1.45;white-space:pre-wrap"></pre>
</div>
<div id="viewSnap" style="display:none;padding:14px 18px">
  <h3 style="margin:0 0 8px">Morning marking snapshot
    <select id="sn8date" style="margin-left:10px"
      onchange="loadSnap8(this.value)"></select>
    <button onclick="loadSnap8(document.getElementById('sn8date').value)"
      style="margin-left:6px">&#8635;</button>
    <button onclick="snap8Now()" style="margin-left:6px"
      title="Close -> ovd for every row, re-nuke, then store QuoteBid/QuoteAsk (replaces today). Runs automatically at 08:00 and again at 08:55 if 08:00 did not save.">Snap now (Close &rarr; ovd)</button>
    <span id="sn8meta" style="margin-left:12px;color:#6e6a63"></span>
  </h3>
  <table class="report" id="sn8tbl" style="min-width:760px">
    <thead><tr><th>Short Name</th><th>ISIN</th><th>Bid</th><th>Ask</th>
      <th>Indic Ask</th><th>Vs</th><th>Fx</th><th>Vs USD</th>
      <th>Delta</th><th>Quantity</th><th>Snapped</th></tr></thead>
    <tbody></tbody></table>
</div>
<div class="layout" id="viewRuns" style="display:none">
  <aside class="side"></aside>
  <main>
    <div class="toprow">
      <h2>Runs</h2>
      <p class="hint">Generated from the selected rows on the Workstation tab.
        Bid/ask use the displayed rounding.</p>
    </div>
    <div class="tablewrap"><table id="runstbl"></table></div>
    <div class="bar">
      <button onclick="copyRuns()">Copy runs (TSV / Excel)</button>
      <button class="b-a" onclick="copyRunsBbg()"
        title="Copies an HTML table + TSV fallback &mdash; the same clipboard shape Excel uses">Copy for BBG chat (table)</button>
      <div class="status" id="runstatus">No runs generated yet.</div>
    </div>
  </main>
</div>
<div class="layout" id="viewCfg" style="display:none">
  <aside class="side"></aside>
  <main>
    <div class="cfg">
      <h2>Config</h2>
      <div class="fieldrow">
        <label for="cfgName">Your name (shown to others on edits)</label>
        <input type="text" id="cfgName" maxlength="24" style="width:150px;
          background:var(--bg);color:var(--text);border:1px solid var(--border2);
          border-radius:0;padding:3px 8px;font:11.5px var(--mono)">
      </div>
      <div class="fieldrow">
        <label for="cfgSec">Server Refinitiv refresh (seconds, all users)</label>
        <input type="number" id="cfgSec" min="2" step="1">
      </div>
      <div class="fieldrow">
        <label for="cfgRef">EQRMS / reference refresh (seconds, all users)</label>
        <input type="number" id="cfgRef" min="30" step="1">
      </div>
      <div class="fieldrow">
        <label for="cfgAuto">Autosave to DB (seconds, 0 = off, all users)</label>
        <input type="number" id="cfgAuto" min="0" step="5">
      </div>
      <div class="fieldrow">
        <label for="cfgThSS">RENUKE: stock move vs last nuke (%, all users)</label>
        <input type="number" id="cfgThSS" min="0.01" step="0.05">
      </div>
      <div class="fieldrow">
        <label for="cfgThSF">RENUKE: fx move vs last nuke (%, all users)</label>
        <input type="number" id="cfgThSF" min="0.01" step="0.05">
      </div>
      <div class="fieldrow">
        <label for="cfgThMS">MOVE flag: stock last vs close (%, all users)</label>
        <input type="number" id="cfgThMS" min="0.1" step="0.5">
      </div>
      <div class="fieldrow">
        <label for="cfgThMF">MOVE flag: fx last vs close (bps, all users)</label>
        <input type="number" id="cfgThMF" min="1" step="5">
      </div>
      <div class="fieldrow">
        <label for="cfgStep">Bid/ask rounding step (live, eod, override, &Delta;)</label>
        <input type="number" id="cfgStep" min="0.0001" step="0.01">
      </div>
      <h4 style="margin:14px 0 4px">Recovery</h4>
      <div class="fieldrow"><label>Names / und_fx missing?</label>
        <span><button type="button" onclick="recoverNames()">Recover from database, change history &amp; last autosave</button> <span id="recoverMsg" class="note"></span></span></div>
      <p class="note">Every change to a name, FX ric, type, X / spread or borrow field is also appended to cb_state_hist; a value overwritten in the live table is restored from there.</p>
      <h4 style="margin:14px 0 4px">IDB quotes band</h4>
      <div class="fieldrow">
        <label for="cfgIdbLayout">Column layout</label>
        <select id="cfgIdbLayout">
          <option value="pairs">pairs (recommended): Ref used &middot; IDB Bid &middot; My Bid &middot; Gap bid &middot; IDB Ofr &middot; My Ofr &middot; Gap ofr &middot; Gap &middot; Flags &middot; @ref/time</option>
          <option value="sides">sides: time &middot; @ref &middot; IDB Bid &middot; time &middot; @ref &middot; IDB Ofr &middot; Ref used &middot; My Bid &middot; My Ofr &middot; gaps &middot; Flags</option>
        </select>
      </div>
      <div class="fieldrow">
        <label>Colours (background; text colour picks itself for contrast)</label>
        <span>IDB Bid <input type="color" id="cfgIdbBid"> My Bid <input type="color" id="cfgMyBid">
        &nbsp; IDB Ofr <input type="color" id="cfgIdbOfr"> My Ofr <input type="color" id="cfgMyOfr">
        <button type="button" onclick="cfgIdbReset()" style="margin-left:8px">Reset colours</button></span>
      </div>
      <p class="note">Blue = bid side, orange = offer side (colour-blind safe); the darker cell of each pair is yours. Layout and colours are saved in this browser.</p>
      <p class="note">Rounding and your name are saved in this browser; the
        refresh interval applies to the shared server poller for everyone.</p>
      <button class="primary" onclick="saveCfg()">Save &amp; apply</button>
    </div>
    <div class="cfg" style="margin-top:16px">
      <h2>Column format</h2>
      <div class="fieldrow">
        <label for="lcol">Column</label>
        <select id="lcol" style="width:200px;background:var(--bg);color:var(--text);
          border:1px solid var(--border2);border-radius:0;padding:3px 7px;
          font:13px var(--mono)" onchange="loadColForm()"></select>
      </div>
      <div class="fieldrow">
        <label for="lw">Width (px, blank = auto)</label>
        <input type="number" id="lw" min="40" step="1" oninput="colFormChanged()">
      </div>
      <div class="fieldrow">
        <label for="lfg">Foreground (font colour)</label>
        <input type="color" id="lfg" oninput="colFormChanged('fg')">
        <button onclick="clearColField('fg')" title="Clear">&times;</button>
      </div>
      <div class="fieldrow">
        <label for="lbg">Background</label>
        <input type="color" id="lbg" oninput="colFormChanged('bg')">
        <button onclick="clearColField('bg')" title="Clear">&times;</button>
      </div>
      <div class="fieldrow">
        <label for="lb">Bold</label>
        <input type="checkbox" id="lb" onchange="colFormChanged()">
      </div>
      <p class="note">Changes preview immediately. Drag the right edge of any
        column header to resize on screen. Save layout makes it permanent
        in this browser.</p>
      <button class="b-t" onclick="saveLayout()">Save layout</button>
      <button onclick="clearColumn()">Clear this column</button>
      <button class="b-r" onclick="resetLayout()">Reset all</button>
    </div>
  </main>
</div>
<script>
const DEFAULT_IDS = __DEFAULT_IDS__;
/* unified editable grid: c0 short_name | c1 und_fx | c2-4 overrides.
   zones keep selections within a column group so ranges never span
   the read-only columns physically in between. */
const COLS = ["short_name","und_fx","n_gamma",
              "ovdSpot","ovdCbFx","ovdUndFx",
              "dc_notl","dc_delta","dc_usd"];
const NCOLS = COLS.length;
const FIELDS = ["ovdSpot","ovdCbFx","ovdUndFx"];
const FSUF = {ovdSpot:"Spot", ovdCbFx:"CbFx", ovdUndFx:"UndFx"};
const zoneOf = c => c===0 ? 0 : (c===1 ? 1 : (c===2 ? 2 :
                       (c<=5 ? 3 : 4)));
const zoneBounds = z => z===0 ? [0,0] : (z===1 ? [1,1] :
                         (z===2 ? [2,2] : (z===3 ? [3,5] : [6,8])));
const isNum = c => c >= 2;
let lastResponse = null;
const refCache = {};

const NL = String.fromCharCode(10);
const CFG = Object.assign(
  {roundStep:0.05, user:"user-"+Math.random().toString(36).slice(2,6)},
  JSON.parse(localStorage.getItem("nukestation.cfg")||"{}"));

/* every visible column after the checkbox, in exact display order */
/* IDB QUOTES band: layout + colours are CONFIG settings (saved in this browser) */
const IDB_LAYOUTS = {
  pairs: ["idb_ref","idb_bid","idb_my_bid","idb_gap_b","idb_ask","idb_my_ask","idb_gap_a","idb_gap","idb_flag","idb_bref","idb_btime","idb_aref","idb_atime"],
  sides: ["idb_btime","idb_bref","idb_bid","idb_atime","idb_aref","idb_ask","idb_ref","idb_my_bid","idb_my_ask","idb_gap_b","idb_gap_a","idb_gap","idb_flag"]
};
const IDB_LABELS = {idb_btime:"time",idb_bref:"@ref",idb_bid:"IDB Bid",idb_atime:"time",idb_aref:"@ref",idb_ask:"IDB Ofr",idb_ref:"Ref used",
                    idb_my_bid:"My Bid",idb_my_ask:"My Ofr",idb_gap_b:"Gap bid",idb_gap_a:"Gap ofr",idb_gap:"Gap",idb_flag:"Flags"};
const IDB_COLOR_DEF = {ibBid:"#dbeafe", myBid:"#93c5fd", ibOfr:"#ffedd5", myOfr:"#fdba74"};   // rgb(219,234,254) rgb(147,197,253) rgb(255,237,213) rgb(253,186,116)
function idbOrder(){ return IDB_LAYOUTS[CFG.idbLayout] || IDB_LAYOUTS.pairs; }
function idbColors(){ return Object.assign({}, IDB_COLOR_DEF, CFG.idbColors || {}); }
function idbTextFor(bg){   // dark text on light fills, white on dark - WCAG-ish luminance
  const m = String(bg||"").match(/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i); if(!m) return "#111";
  const [r,g,b] = [1,2,3].map(i=>parseInt(m[i],16)/255).map(v=>v<=0.03928?v/12.92:Math.pow((v+0.055)/1.055,2.4));
  return (0.2126*r+0.7152*g+0.0722*b) > 0.45 ? "#111827" : "#ffffff";
}
function idbApplyColors(){
  const c = idbColors(), st = document.documentElement.style;
  for(const [k,v] of Object.entries({ibBid:c.ibBid, myBid:c.myBid, ibOfr:c.ibOfr, myOfr:c.myOfr})){
    st.setProperty("--idb-"+k+"-bg", v); st.setProperty("--idb-"+k+"-fg", idbTextFor(v));
  }
  // @ref / time cells: a lighter mix of the side colour, with a deep, readable text of the same hue
  const mix=(hex,wt)=>{ const m=String(hex).match(/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i); if(!m) return "#f3f4f6"; return "#"+[1,2,3].map(i=>Math.round(parseInt(m[i],16)*(1-wt)+255*wt).toString(16).padStart(2,"0")).join(""); };
  const deep=(hex,wt)=>{ const m=String(hex).match(/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i); if(!m) return "#111827"; return "#"+[1,2,3].map(i=>Math.round(parseInt(m[i],16)*(1-wt)).toString(16).padStart(2,"0")).join(""); };
  st.setProperty("--idb-ibBidq-bg", mix(c.ibBid,0.45)); st.setProperty("--idb-ibBidq-fg", deep(c.myBid,0.62));   // tint = IDB price colour + 45% white: always lighter than the price cell
  st.setProperty("--idb-ibOfrq-bg", mix(c.ibOfr,0.45)); st.setProperty("--idb-ibOfrq-fg", deep(c.myOfr,0.62));
}
idbApplyColors();
const COL_DEFS = [
  ["secId","secId","stick1"],["company","company","ref stick2"],
  ["short_name","short_name","ref stick3"],
  ["bond_type","BOND_TYPE",""],["ric","ric","ref"],
  ["isin","isin","ref"],["sec_fx","sec_fx","ref"],
  ["und_fx","und_fx","ref"],["quantity_live","qty_live","ref"],
  ["usd_qty_live","usd_qty_live","ref"],
  ["n_bid","nBid","grp"],["n_gamma","nGamma",""],
  ["n_spread","nSpread",""],
  ["n_spot","nSpot",""],["n_spotfx","nSpotFx",""],
  ["n_delta","nDelta%",""],["m_delta","MDelta%",""],
  ["parityPct","PARITY%",""],["cs_used","CSprd",""],
  ["twq_lim","Limit next","grp"],["twq_bind","Binding",""],["twq_iss","Issued",""],["twq_sub","SBL cap 10%",""],
  ["twq_bal","SBL bal",""],["twq_room","Cap room",""],["twq_used","Used%",""],["twq_trend","Trend",""],
  ["twq_thr","Throttle",""],["twq_cap","1-sess $",""],["twq_roomusd","Room $",""],["twq_sess","Sessions",""],["twq_dt","Session",""],
  ["x_bid","XBid","grp"],
  ["or_bid_sprd","OrBidSprd",""],["ovd_bid","bid",""],
  ["ovd_ask","ask",""],["or_ask_sprd","OrAskSprd",""],
  ["x_ask","XAsk",""],["x_both","X",""],
  ["ov_iv","ImpVol",""],["quote_bid","QuoteBid",""],["quote_ask","QuoteAsk",""],
  ["ovdSpot","ovdSpot",""],["ovdCbFx","ovdCbFx",""],["ovdUndFx","ovdUndFx",""],
  ["be_move","BE move%",""],["stk_move","Stk%",""],["fx_move","FXbps",""],
  ["d_vs","\u0394 vs live bid",""],["mid_drift","MID DRIFT",""],
  ["f_call","CALL","grp"],["f_trig","TRIG%",""],["f_exp","EXP",""],["f_put","PUT",""],
  ["f_div","DIV",""],["f_move","MOVE",""],["f_hedged","HEDGED",""],["f_nuke","NUKE",""],
  ["f_vol","VOL",""],
  ["v_iv","ImpVol","grp"],["v_10","V10",""],["v_30","V30",""],["v_90","V90",""],
  ["v_n",'VN <input id="volN" size="2" oninput="volNChanged(this)">',""],
  ["v_vega","Vega",""],
  ["lr_live","Live \u0394","grp"],["lr_t1","\u0394 T1",""],["lr_ub","Unbooked \u0394",""],
  ["cl_tag","Tag","grp"],["cl_strat","Strategy v3.1",""],["cl_held","Held",""],["cl_r","R",""],["cl_peer","Vs peers",""],
  ["cl_trust","Trust",""],["cl_worst","Worst as held",""],["cl_carry","Carry 1m",""],["cl_cat","Category",""],
  ["dc_notl","Notional","grp"],["dc_delta","Delta%",""],
  ["dc_shares","Eq Shares",""],["dc_usd","USD Delta",""],
  ["cn_start","Start","grp"],["cn_end","End",""],["cn_twap","TWAP",""],["cn_vwap","VWAP",""],["cn_vol","Volume",""],["cn_tk","BBG ticker",""],
  ["hn_delta","Trade \u0394%",""],["hn_astk","A.Stock",""],["hn_afx","A.FX",""],["hn_abond","A.Bond",""],["hn_cstk","C.Stock",""],["hn_cfx","C.FX",""],["hn_dn","$-Neutral",""],
  ...idbOrder().map((k,i)=>[k, IDB_LABELS[k], i===0?"grp":""]),
  ["live_bid","bid","grp"],["live_ask","ask",""],["live_spot","spot",""],
  ["live_cbfx","cbFx",""],["live_undfx","undFx",""],
  ["eod_bid","bid","grp"],["eod_ask","ask",""],["eod_spot","spot",""],
  ["eod_cbfx","cbFx",""],["eod_undfx","undFx",""],
  ["t_m","m%","grp"],["t_rolld","roll\u0394",""],["t_dpnl","\u0394pnl",""],
  ["t_gpnl","\u03b3pnl",""],["t_theo","theo",""],["t_vslive","vs live",""],
  ["stk_last","last","grp rf"],["stk_time","time HKT","rf"],["stk_date","date","rf"],
  ["stk_close","close","rf"],["stk_closedt","close dt","rf"],
  ["cf_last","cb fx last","grp cf"],["cf_time","cb fx time HKT","cf"],
  ["cf_date","cb fx date","cf"],["cf_close","cb fx close","cf"],
  ["cf_closedt","cb fx close dt","cf"],
  ["fx_last","und fx last","grp fx"],["fx_time","und fx time HKT","fx"],
  ["fx_date","und fx date","fx"],["fx_close","und fx close","fx"],
  ["fx_closedt","und fx close dt","fx"],
];
const COL_KEYS = COL_DEFS.map(d=>d[0]);
const DEF_W = {secId:97, company:180, short_name:110, und_fx:92,
               ovdSpot:96, ovdCbFx:96, ovdUndFx:96, be_move:72, ov_iv:64,
               cf_last:78, cf_time:70, cf_date:80, cf_close:78, cf_closedt:80,
               twq_lim:86, twq_bind:78, twq_iss:70, twq_sub:70, twq_bal:78, twq_room:88, twq_used:58, twq_trend:84, twq_thr:84, twq_cap:70, twq_roomusd:70, twq_sess:62, twq_dt:54,
               lr_live:92, lr_t1:92, lr_ub:92, cl_tag:66, cl_strat:170, cl_held:70, cl_r:62, cl_peer:78, cl_trust:170, cl_worst:118, cl_carry:64, cl_cat:72,
               dc_notl:96, dc_delta:64, dc_shares:92, dc_usd:96,
               cn_start:54, cn_end:54, cn_twap:72, cn_vwap:72, cn_vol:84, cn_tk:120,
               hn_delta:56, hn_astk:68, hn_afx:64, hn_abond:68, hn_cstk:68, hn_cfx:64, hn_dn:76,
               f_trig:50, f_hedged:52,
               idb_bid:62, idb_bref:60, idb_btime:64, idb_ask:62, idb_aref:60, idb_atime:64,
               idb_ref:62, idb_my_bid:62, idb_my_ask:62, idb_gap_b:58, idb_gap_a:58, idb_gap:58, idb_flag:170};
const SIDE_W_DEF = 172;   // left secid panel; LAYOUT._side overrides
const SIDE_RAIL = 18;     // collapsed rail width
let SIDE_HID = localStorage.getItem("nukestation.sidehide")==="1";
const LAYOUT = JSON.parse(localStorage.getItem("nukestation.layout")||"{}");
const colIdx = key => COL_KEYS.indexOf(key) + 2;   // nth-child (1=checkbox)

const RES_COLS = ["nBid","nDeltaPct","nSpread","nSpot","nSpotFx",
  "liveMktBid","liveMktAsk","liveSpot","liveCbFx","liveUndFx",
  "eodMktBid","eodMktAsk","eodSpot","eodCbFx","eodUndFx",
  "ovdMktBid","ovdMktAsk","dVsLive"];
const RF_COLS = ["last","last_time","last_date","close","close_date"];
const FX_COLS = ["last","last_time","last_date","close","close_date"];
const CF_COLS = ["last","last_time","last_date","close","close_date"];

const _grp = (v, dpMax, dpMin=0) => Number(v).toLocaleString("en-US",
  {minimumFractionDigits:dpMin, maximumFractionDigits:dpMax});
const fmt = (v, dp=4) => (v===null||v===undefined||isNaN(v)) ? "" :
  _grp(v, dp);
const fmtBA = v => {
  if(v===null||v===undefined||isNaN(v)) return "";
  const s = CFG.roundStep > 0 ? CFG.roundStep : 0.05;
  return _grp(Math.round(Number(v)/s)*s, 2, 2);
};
const fmt2 = v => (v===null||v===undefined||v===""||isNaN(v)) ? "" :
  _grp(v, 2, 2);
const fmt4 = v => (v===null||v===undefined||v===""||isNaN(v)) ? "" :
  _grp(v, 4, 4);
const fmt0 = v => (v===null||v===undefined||v===""||isNaN(v)) ? "" :
  Math.round(Number(v)).toLocaleString("en-US");
const sanitizeNum = s => {
  const v = String(s).replace(/[, ]/g,"").trim();
  return v === "" || isNaN(Number(v)) ? "" : v;
};
const cellVal = (c, raw) => isNum(c) ? sanitizeNum(raw) : String(raw).trim();

/* ---------------- tabs & config ---------------- */
function showTab(t){
  for(const [k,v] of [["main","viewMain"],["cfg","viewCfg"],
                      ["runs","viewRuns"],["snap","viewSnap"],
                      ["log","viewLog"]]){
    const el = document.getElementById(v);
    if(el) el.style.display = t===k ? (k==="snap"?"block":"grid") : "none";
  }
  for(const [k,b] of [["main","tabMain"],["cfg","tabCfg"],
                      ["runs","tabRuns"],["snap","tabSnap"],
                      ["log","tabLog"]]){
    document.getElementById(b).classList.toggle("active", t===k);
  }
  if(t==="snap" && typeof loadSnap8==="function") loadSnap8();
  if(t==="log" && typeof loadLogs==="function") loadLogs();
}
async function volNow(){
  setStatus("Fetching vol history (isolated child, up to ~1 min)...");
  try{
    const base = location.pathname.replace(new RegExp("[/]+$"),"");
    const j = await (await fetch(base+"/api/vol/run",{method:"POST"})).json();
    setStatus(j.ok
      ? `Vol history loaded for ${j.rics} ric(s) - cells fill on the next refresh.`
      : `<span class="warn">Vol fetch: ${j.error}</span>`);
  }catch(e){ setStatus(`<span class="warn">Vol fetch failed: ${e}</span>`); }
}
let _logTimer = null;
async function loadLogs(){
  try{
    const base = location.pathname.replace(new RegExp("[/]+$"),"");
    const j = await (await fetch(base+"/api/logs?n=400")).json();
    const esc = t => t.replace(/&/g,"&amp;").replace(/</g,"&lt;");
    const pre = document.getElementById("logpre");
    const stick = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 30;
    pre.innerHTML = (j.lines||[]).map(l=>{
      const e = esc(l);
      if(l.includes("[ERRO")) return `<span style="color:#ff8f8f">${e}</span>`;
      if(l.includes("[WARN")) return `<span style="color:#ffd37a">${e}</span>`;
      return e;
    }).join("\\n");
    if(stick) pre.scrollTop = pre.scrollHeight;
    document.getElementById("logMeta").textContent =
      (j.lines||[]).length + " lines";
  }catch(e){ document.getElementById("logMeta").textContent = String(e); }
  clearTimeout(_logTimer);
  const on = document.getElementById("logAuto");
  const vis = document.getElementById("viewLog").style.display !== "none";
  if(on && on.checked && vis) _logTimer = setTimeout(loadLogs, 5000);
}
function copyLogs(){
  const t = document.getElementById("logpre").textContent;
  copyText(t).then(ok => document.getElementById("logMeta").textContent =
    ok ? "copied" : "copy blocked");
}
const _p2s = v => v == null || !isFinite(v) ? "\u2014" : Number(v).toFixed(2);
async function loadSnap8(d){
  try{
    const base = location.pathname.replace(new RegExp("[/]+$"),"");
    const j = await (await fetch(base+"/api/snap8"+(d?("?d="+d):""))).json();
    if(!j.ok){ document.getElementById("sn8meta").textContent=j.error; return; }
    const sel = document.getElementById("sn8date");
    sel.innerHTML = (j.dates||[]).map(x=>
      `<option${x===j.date?" selected":""}>${x}</option>`).join("");
    const R="text-align:right";
    const fN=(v,d)=>v==null||!isFinite(v)?"\u2014":Number(v).toFixed(d);
    const fQ=v=>v==null||!isFinite(v)?"\u2014":Number(v).toLocaleString();
    document.querySelector("#sn8tbl tbody").innerHTML =
      (j.rows||[]).map(r=>`<tr><td>${r.short_name}</td><td>${r.isin||""}</td>`+
        `<td style="${R}">${fN(r.ovd_bid,2)}</td>`+
        `<td style="${R}">${fN(r.ovd_ask,2)}</td>`+
        `<td style="${R}">${fN(r.indic_ask,2)}</td>`+
        `<td style="${R}">${fN(r.vs_ref,2)}</td>`+
        `<td style="${R}">${fN(r.fx_ref,4)}</td>`+
        `<td style="${R}">${fN(r.vs_usd,2)}</td>`+
        `<td style="${R}">${fN(r.delta_pct,1)}</td>`+
        `<td style="${R}">${fQ(r.qty)}</td>`+
        `<td>${(r.snapped_at||"").slice(11,19)}</td></tr>`).join("");
    document.getElementById("sn8meta").textContent =
      (j.rows||[]).length + " securities \u00b7 " + (j.date||"no snapshot");
  }catch(e){ document.getElementById("sn8meta").textContent=String(e); }
}
async function snap8Now(){
  const base = location.pathname.replace(new RegExp("[/]+$"),"");
  const j = await (await fetch(base+"/api/snap8/run",{method:"POST"})).json();
  document.getElementById("sn8meta").textContent =
    j.ok ? ("snapped " + j.stored + " securities (close\u2192ovd on " + j.set + " rows" + ((j.skipped||[]).length?", "+j.skipped.length+" without close kept their overrides":"") + ", re-nuked)") : ("NOT snapped: " + j.error);
  loadSnap8();
}

async function recoverNames(){ const base=location.pathname.replace(new RegExp("[/]+$"),""); const el=document.getElementById("recoverMsg"); if(el) el.textContent="recovering...";
  try{ const j=await (await fetch(base+"/api/state/recover",{method:"POST"})).json(); if(el) el.textContent = j.ok ? ("recovered "+j.n+" securities"+(j.n?": "+Object.entries(j.recovered).map(([k,v])=>k+" "+Object.keys(v).join("/")).join(", "):"")+((j.missing||[]).length?" \u00b7 still no name/fx anywhere for "+j.missing.join(", ")+" - type them once":"")) : (j.error||"failed"); }catch(e){ if(el) el.textContent="failed: "+e; } }
function cfgIdbReset(){ delete CFG.idbColors; localStorage.setItem("nukestation.cfg", JSON.stringify(CFG)); idbApplyColors(); loadCfgForm(); }
function loadCfgForm(){
  { const c=idbColors(); const set=(id,v)=>{ const el=document.getElementById(id); if(el) el.value=v; };
    set("cfgIdbBid",c.ibBid); set("cfgMyBid",c.myBid); set("cfgIdbOfr",c.ibOfr); set("cfgMyOfr",c.myOfr);
    const lay=document.getElementById("cfgIdbLayout"); if(lay) lay.value=(IDB_LAYOUTS[CFG.idbLayout]?CFG.idbLayout:"pairs"); }
  document.getElementById("cfgName").value = CFG.user;
  document.getElementById("cfgSec").value = NS.refreshSec;
  document.getElementById("cfgRef").value = NS.refdataSec;
  document.getElementById("cfgAuto").value = NS.autosaveSec;
  document.getElementById("cfgStep").value = CFG.roundStep;
  document.getElementById("cfgThSS").value = FLAG_TH.staleSpotPct;
  document.getElementById("cfgThSF").value = FLAG_TH.staleFxPct;
  document.getElementById("cfgThMS").value = FLAG_TH.stkPct;
  document.getElementById("cfgThMF").value = FLAG_TH.fxBps;
}
function saveCfg(){
  const name = document.getElementById("cfgName").value.trim();
  if(name && name !== CFG.user){ CFG.user = name.slice(0,24); NS.hello(); }
  CFG.roundStep = parseFloat(document.getElementById("cfgStep").value)||0.05;
  { const g=id=>(document.getElementById(id)||{}).value; const cols={ibBid:g("cfgIdbBid"),myBid:g("cfgMyBid"),ibOfr:g("cfgIdbOfr"),myOfr:g("cfgMyOfr")};
    const def=IDB_COLOR_DEF; const changed={}; for(const k in cols) if(cols[k] && cols[k].toLowerCase()!==def[k]) changed[k]=cols[k].toLowerCase();
    if(Object.keys(changed).length) CFG.idbColors=changed; else delete CFG.idbColors;
    const lay=(document.getElementById("cfgIdbLayout")||{}).value; const prev=CFG.idbLayout||"pairs";
    if(lay && IDB_LAYOUTS[lay]) CFG.idbLayout=lay;
    localStorage.setItem("nukestation.cfg", JSON.stringify(CFG)); idbApplyColors();
    if((CFG.idbLayout||"pairs")!==prev){ location.reload(); return; } }   // column order is baked at load: rebuild the page
  localStorage.setItem("nukestation.cfg", JSON.stringify(CFG));
  const sec = Math.max(2, parseInt(document.getElementById("cfgSec").value)||5);
  if(sec !== NS.refreshSec) NS.send({type:"refresh", sec});
  const rsec = Math.max(30, parseInt(document.getElementById("cfgRef").value)||300);
  if(rsec !== NS.refdataSec) NS.send({type:"refInt", sec: rsec});
  const asec = Math.max(0, parseInt(document.getElementById("cfgAuto").value)||0);
  if(asec !== NS.autosaveSec) NS.send({type:"autosave", sec: asec});
  const th = {
    staleSpot: parseFloat(document.getElementById("cfgThSS").value),
    staleFx: parseFloat(document.getElementById("cfgThSF").value),
    moveStk: parseFloat(document.getElementById("cfgThMS").value),
    moveFx: parseFloat(document.getElementById("cfgThMF").value)};
  if(isFinite(th.staleSpot) && (th.staleSpot !== FLAG_TH.staleSpotPct ||
     th.staleFx !== FLAG_TH.staleFxPct || th.moveStk !== FLAG_TH.stkPct ||
     th.moveFx !== FLAG_TH.fxBps))
    NS.send({type:"flagTh", ...th});
  loadCfgForm();
  if(lastResponse) render(lastResponse);
  showTab("main");
  setStatus(`Config saved: rounding ${CFG.roundStep}, ` +
            `server refresh ${sec}s, name ${CFG.user}.`);
}

/* ---------------- selection state ---------------- */
const S = {a:null, f:null, dragging:false, filling:false, fillRect:null};
const rowSel = new Set();
let rowAnchor = null;

const cellAt = (r,c) => document.querySelector(
  `#tbl input[data-row="${r}"][data-col="${c}"]`);
const tdAt = (r,c) => { const i = cellAt(r,c); return i ? i.parentElement : null; };
const trAt = r => document.querySelectorAll("#tbl tr[data-id]")[r];
const cbAt = r => { const tr = trAt(r); return tr ? tr.querySelector(".rowcb") : null; };
const nRows = () => document.querySelectorAll("#tbl tr[data-id]").length;
const rect = () => S.a && S.f ? {
  r1: Math.min(S.a.r,S.f.r), r2: Math.max(S.a.r,S.f.r),
  c1: Math.min(S.a.c,S.f.c), c2: Math.max(S.a.c,S.f.c)} : null;
const area = rc => rc ? (rc.r2-rc.r1+1)*(rc.c2-rc.c1+1) : 0;
const clampZone = c => {
  if(!S.a) return c;
  const [z1,z2] = zoneBounds(zoneOf(S.a.c));
  return Math.max(z1, Math.min(z2, c));
};

function paint(){
  document.querySelectorAll("#tbl td.gc").forEach(td=>
    td.classList.remove("sel","active","fillprev"));
  const rc = rect();
  if(rc){
    for(let r=rc.r1;r<=rc.r2;r++) for(let c=rc.c1;c<=rc.c2;c++){
      const td = tdAt(r,c); if(td) td.classList.add("sel");
    }
    const atd = tdAt(S.a.r,S.a.c); if(atd) atd.classList.add("active");
  }
  if(S.fillRect){
    const fr = S.fillRect;
    for(let r=fr.r1;r<=fr.r2;r++) for(let c=fr.c1;c<=fr.c2;c++){
      const inSel = rc && r>=rc.r1&&r<=rc.r2&&c>=rc.c1&&c<=rc.c2;
      const td = tdAt(r,c); if(td && !inSel) td.classList.add("fillprev");
    }
  }
  positionHandle();
  updateNamebox();
}

function paintRows(){
  document.querySelectorAll("#tbl tr[data-id]").forEach((tr,ri)=>{
    const on = rowSel.has(ri);
    tr.classList.toggle("rowsel", on);
    const cb = tr.querySelector(".rowcb"); if(cb) cb.checked = on;
  });
  const all = document.getElementById("cbAll");
  if(all){
    all.checked = rowSel.size===nRows() && nRows()>0;
    all.indeterminate = rowSel.size>0 && rowSel.size<nRows();
  }
  const go = document.getElementById("go");
  go.textContent = rowSel.size ? `Nuke ${rowSel.size} selected` : "Nuke prices";
}

function positionHandle(){
  const fh = document.getElementById("fh"), rc = rect();
  if(!rc){ fh.style.display="none"; return; }
  const td = tdAt(rc.r2, rc.c2), wrap = document.getElementById("wrap");
  if(!td){ fh.style.display="none"; return; }
  const tr_ = td.getBoundingClientRect(), wr = wrap.getBoundingClientRect();
  fh.style.left = (tr_.right - wr.left + wrap.scrollLeft - 5) + "px";
  fh.style.top  = (tr_.bottom - wr.top + wrap.scrollTop - 5) + "px";
  fh.style.display = "block";
}

function updateNamebox(){
  const nb = document.getElementById("namebox"), rc = rect();
  if(!rc){ nb.innerHTML = rowSel.size ? `${rowSel.size} row(s)` : "&mdash;"; return; }
  const tr_ = trAt(S.a.r);
  const sid = tr_ ? tr_.dataset.id : "?";
  nb.textContent = area(rc)===1 ? `${sid} \u00b7 ${COLS[S.a.c]}` :
    `${rc.r2-rc.r1+1}R \u00d7 ${rc.c2-rc.c1+1}C`;
}

function setActive(r, c, extend=false){
  r = Math.max(0, Math.min(nRows()-1, r));
  c = Math.max(0, Math.min(NCOLS-1, c));
  if(extend && S.a){ S.f = {r, c: clampZone(c)}; }
  else { S.a = {r,c}; S.f = {r,c};
    const el = cellAt(r,c);
    if(el){ el.classList.remove("editing"); el.focus({preventScroll:true});
      el.select();
      el.scrollIntoView({block:"nearest", inline:"nearest"}); } }
  paint();
}

function flash(el){
  el.classList.add("justset");
  setTimeout(()=>el.classList.remove("justset"), 400);
}

/* ---------------- row selection ---------------- */
function selectRowClick(ri, e){
  if(e.shiftKey && rowAnchor!==null){
    if(!e.ctrlKey && !e.metaKey) rowSel.clear();
    const [a,b] = [Math.min(rowAnchor,ri), Math.max(rowAnchor,ri)];
    for(let i=a;i<=b;i++) rowSel.add(i);
  } else if(e.ctrlKey || e.metaKey){
    rowSel.has(ri) ? rowSel.delete(ri) : rowSel.add(ri);
    rowAnchor = ri;
  } else {
    const only = rowSel.size===1 && rowSel.has(ri);
    rowSel.clear();
    if(!only) rowSel.add(ri);
    rowAnchor = ri;
  }
  paintRows(); updateNamebox();
}

function toggleRow(ri){
  rowSel.has(ri) ? rowSel.delete(ri) : rowSel.add(ri);
  rowAnchor = ri;
  paintRows(); updateNamebox();
}

function targetRows(){
  return rowSel.size ? [...rowSel].sort((a,b)=>a-b)
                     : [...Array(nRows()).keys()];
}

/* --------- copy live/eod values into overrides --------- */
function copyResultToInput(ri, field, kind){
  const tr = trAt(ri); if(!tr) return false;
  const src = tr.querySelector(`td[data-c="${kind}${FSUF[field]}"]`);
  const v = sanitizeNum(src ? src.textContent : "");
  if(v===""){ return false; }
  const inp = tr.querySelector(`input[data-f="${field}"]`);
  if(inp){ inp.value = v; flash(inp); return true; }
  return false;
}

/* --------- copy eikon (stock/fx, last/close) into overrides --------- */
function eikonVal(ri, e){
  const tr = trAt(ri); if(!tr) return "";
  const [src, metric] = e.split("_");
  const td = tr.querySelector(src==="stk"
    ? `td[data-rf="${metric}"]` : (src==="cf" ? `td[data-cf="${metric}"]` : `td[data-fx="${metric}"]`));
  return sanitizeNum(td ? td.textContent : "");
}

function vanRow(tr){
  const b=tr&&tr.querySelector('input[data-u="bond_type"]');
  return b&&String(b.value||'').toLowerCase().startsWith('vanil');
}
function vanCbFromUnd(ri){
  const tr=trAt(ri); if(!tr||!vanRow(tr)) return false;
  const u=tr.querySelector('input[data-f="ovdUndFx"]');
  const c=tr.querySelector('input[data-f="ovdCbFx"]');
  if(u&&c&&String(u.value).trim()!==''){
    c.value=u.value; flash(c); return true; }
  return false;
}
function copyEikonToInput(ri, field, e){
  const v = eikonVal(ri, e);
  if(v === "") return false;
  const tr = trAt(ri); if(!tr) return false;
  const inp = tr.querySelector(`input[data-f="${field}"]`);
  if(inp){ inp.value = v; flash(inp); return true; }
  return false;
}

function fillFromEikon(e, r, c, field){
  const metric = e.split("_")[1];
  const srcFor = f => f==="ovdSpot" ? "stk_"+metric
                    : f==="ovdUndFx" ? "fx_"+metric
                    : f==="ovdCbFx" ? "cf_"+metric : null;
  const rc = rect();
  const inSel = rc && rc.c1>=3 && r>=rc.r1 && r<=rc.r2 && c>=rc.c1 && c<=rc.c2;
  if(inSel && area(rc) > 1){
    let ok=0, miss=0, na=0;
    for(let rr=rc.r1; rr<=rc.r2; rr++)
      for(let cc=rc.c1; cc<=rc.c2; cc++){
        const f = FIELDS[cc-3], src = srcFor(f);
        if(!src){ na++; continue; }               // ovdCbFx has no eikon source
        copyEikonToInput(rr, f, src) ? ok++ : miss++;
      }
    const extra = [];
    if(miss) extra.push(`<span class="warn">${miss} without eikon values</span>`);
    if(na) extra.push(`${na} ovdCbFx cell(s) skipped`);
    setStatus(`Filled ${ok} cell(s) from eikon ${metric}` +
              (extra.length ? "; " + extra.join("; ") : "."));
  } else {
    const done = copyEikonToInput(r, field, e);
    if(!done) setStatus(`<span class="warn">No eikon ${metric} value yet.</span>`);
  }
  { const rc2 = rect(); const ris = [r];
    if(rc2 && rc2.c1 >= 3) for(let rr=rc2.r1;rr<=rc2.r2;rr++) ris.push(rr);
    syncRows(ris); }
}

function bulkCopy(kind){
  const rows = targetRows();
  let ok=0, skip=0;
  for(const ri of rows){
    let any=false;
    for(const f of FIELDS) any = copyResultToInput(ri, f, kind) || any;
    vanCbFromUnd(ri);
    any ? ok++ : skip++;
  }
  syncRows(rows);
  rows.forEach(ri=>{ const tr=trAt(ri); if(tr) autoNukeQueue(Number(tr.dataset.id)); });
  const scope = rowSel.size ? `${rows.length} selected row(s)` : "all rows";
  setStatus(skip
    ? `Copied ${kind} &rarr; overrides for ${ok} of ${scope}; ` +
      `<span class="warn">${skip} without ${kind} values &mdash; nuke first</span>`
    : `Copied ${kind} &rarr; overrides for ${scope}.`);
}

function bulkEikon(metric){
  if(!rowSel.size){
    setStatus('<span class="warn">No rows selected &mdash; tick rows first, ' +
              'nothing updated.</span>');
    return;
  }
  const rows = [...rowSel].sort((a,b)=>a-b);
  let ok=0, miss=0;
  for(const ri of rows){
    let any=false;
    any = copyEikonToInput(ri, "ovdSpot",  "stk_"+metric) || any;
    any = copyEikonToInput(ri, "ovdUndFx", "fx_"+metric)  || any;
    any = (vanCbFromUnd(ri)
           || copyEikonToInput(ri, "ovdCbFx", "cf_"+metric)      // non-vanilla: bond-ccy fx (USD = 1)
           || copyResultToInput(ri, "ovdCbFx", "live")) || any;
    any ? ok++ : miss++;
  }
  syncRows(rows);
  rows.forEach(ri=>{ const tr=trAt(ri); if(tr) autoNukeQueue(Number(tr.dataset.id)); });
  setStatus(miss
    ? `Eikon ${metric} &rarr; overrides for ${ok} of ${rows.length} selected row(s); ` +
      `<span class="warn">${miss} without values</span>`
    : `Eikon ${metric} &rarr; overrides (spot, undFx, cbFx from the cb fx band / mirror) ` +
      `for ${ok} selected row(s).`);
}

/* ---------------- grid wiring ---------------- */
function setRowFocus(tr){
  if(!tr || !tr.dataset || tr.dataset.id===undefined) return;
  if(tr.classList.contains("rowfocus")) return;
  document.querySelectorAll("#tbl tr.rowfocus").forEach(x=>x.classList.remove("rowfocus"));
  tr.classList.add("rowfocus");
}
(function(){ const tbl=document.getElementById("tbl"); if(!tbl) return;
  tbl.addEventListener("mousedown", e=>{ const tr=e.target.closest&&e.target.closest("tr[data-id]"); if(tr) setRowFocus(tr); }, true);
  tbl.addEventListener("focusin",  e=>{ const tr=e.target.closest&&e.target.closest("tr[data-id]"); if(tr) setRowFocus(tr); }, true);
})();
function wireGrid(){
  document.querySelectorAll("#tbl input.gridcell").forEach(inp=>{
    const r = +inp.dataset.row, c = +inp.dataset.col;

    inp.addEventListener("mousedown", (e)=>{
      if(e.button!==0) return;
      e.preventDefault();
      if(e.shiftKey && S.a){ S.f = {r, c: clampZone(c)}; paint(); }
      else setActive(r,c);
      S.dragging = true;
    });
    inp.addEventListener("dblclick", ()=>{
      inp.classList.add("editing");
      inp.focus();
      const len = inp.value.length;
      inp.setSelectionRange(len,len);
    });
    inp.addEventListener("mouseover", ()=>{
      if(S.dragging){ S.f = {r, c: clampZone(c)}; paint(); }
      else if(S.filling){ updateFillRect(r,c); paint(); }
    });
    inp.addEventListener("focus", ()=>{
      inp.dataset.orig = inp.value;
      if(!S.a || S.a.r!==r || S.a.c!==c){ S.a={r,c}; S.f={r,c}; paint(); }
    });

    inp.addEventListener("keydown", (e)=>{
      const rc = rect(), multi = area(rc)>1;
      const editing = inp.classList.contains("editing");
      const len = inp.value.length;
      const atStart = inp.selectionStart===0 && inp.selectionEnd===0;
      const atEnd = inp.selectionStart===len && inp.selectionEnd===len;
      const allSel = inp.selectionStart===0 && inp.selectionEnd===len;

      if(e.key==="Enter"){ e.preventDefault(); setActive(e.shiftKey?r-1:r+1, c); }
      else if(e.key==="Tab"){ e.preventDefault();
        setActive(r, e.shiftKey?c-1:c+1); }
      else if(e.key==="ArrowLeft" && c===0 && !e.shiftKey &&
              (allSel||len===0||atStart)){
        e.preventDefault(); const cb = cbAt(r); if(cb) cb.focus(); }
      else if(e.key.startsWith("Arrow")){
        const d = {ArrowUp:[-1,0],ArrowDown:[1,0],ArrowLeft:[0,-1],ArrowRight:[0,1]}[e.key];
        if(e.shiftKey){ e.preventDefault();
          setActive((S.f?S.f.r:r)+d[0], (S.f?S.f.c:c)+d[1], true); }
        else if(!editing || d[0]!==0 || allSel || len===0 ||
                (d[1]===-1&&atStart) || (d[1]===1&&atEnd)){
          e.preventDefault(); setActive(r+d[0], c+d[1]); }
      }
      else if(e.key==="Escape"){ inp.value = inp.dataset.orig ?? "";
        inp.classList.remove("editing"); S.f={r,c}; paint(); inp.select(); }
      else if(e.key==="Delete" || (e.key==="Backspace" && multi)){
        if(multi || allSel){ e.preventDefault(); forSel((el)=>el.value="");
          const rc2 = rect();
          if(rc2){ const ris=[]; for(let rr=rc2.r1;rr<=rc2.r2;rr++) ris.push(rr);
            syncRows(ris); } } }
      else if((e.ctrlKey||e.metaKey) && e.key.toLowerCase()==="a"){
        e.preventDefault();
        const [z1,z2] = zoneBounds(zoneOf(c));
        S.a={r:0,c:z1}; S.f={r:nRows()-1,c:z2}; paint(); }
      else if((e.ctrlKey||e.metaKey) && e.key.toLowerCase()==="d"){
        e.preventDefault();
        if(multi){
          for(let cc=rc.c1;cc<=rc.c2;cc++){
            const src = cellAt(rc.r1,cc);
            for(let rr=rc.r1+1;rr<=rc.r2;rr++){
              const t = cellAt(rr,cc); if(t&&src) t.value = src.value; } } }
        else { const above = cellAt(r-1,c); if(above) inp.value = above.value; }
        { const rc2 = rect(); const ris=[];
          if(rc2) for(let rr=rc2.r1;rr<=rc2.r2;rr++) ris.push(rr); else ris.push(r);
          syncRows(ris); } }
      else if((e.ctrlKey||e.metaKey) && ["c","x"].includes(e.key.toLowerCase()) && multi){
        e.preventDefault(); copySelection(e.key.toLowerCase()==="x"); }
      else if(!editing && !e.ctrlKey && !e.metaKey && e.key.length===1){
        if(multi){ S.f={r,c}; paint(); }
        inp.classList.add("editing");   // typing starts edit: first char
      }                                 // replaces (all selected), rest append
    });

    inp.addEventListener("blur", ()=>inp.classList.remove("editing"));
    inp.addEventListener("change", ()=>syncRows([r], inp.dataset.u || inp.dataset.f));   // the edited cell may be cleared on purpose

    inp.addEventListener("paste", (e)=>{
      const text = (e.clipboardData||window.clipboardData).getData("text");
      e.preventDefault();
      const rc = rect();
      const block = text.replace(/\\r/g,"").split("\\n")
        .filter(l=>l!=="").map(l=>l.split("\\t"));
      if(!block.length) return;
      if(rc && area(rc)>1){
        for(let rr=rc.r1;rr<=rc.r2;rr++) for(let cc=rc.c1;cc<=rc.c2;cc++){
          const raw = block[(rr-rc.r1)%block.length][(cc-rc.c1)%block[0].length];
          const t = cellAt(rr,cc);
          if(t && raw!==undefined) t.value = cellVal(cc, raw); }
        { const ris=[]; for(let rr=rc.r1;rr<=rc.r2;rr++) ris.push(rr); syncRows(ris); }
        setStatus(`Pasted into ${area(rc)} cells.`);
      } else {
        const [z1,z2] = zoneBounds(zoneOf(c));
        let lastR=r, lastC=c;
        block.forEach((line,dr)=>line.forEach((raw,dc)=>{
          const cc = c+dc;
          if(cc>z2) return;                    // stay within the zone
          const t = cellAt(r+dr, cc);
          if(t){ t.value = cellVal(cc, raw); lastR=r+dr; lastC=cc; } }));
        S.a={r,c}; S.f={r:lastR,c:lastC}; paint();
        { const ris=[]; for(let rr=r;rr<=lastR;rr++) ris.push(rr); syncRows(ris); }
        setStatus(`Pasted ${block.length} row(s).`);
      }
    });
  });

  document.querySelectorAll("#tbl td.cpy[data-rf], #tbl td.cpy[data-fx], #tbl td.cpy[data-cf]").forEach(td=>{
    td.addEventListener("click", ()=>{
      const isStk = td.dataset.rf !== undefined, isCf = td.dataset.cf !== undefined;
      const metric = isStk ? td.dataset.rf : (isCf ? td.dataset.cf : td.dataset.fx);
      const e = (isStk ? "stk_" : (isCf ? "cf_" : "fx_")) + metric;
      const field = isStk ? "ovdSpot" : (isCf ? "ovdCbFx" : "ovdUndFx");
      const tr = td.closest("tr");
      const ri = [...document.querySelectorAll("#tbl tr[data-id]")].indexOf(tr);
      fillFromEikon(e, ri, FIELDS.indexOf(field)+2, field);
    });
  });

  document.querySelectorAll("#tbl td.rowclick, #tbl td.inp, #tbl td.rsel").forEach(td=>{
    td.addEventListener("click", (e)=>{
      const tr = td.closest("tr");
      const ri = [...document.querySelectorAll("#tbl tr[data-id]")].indexOf(tr);
      selectRowClick(ri, e);
    });
  });

  document.querySelectorAll("#tbl .rowcb").forEach(cb=>{
    const ri = +cb.dataset.ri;
    cb.addEventListener("change", ()=>toggleRow(ri));
    cb.addEventListener("keydown", (e)=>{
      if(e.key==="Enter"){ e.preventDefault(); toggleRow(ri); }
      else if(e.key==="ArrowDown"||e.key==="ArrowUp"){
        e.preventDefault();
        const nr = ri + (e.key==="ArrowDown"?1:-1);
        if(e.shiftKey && nr>=0 && nr<nRows()){ rowSel.add(ri); rowSel.add(nr); paintRows(); }
        const ncb = cbAt(nr); if(ncb) ncb.focus();
      }
      else if(e.key==="ArrowRight"){ e.preventDefault(); setActive(ri, 0); }
    });
  });

  const all = document.getElementById("cbAll");
  if(all) all.onchange = ()=>{
    rowSel.clear();
    if(all.checked) for(let i=0;i<nRows();i++) rowSel.add(i);
    paintRows(); updateNamebox();
  };

  const rfband = document.getElementById("rfband");
  if(rfband) rfband.onclick = ()=>NS.send({type:"rfxNow"});
  const fxband = document.getElementById("fxband");
  if(fxband) fxband.onclick = ()=>NS.send({type:"rfxNow"});

  bandApplyAll();
  paintAutoLast();
  const vn=document.getElementById("volN");
  if(vn) vn.value = localStorage.getItem("nukestation.volN") || "60";
}

function forSel(fn){
  const rc = rect(); if(!rc) return;
  for(let r=rc.r1;r<=rc.r2;r++) for(let c=rc.c1;c<=rc.c2;c++){
    const el = cellAt(r,c); if(el) fn(el, r, c); }
}

function copySelection(cut=false){
  const rc = rect(); if(!rc) return;
  const lines = [];
  for(let r=rc.r1;r<=rc.r2;r++){
    const vals = [];
    for(let c=rc.c1;c<=rc.c2;c++) vals.push(cellAt(r,c)?.value ?? "");
    lines.push(vals.join("\\t"));
  }
  navigator.clipboard.writeText(lines.join("\\n")).then(()=>{
    if(cut) forSel(el=>el.value="");
    setStatus(`${cut?"Cut":"Copied"} ${area(rc)} cell(s).`);
  }).catch(err=>setStatus(`<span class="err">Copy failed: ${err}</span>`));
}

/* ---------------- drag-fill handle ---------------- */
function updateFillRect(r, c){
  const rc = rect(); if(!rc) return;
  const [, z2] = zoneBounds(zoneOf(rc.c1));
  const cc = Math.min(c, z2);
  const dv = r - rc.r2, dh = cc - rc.c2;
  if(dv >= dh && r > rc.r2)       S.fillRect = {r1:rc.r1, r2:r, c1:rc.c1, c2:rc.c2};
  else if(cc > rc.c2)             S.fillRect = {r1:rc.r1, r2:rc.r2, c1:rc.c1, c2:cc};
  else                            S.fillRect = null;
}

function applyFill(){
  const rc = rect(), fr = S.fillRect;
  if(!rc || !fr) return;
  const srcRows = rc.r2-rc.r1+1, srcCols = rc.c2-rc.c1+1;
  const src = [];
  for(let r=rc.r1;r<=rc.r2;r++){
    const row = [];
    for(let c=rc.c1;c<=rc.c2;c++) row.push(cellAt(r,c)?.value ?? "");
    src.push(row);
  }
  const vertical = fr.r2 > rc.r2;
  if(vertical){
    for(let c=0;c<srcCols;c++){
      const col = rc.c1 + c;
      const nums = src.map(row=>parseFloat(row[c]));
      const numeric = isNum(col) && nums.every(v=>!isNaN(v)) &&
                      src.every(row=>row[c]!=="");
      let step = null;
      if(numeric && srcRows>=2){
        step = nums[1]-nums[0];
        for(let i=2;i<srcRows;i++) if(Math.abs(nums[i]-nums[i-1]-step)>1e-9){ step=null; break; }
      }
      for(let r=rc.r2+1;r<=fr.r2;r++){
        const k = r-rc.r1;
        const t = cellAt(r, col); if(!t) continue;
        if(step!==null) t.value = fmt(nums[0]+step*k, 6) || String(nums[0]+step*k);
        else t.value = src[k%srcRows][c];
      }
    }
  } else {
    for(let r=0;r<srcRows;r++){
      const nums = src[r].map(parseFloat);
      const numeric = isNum(rc.c1) && nums.every(v=>!isNaN(v)) &&
                      src[r].every(v=>v!=="");
      let step = null;
      if(numeric && srcCols>=2){
        step = nums[1]-nums[0];
        for(let i=2;i<srcCols;i++) if(Math.abs(nums[i]-nums[i-1]-step)>1e-9){ step=null; break; }
      }
      for(let c=rc.c2+1;c<=fr.c2;c++){
        const k = c-rc.c1;
        const t = cellAt(rc.r1+r, c); if(!t) continue;
        if(step!==null) t.value = fmt(nums[0]+step*k, 6) || String(nums[0]+step*k);
        else t.value = src[r][k%srcCols];
      }
    }
  }
  S.f = {r:fr.r2, c:fr.c2};
  { const ris=[]; for(let rr=Math.min(rc.r1,fr.r1);rr<=Math.max(rc.r2,fr.r2);rr++)
      ris.push(rr); syncRows(ris);
    ris.forEach(ri=>{ const tr=trAt(ri); if(tr) autoNukeQueue(Number(tr.dataset.id)); }); }
  setStatus("Filled.");
}

document.getElementById("fh").addEventListener("mousedown", (e)=>{
  e.preventDefault(); e.stopPropagation();
  S.filling = true; S.fillRect = null;
});
document.addEventListener("mouseup", ()=>{
  if(S.filling){ applyFill(); S.filling=false; S.fillRect=null; paint(); }
  S.dragging = false;
});
document.getElementById("wrap").addEventListener("scroll", positionHandle);
window.addEventListener("resize", positionHandle);

/* ---------------- table build / data ---------------- */
function parseIds(){
  return [...new Set(document.getElementById("ids").value
    .split(/[^0-9]+/).filter(x=>x.length).map(Number))];
}
function sendIds(){
  const uniq = parseIds();
  if(NS.up()){ NS.send({type:"ids", ids: uniq});
    setStatus(`Loading ${uniq.length} securities for everyone&hellip;`); }
  else { buildTable(uniq);
    setStatus('<span class="warn">Offline &mdash; loaded locally only.</span>'); }
}
function buildTable(idsOpt){
  const uniq = idsOpt || parseIds();
  const t = document.getElementById("tbl");
  let h = `<tr class="band"><td class="stick0"></td><td class="stick1"></td>` +
    `<td class="stick2"></td><td class="uin stick3" colspan="2">yours</td><td colspan="2"></td>` +
    `<td></td><td class="uin"></td><td colspan="2"></td>` +
    `<td colspan="9" class="gm grp bandhd" id="band-model" onclick="bandToggle('model')">model (last nuke) &#9662;</td>` +
    `<td colspan="13" class="tq grp bandhd" id="band-twq" onclick="bandToggle('twq')" title="read-only mirror of the exchange's daily 信用額度總量管制餘額表 (TWSE TWT93U, TPEx margin/sbl): next-session SBL quota, which limit binds, listed shares and the 10% SBL sub-cap, balance, room, usage trend, USD capacity and sessions to fill. Fetched once per session in the background; nothing here touches pricing.">tw short quota &#9662;</td>` +
    `<td colspan="18" class="go grp bandhd" id="band-res" onclick="bandToggle('res')">override result &#9662;</td>` +
    `<td colspan="9" class="fb grp bandhd" id="band-flags" onclick="bandToggle('flags')">flags &#9662;</td>` +
    `<td colspan="6" class="vb grp bandhd" id="band-vol" onclick="bandToggle('vol')">vol &#9662;</td>` +
    `<td colspan="3" class="lr grp bandhd" id="band-lrisk" onclick="bandToggle('lrisk')" title="read-only mirror of Delta Scan (same numbers, mapped by RIC): Live delta = net_live, delta T1 = net_t1, Unbooked = unbooked_est, in USD; refreshed from the cached scan at most every 15s">live risk &#9662;</td>` +
    `<td colspan="9" class="cl grp bandhd" id="band-cls" onclick="bandToggle('cls')" title="CB Code v3.1 (classification): computed server-side from the live grid - delta, parity, override mid, CSprd, ImpVol, Bloomberg dates, realised vols. Click the workspace link for sections, carry inputs and the what-if.">class &#9662; <a href="${location.pathname.replace(/[/]+$/,'')}/class" target="_blank" onclick="event.stopPropagation()" style="color:#1e3a6e;text-decoration:underline;font-weight:600;margin-left:6px">workspace &#8599;</a></td>` +
    `<td colspan="4" class="dc grp bandhd" id="band-dcalc" onclick="bandToggle('dcalc')">delta calc &#9662;</td>` +
    `<td colspan="13" class="cn grp bandhd" id="band-cbnuke" onclick="bandToggle('cbnuke')" title="VWAP/TWAP: Bloomberg-computed over [start,end) (exchange local, today); RIC -> BBG ticker (329180.KS -> 329180 KP Equity). Hedge: $-Neutral = A.Bond + Trade delta x (parity_c - parity_a), parity = CR x Stock/FX with CR = 100 x fixed FX / CP (refdata) - matches the OVCV hedge-tab dollar-neutral price; C.Stock / C.FX follow the live cells until you type over them">cb nuke &#9662;</td>` +
    `<td colspan="13" class="ib grp bandhd" id="band-idb" onclick="bandToggle('idb')" title="read-only mirror of the Lagrange IDB QUOTES tab (mkt = broker as quoted, my = desk quote re-nuked at the broker ref); auto-refreshes every 5s. Flags: ${IDB_PILL_LEGEND}">idb quotes &#9662;</td>` +
    `<td colspan="5" class="gl grp bandhd" id="band-live" onclick="bandToggle('live')">live &#9662;</td>` +
    `<td colspan="5" class="ge grp bandhd" id="band-eod" onclick="bandToggle('eod')">eod &#9662;</td>` +
    `<td colspan="6" class="grp bandhd" id="band-theo" onclick="bandToggle('theo')">theo &middot; &gamma;-adj &#9662;</td>` +
    `<td colspan="5" class="grp rfx bandhd" id="band-stk" onclick="bandToggle('stk')">stock <span id="rfxts" onclick="event.stopPropagation(); rfxNow&&rfxNow()">&mdash;</span> &#9662;</td>` +
    `<td colspan="5" class="grp cfx bandhd" id="band-cbfx" onclick="bandToggle('cbfx')" title="bond-currency fx as USD/CCY: USD = 1; CNH, JPY, HKD... from CCY=; EUR, GBP, AUD, NZD from CCY= inverted (Refinitiv quotes those as CCY/USD)">cb fx &#9662;</td>` +
    `<td colspan="5" class="grp fxx bandhd" id="band-fx" onclick="bandToggle('fx')">und fx &#9662;</td></tr>`;
  h += `<tr><th class="stick0"><input type="checkbox" id="cbAll" title="Select all"></th>` +
    COL_DEFS.map(([k,label,cls])=>
      `<th class="${cls}${BAND_FIRST.has(k)?" bfirst":""}" data-key="${k}" data-band="${bandOf(k)}">` +
      `<span class="rz" data-key="${k}"></span>${label}</th>`
    ).join("") + "</tr>";
  uniq.forEach((id, ri)=>{
    h += `<tr data-id="${id}">` +
      `<td class="stick0"><input type="checkbox" class="rowcb" data-ri="${ri}"></td>` +
      `<td class="stick1 rowclick">${id}</td>` +
      `<td class="ref co stick2 rowclick" data-r="company_name"></td>` +
      `<td class="gc uinp stick3"><input class="gridcell" data-u="short_name"
         data-row="${ri}" data-col="0" autocomplete="off"
         placeholder="&#8212;"></td>` +
      `<td class="gc uinp"><input class="sprd" data-u="bond_type"
         data-id="${id}" onchange="sprdChanged(this)" autocomplete="off"
         placeholder="&#8212;" style="width:64px"></td>` +
      `<td class="ref rowclick" data-r="ric"></td>` +
      `<td class="gc uinp isincell" data-r="isin"><span class="isin-db" style="display:none"></span>` +
      `<input class="sprd isinv" data-u="isin_manual" data-id="${id}" placeholder="ISIN" maxlength="12" autocomplete="off" spellcheck="false" ` +
      `onchange="isinChanged(this)" title="no ISIN in the database - type it here; saved for everyone and used for the Bloomberg dates until the database provides one"></td>` +
      `<td class="ref rowclick" data-r="sec_fx"></td>` +
      `<td class="gc uinp"><input class="gridcell" data-u="und_fx"
         data-row="${ri}" data-col="1" autocomplete="off"
         placeholder="&#8212;"></td>` +
      `<td class="ref rowclick" data-r="quantity_live"></td>` +
      `<td class="ref rowclick" data-r="usd_qty_live"></td>` +
      RES_COLS.slice(0,1).map((c,i)=>
        `<td data-c="${c}" data-band="model" class="rowclick gM${i===0?" grp bfirst":""}"></td>`).join("") +
      `<td class="gc uinp" data-band="model"><input class="gridcell" data-u="n_gamma"
         data-row="${ri}" data-col="2" inputmode="decimal" autocomplete="off"
         placeholder="&#8212;" title="\u0394-points per 1% und move (manual)"></td>` +
      RES_COLS.slice(2,5).map(c=>
        `<td data-c="${c}" data-band="model" class="rowclick gM"></td>`).join("") +
      `<td data-c="nDeltaPct" data-band="model" class="rowclick gM"></td>` +
      `<td data-r="lp_delta" data-band="model" class="rowclick gM"
         title="model delta (cbanalytics.lp_model_output) x 100"></td>` +
      `<td data-c="parityPct" data-band="model" class="rowclick gM"
         title="(ovdSpot x fxSpot) / (conversion_price x conversion_fixed_fx), lp_model_output"></td>` +
      `<td data-r="credit_spread_used" data-band="model" class="rowclick gM"
         title="credit_spread_used x 10000, bps (cbanalytics.lp_model_output)"></td>` +
      `<td data-c="twq_lim" data-band="twq" class="rowclick tqc grp bfirst" title="次一營業日可限額 - SBL short-sale quota for the next session (shares)"></td>` +
      ["twq_bind","twq_iss","twq_sub","twq_bal","twq_room","twq_used","twq_trend","twq_thr","twq_cap","twq_roomusd","twq_sess","twq_dt"].map(k=>`<td data-c="${k}" data-band="twq" class="rowclick tqc"></td>`).join("") +
      `<td class="gc inp gO grp rsel bfirst" data-band="res"><input class="sprd"
         data-u="x_bid" data-id="${id}" onchange="sprdChanged(this)"
         autocomplete="off" placeholder="&#8212;"></td>` +
      `<td class="gc inp gO rsel" data-band="res"><input class="sprd" data-u="or_bid_sprd"
         data-id="${id}" onchange="sprdChanged(this)"
         autocomplete="off" placeholder="&#8212;"></td>` +
      `<td data-c="ovdMktBid" data-band="res" class="rowclick gO"></td>` +
      `<td data-c="ovdMktAsk" data-band="res" class="rowclick gO"></td>` +
      `<td class="gc inp gO rsel" data-band="res"><input class="sprd" data-u="or_ask_sprd"
         data-id="${id}" onchange="sprdChanged(this)"
         autocomplete="off" placeholder="&#8212;"></td>` +
      `<td class="gc inp gO rsel" data-band="res"><input class="sprd"
         data-u="x_ask" data-id="${id}" onchange="sprdChanged(this)"
         autocomplete="off" placeholder="&#8212;"></td>` +
      `<td class="gc inp gO rsel" data-band="res"><input class="sprd"
         data-u="x_both" data-id="${id}" onchange="sprdChanged(this)"
         autocomplete="off" placeholder="&#8212;"
         title="X: shifts QuoteBid and QuoteAsk together"></td>` +
      `<td data-c="ov_iv" data-band="res" class="gc uinp ivcell" title="implied vol - same as the VOL band; type a vol (in %) when the database has none"><span class="iv-db" style="display:none"></span>` +
      `<input class="sprd ivv" data-u="iv_manual" data-id="${id}" placeholder="vol %" inputmode="decimal" autocomplete="off" onchange="ivChanged(this)"></td>` +
      `<td data-q="quote_bid" data-band="res" class="rowclick qcell"
         title="override bid + XBid"></td>` +
      `<td data-q="quote_ask" data-band="res" class="rowclick qcell"
         title="override ask + XAsk"></td>` +
      FIELDS.map((f,ci)=>
        `<td class="gc inp" data-band="res">` +
        `<input class="gridcell" data-f="${f}" data-row="${ri}" data-col="${ci+3}"
          inputmode="decimal" autocomplete="off" placeholder="&#8212;"></td>`).join("") +
      `<td data-c="be_move" data-band="res" class="rowclick bemv" title="break-even daily stock move for gamma trading = ImpVol / sqrt(252) (1-sigma daily move)"></td>` +
      `<td data-fl="stk_move" data-band="res" class="rowclick"
         title="stock last / close - 1"></td>` +
      `<td data-fl="fx_move" data-band="res" class="rowclick"
         title="fx last / close - 1, in bps"></td>` +
      `<td data-c="dVsLive" data-band="res" class="rowclick gO"></td>` +
      `<td data-q="mid_drift" data-band="res" class="rowclick"
         title="(QuoteBid+QuoteAsk)/2 vs 8am snapshot mid"></td>` +
      ["f_call","f_trig","f_exp","f_put","f_div","f_move"].map((f,i)=>
        `<td data-fl="${f}" data-band="flags" class="rowclick${i===0?" grp bfirst":""}"></td>`).join("") +
      `<td data-fl="f_hedged" data-band="flags" class="hdg" title="tick when the move is hedged - shared with the desk, blank again each new day"><input type="checkbox" class="hdgcb" data-id="${id}" onchange="hedgedChanged(this)"></td>` +
      `<td data-fl="f_nuke" data-band="flags" class="rowclick"></td>` +
      `<td data-band="flags" class="gc"><select class="volsel"
         data-u="vol_flag" data-id="${id}" onchange="volChanged(this)">
         <option value=""></option><option>Cheap</option>
         <option>Rich</option></select></td>` +
      ["iv","10","30","90","n","vega"].map((n,i)=>
        `<td data-v="${n}" data-band="vol" class="rowclick${i===0?" grp bfirst":""}"
           title="realised vol, annualised (\u221a252), from daily closes"></td>`).join("") +
      `<td data-c="lr_live" data-band="lrisk" class="rowclick lrc grp bfirst" title="Delta Scan net_live (USD)"></td>` +
      `<td data-c="lr_t1" data-band="lrisk" class="rowclick lrc" title="Delta Scan net_t1 (USD)"></td>` +
      `<td data-c="lr_ub" data-band="lrisk" class="rowclick lrc" title="Delta Scan unbooked_est (USD)"></td>` +
      ["cl_tag","cl_strat","cl_held","cl_r","cl_peer","cl_trust","cl_worst","cl_carry","cl_cat"].map((k,i)=>`<td data-c="${k}" data-band="cls" class="rowclick clc${i===0?" grp bfirst":""}"></td>`).join("") +
      `<td class="gc uinp grp bfirst" data-band="dcalc"><input class="gridcell dcv" data-dc="notl" data-id="${id}" data-row="${ri}" data-col="6" oninput="dcChg(this)" inputmode="decimal" autocomplete="off" placeholder="&#8212;" title="bond notional (face) - session only, blank at launch"></td>` +
      `<td class="gc uinp" data-band="dcalc"><input class="gridcell dcv" data-dc="delta" data-id="${id}" data-row="${ri}" data-col="7" oninput="dcChg(this)" inputmode="decimal" autocomplete="off" placeholder="&#8212;" title="delta % (e.g. 56)"></td>` +
      `<td data-c="dc_shares" data-band="dcalc" class="rowclick" title="notional x (ovdUndFx/ovdCbFx) x delta% x PARITY% / ovdSpot"></td>` +
      `<td class="gc uinp" data-band="dcalc" data-c="dc_usd"><input class="gridcell dcv" data-dc="usd" data-id="${id}" data-row="${ri}" data-col="8" oninput="dcChg(this)" inputmode="decimal" autocomplete="off" placeholder="&#8212;" title="USD delta - computed from notional x delta% x PARITY% / ovdCbFx, or type it to drive Eq Shares directly"></td>` +
      `<td class="gc uinp grp bfirst" data-band="cbnuke"><input class="sprd cnv" data-cn="start" data-id="${id}" placeholder="09:00" autocomplete="off" title="window start, exchange local HH:MM" onchange="cnChanged(this)"></td>` +
      `<td class="gc uinp" data-band="cbnuke"><input class="sprd cnv" data-cn="end" data-id="${id}" placeholder="10:00" autocomplete="off" title="window end (exclusive), exchange local HH:MM" onchange="cnChanged(this)"></td>` +
      `<td data-c="cn_twap" data-band="cbnuke" class="rowclick cnc" title="mean of 1-min bar closes in [start,end)"></td>` +
      `<td data-c="cn_vwap" data-band="cbnuke" class="rowclick cnc" title="Bloomberg-computed VWAP: EQY_WEIGHTED_AVG_PX with VWAP_START_TIME / VWAP_END_TIME / VWAP_DT overrides"></td>` +
      `<td data-c="cn_vol" data-band="cbnuke" class="rowclick cnc" title="traded volume in the window"></td>` +
      `<td data-c="cn_tk" data-band="cbnuke" class="rowclick cnt" title="Bloomberg ticker derived from the RIC"></td>` +
      [["delta","trade delta % (e.g. 60)"],["astk","anchor stock price"],["afx","anchor fx (stock ccy per USD; 1 if same ccy)"],["abond","anchor bond price"],["cstk","current stock - follows live last until you type"],["cfx","current fx - follows live last until you type"]].map(([k,t])=>
        `<td class="gc uinp" data-band="cbnuke"><input class="sprd hnv" data-hn="${k}" data-id="${id}" autocomplete="off" inputmode="decimal" placeholder="&#8212;" title="${t}" onchange="hnChanged(this)"></td>`).join("") +
      `<td data-c="hn_dn" data-band="cbnuke" class="rowclick hnc" title="dollar-neutral bond price = A.Bond + delta x (parity_c - parity_a), parity = CR x stock/fx (CR = 100 x fixed FX / CP from refdata)"></td>` +
      idbOrder().map((k,i)=>`<td data-c="${k}" data-band="idb" class="rowclick${i===0?" grp bfirst":""}${({idb_bid:" ib-bid",idb_bref:" ib-bidq",idb_btime:" ib-bidq",idb_ask:" ib-ofr",idb_aref:" ib-ofrq",idb_atime:" ib-ofrq",idb_ref:" ib-refu",idb_my_bid:" my-bid",idb_my_ask:" my-ofr"})[k]||" ichk"}"></td>`).join("") +
      RES_COLS.slice(5,10).map((c,i)=>
        `<td data-c="${c}" data-band="live" class="rowclick gL${i===0?" grp bfirst":""}"></td>`).join("") +
      RES_COLS.slice(10,15).map((c,i)=>
        `<td data-c="${c}" data-band="eod" class="rowclick gE${i===0?" grp bfirst":""}"></td>`).join("") +
      ["m","rolld","dpnl","gpnl","theo","vslive"].map((c,i)=>
        `<td data-t="${c}" data-band="theo" class="rowclick tcell${i===0?" grp bfirst":""}"></td>`).join("") +
      RF_COLS.map((c,i)=>{
        const cp = (c==="last"||c==="close");
        const cls = `rf${i===0?" grp bfirst":""}${cp?" cpy":" rowclick"}`;
        const t = cp ? ' title="Click to copy into ovdSpot"' : "";
        return `<td data-rf="${c}" data-band="stk" class="${cls}"${t}></td>`;
      }).join("") +
      CF_COLS.map((c,i)=>{
        const cp = (c==="last"||c==="close");
        const cls = `cf${i===0?" grp bfirst":""}${cp?" cpy":" rowclick"}`;
        const t = cp ? ' title="Click to copy into ovdCbFx (non-vanilla)"' : "";
        return `<td data-cf="${c}" data-band="cbfx" class="${cls}"${t}></td>`;
      }).join("") +
      FX_COLS.map((c,i)=>{
        const cp = (c==="last"||c==="close");
        const cls = `fx${i===0?" grp bfirst":""}${cp?" cpy":" rowclick"}`;
        const t = cp ? ' title="Click to copy into ovdUndFx"' : "";
        return `<td data-fx="${c}" data-band="fx" class="${cls}"${t}></td>`;
      }).join("") +
      "</tr>";
  });
  t.innerHTML = h;
  S.a = S.f = null; S.fillRect = null;
  rowSel.clear(); rowAnchor = null;
  wireGrid(); wireResizers(); applyLayout(); bandApplyAll(); paint(); paintRows();
  setStatus(`${uniq.length} securities loaded.`);
  document.getElementById("dl").disabled = true;
  lastResponse = null;
  applyState();                                   // saved names / types / X / spreads: show at once
  loadRefData(uniq).catch(e=>setStatus(`<span class="warn">Reference lookup failed: ${e}</span>`))
    .finally(()=>{ applyState(); applyNuke(); applyRfx(); if(typeof hnPaintAll==="function") hnPaintAll(); if(typeof bdRefresh==="function") bdRefresh(true); if(typeof dvRefresh==="function") dvRefresh(true); if(typeof twqRefresh==="function") twqRefresh(true); });   // and again after refdata (never skipped)
}

const BAND_FIRST = new Set(["n_bid","twq_lim","x_bid","f_call","v_iv","lr_live","cl_tag","dc_notl","cn_start",idbOrder()[0],
  "live_bid","eod_bid","t_m","stk_last","cf_last","fx_last"]);
const BANDS = {
  model:{label:"model (last nuke)",span:9},
  twq:{label:"tw short quota",span:13},
  res:{label:"override result",span:18},
  flags:{label:"flags",span:9},
  vol:{label:"vol",span:6,vr:true},
  lrisk:{label:"live risk",span:3},
  cls:{label:"class",span:9},
  dcalc:{label:"delta calc",span:4},
  cbnuke:{label:"cb nuke",span:13},
  idb:{label:"idb quotes",span:13},
  live:{label:"live",span:5}, eod:{label:"eod",span:5},
  theo:{label:"theo \u00b7 \u03b3-adj",span:6},
  stk:{label:"stock",span:5,ts:true}, cbfx:{label:"cb fx",span:5,rf:true}, fx:{label:"und fx",span:5,rf:true}};
function bandOf(k){
  if(k.startsWith("twq_")) return "twq";
  if(k.startsWith("lr_")) return "lrisk";
  if(k.startsWith("cl_")) return "cls";
  if(k.startsWith("dc_")) return "dcalc";
  if(k.startsWith("cn_")||k.startsWith("hn_")) return "cbnuke";
  if(k.startsWith("idb_")) return "idb";
  if(k==="parityPct"||k==="cs_used"||k==="m_delta"||k.startsWith("n_")) return "model";
  if(k==="stk_move"||k==="fx_move"||k==="x_bid"||k==="x_ask"||
     k==="quote_bid"||k==="quote_ask"||k==="mid_drift"||
     k==="x_both"||k==="be_move"||k==="ov_iv") return "res";
  if(k.startsWith("f_")) return "flags";
  if(k.startsWith("v_")) return "vol";
  if(k.startsWith("or_")||k==="ovd_bid"||k==="ovd_ask"||k==="d_vs") return "res";
  if(k==="ovdSpot"||k==="ovdCbFx"||k==="ovdUndFx") return "res";
  if(k.startsWith("live_")) return "live";
  if(k.startsWith("eod_")) return "eod";
  if(k.startsWith("t_")) return "theo";
  if(k.startsWith("stk_")) return "stk";
  if(k.startsWith("cf_")) return "cbfx";
  if(k.startsWith("fx_")) return "fx";
  return "";
}
function rfxNow(){ if(NS.up&&NS.up()) NS.send({type:"rfxNow"}); }
function bandHidden(){
  try{ return JSON.parse(localStorage.getItem("nukestation.bands"))||{}; }
  catch(e){ return {}; }
}
function bandPaint(k, hid){
  const tbl=document.getElementById("tbl");
  const td=document.getElementById("band-"+k);
  if(!tbl||!td) return;
  tbl.classList.toggle("hb-"+k, hid);
  td.colSpan = hid ? 1 : BANDS[k].span;
  const old = document.getElementById("rfxts");
  const ts = BANDS[k].ts
    ? ' <span id="rfxts" onclick="event.stopPropagation(); rfxNow&&rfxNow()">' +
      (old ? old.innerHTML : "&mdash;") + "</span>"
    : "";
  const rf = BANDS[k].rf
    ? ' <span class="fxnow" title="refresh now" ' +
      'onclick="event.stopPropagation(); rfxNow()">&#8635;</span>'
    : "";
  const vr = BANDS[k].vr
    ? ' <span class="fxnow" title="fetch daily-close history now ' +
      '(isolated child process)" ' +
      'onclick="event.stopPropagation(); volNow()">&#8635;</span>'
    : "";
  if(hid){
    td.innerHTML = "&#9656;";
    td.title = BANDS[k].label + " (collapsed - click to expand)";
  } else {
    const ws = (k==="cls") ? ' <a class="wslink" href="'+location.pathname.replace(/[/]+$/,"")+'/class" target="_blank" onclick="event.stopPropagation()" title="open the CLASS workspace (sections, carry inputs, what-if)">workspace &#8599;</a>' : "";
    const tq = (k==="twq" && typeof twqHdr==="function") ? (twqHdr() + ' <span class="fxnow" title="fetch the missing sessions from the exchange now (forgets the back-off)" onclick="event.stopPropagation(); twqNow()">&#8635;</span>') : "";
    td.innerHTML = BANDS[k].label + ts + rf + vr + ws + tq + " &#9662;";
    td.title = (k==="cls") ? "CB Code v3.1 from the live grid: delta, parity, override mid, CSprd, ImpVol, Bloomberg dates, realised vols. Click to collapse." : "Click to collapse";
  }
}
function bandToggle(k){
  if(k==="idb") idbMirrorRefresh();
  if(k==="lrisk") lrRefresh(true);
  if(k==="cls") clRefresh(true);
  if(k==="twq") twqRefresh(true);
  const hids=bandHidden();
  hids[k]=!hids[k];
  localStorage.setItem("nukestation.bands", JSON.stringify(hids));
  bandPaint(k, !!hids[k]);
  cfgSend("bands", JSON.stringify(hids));
}
function bandApplyAll(){
  const hids=bandHidden();
  if(localStorage.getItem("nukestation.eodhide")==="1" && !("eod" in hids)){
    hids.eod=true;
    localStorage.setItem("nukestation.bands", JSON.stringify(hids));
    localStorage.removeItem("nukestation.eodhide");
  }
  for(const k of Object.keys(BANDS)) bandPaint(k, !!hids[k]);
}

/* auto-renuke: any override-input change re-prices those rows through
   the normal server nuke (debounced) - override result stays automatic */
let _anTimer=null; const _anPend=new Set();
let _uiTimer=null;
function uiRecalcSoon(){         // coalesce per-keystroke board recomputes
  if(_uiTimer) return;
  _uiTimer=setTimeout(()=>{ _uiTimer=null;
    updParityAll(); updMovesFlags(); }, 120);
}
function nukeDone(){             // called when any nuke reply lands
  NS._nukeInFlight=false;
  if(NS._nukeSafety){ clearTimeout(NS._nukeSafety); NS._nukeSafety=null; }
}
function autoNukeQueue(sid){
  if(sid) _anPend.add(Number(sid));
  uiRecalcSoon();
  if(_anTimer) clearTimeout(_anTimer);
  _anTimer=setTimeout(function fire(){
    _anTimer=null;
    if(!_anPend.size || !NS.up()){ return; }
    if(NS._nukeInFlight){       // exactly one auto-nuke outstanding:
      _anTimer=setTimeout(fire, 400);   // wait, keep coalescing
      return;
    }
    const secIds=[..._anPend]; _anPend.clear();
    NS._nukeInFlight=true;
    NS._nukeSafety=setTimeout(nukeDone, 8000);   // never wedge
    NS.send({type:"nuke", secIds});
    setStatus(`Auto-renuking ${secIds.length} row(s) on override change&hellip;`);
  }, 800);
}
document.getElementById("tbl").addEventListener("change",(e)=>{
  if(e.target && e.target.dataset && e.target.dataset.f){
    const tr=e.target.closest("tr");
    if(tr) autoNukeQueue(Number(tr.dataset.id));
  }
});
document.getElementById("tbl").addEventListener("input",(e)=>{
  if(e.target && e.target.dataset && e.target.dataset.f){
    const tr=e.target.closest("tr");   // typing pause renukes, no blur needed
    if(tr) autoNukeQueue(Number(tr.dataset.id));
  }
});

const FLAG_TH = { yearsRed:1.0, yearsAmb:2.0, divRed:14, divAmb:30,
                  stkPct:3.0, fxBps:30, staleSpotPct:0.5, staleFxPct:0.25 };
function applyFlagTh(th){
  if(!th) return;
  if(th.moveStk !== undefined) FLAG_TH.stkPct = th.moveStk;
  if(th.moveFx !== undefined) FLAG_TH.fxBps = th.moveFx;
  if(th.staleSpot !== undefined) FLAG_TH.staleSpotPct = th.staleSpot;
  if(th.staleFx !== undefined) FLAG_TH.staleFxPct = th.staleFx;
  ["cfgThMS","cfgThMF","cfgThSS","cfgThSF"].forEach((id,i)=>{
    const el = document.getElementById(id);
    if(el) el.value = [FLAG_TH.stkPct, FLAG_TH.fxBps,
                       FLAG_TH.staleSpotPct, FLAG_TH.staleFxPct][i];
  });
  if(typeof updMovesFlags === "function") updMovesFlags();
}
function _numTxt(td){
  if(!td) return NaN;
  return parseFloat(String(td.textContent).split(",").join("").trim());
}
/* Bond dates from Bloomberg (cached daily on the server): CALL / EXP / PUT cells show the dates */
const BD = {dates:{}, ts:0, inflight:false};
function bdIsinOf(tr){ const sid=Number(tr.dataset.id); const db=String(((refCache[sid]||{}).isin)||"").trim().toUpperCase(); if(db) return db;
  const inp=tr.querySelector('input.isinv'); const mv=inp?inp.value:(((NS.rows||{})[sid]||{}).isin_manual||""); return String(mv||"").trim().toUpperCase(); }   // database first, then the manual entry
function isinChanged(el){
  const v=String(el.value||"").trim().toUpperCase().replace(/[^A-Z0-9]/g,""); el.value=v;
  if(v && !/^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.test(v)){ el.classList.add("cn-bad"); el.title="not a valid ISIN: "+v; return; }
  el.classList.remove("cn-bad"); el.title=v?"manual ISIN (database has none)":"no ISIN in the database - type it here";
  const sid=Number(el.dataset.id); NS.rows[sid]=Object.assign({}, NS.rows[sid]||{}, {isin_manual:v});
  NS.send({type:"rows", list:[{secId:sid, isin_manual:v}]});      // shared + persisted like short_name
  if(typeof bdRefresh==="function"){ BD.ts=0; bdRefresh(true); }
}
async function bdRefresh(force){
  if(BD.inflight) return; const now=Date.now(); if(!force && now-BD.ts < 60000) return;
  const isins=[...new Set([...document.querySelectorAll("#tbl tr[data-id]")].map(bdIsinOf).filter(Boolean))];
  if(!isins.length) return;
  BD.inflight=true;
  try{ const base=location.pathname.replace(new RegExp("[/]+$"),"");
    const j=await (await fetch(base+"/api/bond_dates?isins="+encodeURIComponent(isins.join(",")))).json();
    if(j&&j.ok){ Object.assign(BD.dates, j.dates||{}); BD.ts=now; BD.err=j.err||""; BD.schedErr=j.sched_err||""; bdPaintAll(); if(j.pending||j.busy) setTimeout(()=>bdRefresh(true), 4000); }
  }catch(e){ BD.err=String(e); } finally{ BD.inflight=false; }
}
/* HEDGED: a date-stamped mark; shows ticked only when the stamp is today's date (so it is blank again every new day) */
function hdgToday(){ const d=new Date(); return d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0"); }
function hdgPaintRow(tr){ const sid=Number(tr.dataset.id); const cb=tr.querySelector("input.hdgcb"); if(!cb) return; const v=String(((NS.rows||{})[sid]||{}).hedged||""); const on=(v===hdgToday()); cb.checked=on; cb.parentElement.classList.toggle("on",on); cb.title=on?"hedged today":(v?"last hedged "+v:"not hedged today"); }
function hdgPaintAll(){ document.querySelectorAll("#tbl tr[data-id]").forEach(hdgPaintRow); }
function hedgedChanged(cb){ const sid=Number(cb.dataset.id); const v=cb.checked?hdgToday():""; NS.rows[sid]=Object.assign({}, NS.rows[sid]||{}, {hedged:v}); NS.send({type:"rows", list:[{secId:sid, hedged:v}]}); hdgPaintRow(cb.closest("tr")); }
/* DIV: next ex-dividend date from Bloomberg (DVD_EX_DT), cached daily on the server; model date as fallback/cross-check */
const DV = {byRic:{}, ts:0, inflight:false, err:""};
async function dvRefresh(force){
  if(DV.inflight) return; const now=Date.now(); if(!force && now-DV.ts<60000) return;
  const rics=[...new Set([...document.querySelectorAll("#tbl tr[data-id]")].map(tr=>((tr.querySelector("td[data-r='ric']")||{}).textContent||"").trim().toUpperCase()).filter(x=>x&&x!=="\u2014"))];
  if(!rics.length) return; DV.inflight=true;
  try{ const base=location.pathname.replace(new RegExp("[/]+$"),"");
    const j=await (await fetch(base+"/api/exdiv?rics="+encodeURIComponent(rics.join(",")))).json();
    if(j&&j.ok){ Object.assign(DV.byRic, j.exdiv||{}); DV.ts=now; DV.err=j.err||""; updMovesFlags(); if(j.pending||j.busy) setTimeout(()=>dvRefresh(true), 4000); }
  }catch(e){ DV.err=String(e); } finally{ DV.inflight=false; }
}
function dvPaint(tr, dv, ref, today){
  dv.classList.remove("fl-red","fl-amb","fl-dim");
  const ric=((tr.querySelector("td[data-r='ric']")||{}).textContent||"").trim().toUpperCase(); const r=ric?DV.byRic[ric]:null;
  const model=ref.next_div_date?String(ref.next_div_date).slice(0,10):"";
  if(r && !r.error){
    if(!r.exdiv){ dv.textContent="\u2014"; dv.classList.add("fl-dim"); dv.title="Bloomberg: no ex-dividend date for "+(r.ticker||ric)+(model?" \u00b7 model: "+model:""); return; }
    const d=new Date(r.exdiv+"T00:00:00"); const days=Math.round((d-today)/86400000);
    if(days<0){ dv.textContent="\u2014"; dv.classList.add("fl-dim"); dv.title="last ex-div "+r.exdiv+" (passed "+(-days)+"d ago) - no future ex-date on Bloomberg"+(model?" \u00b7 model: "+model:""); return; }
    dv.textContent=r.exdiv.slice(5); dv.classList.add(days<=FLAG_TH.divRed?"fl-red":days<=FLAG_TH.divAmb?"fl-amb":"fl-dim");
    dv.title="next ex-div "+r.exdiv+" (in "+days+"d) \u00b7 Bloomberg "+(r.ticker||"")+(r.amount!=null?" \u00b7 last dividend "+r.amount:"")+(model&&model!==r.exdiv?" \u00b7 model: "+model:""); return; }
  // fallback: model date while Bloomberg has not answered (or failed)
  if(model){ const dd=new Date(model); const days=(dd-today)/86400000;
    if(isFinite(days) && days>=0){ dv.textContent=model.slice(5); dv.classList.add(days<=FLAG_TH.divRed?"fl-red":days<=FLAG_TH.divAmb?"fl-amb":"fl-dim"); dv.title="model next ex-div "+model+(r&&r.error?" \u00b7 Bloomberg: "+r.error:" \u00b7 waiting for Bloomberg"); }
    else { dv.textContent=""; dv.title=""; } }
  else { dv.textContent=""; dv.title=(r&&r.error)?"Bloomberg: "+r.error:"no dividend data yet"; }
}
function _dateChip(td, iso, yearsFallback, kind, flag){
  if(!td) return;
  td.classList.remove("fl-red","fl-amb","fl-dim");
  if(!iso){ if(flag==="N"){ td.textContent="\u2014"; td.title=kind+": none (Bloomberg)"; td.classList.add("fl-dim"); return; }
    if(isFinite(yearsFallback)){ td.textContent=yearsFallback.toFixed(1)+"y"; td.title=kind+": Bloomberg date not available - model years shown"; td.classList.add(yearsFallback<FLAG_TH.yearsRed?"fl-red":yearsFallback<FLAG_TH.yearsAmb?"fl-amb":"fl-dim"); return; }
    td.textContent=""; td.title=kind+": no date"; return; }
  const d=new Date(iso+"T00:00:00"); const days=Math.round((d-new Date(new Date().toDateString()))/86400000);
  td.textContent = d.toLocaleDateString("en-GB",{day:"2-digit",month:"short",year:"2-digit"});
  if(days<0) td.classList.add("fl-dim"); else if(days<=30) td.classList.add("fl-red"); else if(days<=90) td.classList.add("fl-amb");
  td.title = kind+" "+iso+(days<0?" (passed "+(-days)+"d ago)":" (in "+days+"d)")+(isFinite(yearsFallback)?" \u00b7 model: "+yearsFallback.toFixed(2)+"y":"");
}
function bdPaintRow(tr){
  const sid=Number(tr.dataset.id); const ref=refCache[sid]||{}; const isin=bdIsinOf(tr); const r=isin?BD.dates[isin]:null;
  const g=k=>tr.querySelector('td[data-fl="'+k+'"]');
  let ye=NaN; if(ref.expiry_date){ const d=new Date(ref.expiry_date); if(!isNaN(d)) ye=(d-new Date())/(365.25*24*3600*1000); }
  { const tg=g("f_trig"); if(tg){ const tv=(r&&!r.error&&r.trigger!=null&&isFinite(r.trigger))?Number(r.trigger):NaN; tg.textContent=isFinite(tv)?(tv.toFixed(tv%1?1:0)+"%"):""; tg.title=isFinite(tv)?"soft-call trigger: stock must trade at "+tv+"% of the conversion price":""; } }
  if(!r || r.error){ _yearsChip(g("f_call"), parseFloat(ref.years_to_call), (r&&r.error)?"Bloomberg: "+r.error:"call: waiting for Bloomberg dates");
    _yearsChip(g("f_exp"), ye, "no expiry_date"); _yearsChip(g("f_put"), parseFloat(ref.years_to_put), "put: waiting for Bloomberg dates");
    const why=(r&&r.error)?"Bloomberg: "+r.error+" - model years shown":"waiting for Bloomberg dates - model years shown"; ["f_call","f_exp","f_put"].forEach(k=>{ const c=g(k); if(c && c.textContent) c.title=why; }); return; }
  if(r.hard_call){ _dateChip(g("f_call"), r.hard_call, parseFloat(ref.years_to_call), "next issuer (hard) call", r.callable);
    { const c=g("f_call"); if(c && r.soft_call) c.title += " \u00b7 soft call from "+r.soft_call+(r.trigger!=null?" (trigger "+r.trigger+"%)":""); } }
  else if(!(r.soft_call || r.call) && r.callable==="Y"){ const c=g("f_call"); if(c){ c.textContent="no sched"; c.classList.remove("fl-red","fl-amb"); c.classList.add("fl-dim");
      c.title="callable, but Bloomberg returned no call schedule for "+(bdIsinOf(tr)||"this ISIN")+(BD.schedErr?" - "+BD.schedErr:"")+" - run /nuke/api/bond_dates/probe?isin="+(bdIsinOf(tr)||"")+" to see what the Terminal returns"; } }
  else { _dateChip(g("f_call"), r.soft_call || r.call, parseFloat(ref.years_to_call), "soft call", r.callable);
    { const c=g("f_call"); if(c && (r.soft_call||r.call)){ const d0=new Date((r.soft_call||r.call)+"T00:00:00"), tnow=new Date(new Date().toDateString());
      c.title = (d0<=tnow ? "soft call period running since " : "soft call period starts ")+(r.soft_call||r.call)+(r.trigger!=null?" \u00b7 trigger "+r.trigger+"%":"")+(r.call&&r.call!==r.soft_call?" \u00b7 next schedule date "+r.call:"")+(isFinite(parseFloat(ref.years_to_call))?" \u00b7 model: "+parseFloat(ref.years_to_call).toFixed(2)+"y":""); c.textContent = "S "+c.textContent; } } }
  _dateChip(g("f_exp"), r.expiry, ye, "maturity", "");
  _dateChip(g("f_put"), r.put, parseFloat(ref.years_to_put), "next put", r.putable);
}
function bdPaintAll(){ document.querySelectorAll("#tbl tr[data-id]").forEach(bdPaintRow); if(typeof applyFilters==="function") applyFilters(); }
setInterval(()=>{ bdRefresh(false); dvRefresh(false); if(typeof twqRefresh==="function") twqRefresh(false); }, 60000);
(function(){ let last=hdgToday(); setInterval(()=>{ const t=hdgToday(); if(t!==last){ last=t; hdgPaintAll(); } }, 60000); })();   // midnight: marks go blank
function _yearsChip(td, v, missingTitle){
  if(!isFinite(v)){ td.textContent="";
    td.classList.remove("fl-red","fl-amb","fl-dim");
    td.title=missingTitle; return; }
  td.textContent = v.toFixed(1)+"y";
  td.classList.remove("fl-red","fl-amb","fl-dim");
  td.classList.add(v < FLAG_TH.yearsRed ? "fl-red"
                 : v < FLAG_TH.yearsAmb ? "fl-amb" : "fl-dim");
  td.title = "";
}
/* signed magnitude scale: level 0 = coloured text only, 1..4 = light -> solid fill */
const MV_TH = {stk:[0.5,1,2,4], fx:[10,25,50,100], pts:[0.10,0.25,0.50,1.00]};
const MV_CLS = ["mv-p0","mv-p1","mv-p2","mv-p3","mv-p4","mv-n0","mv-n1","mv-n2","mv-n3","mv-n4","bgpos","bgneg","pos","neg"];
function mvClass(td, v, kind){
  if(!td) return;
  td.classList.remove(...MV_CLS);
  if(!isFinite(v) || v===0) return;
  const th = MV_TH[kind] || MV_TH.pts, a = Math.abs(v);
  let lv = 0; for(let i=0;i<th.length;i++) if(a >= th[i]) lv = i+1;
  td.classList.add((v>0?"mv-p":"mv-n")+lv);
}
const FILTERS = Object.assign({move:false, vol:[], idb:[], opp:[], dt:[]}, JSON.parse(localStorage.getItem("nukestation.filters")||"{}"));
function _addMonths(d,n){ const x=new Date(d.getTime()); const day=x.getDate(); x.setMonth(x.getMonth()+n); if(x.getDate()!==day) x.setDate(0); return x; }   // calendar months, clamped (31 Jan + 1m = 28/29 Feb)
function rowDates(tr){ const s=new Set(); const isin=(typeof bdIsinOf==="function")?bdIsinOf(tr):""; const r=isin?(BD.dates||{})[isin]:null; if(!r||r.error) return s;
  const today=new Date(new Date().toDateString()); const P=v=>{ if(!v) return null; const d=new Date(String(v).slice(0,10)+"T00:00:00"); return isNaN(d)?null:d; };
  const cs=P(r.call_start)||((r.callable==="Y")?P(r.call):null), ex=P(r.expiry), pt=P(r.put);
  if(cs && cs<=today && (!ex || ex>=today)) s.add("in_call");
  if(cs && cs>today && cs<=_addMonths(today,3)) s.add("call_3m");
  if(ex && ex>=today && ex<=_addMonths(today,6)) s.add("exp_6m");
  if(pt && pt>=today && pt<=_addMonths(today,6)) s.add("put_6m");
  return s; }
function rowQtyLive(tr){ const sid=Number(tr.dataset.id); const q=parseFloat(String(((refCache[sid]||{}).quantity_live)??"").replace(/,/g,"")); return isFinite(q)?q:0; }   // blank -> 0 (flat)
function rowGap(tr,k){ const td=tr.querySelector('td[data-c="'+k+'"]'); const v=parseFloat(String(td?td.textContent:"").replace(/[+,]/g,"")); return isFinite(v)?v:NaN; }   // blank / not compared -> NaN
function rowOpp(tr){ const q=rowQtyLive(tr), gb=rowGap(tr,"idb_gap_b"), go=rowGap(tr,"idb_gap_a"); const s=new Set();
  if(q===0 && isFinite(gb) && gb>0) s.add("flat_gb");
  if(q>0 && isFinite(go) && go<0) s.add("long_go");
  return s; }
function rowVol(tr){ const s=tr.querySelector('select[data-u="vol_flag"]'); return s ? String(s.value||"") : ""; }   // "Rich" | "Cheap" | "" (neutral)
function rowIdbGaps(tr){ const out=new Set(); tr.querySelectorAll('td[data-c="idb_flag"] span.ip').forEach(p=>{ const t=p.textContent; if(t.startsWith("G.B")) out.add("gb"); else if(t.startsWith("G.O")) out.add("go"); else if(/^G [+-]/.test(t)) out.add("g"); }); return out; }
function rowMoveFlag(tr){ const mv=tr.querySelector('td[data-fl="f_move"]'); if(!mv) return false; const c=mv.classList;
  return [...c].some(x=>/^mv-[pn][34]$/.test(x)) || c.contains("fl-red") || c.contains("fl-amb"); }   // beyond BE (strong/solid), fixed-threshold hit, or FX part
function applyFilters(){
  const rows=[...document.querySelectorAll("#tbl tr[data-id]")]; let shown=0;
  const vol=FILTERS.vol||[], idb=FILTERS.idb||[], opp=FILTERS.opp||[], dts=FILTERS.dt||[]; const active = FILTERS.move || vol.length || idb.length || opp.length || dts.length;
  rows.forEach(tr=>{
    let ok = !FILTERS.move || rowMoveFlag(tr);                       // AND across filters ...
    if(ok && vol.length) ok = vol.includes(rowVol(tr));               // ... OR within a filter
    if(ok && idb.length){ const g=rowIdbGaps(tr); ok = idb.some(k=>g.has(k)); }
    if(ok && opp.length){ const o=rowOpp(tr); ok = opp.some(k=>o.has(k)); }
    if(ok && dts.length){ const dd=rowDates(tr); ok = dts.some(k=>dd.has(k)); }
    tr.classList.toggle("flt-hide", !ok); if(ok) shown++; });
  const c=document.getElementById("fltCount"); if(c) c.textContent = (active ? (shown+" of "+rows.length+" rows") : "");
}
function filtersChanged(){ const el=document.getElementById("fltMove"); FILTERS.move=!!(el&&el.checked);
  FILTERS.vol=[...document.querySelectorAll('#filters input[data-flt="vol"]:checked')].map(i=>i.value);
  FILTERS.idb=[...document.querySelectorAll('#filters input[data-flt="idb"]:checked')].map(i=>i.value);
  FILTERS.opp=[...document.querySelectorAll('#filters input[data-flt="opp"]:checked')].map(i=>i.value);
  FILTERS.dt=[...document.querySelectorAll('#filters input[data-flt="dt"]:checked')].map(i=>i.value);
  localStorage.setItem("nukestation.filters", JSON.stringify(FILTERS)); applyFilters(); }
(function(){ const el=document.getElementById("fltMove"); if(el) el.checked=!!FILTERS.move;
  document.querySelectorAll('#filters input[data-flt="vol"]').forEach(i=>{ i.checked=(FILTERS.vol||[]).includes(i.value); });
  document.querySelectorAll('#filters input[data-flt="idb"]').forEach(i=>{ i.checked=(FILTERS.idb||[]).includes(i.value); });
  document.querySelectorAll('#filters input[data-flt="opp"]').forEach(i=>{ i.checked=(FILTERS.opp||[]).includes(i.value); });
  document.querySelectorAll('#filters input[data-flt="dt"]').forEach(i=>{ i.checked=(FILTERS.dt||[]).includes(i.value); }); })();
function updMovesFlags(scope){
  const today = new Date();
  document.querySelectorAll("#tbl tr[data-id]").forEach(tr=>{
    const sid = Number(tr.dataset.id);
    if(scope && !scope.has(sid)) return;
    const ref = refCache[sid] || {};
    const g = f => tr.querySelector(`td[data-fl="${f}"]`);
    // moves: last vs close from the stock / fx bands
    const sl=_numTxt(tr.querySelector('td[data-rf="last"]'));
    const sc=_numTxt(tr.querySelector('td[data-rf="close"]'));
    const fl=_numTxt(tr.querySelector('td[data-fx="last"]'));
    const fc=_numTxt(tr.querySelector('td[data-fx="close"]'));
    const sm = (isFinite(sl)&&isFinite(sc)&&sc!==0)?(sl/sc-1)*100:NaN;
    const fm = (isFinite(fl)&&isFinite(fc)&&fc!==0)?(fl/fc-1)*10000:NaN;
    const smTd=g("stk_move"), fmTd=g("fx_move");
    if(smTd){ smTd.textContent=isFinite(sm)?((sm>0?"+":"")+sm.toFixed(2)+"%"):"";
      mvClass(smTd, sm, "stk"); }
    if(fmTd){ fmTd.textContent=isFinite(fm)?((fm>0?"+":"")+fm.toFixed(0)):"";
      mvClass(fmTd, fm, "fx"); }
    // flags
    bdPaintRow(tr);                                   // CALL / EXP / PUT: Bloomberg dates (cached daily), years chips until they arrive
    const dv = g("f_div");
    if(dv) dvPaint(tr, dv, ref, today);          // Bloomberg next ex-div (DVD_EX_DT), model date as fallback
    // break-even daily move from ImpVol (annualised): sigma / sqrt(252)
    const ivPct = (typeof ivEffective==="function") ? ivEffective(tr).v : parseFloat(String((tr.querySelector('td[data-v="iv"]')||{}).textContent || "").replace("%",""));
    const be = (isFinite(ivPct) && ivPct > 0) ? ivPct / Math.sqrt(252) : NaN;
    const beTd = tr.querySelector('td[data-c="be_move"]');
    if(beTd){ beTd.textContent = isFinite(be) ? ("\u00b1" + be.toFixed(2) + "%") : ""; }
    const mv = g("f_move");
    if(mv){
      mv.classList.remove("fl-red","fl-amb","fl-dim",...MV_CLS);
      let txt = "", title = "";
      if(isFinite(sm) && isFinite(be)){
        const r = Math.abs(sm) / be;
        const arrow = sm > 0 ? "\u25b2 " : (sm < 0 ? "\u25bc " : "");
        if(r >= 1){
          txt = arrow + (sm>0?"+":"") + sm.toFixed(2) + "% > " + be.toFixed(2) + "% BE";
          mv.classList.add((sm>0?"mv-p":"mv-n") + (r >= 1.5 ? 4 : 3));
          title = "stock moved " + r.toFixed(2) + "x the break-even daily move (ImpVol/sqrt252) - gamma in the money";
        } else {
          txt = (sm>0?"+":"") + sm.toFixed(2) + "% / " + be.toFixed(2) + "%";
          mv.classList.add("fl-dim");
          title = "stock move is " + r.toFixed(2) + "x the break-even daily move";
        }
      } else if(isFinite(sm) && Math.abs(sm) >= FLAG_TH.stkPct){
        txt = "S" + sm.toFixed(1) + "%"; mv.classList.add("fl-red"); title = "no ImpVol - fixed threshold " + FLAG_TH.stkPct + "%";
      }
      if(isFinite(fm) && Math.abs(fm) >= FLAG_TH.fxBps){
        txt = (txt ? txt + "  " : "") + "F" + fm.toFixed(0) + "bp";
        if(!mv.classList.contains("mv-p4") && !mv.classList.contains("mv-n4") && !mv.classList.contains("mv-p3") && !mv.classList.contains("mv-n3")) mv.classList.add("fl-amb");
      }
      mv.textContent = txt; mv.title = title;
    }
    const ovb=_numTxt(tr.querySelector('td[data-c="ovdMktBid"]'));
    const ova=_numTxt(tr.querySelector('td[data-c="ovdMktAsk"]'));
    const xin=f=>{ const i=tr.querySelector(`input[data-u="${f}"]`);
      const n=parseFloat(i?i.value:""); return isFinite(n)?n:0; };
    const x2 = xin("x_both");
    const qb = isFinite(ovb) ? ovb + xin("x_bid") + x2 : NaN;
    const qa = isFinite(ova) ? ova + xin("x_ask") + x2 : NaN;
    const qbTd=tr.querySelector('td[data-q="quote_bid"]');
    const qaTd=tr.querySelector('td[data-q="quote_ask"]');
    if(qbTd) qbTd.textContent = isFinite(qb)?qb.toFixed(2):"";
    if(qaTd) qaTd.textContent = isFinite(qa)?qa.toFixed(2):"";
    const drTd=tr.querySelector('td[data-q="mid_drift"]');
    if(drTd){
      const smid=parseFloat(ref.snap8_mid);
      drTd.classList.remove("pos","neg","bgpos","bgneg");
      if(isFinite(qb)&&isFinite(qa)&&isFinite(smid)){
        const st = (CFG.roundStep>0?CFG.roundStep:0.05);
        const dd = Math.round(((qb+qa)/2 - smid)/st)*st;
        drTd.textContent = (dd>0?"+":"")+dd.toFixed(2);
        mvClass(drTd, dd, "pts");
        drTd.title = "mid " + ((qb+qa)/2).toFixed(2) + " vs 8am " +
                     smid.toFixed(2);
      } else { drTd.textContent="";
        drTd.title = isFinite(smid)?"":"no 8am snapshot yet today"; }
    }
    const px = ref.px_hist;
    const vsig = (px?px.length:0) + ":" +
      (px&&px.length?px[px.length-1]:"") + ":" + (volN()||"") + ":" +
      (ref.implied_vol ?? "") + ":" + (ref.vega ?? "");
    if(tr.dataset.vsig !== vsig){
      tr.dataset.vsig = vsig;
      const ivTd = tr.querySelector('td[data-v="iv"]');
      if(ivTd){ if(typeof ivPaintRow==="function") ivPaintRow(tr); else { const iv = parseFloat(ref.implied_vol); ivTd.textContent = isFinite(iv) ? (iv*100).toFixed(1)+"%" : ""; } }
      const vgTd = tr.querySelector('td[data-v="vega"]');
      if(vgTd){
        const vg = parseFloat(ref.vega);
        vgTd.textContent = isFinite(vg) ? vg.toFixed(2) : "";
        vgTd.title = "vega (cbanalytics.lp_model_output)";
      }
      for(const [n, key] of [[10,"10"],[30,"30"],[90,"90"],[volN(),"n"]]){
        const td = tr.querySelector(`td[data-v="${key}"]`);
        if(!td) continue;
        const v = n ? realVol(px, n) : null;
        td.textContent = v==null ? "" : v.toFixed(1)+"%";
      }
    }
    const nk = g("f_nuke");
    if(nk){
      const nSpot=_numTxt(tr.querySelector('td[data-c="nSpot"]'));
      const nFx=_numTxt(tr.querySelector('td[data-c="nSpotFx"]'));
      nk.classList.remove("fl-red","fl-amb","fl-dim");
      if(!isFinite(nSpot)){
        nk.textContent="\u2014"; nk.classList.add("fl-dim");
        nk.title="not nuked yet";
      } else {
        const ds = isFinite(sl)&&nSpot!==0 ? Math.abs(sl/nSpot-1)*100 : 0;
        const df = isFinite(fl)&&isFinite(nFx)&&nFx!==0
          ? Math.abs(fl/nFx-1)*100 : 0;
        if(ds > FLAG_TH.staleSpotPct || df > FLAG_TH.staleFxPct){
          nk.textContent="RENUKE"; nk.classList.add("fl-red");
          nk.title=`spot ${ds.toFixed(2)}% / fx ${df.toFixed(2)}% since nuke`;
        } else { nk.textContent="ok"; nk.classList.add("fl-dim"); nk.title=""; }
      }
    }
  });
  if(typeof applyFilters==="function") applyFilters();
}

let AUTO_LAST = localStorage.getItem("nukestation.autolast")==="1";
function paintAutoLast(){
  const b=document.getElementById("autoLastBtn");
  if(!b) return;
  b.textContent = "AUTO last: " + (AUTO_LAST?"ON":"OFF");
  b.style.background = AUTO_LAST ? "#0b6e66" : "";
  b.style.color = AUTO_LAST ? "#fff" : "";
}
function cfgSend(key,val){
  try{ WS.send({type:"cfg", user:CFG.user, key, val}); }catch(e){}
}
function cfgApply(key,val,boot){
  if(key==="bands"){
    try{
      localStorage.setItem("nukestation.bands", String(val||"{}"));
      bandApplyAll();
      if(!boot) setStatus("bands layout synced from another window");
    }catch(e){}
    return;
  }
  if(key==="autolast"){
    AUTO_LAST = val==="1"||val===true;
    localStorage.setItem("nukestation.autolast", AUTO_LAST?"1":"0");
    paintAutoLast();
    if(!boot) setStatus("AUTO last "+(AUTO_LAST?"ON":"OFF")+
      " (synced from another window)");
  }
}
function toggleAutoLast(){
  AUTO_LAST = !AUTO_LAST;
  localStorage.setItem("nukestation.autolast", AUTO_LAST?"1":"0");
  cfgSend("autolast", AUTO_LAST?"1":"0");
  paintAutoLast();
  setStatus(AUTO_LAST
    ? "AUTO last ON: stock/fx last feed the overrides each tick and renuke."
    : "AUTO last OFF.");
  if(AUTO_LAST) doAutoLast();
}
function doAutoLast(){
  if(!AUTO_LAST) return;
  const dirty=[];
  document.querySelectorAll("#tbl tr[data-id]").forEach((tr,ri)=>{
    const g=(sel)=>{ const el=tr.querySelector(sel);
      return el ? el.textContent.trim() : ""; };
    const pairs=[["ovdSpot", g('td[data-rf="last"]')],
                 ["ovdUndFx", g('td[data-fx="last"]')]];
    if(vanRow(tr)){ const fl=g('td[data-fx="last"]');
      if(fl!=="") pairs.push(["ovdCbFx", fl]); }
    else { const cl=g('td[data-cf="last"]');                     // non-vanilla: bond-ccy fx last (USD = 1)
      if(cl!=="") pairs.push(["ovdCbFx", cl]); }
    let changed=false;
    for(const [f,src] of pairs){
      if(src==="") continue;
      const v=src.split(",").join("");
      const inp=tr.querySelector(`input[data-f="${f}"]`);
      if(inp && inp.value!==v && document.activeElement!==inp){
        inp.value=v; changed=true;
      }
    }
    if(changed){ dirty.push(ri);
      autoNukeQueue(Number(tr.dataset.id)); }
  });
  if(dirty.length) syncRows(dirty);
}
function volN(){
  const el=document.getElementById("volN");
  const n=parseInt(el?el.value:"",10);
  return (isFinite(n)&&n>=2)?n:null;
}
function volNChanged(el){
  localStorage.setItem("nukestation.volN", el.value.trim());
  updMovesFlags();
}
function realVol(px, n){
  if(!Array.isArray(px) || px.length < n+1) return null;
  const tail = px.slice(-(n+1));
  const rets = [];
  for(let i=1;i<tail.length;i++){
    const a=tail[i-1], b=tail[i];
    if(a>0 && b>0) rets.push(Math.log(b/a));
  }
  if(rets.length < 2) return null;
  const m = rets.reduce((x,y)=>x+y,0)/rets.length;
  const v = rets.reduce((x,y)=>x+(y-m)*(y-m),0)/(rets.length-1);
  return Math.sqrt(v)*Math.sqrt(252)*100;
}
function updVolCls(sel){
  sel.classList.remove("vol-cheap","vol-rich");
  if(sel.value==="Cheap") sel.classList.add("vol-cheap");
  else if(sel.value==="Rich") sel.classList.add("vol-rich");
}
function volChanged(sel){
  updVolCls(sel);
  sprdChanged(sel);
  if(typeof applyFilters==="function") applyFilters();
}
function updParity(tr){
  const td = tr.querySelector('td[data-c="parityPct"]');
  if(!td) return;
  const sid = Number(tr.dataset.id);
  const ref = refCache[sid] || {};
  const cp = parseFloat(ref.lp_conversion_price) ||
             parseFloat(ref.conversion_price);
  const ff = parseFloat(ref.conversion_fixed_fx) || 1;
  const inp = tr.querySelector('input[data-f="ovdSpot"]');
  const sp = parseFloat(inp ? inp.value : "");
  const fxi = tr.querySelector('input[data-f="ovdUndFx"]');
  let fx = parseFloat(fxi ? fxi.value : "");
  if(!isFinite(fx))
    fx = parseFloat((tr.querySelector('td[data-c="nSpotFx"]')||{})
                    .textContent || "");
  const bt = ((tr.querySelector('input[data-u="bond_type"]')||{value:""})
              .value || "").trim().toLowerCase();
  if(bt.startsWith("vanil")){
    if(isFinite(cp) && cp > 0 && isFinite(sp)){
      td.textContent = (sp / cp * 100).toFixed(2);
      td.title = `Vanilla: ovdSpot ${sp} / CP ${cp}`;
      updDeltaCalc(tr);
    } else {
      td.textContent = "\u2014";
      td.title = "Vanilla: set ovdSpot (needs conversion_price)";
    }
    return;
  }
  if(isFinite(cp) && cp > 0 && isFinite(sp) && isFinite(fx) && fx > 0
     && ff > 0){
    td.textContent = ((sp / fx) / (cp / ff) * 100).toFixed(2);
    td.title = `(${sp} / fx ${fx}) / (CP ${cp} / fixedFx ${ff}) x 100`;
    updDeltaCalc(tr);
  } else {
    td.textContent = "\u2014";
    td.title = !isFinite(cp) || cp <= 0
      ? "no conversion_price (lp_model_output / nuked_price)"
      : "set ovdSpot and fx to compute parity";
  }
}
/* LIVE RISK mirror: Delta Scan's cached result (same origin via Lagrange); no math here */
const LR = {byRic:{}, stamp:"", ts:0, inflight:false, err:""};
const lrKey = r => String(r||"").trim().toUpperCase();
const lrLoose = r => { const s=lrKey(r); const i=s.lastIndexOf("."); return i>0 ? s.slice(0,i).replace(/^0+(?=\\d)/,"")+s.slice(i) : s; };
function lrFmt(v){ if(v==null||v===""||isNaN(v)) return ""; const n=Number(v); return (n<0?"-":"")+Math.round(Math.abs(n)).toLocaleString("en-US"); }
function lrPaintRow(tr){
  const ric=(tr.querySelector("td[data-r='ric']")||{}).textContent||"";
  const key=(LR.stamp||"")+"|"+ric; if(tr.dataset.lrStamp===key) return;   // painted for this scan + this ric already (also when no scan is loaded yet)
  tr.dataset.lrStamp=key; const r=LR.byRic[lrKey(ric)]||LR.byRic["~"+lrLoose(ric)];
  [["lr_live","net_live"],["lr_t1","net_t1"],["lr_ub","unbooked_est"]].forEach(([k,f])=>{ const td=tr.querySelector('td[data-c="'+k+'"]'); if(!td) return;
    td.classList.remove("pos","neg","lr-na"); if(!r){ td.textContent=""; return; }
    const v=r[f]; td.textContent=lrFmt(v); if(v>0) td.classList.add("pos"); else if(v<0) td.classList.add("neg"); });
  const live=tr.querySelector('td[data-c="lr_live"]'); if(live){ live.title = r ? ("Delta Scan "+(r.name||"")+" "+(r.flag||"")+(r.action?" \u00b7 "+r.action:"")+" \u00b7 scan "+LR.stamp) : ("no Delta Scan row for "+ric+(LR.err?" \u00b7 "+LR.err:"")); }
}
function lrPaintAll(){ document.querySelectorAll("#tbl tbody tr[data-id]").forEach(lrPaintRow); }
async function lrRefresh(force){
  if(LR.inflight) return; if(document.hidden && !force) return;
  const now=Date.now(); if(!force && now-LR.ts<15000) return; LR.ts=now; LR.inflight=true;
  try{
    const base=location.pathname.replace(new RegExp("[/]+$"),"").replace(/\\/nuke$/,"");
    const res=await fetch(base+"/dscan/api/scan"); if(!res.ok){ LR.err="scan HTTP "+res.status; return; }
    const j=await res.json(); if(!j || j.pending){ LR.err=j&&j.pending?"scan pending":"no scan"; return; }
    const stamp=(j.meta&&j.meta.generated)||""; if(stamp && stamp===LR.stamp) return;     // unchanged snapshot: nothing to repaint
    const by={}; (j.markets||[]).forEach(m=>(m.underlyings||[]).forEach(u=>{ if(!u||!u.ric) return; by[lrKey(u.ric)]=u; by["~"+lrLoose(u.ric)]=u; }));
    LR.byRic=by; LR.stamp=stamp; LR.err=""; lrPaintAll();
  }catch(e){ LR.err=String(e); } finally{ LR.inflight=false; }
}
setInterval(()=>lrRefresh(false), 15000);
setTimeout(()=>lrRefresh(true), 2500);
document.addEventListener("visibilitychange", ()=>{ if(!document.hidden) lrRefresh(true); });
/* TW SHORT QUOTA mirror: TWSE TWT93U / TPEx margin-sbl sessions cached by the server (one fetch per session);
   the page only formats what /api/twq returns - no exchange call, no quota math here, repaint gated per row */
const TQ = {rows:{}, meta:{}, stamp:"", ts:0, inflight:false, err:""};
const TQ_COLS = ["twq_lim","twq_bind","twq_iss","twq_sub","twq_bal","twq_room","twq_used","twq_trend","twq_thr","twq_cap","twq_roomusd","twq_sess","twq_dt"];
function tqN(v){ return (v==null||!isFinite(v)) ? "" : Math.round(v).toLocaleString("en-US"); }
function tqM(v){ if(v==null||!isFinite(v)) return ""; const a=Math.abs(v); if(a>=1e9) return (v/1e9).toFixed(2)+"bn"; if(a>=1e7) return (v/1e6).toFixed(1)+"m"; if(a>=1e6) return (v/1e6).toFixed(2)+"m"; if(a>=1e3) return (v/1e3).toFixed(0)+"k"; return String(Math.round(v)); }
function tqUsd(v){ if(v==null||!isFinite(v)) return ""; const a=Math.abs(v), s=(v<0?"-$":"$"); if(a>=1e9) return s+(a/1e9).toFixed(2)+"bn"; if(a>=1e8) return s+(a/1e6).toFixed(0)+"m"; if(a>=1e6) return s+(a/1e6).toFixed(1)+"m"; if(a>=1e3) return s+(a/1e3).toFixed(0)+"k"; return s+Math.round(a); }
function tqPct(v){ return (v==null||!isFinite(v)) ? "" : v.toFixed(1)+"%"; }
function tqSess(v){ if(v==null||!isFinite(v)) return ""; return v<=0 ? "0" : (v<1.5 ? "1" : "≈"+Math.round(v)); }
const tqEsc = s => String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/"/g,"&quot;");
function tqTrend(x){
  const t=x.trend||[], ds=(TQ.meta&&TQ.meta.tdates)||[]; let h="", tt=[];
  t.forEach((u,i)=>{ if(u==null){ h+='<i class="tqb tq0"></i>'; return; } h+='<i class="tqb" style="height:'+Math.max(2,Math.round(u/100*12))+'px"></i>'; tt.push(String(ds[i]||"").slice(5)+" "+u.toFixed(1)+"%"); });
  const arrow = x.tdir==="up" ? "↑" : (x.tdir==="down" ? "↓" : (x.tdir==="flat" ? "→" : ""));
  const n=t.filter(v=>v!=null).length;
  return {html: h+(arrow?' <span class="tqa '+x.tdir+'">'+arrow+'</span>':""),
          title: (tt.length?tt.join(" · "):"no history yet")+(x.tchg!=null?" · Δ "+(x.tchg>=0?"+":"")+x.tchg.toFixed(1)+" pts over "+n+" sessions ("+(x.tdir==="up"?"rising ≥ 1.5":x.tdir==="down"?"falling ≤ -1.5":"flat")+")":"")};
}
function twqPaintRow(tr){
  if(tr.dataset.twqStamp === (TQ.stamp||"")) return;         // nothing new from the server since this row was painted (state ticks)
  tr.dataset.twqStamp = TQ.stamp||"";
  const x = TQ.rows[tr.dataset.id];
  const sig = x ? JSON.stringify(x) : (TQ.stamp ? "-" : "");
  if(tr.dataset.twqSig === sig) return;                     // new payload, but this row's numbers did not move
  tr.dataset.twqSig = sig;
  const g = k => tr.querySelector('td[data-c="'+k+'"]');
  const reset = td => td.classList.remove("u1","u2","u3","u4","tqw");
  if(!x || x.na){
    TQ_COLS.forEach(k=>{ const td=g(k); if(!td) return; reset(td); td.textContent=""; td.title = x ? x.na : (TQ.stamp ? "not a TWSE / TPEx listing (RIC suffix .TW or .TWO)" : (TQ.err||"waiting for the quota mirror")); });
    if(x){ const l=g("twq_lim"); if(l) l.textContent="n/a"; }
    return;
  }
  const m=TQ.meta||{}; const src=(x.x==="TPEX"?"TPEx":"TWSE"); const dt=x.dt||"";
  const S='<em class="tqt tS" title="from the exchange report">S</em>', D='<em class="tqt tD" title="derived from the report: margin limit ÷ '+(m.ceil_pct||25)+'%">D</em>';
  const set=(k,txt,title,html)=>{ const td=g(k); if(!td) return; reset(td); if(html!=null) td.innerHTML=html; else td.textContent=txt; td.title=title||""; if(x.watch) td.classList.add("tqw"); };
  const bindTxt = {"sub-cap":"SUB-CAP","throttle":"THROTTLE","restricted":"RESTRICTED","check":"CHECK"}[x.bind]||"";
  const bindCls = {"sub-cap":"sc","throttle":"th","restricted":"rs","check":"ck"}[x.bind]||"";
  const bindTitle = x.bind==="sub-cap" ? "sub-cap binds: the next-session quota equals the room left under the "+(m.sbl_pct||10)+"% SBL cap ("+tqN(x.room)+" sh) - new shorts wait for returns"
    : x.bind==="throttle" ? "throttle binds: 30% x 30-session ADV (ADV30 ≈ "+tqN(x.lim/0.3)+" sh) is below the cap room "+tqN(x.room)+" sh"
    : x.bind==="restricted" ? "exchange published a zero quota although cap room remains (restriction / disposition / suspension)"
    : x.bind==="check" ? "quota is above the derived cap room: the listed-share basis (margin limit ÷ "+(m.ceil_pct||25)+"%) does not fit - check NUKE_TWQ_CEIL_MODE" : "";
  set("twq_lim", "", "次一營業日可限額 "+tqN(x.lim)+" sh - SBL short sales the whole market may add next session ("+src+" "+dt+")"+(x.note?" · note: "+x.note:""), x.lim==null ? "" : tqN(x.lim)+S);
  set("twq_bind", "", bindTitle, bindTxt ? '<span class="tqp '+bindCls+'">'+bindTxt+'</span>' : "");
  set("twq_iss", "", "listed shares "+tqN(x.iss)+" = 融券限額 "+tqN(x.ce)+" ÷ "+(m.ceil_pct||25)+"%"+(m.mode==="net"?" (net mode: balances added back)":""), x.iss==null ? "" : tqM(x.iss)+D);
  set("twq_sub", "", "SBL sub-cap "+(m.sbl_pct||10)+"% of listed shares = "+tqN(x.sub)+" sh", x.sub==null ? "" : tqM(x.sub)+D);
  set("twq_bal", "", "借券賣出當日餘額 "+tqN(x.bal)+" sh · 融券今日餘額 "+tqN(x.mb)+" sh · combined "+tqPct(x.comb)+" of the "+(m.ceil_pct||25)+"% ceiling", x.bal==null ? "" : tqM(x.bal)+S);
  set("twq_room", tqN(x.room), "sub-cap "+tqN(x.sub)+" - balance "+tqN(x.bal)+" = "+tqN(x.room)+" sh that may still be outstanding");
  set("twq_used", tqPct(x.used), "SBL balance ÷ sub-cap"+(x.used!=null ? (x.used>=95?" - cap effectively full":x.used>=75?" - tight":x.used>=50?" - half used":" - ample") : ""));
  const u=g("twq_used"); if(u && x.used!=null) u.classList.add(x.used>=95?"u4":x.used>=75?"u3":x.used>=50?"u2":"u1");
  const tr_=tqTrend(x); set("twq_trend", "", tr_.title, tr_.html);
  set("twq_thr", x.bind==="throttle" ? tqN(x.lim) : (x.bind==="sub-cap" ? "≥ room" : ""), "30% x 30-session average volume: equals the published quota when it binds; only known to be ≥ the cap room when the sub-cap binds (the report carries no volume)");
  const pxt = (x.px!=null ? "price "+x.px+" ("+(x.px_src||"")+")" : "no price yet")+" · TWD/USD "+(x.fx!=null?Number(x.fx).toFixed(3):"")+" ("+(x.fx_src||"")+")";
  set("twq_cap", tqUsd(x.cap_usd), "next-session quota in USD: "+tqN(x.lim)+" sh x "+pxt);
  set("twq_roomusd", tqUsd(x.room_usd), "cap room in USD: "+tqN(x.room)+" sh x "+pxt);
  set("twq_sess", tqSess(x.sess), x.sess!=null ? "room "+tqN(x.room)+" ÷ quota "+tqN(x.lim)+" = "+x.sess.toFixed(2)+" sessions to fill the sub-cap at the current pace" : (x.lim===0 ? "no quota next session" : ""));
  set("twq_dt", dt.slice(5), src+" report "+dt+(m.pending&&m.expected?" · waiting for "+m.expected:"")+(x.watch?" · WATCH: sub-cap binding or ≥70% used and rising":""));
}
function twqPaintAll(){ document.querySelectorAll("#tbl tbody tr[data-id]").forEach(twqPaintRow); }
function twqHdr(){
  const m=TQ.meta||{}; let h="";
  const diary = (m.log&&m.log.length) ? "\\n\\nworker diary:\\n"+m.log.slice(-5).join("\\n") : "";
  if(!m.session && TQ.stamp) h+=' <span class="tqhd pend" title="'+tqEsc((m.busy ? "fetching from TWSE / TPEx now"+(m.cur?" ("+m.cur+")":"")+" - a few seconds per session" : (m.layout||m.err||TQ.err ? "nothing could be fetched yet: "+(m.layout||m.err||TQ.err) : "nothing fetched yet - the poller starts 15 s after launch"))+(m.retry_in?" \u00b7 next automatic retry in "+Math.ceil(m.retry_in/60)+" min":"")+" \u00b7 click \u21bb to fetch now, or drop the exchange file into "+(m.drop_dir||"twq_drop")+diary)+'">'+(m.busy ? "fetching"+(m.cur?" "+String(m.cur).slice(5):"")+"\u2026" : "no session yet")+'</span>';
  if(m.session) h+=' <span class="tqhd" title="latest session in the mirror: '+m.session+(m.fetched_at?" (fetched "+m.fetched_at+")":"")+(m.tdates?" · "+m.tdates.length+" sessions of history":"")+'">'+String(m.session).slice(5)+'</span>';
  if(m.pending && m.expected && m.exp_state==="holiday") h+=' <span class="tqhd" title="the exchange published no report for '+m.expected+' (holiday / no trading) - showing the latest session">'+String(m.expected).slice(5)+' no trading</span>';
  else if(m.pending && m.expected) h+=' <span class="tqhd pend" title="the '+m.expected+' report is not out yet - the exchange publishes after the close; on a holiday the band stays on the previous session'+tqEsc(diary)+'">waiting '+String(m.expected).slice(5)+'</span>';
  const e=m.layout||m.err||TQ.err; if(e) h+=' <span class="tqhd bad" title="'+tqEsc(e)+'">!</span>';
  if(m.hint) h+=' <span class="tqhd pend" title="'+tqEsc(m.hint)+'">basis?</span>';
  return h;
}
function twqHdrPaint(){ if(!document.getElementById("band-twq")) return; if(!bandHidden().twq) bandPaint("twq", false); }
async function twqNow(){
  try{ const base=location.pathname.replace(new RegExp("[/]+$"),"");
    const r=await fetch(base+"/api/twq/fetch",{method:"POST"}); const j=await r.json();
    TQ.meta=Object.assign({}, TQ.meta||{}, {busy:true, log:(j&&j.log)||[]}); twqHdrPaint();
    let n=0; const tick=()=>{ TQ.ts=0; twqRefresh(true); if(++n<24) setTimeout(tick, 5000); }; setTimeout(tick, 3000);   // follow the fetch for 2 min
  }catch(e){ TQ.err=String(e); twqHdrPaint(); }
}
async function twqRefresh(force){
  if(TQ.inflight) return; if(document.hidden && !force) return;
  const now=Date.now(); if(!force && now-TQ.ts<60000) return; TQ.ts=now; TQ.inflight=true;
  try{ const base=location.pathname.replace(new RegExp("[/]+$"),"");
    const res=await fetch(base+"/api/twq"); if(!res.ok){ TQ.err="twq HTTP "+res.status; twqHdrPaint(); return; }
    const j=await res.json(); if(!j||!j.ok){ TQ.err=(j&&j.err)||"no reply"; twqHdrPaint(); return; }
    TQ.meta=j; TQ.err=j.layout||j.err||"";
    if(j.stamp && j.stamp===TQ.stamp) return;               // same sessions, same prices, same errors: nothing to repaint
    TQ.rows=j.rows||{}; TQ.stamp=j.stamp||""; twqPaintAll(); twqHdrPaint();
    if(!j.session && (TQ.boot=(TQ.boot||0)+1) <= 36) setTimeout(()=>{ TQ.ts=0; twqRefresh(true); }, 5000);   // nothing cached yet: re-poll every 5 s (3 min) so the first session shows at once
  }catch(e){ TQ.err=String(e); } finally{ TQ.inflight=false; }
}
setTimeout(()=>twqRefresh(true), 3500);
document.addEventListener("visibilitychange", ()=>{ if(!document.hidden) twqRefresh(false); });
/* CLASS mirror: CB Code v3.1 computed by the server from the live grid; no math here */
const CL = {rows:{}, stamp:"", ts:0, inflight:false, err:""};
function clAssump(){ try{ const j=JSON.parse(localStorage.getItem("nukestation.class.assump")||"{}"); return (j&&typeof j==="object")?j:{}; }catch(e){ return {}; } }
function clTagChip(tag){ const g=(["GAMMA","VOL","PARITY","CONVERT"].includes(tag))?"h":(["CREDIT","YIELD","COUPON","DELTA"].includes(tag)?"o":"n"); return '<span class="ct '+g+'">'+tag+'</span>'; }
function clPaintRow(tr){
  if(tr.dataset.clStamp===(CL.stamp||"")) return;                  // already painted for this data: no DOM work on state ticks
  tr.dataset.clStamp=CL.stamp||"";
  const sid=Number(tr.dataset.id); const x=CL.rows[sid]; const g=k=>tr.querySelector('td[data-c="'+k+'"]');
  const set=(k,html,title)=>{ const td=g(k); if(!td) return; td.innerHTML=html||""; td.title=title||""; };
  if(!x){ ["cl_tag","cl_strat","cl_held","cl_r","cl_peer","cl_trust","cl_worst","cl_carry","cl_cat"].forEach(k=>set(k,"",CL.err||"waiting for the class engine")); return; }
  set("cl_tag", clTagChip(x.tag), x.tag_group+" - "+x.strategy);
  set("cl_strat", x.strategy+(x.strategy!==x.strategy_v3?' <span class="ct tr">V3.1</span>':""), x.why+(x.exit?" Exit: "+x.exit:"")+(x.exit_tags&&x.exit_tags.length?" | "+x.exit_tags.join(", "):""));
  set("cl_held", x.held_as, "lens: "+x.lens);
  const rt=x.read_tags||{}; const rcls=rt.r.startsWith("CHEAP")?"chp":(rt.r.startsWith("RICH")?"rch":"tr"); set("cl_r", rt.r?('<span class="ct '+rcls+(rt.r.endsWith("?")?" q":"")+'">'+rt.r+'</span>'):"", [x.r_margin_txt, rt.r250, rt.trend].filter(Boolean).join(" | "));
  const pv=x.vs_peers||{}; const pcls=x.peer_tag==="CHEAP"?"chp":(x.peer_tag==="RICH"?"rch":"tr");
  set("cl_peer", x.peer_tag&&x.peer_tag!=="N/A"?('<span class="ct '+pcls+'">'+x.peer_tag+'</span>'+(pv.gap_px!=null?" "+(pv.gap_px>=0?"+":"\u2212")+Math.abs(pv.gap_px).toFixed(1)+"%":"")):'<span class="ct n">N/A</span>', pv.gap_vol!=null?("fair IV "+pv.fair_iv.toFixed(1)+"% ("+pv.peer_set+", "+pv.peers+" peers); gap "+pv.gap_vol.toFixed(1)+" vol pts = "+pv.gap_px.toFixed(1)+"% of price"):"little vega: no peer verdict");
  set("cl_trust", (x.trust||[]).map(t=>'<span class="ct tr'+(t==="OK"?" ok":(t==="CHECK INPUTS"?" bad":""))+'">'+t+'</span>').join(""), x.input_check||"");
  set("cl_worst", x.worst_txt, x.makes_money_in?("makes money in "+x.makes_money_in+" ("+x.held_as.toLowerCase()+")"):"");
  const e=x.economics||{}; set("cl_carry", x.carry_txt, e.carry_net!=null?("time & coupon "+e.time_coupon.toFixed(3)+", funding "+e.funding.toFixed(3)+", borrow "+e.borrow.toFixed(3)+" (funding "+e.funding_rate+"%"+(e.borrow_rate!=null?", borrow "+e.borrow_rate+"%":"")+")"):"");
  set("cl_cat", x.cat_txt, x.category?("best of its category in "+(x.category.top||[]).length+" regimes, worst in "+(x.category.bottom||[]).length):"");
  const td=g("cl_carry"); if(td && e.carry_net!=null){ td.style.color = e.carry_net>0?"#13786C":(e.carry_net<0?"#B23A62":""); }
}
function clPaintAll(){ document.querySelectorAll("#tbl tbody tr[data-id]").forEach(clPaintRow); }
async function clRefresh(force){
  if(CL.inflight) return; if(document.hidden && !force) return;
  const now=Date.now(); if(!force && now-CL.ts<15000) return; CL.ts=now; CL.inflight=true;
  try{ const base=location.pathname.replace(new RegExp("[/]+$"),"");
    const j=await (await fetch(base+"/api/class?a="+encodeURIComponent(JSON.stringify(clAssump())))).json();
    if(!j||!j.ok){ CL.err=(j&&j.error)||"class engine unavailable"; CL.rows={}; CL.stamp="err"+Date.now(); clPaintAll(); return; }
    if(j.stamp && j.stamp===CL.stamp) return;
    const rows={}; Object.keys(j.rows||{}).forEach(k=>{ rows[Number(k)]=j.rows[k]; }); CL.rows=rows; CL.stamp=j.stamp||""; CL.err=""; clPaintAll();
  }catch(e){ CL.err=String(e); } finally{ CL.inflight=false; }
}
setInterval(()=>clRefresh(false), 15000);
setTimeout(()=>clRefresh(true), 3000);
/* IDB QUOTES mirror: values computed by the Lagrange IDB tab (same origin); no math here */
const IDB_MIRROR = {};   // secId -> row from /api/idb/grid
const IDB_KEYS = idbOrder();
function idbPaintRow(tr){
  const sid = Number(tr.dataset.id); const r = IDB_MIRROR[sid];
  const set = (k, v, html) => { const c = tr.querySelector('td[data-c="'+k+'"]'); if(!c) return; if(html) c.innerHTML = v; else c.textContent = v; };
  if(!r){ IDB_KEYS.forEach(k=>set(k,"")); return; }
  const F2 = v => (v==null||v===""||isNaN(v)) ? "" : Number(v).toFixed(2);
  set("idb_bid",F2(r.idb_bid)); set("idb_bref",F2(r.idb_bref)); set("idb_btime",r.idb_btime||"");
  set("idb_ask",F2(r.idb_ask)); set("idb_aref",F2(r.idb_aref)); set("idb_atime",r.idb_atime||"");
  set("idb_ref",F2(r.idb_ref)); set("idb_my_bid",F2(r.idb_rb)); set("idb_my_ask",F2(r.idb_ra));
  const G=v=>(v==null||v===""||isNaN(v))?"":((v>0?"+":"")+Number(v).toFixed(3));
  set("idb_gap_b",G(r.idb_gap_b)); set("idb_gap_a",G(r.idb_gap_a)); set("idb_gap",G(r.idb_gap));
  set("idb_flag", idbPills(r.idb_flag), true);
}
const IDB_PILL_LEGEND = "X BID = broker bid above my offer; X OFR = broker offer below my bid; G.B / G.O / G = gap on bid / offer / mid (orange deepens with size); RE-NUKE = inputs changed, not repriced; NOT PRICED = no IDB run yet; NO MARK = unmapped; T\u2260 = bid/offer quoted at different times (ref from the later side); B n/c, O n/c = that side quoted at another ref, not compared; REF\u2260 = my ref differs from the broker ref; STALE = > 90 min old; HIST = previous day";
function idbPill(txt){
  const esc = s => String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  const t = String(txt||"").trim(); if(!t) return "";
  const num = (m)=>{ const v=parseFloat(m); return isFinite(v)?v:0; };
  const shade = v => Math.abs(v) >= 2 ? "ip-g3" : (Math.abs(v) >= 1 ? "ip-g2" : "ip-g1");
  const shadeB = v => Math.abs(v) >= 2 ? "ip-b3" : (Math.abs(v) >= 1 ? "ip-b2" : "ip-b1");   // bid gap: blue family
  const shadeO = v => Math.abs(v) >= 2 ? "ip-o3" : (Math.abs(v) >= 1 ? "ip-o2" : "ip-o1");   // offer gap: orange family
  let m;
  if(t === "MKT BID > MY OFFER") return `<span class="ip ip-x" title="${esc(t)}">X BID</span>`;
  if(t === "MKT OFFER < MY BID") return `<span class="ip ip-x" title="${esc(t)}">X OFR</span>`;
  if((m = t.match(/^BID GAP ([+-]?[\\d.]+)/))) return `<span class="ip ${shadeB(num(m[1]))}" title="${esc(t)}">G.B ${num(m[1])>0?"+":""}${num(m[1]).toFixed(2)}</span>`;
  if((m = t.match(/^OFR GAP ([+-]?[\\d.]+)/))) return `<span class="ip ${shadeO(num(m[1]))}" title="${esc(t)}">G.O ${num(m[1])>0?"+":""}${num(m[1]).toFixed(2)}</span>`;
  if((m = t.match(/^GAP ([+-]?[\\d.]+)/))) return `<span class="ip ${shade(num(m[1]))}" title="${esc(t)}">G ${num(m[1])>0?"+":""}${num(m[1]).toFixed(2)}</span>`;
  if(t.startsWith("NOT REPRICED")) return `<span class="ip ip-grey" title="${esc(t)}">RE-NUKE</span>`;
  if(t.startsWith("NOT PRICED")) return `<span class="ip ip-grey" title="${esc(t)}">NOT PRICED</span>`;
  if(t === "NO MARK") return `<span class="ip ip-out" title="${esc(t)}">NO MARK</span>`;
  if(t.startsWith("SIDES @ DIFF TIME")){ const s=(t.match(/ref from (\\w+)/)||[])[1]||""; return `<span class="ip ip-blue" title="${esc(t)}">T\u2260${s?" ("+s+")":""}</span>`; }
  if(t.startsWith("BID @") && /not compared/.test(t)) return `<span class="ip ip-out" title="${esc(t)}">B n/c</span>`;
  if(t.startsWith("OFR @") && /not compared/.test(t)) return `<span class="ip ip-out" title="${esc(t)}">O n/c</span>`;
  if(t.startsWith("REF ")) return `<span class="ip ip-blue" title="${esc(t)}">REF\u2260</span>`;
  if(t === "STALE") return `<span class="ip ip-purple" title="${esc(t)}">STALE</span>`;
  if(t.startsWith("HIST")) return `<span class="ip ip-out" title="${esc(t)}">${esc(t)}</span>`;
  if(t === "NO QUOTE") return `<span class="ip ip-out" title="${esc(t)}">NO QUOTE</span>`;
  return `<span class="ip ip-grey" title="${esc(t)}">${esc(t)}</span>`;
}
function idbPills(flags){ return String(flags||"").split(" | ").map(idbPill).filter(Boolean).join(" "); }
function idbPaintAll(){ document.querySelectorAll("#tbl tbody tr[data-id]").forEach(idbPaintRow); if(typeof applyFilters==="function") applyFilters(); }
async function idbMirrorRefresh(){
  try{
    const d=new Date(); const iso=d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
    const res=await fetch("/api/idb/grid?date="+iso); if(!res.ok) return;      // Lagrange, same origin
    const j=await res.json(); if(!j.ok) return;
    for(const k in IDB_MIRROR) delete IDB_MIRROR[k];
    for(const row of j.rows) if(row.secId) IDB_MIRROR[Number(row.secId)]=row;
    idbPaintAll();
  }catch(_){}
}
setInterval(idbMirrorRefresh, 5000);
setTimeout(idbMirrorRefresh, 2000);
/* CB NUKE: session-only start/end per secId; TWAP/VWAP from /api/twap (Bloomberg) */
const CN = {};
const RIC_BBG_JS = {KS:"KP",KQ:"KQ",HK:"HK",T:"JT",TW:"TT",TWO:"TT",SS:"CG",SZ:"CS",N:"UN",O:"UW",OQ:"UW",L:"LN",SI:"SP",AX:"AT",KL:"MK",BK:"TB",JK:"IJ",NS:"IS",BO:"IB",PA:"FP",DE:"GY",MI:"IM",AS:"NA",SW:"SW",ST:"SS",TO:"CN",MC:"SM"};
function cnTicker(ric){ const s=String(ric||"").trim().toUpperCase(); const i=s.lastIndexOf("."); if(i<0) return ""; const sym0=s.slice(0,i), suf=s.slice(i+1); const code=RIC_BBG_JS[suf]; if(!code||!sym0) return ""; const sym=(code==="HK")?sym0.replace(/^0+/,""):sym0; return sym+" "+code+" Equity"; }
function parseTimeSmart(raw){
  let s=String(raw||"").trim().toLowerCase().replace(/\\s+/g,""); if(!s) return null;
  if(s==="now"){ const n=new Date(); return [n.getHours(),n.getMinutes(),0]; }
  let ap=null; const m=s.match(/^(.*?)(am|pm|a|p)$/); if(m&&m[1]){ s=m[1]; ap=m[2][0]; }
  s=s.replace(/\\./g,":").replace(/h/g,":"); let h,mi,se=0;
  if(s.includes(":")){ const p=s.split(":").filter(x=>x!==""); if(!p.length||!p.every(x=>/^\\d+$/.test(x))) return null; h=+p[0]; mi=p.length>1?+p[1]:0; se=p.length>2?+p[2]:0; }
  else if(/^\\d+$/.test(s)){ if(s.length<=2){h=+s;mi=0;} else if(s.length===3){h=+s[0];mi=+s.slice(1);} else if(s.length===4){h=+s.slice(0,2);mi=+s.slice(2);} else if(s.length===6){h=+s.slice(0,2);mi=+s.slice(2,4);se=+s.slice(4);} else return null; }
  else return null;
  if(ap==="p"&&h<12) h+=12; if(ap==="a"&&h===12) h=0;
  if(!(h>=0&&h<24&&mi>=0&&mi<60&&se>=0&&se<60)) return null;
  return [h,mi,se];
}
function fmtHM(t){ return t?String(t[0]).padStart(2,"0")+":"+String(t[1]).padStart(2,"0")+(t[2]?":"+String(t[2]).padStart(2,"0"):""):""; }
/* ImpVol: database (lp_model_output implied_vol) first, then the manual entry */
function ivDb(tr){ const sid=Number(tr.dataset.id); const v=parseFloat(((refCache[sid]||{}).implied_vol)); return isFinite(v)&&v>0 ? v*100 : NaN; }   // percent
function ivManual(tr){ const sid=Number(tr.dataset.id); const inp=tr.querySelector('input.ivv'); const raw=inp&&inp.value!==''?inp.value:(((NS.rows||{})[sid]||{}).iv_manual||''); const v=parseFloat(raw); return isFinite(v)&&v>0 ? v : NaN; }
function ivEffective(tr){ const d=ivDb(tr); if(isFinite(d)) return {v:d, src:'db'}; const m=ivManual(tr); return isFinite(m) ? {v:m, src:'manual'} : {v:NaN, src:''}; }
function ivPaintRow(tr){ const td=tr.querySelector('td[data-c="ov_iv"]'); if(!td) return; const sp=td.querySelector('.iv-db'), inp=td.querySelector('input.ivv'); const d=ivDb(tr);
  if(isFinite(d)){ sp.textContent=d.toFixed(1)+'%'; sp.style.display=''; inp.style.display='none'; td.title='implied vol from the database (lp_model_output)'; }
  else { sp.style.display='none'; inp.style.display=''; const sid=Number(tr.dataset.id); const mv=((NS.rows||{})[sid]||{}).iv_manual; if(mv!==undefined && document.activeElement!==inp && !inp.classList.contains('cn-bad')) inp.value=mv||''; td.title=(inp.value?'manual implied vol (database has none)':'no implied vol in the database - type it here (in %)'); }
  const vt=tr.querySelector('td[data-v="iv"]'); if(vt){ const e=ivEffective(tr); vt.textContent=isFinite(e.v)?(e.v.toFixed(1)+'%'+(e.src==='manual'?' m':'')):''; vt.title=e.src==='db'?'implied_vol (cbanalytics.lp_model_output)':(e.src==='manual'?'manual implied vol (typed in the override result ImpVol cell)':''); }
}
function ivPaintAll(){ document.querySelectorAll('#tbl tr[data-id]').forEach(ivPaintRow); }
function ivChanged(el){ const raw=String(el.value||'').trim().replace('%',''); const v=parseFloat(raw);
  if(raw && !(isFinite(v) && v>0 && v<=500)){ el.classList.add('cn-bad'); el.title='not a valid vol %: '+raw; return; }
  el.classList.remove('cn-bad'); el.value=raw?String(v):''; const sid=Number(el.dataset.id);
  NS.rows[sid]=Object.assign({}, NS.rows[sid]||{}, {iv_manual:el.value}); NS.send({type:'rows', list:[{secId:sid, iv_manual:el.value, _clear:(el.value===''?['iv_manual']:[])}]});
  ivPaintRow(el.closest('tr')); updMovesFlags(new Set([sid])); }
function cnPaintTicker(tr){ const td=tr.querySelector('td[data-c="cn_tk"]'); if(!td) return; const ric=(tr.querySelector("td[data-r='ric']")||{}).textContent||""; const tk=cnTicker(ric.trim()); td.textContent = tk || (ric.trim()?("no bbg code for ."+ric.trim().split(".").pop()):""); td.classList.toggle("cn-err", !tk && !!ric.trim()); }
async function cnChanged(el){
  const tr=el.closest("tr"); const sid=Number(tr.dataset.id); CN[sid]=CN[sid]||{};
  const raw=el.value.trim(); const t=parseTimeSmart(raw);
  if(raw && !t){ el.classList.add("cn-bad"); el.title="not a time: "+raw; CN[sid][el.dataset.cn]=""; const c=k=>{const x=tr.querySelector('td[data-c="'+k+'"]'); if(x) x.textContent="";}; c("cn_twap"); c("cn_vwap"); c("cn_vol"); return; }
  el.classList.remove("cn-bad"); el.title=""; el.value=fmtHM(t); el.defaultValue=el.value;
  CN[sid][el.dataset.cn]=el.value;
  const st=CN[sid].start||"", en=CN[sid].end||"";
  const set=(k,v)=>{ const c=tr.querySelector('td[data-c="'+k+'"]'); if(c) c.textContent=v; };
  if(!st||!en){ set("cn_twap",""); set("cn_vwap",""); set("cn_vol",""); return; }
  const ric=((tr.querySelector("td[data-r='ric']")||{}).textContent||"").trim();
  set("cn_twap","\u2026"); set("cn_vwap","\u2026"); set("cn_vol","");
  const base = location.pathname.replace(new RegExp("[/]+$"),"");
  let j; try{ j=await (await fetch(base+"/api/twap",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({sec_id:String(sid),ric,start:st,end:en})})).json(); }catch(e){ j={ok:false,error:String(e)}; }
  const tk=tr.querySelector('td[data-c="cn_tk"]'); if(tk && j.ticker){ tk.textContent=j.ticker; tk.classList.remove("cn-err"); }
  if(!j.ok){ set("cn_twap",""); set("cn_vwap",""); set("cn_vol",""); if(tk){ tk.textContent=(j.ticker?j.ticker+" \u00b7 ":"")+(j.error||j.status||"error"); tk.classList.add("cn-err"); } return; }
  if(j.start_eff && j.end_eff){ const si=tr.querySelector('input[data-cn="start"]'), ei=tr.querySelector('input[data-cn="end"]');
    if(si && document.activeElement!==si){ si.value=j.start_eff; si.defaultValue=j.start_eff; CN[sid].start=j.start_eff; }
    if(ei && document.activeElement!==ei){ ei.value=j.end_eff; ei.defaultValue=j.end_eff; CN[sid].end=j.end_eff; } }
  set("cn_twap", j.twap==null?"":Number(j.twap).toFixed(4)); set("cn_vwap", j.vwap==null?"":Number(j.vwap).toFixed(4));
  set("cn_vol", j.volume==null?"":Number(j.volume).toLocaleString("en-US",{maximumFractionDigits:0}));
  if(tk){ tk.textContent=j.ticker+" \u00b7 VWAP: BBG"+(j.twap!=null?" \u00b7 TWAP: "+(j.twap_src||"BBG")+(j.twap_interval?" "+j.twap_interval:""):(j.twap_note?" \u00b7 "+j.twap_note:""))+(j.snap_note?" \u00b7 "+j.snap_note:"")+(j.status&&j.status!=="ok"?" \u00b7 "+j.status:""); tk.title=tk.textContent; }
}
(function(){ const tbl=document.getElementById("tbl"); if(!tbl) return;
  const inputsOf=tr=>tr.querySelectorAll("input[data-cn]");
  const rowOf=(tr,d)=>{ let x=tr; while(x){ x=d>0?x.nextElementSibling:x.previousElementSibling; if(x&&x.dataset&&x.dataset.id!==undefined) return x; } return null; };
  const HN_ORDER=["delta","astk","afx","abond","cstk","cfx"];
  tbl.addEventListener("keydown", e=>{ const el=e.target; if(!(el&&el.dataset&&(el.dataset.cn||el.dataset.hn))) return; const tr=el.closest("tr");
    if(el.dataset.hn){ const k=el.dataset.hn; const idx=HN_ORDER.indexOf(k);
      if(e.key==="Enter"||e.key==="ArrowDown"||e.key==="ArrowUp"){ e.preventDefault(); if(e.key!=="ArrowUp") el.dispatchEvent(new Event("change")); const nr=rowOf(tr,e.key==="ArrowUp"?-1:1); const nx=nr&&nr.querySelector('input[data-hn="'+k+'"]'); if(nx){ nx.focus(); nx.select(); } return; }
      if(e.key==="Tab"){ e.preventDefault(); el.dispatchEvent(new Event("change")); let nx=null; if(!e.shiftKey){ nx=idx<HN_ORDER.length-1?tr.querySelector('input[data-hn="'+HN_ORDER[idx+1]+'"]'):(rowOf(tr,1)&&rowOf(tr,1).querySelector('input[data-hn="delta"]')); } else { nx=idx>0?tr.querySelector('input[data-hn="'+HN_ORDER[idx-1]+'"]'):(rowOf(tr,-1)&&rowOf(tr,-1).querySelector('input[data-hn="cfx"]')); } if(nx){ nx.focus(); nx.select(); } return; }
      if(e.key==="Escape"){ el.value=el.defaultValue||""; el.classList.remove("cn-bad"); el.blur(); return; }
      if((e.key==="Delete") && (k==="cstk"||k==="cfx") && el.value!=="" && !el.classList.contains("hn-live")){ e.preventDefault(); el.value=""; el.dispatchEvent(new Event("change")); el.select(); return; }   // typed leg -> back to live
      return; }
    const which=el.dataset.cn;
    if(e.key==="Enter"||e.key==="ArrowDown"||e.key==="ArrowUp"){ e.preventDefault(); if(e.key!=="ArrowUp") el.dispatchEvent(new Event("change")); const nr=rowOf(tr,e.key==="ArrowUp"?-1:1); const nx=nr&&nr.querySelector('input[data-cn="'+which+'"]'); if(nx){ nx.focus(); nx.select(); } return; }
    if(e.key==="Tab"){ e.preventDefault(); el.dispatchEvent(new Event("change")); let nx; if(!e.shiftKey){ nx=which==="start"?tr.querySelector('input[data-cn="end"]'):(rowOf(tr,1)||{}).querySelector&&rowOf(tr,1).querySelector('input[data-cn="start"]'); } else { nx=which==="end"?tr.querySelector('input[data-cn="start"]'):(rowOf(tr,-1)&&rowOf(tr,-1).querySelector('input[data-cn="end"]')); } if(nx){ nx.focus(); nx.select(); } return; }
    if(e.key==="Escape"){ el.value=el.defaultValue||""; el.classList.remove("cn-bad"); el.blur(); return; }
  }, true);
  tbl.addEventListener("paste", e=>{ const el=e.target; if(!(el&&el.dataset&&el.dataset.cn)) return; const txt=(e.clipboardData||window.clipboardData).getData("text")||""; const parts=txt.split(/[\t,;]| - |-/).map(x=>x.trim()).filter(Boolean); if(parts.length<2) return; e.preventDefault();
    const tr=el.closest("tr"); const a=tr.querySelector('input[data-cn="start"]'), b=tr.querySelector('input[data-cn="end"]'); if(a&&b){ a.value=parts[0]; b.value=parts[1]; a.dispatchEvent(new Event("change")); b.dispatchEvent(new Event("change")); } }, true);
})();
/* OVCV-style hedge: $-neutral = A.Bond x [1 + delta x ((C.Stock/C.FX)/(A.Stock/A.FX) - 1)] */
const HN = {};   // secId -> {delta, astk, afx, abond, cstk, cfx}; cstk/cfx undefined = follow live
function hnNum(v){ const n=parseFloat(String(v==null?"":v).replace(/,/g,"")); return isFinite(n)?n:NaN; }
function hnLive(tr, which){
  if(which==="cstk"){ const t=tr.querySelector('td[data-rf="last"]'); return hnNum(t?t.textContent:""); }
  const fx=uVal(tr,"und_fx"); const c=hnNum(fx); if(isFinite(c)) return c;      // numeric constant und fx
  const t=tr.querySelector('td[data-fx="last"]'); return hnNum(t?t.textContent:"");
}
function hnDollarNeutral(delta, astk, afx, abond, cstk, cfx, cr){
  // OVCV dollar-neutral: bond moves by delta x change in parity, parity = CR x stock in bond ccy (per 100 face)
  const d=delta/100;
  if(![delta,astk,afx,abond,cstk,cfx,cr].every(x=>isFinite(x))) return NaN;
  if(astk<=0||afx<=0||cfx<=0||abond<=0||cstk<=0||cr<=0) return NaN;
  const parA=cr*astk/afx, parC=cr*cstk/cfx;
  return abond + d*(parC-parA);
}
function hnConvRatio(tr){   // shares per 100 face in bond ccy: 100 x fixedFX / CP (vanilla: 100 / CP)
  const sid=Number(tr.dataset.id); const ref=refCache[sid]||{};
  const cp=parseFloat(ref.lp_conversion_price)||parseFloat(ref.conversion_price); if(!isFinite(cp)||cp<=0) return NaN;
  const bt=((tr.querySelector('input[data-u="bond_type"]')||{value:""}).value||"").trim().toLowerCase();
  const ff=bt.startsWith("vanil")?1:(parseFloat(ref.conversion_fixed_fx)||1);
  return 100*ff/cp;
}
function hnPaintRow(tr){
  const sid=Number(tr.dataset.id); const v=HN[sid]||{};
  const get=k=>{ const i=tr.querySelector('input[data-hn="'+k+'"]'); if(!i) return NaN;
    if((k==="cstk"||k==="cfx") && (v[k]===undefined||v[k]==="")){ const lv=hnLive(tr,k); if(document.activeElement!==i && !i.classList.contains("cn-bad")){ i.value=isFinite(lv)?(k==="cfx"?lv.toFixed(4):String(lv)):""; i.defaultValue=i.value; i.classList.add("hn-live"); i.title=(k==="cstk"?"current stock":"current fx")+" - following live (type to override; clear or Delete to follow live again)"; } return lv; }
    if(document.activeElement!==i) i.value=(v[k]===undefined?"":v[k]); i.classList.remove("hn-live"); if(k==="cstk"||k==="cfx") i.title=(k==="cstk"?"current stock":"current fx")+" - typed override (clear or Delete to follow live again)"; return hnNum(v[k]); };
  const cr=hnConvRatio(tr);
  const out=hnDollarNeutral(get("delta"),get("astk"),get("afx"),get("abond"),get("cstk"),get("cfx"),cr);
  const td=tr.querySelector('td[data-c="hn_dn"]'); if(td){ td.textContent=isFinite(out)?out.toFixed(4):"";
    const a=get("astk"),af=get("afx"),c=get("cstk"),cf=get("cfx");
    td.title=isFinite(cr)?("$-neutral = A.Bond + delta x (parity_c - parity_a); CR "+cr.toFixed(4)+" per 100"+(isFinite(a*af*c*cf)?" ; parity_a "+(cr*a/af).toFixed(3)+" -> parity_c "+(cr*c/cf).toFixed(3):"")):"needs conversion price (refdata) to compute parity"; }
}
function hnChanged(el){
  const tr=el.closest("tr"); const sid=Number(tr.dataset.id); HN[sid]=HN[sid]||{};
  const raw=el.value.trim(); const n=hnNum(raw);
  if(raw && !isFinite(n)){ el.classList.add("cn-bad"); el.title="not a number: "+raw; return; }
  el.classList.remove("cn-bad");
  if(raw===""){ delete HN[sid][el.dataset.hn]; } else { HN[sid][el.dataset.hn]=String(n); el.value=String(n); }
  el.defaultValue=el.value;
  hnPaintRow(tr);
  if(raw==="" && (el.dataset.hn==="cstk"||el.dataset.hn==="cfx")){ const lv=hnLive(tr,el.dataset.hn); el.value=isFinite(lv)?(el.dataset.hn==="cfx"?lv.toFixed(4):String(lv)):""; el.defaultValue=el.value; el.classList.add("hn-live"); }   // cleared -> show live at once, even while focused
}
function hnPaintAll(){ document.querySelectorAll("#tbl tbody tr[data-id]").forEach(hnPaintRow); }
function cnPaintAll(){ document.querySelectorAll("#tbl tbody tr[data-id]").forEach(tr=>{ cnPaintTicker(tr); const sid=Number(tr.dataset.id); const v=CN[sid]||{}; tr.querySelectorAll("input[data-cn]").forEach(i=>{ if(document.activeElement!==i) i.value=v[i.dataset.cn]||""; }); }); hnPaintAll(); }
const DC = {};   // session-only inputs per secId (blank at launch)
function dcChg(el){
  const sid = Number(el.dataset.id);
  DC[sid] = DC[sid] || {};
  DC[sid][el.dataset.dc] = el.value.trim();
  DC[sid].mode = (el.dataset.dc==="usd") ? "usd" : "nd";          // the last-edited side drives
  updDeltaCalc(el.closest("tr"));
}
function updDeltaCalc(tr){
  if(!tr) return;
  const sid = Number(tr.dataset.id);
  const nI = tr.querySelector('input[data-dc="notl"]');
  const dI = tr.querySelector('input[data-dc="delta"]');
  const uI = tr.querySelector('input[data-dc="usd"]');
  if(!nI || !dI) return;
  const mem = DC[sid] || {};
  if(nI.value==="" && mem.notl) nI.value = mem.notl;
  if(dI.value==="" && mem.delta) dI.value = mem.delta;
  if(uI && uI.value==="" && mem.mode==="usd" && mem.usd) uI.value = mem.usd;
  const notl = parseFloat(String(nI.value).replace(/,/g,""));
  const dl = parseFloat(dI.value) / 100;
  const par = parseFloat((tr.querySelector('td[data-c="parityPct"]')||{}).textContent) / 100;
  const sp = parseFloat((tr.querySelector('input[data-f="ovdSpot"]')||{}).value);
  const cb = parseFloat((tr.querySelector('input[data-f="ovdCbFx"]')||{}).value);
  const uf = parseFloat((tr.querySelector('input[data-f="ovdUndFx"]')||{}).value);
  const shT = tr.querySelector('td[data-c="dc_shares"]');
  const fmt = v => isFinite(v) ? Math.round(v).toLocaleString("en-US") : "\u2014";
  const typing = (document.activeElement === uI);
  if(mem.mode === "usd"){
    // USD delta typed -> Eq Shares = USD x ovdUndFx / ovdSpot (same convention as base x (undFx/cbFx) / spot)
    const usd = parseFloat(String(uI ? uI.value : "").replace(/,/g,""));
    if(uI) uI.classList.remove("derived");
    if(shT) shT.textContent = (isFinite(usd) && isFinite(sp) && sp>0 && isFinite(uf) && uf>0) ? fmt(usd*uf/sp) : "\u2014";
  } else {
    const base = notl * dl * par;
    const ok = isFinite(notl) && isFinite(dl) && isFinite(par);
    const usd = (ok && isFinite(cb) && cb>0) ? base/cb : NaN;
    if(shT) shT.textContent = (ok && isFinite(sp) && sp>0 && isFinite(uf) && isFinite(cb) && cb>0) ? fmt(base*(uf/cb)/sp) : "\u2014";
    if(uI && !typing){ uI.value = isFinite(usd) ? String(Math.round(usd)) : ""; uI.classList.add("derived"); }   // shown as derived (grey italic) until typed over
  }
  idbPaintRow(tr);
}
function updParityAll(){
  document.querySelectorAll("#tbl tr[data-id]").forEach(updParity);
}
document.getElementById("tbl").addEventListener("input", (e)=>{
  if(e.target && e.target.dataset && e.target.dataset.u === "bond_type"){
    const tr = e.target.closest("tr");
    if(tr) updParity(tr);
  }
  if(e.target && e.target.dataset && e.target.dataset.f === "ovdSpot"){
    const tr = e.target.closest("tr"); if(tr) updParity(tr);
  }
});

function uVal(tr, u){
  const i = tr.querySelector(`input[data-u="${u}"]`);
  return i ? i.value.trim() : "";
}

function paintRefRow(tr, ref){
  tr.querySelectorAll("td[data-r]").forEach(td=>{
    let v;
    if(td.dataset.r === "isin"){
      const sp=td.querySelector(".isin-db"), inp=td.querySelector("input.isinv"); const db=String(ref.isin||"").trim();
      if(sp){ sp.textContent = db; sp.style.display = db ? "" : "none"; }
      if(inp){ inp.style.display = db ? "none" : ""; if(!db){ const sid=Number(tr.dataset.id); const mv=((NS.rows||{})[sid]||{}).isin_manual; if(mv!==undefined && document.activeElement!==inp) inp.value=mv||""; } }
      td.title = db ? "ISIN from the database" : (inp&&inp.value ? "manual ISIN (database has none)" : "no ISIN in the database - type it here");
      return;
    }
    if(td.dataset.r === "lp_delta"){
      const n = parseFloat(ref.lp_delta);
      td.textContent = isFinite(n) ? (n * 100).toFixed(1) + "%" : "\u2014";
      return;
    }
    if(td.dataset.r === "credit_spread_used"){
      const n = parseFloat(ref.credit_spread_used);
      td.textContent = isFinite(n) ? (n * 10000).toFixed(0) : "\\u2014";
      return;
    }
    if(td.dataset.r === "quantity_live" || td.dataset.r === "usd_qty_live"){
      const q = fmt0(ref[td.dataset.r]);
      v = q === "" ? "\\u2014" : q;
    } else {
      v = ref[td.dataset.r] || "\\u2014";
    }
    td.textContent = v;
    if(td.dataset.r==="company_name") td.title = v;
  });
}

async function loadRefData(ids){
  const need = ids.filter(id=>!(id in refCache));
  if(need.length){
    try{
      const resp = await fetch(location.pathname.replace(/[/]+$/, "") + "/api/refdata",{method:"POST",
        headers:{"Content-Type":"application/json"},
        body: JSON.stringify({sec_ids: need})});
      const data = await resp.json();
      for(const r of data.rows) refCache[r.secId] = r;
      for(const id of need) if(!(id in refCache))
        refCache[id] = {company_name:"", expiry_date:"", isin:"", ric:"",
                        short_name:"", und_fx:"", quantity_live:"",
                        sec_fx:"", usd_qty_live:""};
      if(data.error) setStatus(`<span class="warn">${data.error}</span>`);
    }catch(err){
      setStatus(`<span class="warn">Reference lookup failed: ${err}</span>`);
    }
  }
  for(const id of ids){
    const tr = document.querySelector(`#tbl tr[data-id="${id}"]`);
    const ref = refCache[id] || {};
    if(!tr) continue;
    paintRefRow(tr, ref);
    const sn = tr.querySelector('input[data-u="short_name"]');
    if(sn && !sn.value) sn.value = ref.short_name || "";
    const uf = tr.querySelector('input[data-u="und_fx"]');
    if(uf && !uf.value) uf.value = ref.und_fx || "";
  }
}

/* ---------------- Refinitiv (server-pushed) ---------------- */
const CBFX_INVERT = new Set(["EUR","GBP","AUD","NZD"]);   // Refinitiv quotes these as CCY/USD; the band shows USD/CCY
function cbFxCcyOf(tr){
  const sid = Number(tr.dataset.id);
  const fromState = (NS.cbFxCcy||{})[sid];
  if(fromState !== undefined) return String(fromState||"").toUpperCase();
  return String(((refCache[sid]||{}).sec_fx)||"").trim().toUpperCase();
}
function cbFxVal(tr, x){ const v = Number(x); if(!isFinite(v) || v===0) return ""; return CBFX_INVERT.has(cbFxCcyOf(tr)) ? 1/v : v; }
function cbFxRicOf(tr){
  const sid = Number(tr.dataset.id);
  const fromState = (NS.cbFxRics||{})[sid];
  if(fromState !== undefined) return fromState || "";
  const ccy = String(((refCache[sid]||{}).sec_fx)||"").trim().toUpperCase();
  return (!ccy || ccy==="USD") ? "" : ccy + "=";
}
const HKT_OFFSET_MIN = 8*60;
function toHKT(timeStr, dateStr){   // GMT 'HH:MM[:SS]' (+ 'YYYY-MM-DD') -> HKT, rolling the date past midnight
  const m=String(timeStr||"").match(/^(\\d{1,2}):(\\d{2})(?::(\\d{2}))?/); if(!m) return {t:String(timeStr||""), d:String(dateStr||"")};
  let mins=(+m[1])*60+(+m[2])+HKT_OFFSET_MIN; let roll=0; if(mins>=1440){ mins-=1440; roll=1; }
  const hh=String(Math.floor(mins/60)).padStart(2,"0"), mm=String(mins%60).padStart(2,"0"), ss=m[3]?":"+m[3]:"";
  let d=String(dateStr||""); if(roll&&/^\\d{4}-\\d{2}-\\d{2}/.test(d)){ const dt=new Date(d.slice(0,10)+"T00:00:00Z"); dt.setUTCDate(dt.getUTCDate()+1); d=dt.toISOString().slice(0,10); }
  return {t:hh+":"+mm+ss, d};
}
function rfxCell(k, rec){ if(k==="last_time") return toHKT(rec.last_time, rec.last_date).t; if(k==="last_date") return toHKT(rec.last_time, rec.last_date).d; return rec[k] ?? ""; }
function applyRfx(){
  const byRic = NS.rfx;
  const ts = document.getElementById("rfxts");
  const _touched = new Set();
  document.querySelectorAll("#tbl tr[data-id]").forEach(tr=>{
    const stkRic = tr.querySelector("td[data-r='ric']").textContent.trim();
    const stk = byRic[stkRic];
    if(stk || byRic[uVal(tr, "und_fx")]) _touched.add(Number(tr.dataset.id));
    tr.querySelectorAll("td[data-rf]").forEach(td=>{
      if(!stk){ td.textContent = ""; return; }
      const k = td.dataset.rf, v = stk[k];
      td.textContent = (k==="last"||k==="close") ? fmt2(v) : rfxCell(k, stk);   // times HKT
    });
    const cfRic = cbFxRicOf(tr);
    const cf = cfRic ? byRic[cfRic] : null;
    tr.querySelectorAll("td[data-cf]").forEach(td=>{
      const k = td.dataset.cf;
      if(!cfRic){ td.textContent = (k==="last"||k==="close") ? "1.0000" : ""; return; }   // USD bond
      if(!cf){ td.textContent = ""; return; }
      td.textContent = (k==="last"||k==="close") ? fmt4(cbFxVal(tr, cf[k])) : rfxCell(k, cf);   // USD/CCY always; times HKT
    });
    const fxRic = uVal(tr, "und_fx");
    const isConst = fxRic !== "" && !isNaN(Number(fxRic));
    const fx = byRic[fxRic];
    tr.querySelectorAll("td[data-fx]").forEach(td=>{
      const k = td.dataset.fx;
      if(isConst){ td.textContent = k==="last"||k==="close" ? fmt4(fxRic) : ""; return; }
      if(!fx){ td.textContent = ""; return; }
      td.textContent = (k==="last"||k==="close") ? fmt4(fx[k]) : rfxCell(k, fx);
    });
  });
  if(ts){
    ts.textContent = NS.rfxErrMsg ? "ERR" : (NS.rfxTs || "");
  if(typeof hnPaintAll==="function") hnPaintAll();
    ts.title = NS.rfxErrMsg || "";
  }
  computeTheoAll();
  doAutoLast();
  updMovesFlags(_touched.size ? _touched : undefined);
}

/* ---------------- theo \u00b7 \u03b3-adj (client-computed) ---------------- */
const T_KEYS = ["m","rolld","dpnl","gpnl","theo","vslive"];
function computeTheoAll(){
  document.querySelectorAll("#tbl tr[data-id]").forEach(tr=>{
    const put = (k,v)=>{ const td = tr.querySelector(`td[data-t="${k}"]`);
      if(td) td.textContent = v; };
    const cls = (k,c,on)=>{ const td = tr.querySelector(`td[data-t="${k}"]`);
      if(td) td.classList.toggle(c, !!on); };
    const blank = ()=>{ T_KEYS.forEach(k=>put(k,""));
      cls("rolld","gwarn",false); cls("theo","ovd-on",false);
      cls("vslive","pos",false); cls("vslive","neg",false); };
    const nk = NS.nuke[Number(tr.dataset.id)];
    if(!nk || nk.nBid==null || nk.nDelta==null || !Number(nk.nSpot)){
      blank(); return; }
    const ovdS = parseFloat(
      (tr.querySelector('input[data-f="ovdSpot"]')||{value:""}).value);
    const stkRic = tr.querySelector('td[data-r="ric"]').textContent.trim();
    const live = NS.rfx[stkRic];
    const liveUnd = live ? Number(live.last) : NaN;
    const undEff = !isNaN(ovdS) ? ovdS
                 : !isNaN(liveUnd) ? liveUnd : Number(nk.liveSpot);
    if(isNaN(undEff) || !undEff){ blank(); return; }
    const g = parseFloat(
      (tr.querySelector('input[data-u="n_gamma"]')||{value:""}).value);
    const hasG = !isNaN(g);
    const nSpot = Number(nk.nSpot), nD = Number(nk.nDelta)*100;
    const m = 100*(undEff - nSpot)/nSpot;
    const roll = nD + (hasG ? g : 0)*m;
    const dpnl = (nD/100)*m;
    const gpnl = hasG ? 0.5*(g/100)*m*m : NaN;
    const theo = Number(nk.nBid) + dpnl + (hasG ? gpnl : 0);
    const lb = nk.liveMktBid;
    const vs = (lb===null||lb===undefined||lb==="") ? NaN : Number(lb) - theo;
    put("m", m.toFixed(2));
    put("rolld", roll.toFixed(1));
    put("dpnl", dpnl.toFixed(2));
    put("gpnl", hasG ? gpnl.toFixed(3) : "\u2014");
    put("theo", theo.toFixed(2));
    put("vslive", isNaN(vs) ? "" : vs.toFixed(2));
    cls("rolld","gwarn", roll<0 || roll>100 || Math.abs(m)>15);
    cls("theo","ovd-on", !isNaN(ovdS));
    cls("vslive","pos", vs>0); cls("vslive","neg", vs<0);
  });
}

/* ---------------- shared state (WebSocket) ---------------- */
function applyState(){ try{
  for(const [sid, row] of Object.entries(NS.rows)){
    const tr = document.querySelector(`#tbl tr[data-id="${sid}"]`);
    if(!tr) continue;
    for(const f of ["short_name","und_fx","n_gamma","isin_manual","iv_manual","or_bid_sprd",
                    "or_ask_sprd","x_bid","x_ask","x_both","vol_flag","bond_type"]){
      const i = tr.querySelector(`[data-u="${f}"]`);
      if(i && document.activeElement !== i){
        let v = row[f] ?? "";
        if(f==="vol_flag" && v==="Expensive") v = "Rich";
        i.value = v;
        if(f==="vol_flag") updVolCls(i);
      }
    }
    for(const f of FIELDS){
      const i = tr.querySelector(`input[data-f="${f}"]`);
      if(i && document.activeElement !== i) i.value = row[f] ?? "";
    }
  }
  computeTheoAll();} finally{ if(typeof updParityAll==='function') updParityAll(); if(typeof ivPaintAll==="function") ivPaintAll(); updMovesFlags(); if(typeof twqPaintAll==="function") twqPaintAll(); if(typeof cnPaintAll==="function") cnPaintAll(); if(typeof hdgPaintAll==="function") hdgPaintAll(); if(typeof lrPaintAll==="function") lrPaintAll(); if(typeof clPaintAll==="function") clPaintAll(); }   // mirrors are stamp-gated: a no-op unless their data changed
}



function applyNuke(){
  if(typeof nukeDone==="function") nukeDone();
  const ids = Object.keys(NS.nuke);
  if(!ids.length) return;
  const data = {rows: Object.values(NS.nuke),
    host: NS.nukeMeta.host || "?", elapsed: NS.nukeMeta.elapsed || 0,
    requested: ids.length, returned: ids.length,
    missing: NS.nukeMeta.missing || [],
    secIds: ids.map(Number), by: NS.nukeMeta.by};
  lastResponse = data;
  document.getElementById("dl").disabled = false;
  render(data, null, true);  updParityAll(); updMovesFlags();
}

function rowPayload(ri, edited){
  const tr = trAt(ri); if(!tr) return null;
  const p = {secId: Number(tr.dataset.id)};
  // names / fx / gamma: only when present in the grid, or when this exact cell was edited (a deliberate clear is flagged)
  const clear = [];
  for(const f of ["short_name","und_fx","n_gamma"]){ const v = uVal(tr, f); if(v !== "") p[f] = v; else if(edited === f){ p[f] = ""; clear.push(f); } }
  if(clear.length) p._clear = clear;
  for(const f of FIELDS){
    const i = tr.querySelector(`input[data-f="${f}"]`);
    p[f] = i ? i.value.trim() : "";
  }
  return p;
}

function syncRows(ris, edited){
  [...new Set(ris)].forEach(ri=>{ const tr=trAt(ri); if(!tr) return;
    const sid=Number(tr.dataset.id); DC[sid]=DC[sid]||{};
    tr.querySelectorAll("input[data-dc]").forEach(i=>{
      DC[sid][i.dataset.dc]=i.value.trim(); });
    updDeltaCalc(tr); });
  const list = [...new Set(ris)].map(ri=>rowPayload(ri, edited)).filter(Boolean);
  if(!list.length) return;
  for(const p of list) NS.rows[p.secId] = Object.assign({}, NS.rows[p.secId] || {}, p);   // merge, keep X / type / borrow fields
  NS.send({type:"rows", list});
  computeTheoAll();
}

const NS = {
  ws: null, tries: 0, lastMsg: 0, refreshSec: 5, refdataSec: 300,
  autosaveSec: 300,
  rfxErrMsg: null,
  rows: {}, nuke: {}, nukeMeta: {}, rfx: {}, rfxTs: null,
  up(){ return this.ws && this.ws.readyState === 1; },
  send(m){ if(this.up()) this.ws.send(JSON.stringify(m)); },
  hello(){ this.send({type:"hello", user: CFG.user}); },
  setConn(cls, title){
    const el = document.getElementById("conn");
    if(el){ el.className = "conn " + cls; el.title = title; }
  },
  connect(){
    const proto = location.protocol === "https:" ? "wss://" : "ws://";
    const base = location.pathname.replace(/[/]+$/, "");
    let ws;
    try{ ws = new WebSocket(proto + location.host + base + "/ws"); }
    catch(e){ this.setConn("err","No WebSocket"); return; }
    this.ws = ws;
    this.setConn("warn","Connecting...");
    ws.onopen = ()=>{ this.tries = 0; this.setConn("ok","Live");
      this.lastMsg = Date.now(); this.hello(); };
    ws.onmessage = (ev)=>{ this.lastMsg = Date.now();
      let m; try{ m = JSON.parse(ev.data); }catch(e){ return; }
      this.onMsg(m); };
    ws.onclose = ()=>{ this.setConn("err","Disconnected - retrying");
      const wait = Math.min(15000, 500 * Math.pow(2, this.tries++));
      setTimeout(()=>this.connect(), wait); };
    ws.onerror = ()=>{ try{ ws.close(); }catch(e){} };
  },
  onMsg(m){
    if(m.type === "cfg"){
      if(String(m.by||"")===String(CFG.user||"")) cfgApply(m.key, m.val);
      return;
    }
    if(m.type === "snapshot"){
      if(m.state && typeof rfxBanner==="function") rfxBanner(m.state.rfxBanner || "");   // banner survives reconnects
      try{
        const uc=((m.state||{}).userCfg||{})[CFG.user]||{};
        Object.entries(uc).forEach(([k,v])=>cfgApply(k,v,true));
      }catch(e){}
      const st = m.state;
      this.rows = st.rows || {}; this.nuke = st.nuke || {};
      this.nukeMeta = st.nukeMeta || {}; this.rfx = st.rfx || {};
      this.rfxTs = st.rfxTs; this.rfxErrMsg = st.rfxErr || null;
      this.cbFxRics = st.cbFxRics || {}; this.cbFxCcy = st.cbFxCcy || {};
      this.refreshSec = st.refreshSec || 5;
      this.refdataSec = st.refdataSec || 300;
      this.autosaveSec = st.autosaveSec ?? 300;
      applyFlagTh(st.flagTh);
      document.getElementById("online").textContent =
        "\u00b7 " + (m.online||1) + " online";
      document.getElementById("ids").value = (st.ids||[]).join(NL);
      buildTable(st.ids || []);
      setStatus(`Connected as <b>${CFG.user}</b> &middot; shared book of ` +
                `${(st.ids||[]).length} securities.`);
    }
    else if(m.type === "ids"){
      this.rows = m.rows || this.rows;
      document.getElementById("ids").value = (m.ids||[]).join(NL);
      buildTable(m.ids || []);
      setStatus(`Book set to ${(m.ids||[]).length} securities by ${m.by}.`);
    }
    else if(m.type === "rows"){
      for(const it of m.list || []){
        this.rows[it.secId] = Object.assign({}, this.rows[it.secId] || {}, it);   // merge: never drop a field
        if("hedged" in it){ const trh=document.querySelector(`#tbl tr[data-id="${it.secId}"]`); if(trh && typeof hdgPaintRow==="function") hdgPaintRow(trh); }
        if("iv_manual" in it){ const tri=document.querySelector(`#tbl tr[data-id="${it.secId}"]`); if(tri && typeof ivPaintRow==="function"){ ivPaintRow(tri); updMovesFlags(new Set([Number(it.secId)])); } }
        const tr = document.querySelector(`#tbl tr[data-id="${it.secId}"]`);
        if(!tr) continue;
        for(const f of ["short_name","und_fx","n_gamma","isin_manual","iv_manual","or_bid_sprd","or_ask_sprd","x_bid","x_ask","x_both","vol_flag","bond_type"]){
          if(!(f in it)) continue;                                   // only fields the message carries
          const i = tr.querySelector(`[data-u="${f}"]`);
          if(i && document.activeElement !== i){ i.value = it[f] ?? ""; if(f==="vol_flag" && typeof updVolCls==="function") updVolCls(i); }
        }
        for(const f of FIELDS){
          const i = tr.querySelector(`input[data-f="${f}"]`);
          if(i && document.activeElement !== i) i.value = it[f] ?? "";
        }
      }
      setStatus(`Edited by ${m.by}.`);
      applyRfx();
    }
    else if(m.type === "nukeStart"){
      document.getElementById("go").disabled = true;
      setStatus(`Nuking ${m.n} securities (by ${m.by})&hellip;`);
    }
    else if(m.type === "nuke"){
      document.getElementById("go").disabled = false;
      for(const r of m.data.rows || [])
        if(r.secId !== undefined) this.nuke[r.secId] = r;
      this.nukeMeta = {host: m.data.host, elapsed: m.data.elapsed,
        by: m.data.by, missing: m.data.missing};
      lastResponse = m.data;
      document.getElementById("dl").disabled = !(m.data.rows||[]).length;
      render(m.data);
      paintRows();
    }
    else if(m.type === "nukeErr"){
      document.getElementById("go").disabled = false;
      setStatus(`<span class="err">${m.error} (by ${m.by})</span>`);
    }
    else if(m.type === "rfx"){
      for(const r of m.rows || []) if(r.ric) this.rfx[r.ric] = r;
      this.rfxTs = m.ts;
      this.rfxErrMsg = null;
      applyRfx();
    }
    else if(m.type === "rfxBanner"){ rfxBanner(m.text || ""); }
    else if(m.type === "rfxErr"){
      this.rfxErrMsg = m.error;
      const b = document.getElementById("rfxts");
      if(b){ b.textContent = "ERR"; b.title = m.error; }
      setStatus(`<span class="warn">${m.error}</span>`);
    }
    else if(m.type === "cfg"){
      if(m.refreshSec !== undefined){
        this.refreshSec = m.refreshSec;
        const el = document.getElementById("cfgSec");
        if(el) el.value = m.refreshSec;
        setStatus(`Server refresh set to ${m.refreshSec}s by ${m.by}.`);
      }
      if(m.flagTh !== undefined){
        applyFlagTh(m.flagTh);
        setStatus(`Flag thresholds set by ${m.by}: RENUKE ` +
          `${m.flagTh.staleSpot}%/${m.flagTh.staleFx}%, MOVE ` +
          `${m.flagTh.moveStk}%/${m.flagTh.moveFx}bp.`);
      }
      if(m.autosaveSec !== undefined){
        this.autosaveSec = m.autosaveSec;
        const el = document.getElementById("cfgAuto");
        if(el) el.value = m.autosaveSec;
      }
      if(m.refdataSec !== undefined){
        this.refdataSec = m.refdataSec;
        const el2 = document.getElementById("cfgRef");
        if(el2) el2.value = m.refdataSec;
        setStatus(`Reference refresh set to ${m.refdataSec}s by ${m.by}.`);
      }
    }
    else if(m.type === "refdata"){
      for(const r of m.rows || []){
        refCache[r.secId] = Object.assign(refCache[r.secId] || {}, r);
        const tr = document.querySelector(`#tbl tr[data-id="${r.secId}"]`);
        if(tr) paintRefRow(tr, refCache[r.secId]);
      }
      applyRfx();       // ric set may have changed; also recomputes theo
      updParityAll(); updMovesFlags();
    }
    else if(m.type === "refErr"){
      setStatus(`<span class="warn">${m.error}</span>`);
    }
    else if(m.type === "online"){
      document.getElementById("online").textContent = "\u00b7 " + m.n + " online";
      if(m.note) setStatus(m.note + ".");
    }
  },
};
setInterval(()=>{ if(NS.up()){ NS.send({type:"ping"});
  if(Date.now() - NS.lastMsg > 45000){ try{ NS.ws.close(); }catch(e){} } }
}, 15000);

/* ---------------- column layout ---------------- */
function applyLayout(){
  const rules = [];
  const wSide = (LAYOUT._side && LAYOUT._side.w) || SIDE_W_DEF;
  rules.push(`.layout{grid-template-columns:${SIDE_HID?SIDE_RAIL:wSide}px 1fr}`);
  const wSec = (LAYOUT.secId && LAYOUT.secId.w) || DEF_W.secId;
  const wCo  = (LAYOUT.company && LAYOUT.company.w) || DEF_W.company;
  rules.push(`#tbl .stick1{min-width:${wSec}px;max-width:${wSec}px}`);
  rules.push(`#tbl .stick2{left:${28 + wSec}px;min-width:${wCo}px;max-width:${wCo}px}`);
  rules.push(`#tbl .stick3{left:${28 + wSec + wCo}px}`);
  for(const key of COL_KEYS){
    const st = LAYOUT[key] || {};
    const w = st.w || DEF_W[key];
    const i = colIdx(key);
    const sel = `#tbl tr[data-id] td:nth-child(${i})`;
    const selH = `#tbl tr.band ~ tr th:nth-child(${i})`;
    if(w) rules.push(`${sel},${selH}{min-width:${w}px;max-width:${w}px}`,
                     `${sel}{overflow:hidden;text-overflow:ellipsis}`);
    const decl = [];
    if(st.bg) decl.push(`background:${st.bg}!important`);
    if(st.fg) decl.push(`color:${st.fg}`);
    if(st.b)  decl.push(`font-weight:600`);
    if(decl.length){
      rules.push(`${sel}{${decl.join(";")}}`);
      const inner = [];
      if(st.fg) inner.push(`color:${st.fg}`);
      if(st.b)  inner.push(`font-weight:600`);
      if(st.bg) inner.push(`background:transparent`);
      if(inner.length) rules.push(`${sel} input{${inner.join(";")}}`);
    }
  }
  document.getElementById("colstyle").textContent = rules.join("\\n");
  positionHandle();
}

function wireResizers(){
  document.querySelectorAll("#tbl .rz").forEach(rz=>{
    rz.addEventListener("mousedown",(e)=>{
      e.preventDefault(); e.stopPropagation();
      const key = rz.dataset.key;
      const th = rz.closest("th");
      const startX = e.clientX;
      const startW = th.getBoundingClientRect().width;
      const move = ev=>{
        const w = Math.max(40, Math.round(startW + ev.clientX - startX));
        LAYOUT[key] = Object.assign({}, LAYOUT[key], {w});
        applyLayout();
      };
      const up = ()=>{
        document.removeEventListener("mousemove", move);
        document.removeEventListener("mouseup", up);
        setStatus(`Width of ${key} set &mdash; Save layout to keep.`);
        syncColForm();
      };
      document.addEventListener("mousemove", move);
      document.addEventListener("mouseup", up);
    });
  });
}

function sideToggle(){
  SIDE_HID = !SIDE_HID;
  localStorage.setItem("nukestation.sidehide", SIDE_HID?"1":"0");
  sidePaint(); applyLayout();
}
function sidePaint(){
  const a = document.querySelector("#viewMain aside.side");
  if(a) a.classList.toggle("collapsed", SIDE_HID);
  const b = document.getElementById("sidecol");
  if(b){ b.innerHTML = SIDE_HID ? "&#9656;" : "&#9666;";
    b.title = (SIDE_HID?"expand":"collapse")+" security IDs panel"; }
}

function wireSideResizer(){
  const rz = document.getElementById("siderz");
  if(!rz) return;
  rz.addEventListener("mousedown",(e)=>{
    e.preventDefault(); e.stopPropagation();
    const startX = e.clientX;
    const startW = (LAYOUT._side && LAYOUT._side.w) || SIDE_W_DEF;
    const move = ev=>{
      const w = Math.max(70, Math.min(480,
        Math.round(startW + ev.clientX - startX)));
      LAYOUT._side = Object.assign({}, LAYOUT._side, {w});
      applyLayout();
    };
    const up = ()=>{
      document.removeEventListener("mousemove", move);
      document.removeEventListener("mouseup", up);
      setStatus("Sidebar width set &mdash; Save layout to keep.");
    };
    document.addEventListener("mousemove", move);
    document.addEventListener("mouseup", up);
  });
  rz.addEventListener("dblclick", ()=>{
    delete LAYOUT._side;
    applyLayout();
    setStatus("Sidebar width reset &mdash; Save layout to keep.");
  });
}

function saveLayout(){
  localStorage.setItem("nukestation.layout", JSON.stringify(LAYOUT));
  setStatus(`<span class="ok">Layout saved</span> &middot; ` +
            `${Object.keys(LAYOUT).length} column(s) customised.`);
}
function resetLayout(){
  for(const k of Object.keys(LAYOUT)) delete LAYOUT[k];
  localStorage.removeItem("nukestation.layout");
  applyLayout(); syncColForm();
  setStatus("Layout reset to defaults.");
}
function clearColumn(){
  const key = document.getElementById("lcol").value;
  delete LAYOUT[key];
  applyLayout(); syncColForm();
}
function clearColField(f){
  const key = document.getElementById("lcol").value;
  if(LAYOUT[key]){ delete LAYOUT[key][f];
    if(!Object.keys(LAYOUT[key]).length) delete LAYOUT[key]; }
  applyLayout(); syncColForm();
}
function loadColForm(){ syncColForm(); }
function syncColForm(){
  const key = document.getElementById("lcol").value;
  const st = LAYOUT[key] || {};
  document.getElementById("lw").value = st.w || DEF_W[key] || "";
  document.getElementById("lfg").value = st.fg || "#16181d";
  document.getElementById("lbg").value = st.bg || "#ffffff";
  document.getElementById("lb").checked = !!st.b;
}
function colFormChanged(which){
  const key = document.getElementById("lcol").value;
  const st = LAYOUT[key] = Object.assign({}, LAYOUT[key]);
  const w = parseInt(document.getElementById("lw").value);
  if(w) st.w = w; else delete st.w;
  if(which==="fg" || st.fg) st.fg = document.getElementById("lfg").value;
  if(which==="bg" || st.bg) st.bg = document.getElementById("lbg").value;
  st.b = document.getElementById("lb").checked;
  if(!st.b) delete st.b;
  applyLayout();
}
function initColPicker(){
  const sel = document.getElementById("lcol");
  sel.innerHTML = COL_DEFS.map(([k,label])=>
    `<option value="${k}">${label} (${k})</option>`).join("");
  syncColForm();
}

/* ---------------- runs ---------------- */
const RUN_DEFS = [["short_name","Short Name"],["isin","ISIN"],
  ["override_bid","Bid"],["override_ask","Ask"],["indic_ask","Indic Ask"],
  ["ovdSpot","Vs"],["ovdUndFx","Fx"],["vs_usd","Vs USD"],
  ["nDelta%","Delta"],
  ["quantity_live","Quantity"]];
const RUN_COLS = RUN_DEFS.map(d=>d[0]);
const RUN_LABEL = Object.fromEntries(RUN_DEFS);
let runsData = [];

function generateRuns(){
  if(!rowSel.size){
    setStatus('<span class="warn">No rows selected &mdash; tick rows first, ' +
              'no runs generated.</span>');
    return;
  }
  const rows = [...rowSel].sort((a,b)=>a-b);
  runsData = [];
  let noBid = 0;
  for(const ri of rows){
    const tr = trAt(ri); if(!tr) continue;
    const sid = Number(tr.dataset.id);
    const tv = sel => { const td = tr.querySelector(sel);
      return td ? td.textContent.trim() : ""; };
    const iv = f => { const i = tr.querySelector(`input[data-f="${f}"]`);
      return i ? i.value.trim() : ""; };
    const _x = k => { const i = tr.querySelector(`input[data-u="${k}"]`);
      const n = parseFloat(i ? i.value : ""); return isFinite(n) ? n : 0; };
    const _q = v => { const n = parseFloat(v);
      return isFinite(n) ? (n + _x("x_both")).toFixed(2) : ""; };
    const _b0 = tv('td[data-c="ovdMktBid"]');
    const bid = _b0 === "" ? ""
      : (parseFloat(_b0) + _x("x_bid") + _x("x_both")).toFixed(2);
    if(bid === "") noBid++;
    const qty = fmt0((refCache[sid]||{}).quantity_live);
    const _a0 = qty === "0" ? "" : tv('td[data-c="ovdMktAsk"]');
    const ask = _a0 === "" ? ""
      : (parseFloat(_a0) + _x("x_ask") + _x("x_both")).toFixed(2);
    const nsprd = parseFloat(tv('td[data-c="nSpread"]'));
    runsData.push({
      short_name: uVal(tr, "short_name") || (refCache[sid]||{}).company_name || String(sid),
      isin: (refCache[sid]||{}).isin || ((tr.querySelector('input.isinv')||{}).value||"") || tv('td[data-r="isin"] .isin-db'),
      override_bid: bid,
      override_ask: ask,
      indic_ask: ask !== "" ? ask
               : (bid !== "" ? (parseFloat(bid) +
                   (isFinite(nsprd) ? nsprd : 1)).toFixed(2) : ""),
      ovdSpot: iv("ovdSpot"),
      ovdUndFx: iv("ovdUndFx"),
      vs_usd: (()=>{ const v=parseFloat(String(iv("ovdSpot")).split(",").join(""));
        const f=parseFloat(String(iv("ovdUndFx")).split(",").join(""));
        return (isFinite(v)&&isFinite(f)&&f!==0)?(v/f).toFixed(2):""; })(),
      "nDelta%": tv('td[data-c="nDeltaPct"]').replace("%",""),
      quantity_live: qty,
    });
  }
  renderRuns();
  showTab("runs");
  const note = noBid
    ? `${runsData.length} run(s) &middot; <span class="warn">${noBid} without ` +
      `override bid/ask &mdash; nuke with overrides first</span>`
    : `${runsData.length} run(s) generated.`;
  document.getElementById("runstatus").innerHTML = note;
}

function renderRuns(){
  const t = document.getElementById("runstbl");
  let h = "<tr>" + RUN_COLS.map((c,i)=>
    `<th${i<2?' class="ref"':''}>${RUN_LABEL[c]}</th>`).join("") + "</tr>";
  for(const r of runsData){
    h += "<tr>" + RUN_COLS.map((c,i)=>
      `<td${i<2?' class="ref"':''}>${r[c] ?? ""}</td>`).join("") + "</tr>";
  }
  t.innerHTML = h;
}

function runsAsTsv(){
  const lines = [RUN_COLS.map(c=>RUN_LABEL[c]).join("\\t")];
  for(const r of runsData)
    lines.push(RUN_COLS.map(c=>r[c] ?? "").join("\\t"));
  return lines.join("\\n");
}

function runsAsHtml(){
  const isText = c => c==="short_name" || c==="isin";
  const esc = v => String(v ?? "").replace(/&/g,"&amp;")
    .replace(/</g,"&lt;").replace(/>/g,"&gt;");
  const cellCss = c =>
    `border:1px solid #999;padding:4px 9px;text-align:${isText(c)?"left":"right"};` +
    `font-family:'Segoe UI',Arial,sans-serif;font-size:13px;`;
  const head = RUN_COLS.map(c =>
    `<th style="${cellCss(c)}font-weight:600;background:#f2f2f2;">` +
    `${esc(RUN_LABEL[c])}</th>`
  ).join("");
  const body = runsData.map(r =>
    "<tr>" + RUN_COLS.map(c =>
      `<td style="${cellCss(c)}">${esc(r[c])}</td>`).join("") + "</tr>"
  ).join("");
  return `<table style="border-collapse:collapse;">` +
         `<thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

async function copyText(text){
  if(navigator.clipboard && navigator.clipboard.writeText){
    try{ await navigator.clipboard.writeText(text); return true; }catch(e){}
  }
  const ta = document.createElement("textarea");
  ta.style.position = "fixed"; ta.style.left = "-9999px"; ta.style.top = "0";
  ta.value = text;
  document.body.appendChild(ta);
  ta.focus(); ta.select();
  let ok = false;
  try{ ok = document.execCommand("copy"); }catch(e){}
  ta.remove();
  return ok;
}

async function copyRich(html, text){
  if(navigator.clipboard && navigator.clipboard.write && window.ClipboardItem){
    try{
      await navigator.clipboard.write([new ClipboardItem({
        "text/html":  new Blob([html], {type: "text/html"}),
        "text/plain": new Blob([text], {type: "text/plain"}),
      })]);
      return true;
    }catch(e){}
  }
  // legacy path (works on http:// origins): select an off-screen node
  // containing the table and let the browser copy it with both flavors
  const host = document.createElement("div");
  host.style.position = "fixed"; host.style.left = "-9999px"; host.style.top = "0";
  host.setAttribute("contenteditable", "true");
  host.innerHTML = html;
  document.body.appendChild(host);
  let ok = false;
  try{
    const range = document.createRange();
    range.selectNodeContents(host);
    const sel = window.getSelection();
    sel.removeAllRanges(); sel.addRange(range);
    ok = document.execCommand("copy");
    sel.removeAllRanges();
  }catch(e){}
  host.remove();
  return ok;
}

async function copyRunsBbg(){
  if(!runsData.length){
    document.getElementById("runstatus").innerHTML =
      '<span class="warn">Nothing to copy &mdash; generate runs first.</span>';
    return;
  }
  const html = runsAsHtml(), tsv = runsAsTsv();
  if(await copyRich(html, tsv)){
    document.getElementById("runstatus").innerHTML =
      "Runs copied as a <b>table</b> (HTML + text) &mdash; paste into Bloomberg " +
      "chat. If IB pastes it flat, route via Excel: paste there, copy, paste to IB.";
  } else if(await copyText(tsv)){
    document.getElementById("runstatus").innerHTML =
      '<span class="warn">Rich clipboard unavailable &mdash; copied TSV; ' +
      'paste into Excel first, then copy from Excel into IB.</span>';
  } else {
    document.getElementById("runstatus").innerHTML =
      '<span class="err">Copy blocked by the browser &mdash; select the ' +
      'table with the mouse and press Ctrl+C.</span>';
  }
}

async function copyRuns(){
  if(!runsData.length){
    document.getElementById("runstatus").innerHTML =
      '<span class="warn">Nothing to copy &mdash; generate runs first.</span>';
    return;
  }
  copyText(runsAsTsv()).then(ok =>
    document.getElementById("runstatus").innerHTML = ok
      ? "Runs copied as TSV &mdash; paste into Excel or chat."
      : '<span class="err">Copy blocked by the browser &mdash; select the ' +
        'table with the mouse and press Ctrl+C.</span>');
}


/* ---------------- save to DB ---------------- *//* ---------------- save to DB ---------------- */
function gatherRows(){
  const byId = {};
  if(lastResponse) for(const r of lastResponse.rows) byId[r.secId] = r;
  const rows = [];
  document.querySelectorAll("#tbl tr[data-id]").forEach(tr=>{
    const sid = Number(tr.dataset.id);
    const ref = refCache[sid] || {};
    const api = byId[sid] || {};
    const gv = f => { const i = tr.querySelector(`input[data-f="${f}"]`);
      return i ? sanitizeNum(i.value) : ""; };
    const tv = sel => { const td = tr.querySelector(sel);
      return td ? td.textContent.trim() : ""; };
    rows.push({
      sec_id: sid,
      short_name: uVal(tr,"short_name"),
      company_name: ref.company_name || "",
      ric: ref.ric || "",
      expiry_date: ref.expiry_date || "",
      isin: ref.isin || "",
      und_fx: uVal(tr,"und_fx"),
      ovd_spot: gv("ovdSpot"), ovd_cbfx: gv("ovdCbFx"), ovd_undfx: gv("ovdUndFx"),

      n_bid: api.nBid, n_delta: api.nDelta, n_spread: api.nSpread,
      n_spot: api.nSpot, n_spotfx: api.nSpotFx,
      live_bid: api.liveMktBid, live_ask: api.liveMktAsk, live_spot: api.liveSpot,
      live_cbfx: api.liveCbFx, live_undfx: api.liveUndFx,
      eod_bid: api.eodMktBid, eod_ask: api.eodMktAsk, eod_spot: api.eodSpot,
      eod_cbfx: api.eodCbFx, eod_undfx: api.eodUndFx,
      ovd_bid: api.ovdMktBid, ovd_ask: api.ovdMktAsk,
      stk_last: tv("td[data-rf='last']"), stk_time: tv("td[data-rf='last_time']"),
      stk_date: tv("td[data-rf='last_date']"),
      stk_close: tv("td[data-rf='close']"), stk_close_date: tv("td[data-rf='close_date']"),
      fx_last: tv("td[data-fx='last']"), fx_time: tv("td[data-fx='last_time']"),
      fx_date: tv("td[data-fx='last_date']"),
      fx_close: tv("td[data-fx='close']"), fx_close_date: tv("td[data-fx='close_date']"),
    });
  });
  return rows;
}

async function saveToDb(){
  const rows = gatherRows();
  if(!rows.length){ setStatus('<span class="err">Nothing to save.</span>'); return; }
  setStatus("Saving to cba_app.cb_nuke&hellip;");
  try{
    const resp = await fetch(location.pathname.replace(/[/]+$/, "") + "/api/save",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify({rows, user: CFG.user})});
    const data = await resp.json();
    if(!resp.ok){ setStatus(`<span class="err">${data.error||resp.status}</span>`); return; }
    setStatus(`<span class="ok">Saved ${data.saved} row(s)</span> &middot; ${data.table}`);
  }catch(err){
    setStatus(`<span class="err">Save failed: ${err}</span>`);
  }
}

/* ---------------- actions ---------------- */
function clearOverrides(){
  const rows = targetRows();
  for(const ri of rows){
    const tr = trAt(ri); if(!tr) continue;
    tr.querySelectorAll("input[data-f]").forEach(i=>i.value="");
  }
  syncRows(rows);
  rows.forEach(ri=>{ const tr=trAt(ri); if(tr) autoNukeQueue(Number(tr.dataset.id)); });
  setStatus(rowSel.size ? `Cleared overrides on ${rows.length} selected row(s).`
                        : "Cleared all overrides.");
}

function setStatus(html){ document.getElementById("status").innerHTML = html; }

function nuke(){
  if(!NS.up()){
    setStatus('<span class="err">Not connected &mdash; reconnecting, try again.</span>');
    return;
  }
  const rows = rowSel.size ? [...rowSel].sort((a,b)=>a-b)
                           : [...Array(nRows()).keys()];
  if(!rows.length){ setStatus('<span class="err">Load security IDs first.</span>'); return; }
  syncRows(rows);
  const secIds = rows.map(ri => Number(trAt(ri).dataset.id));
  document.getElementById("go").disabled = true;
  NS._nukeInFlight=true;                 // manual has priority: send now,
  NS._nukeSafety=setTimeout(nukeDone, 8000);   // autos coalesce behind it
  NS.send({type:"nuke", secIds});
  setStatus(`Requested nuke for ${secIds.length} securities&hellip;`);
}

function render(data, _unused, quiet){
  const scope = new Set((data.secIds || data.rows.map(r=>r.secId)).map(Number));
  document.querySelectorAll("#tbl tr[data-id]").forEach(tr=>{
    if(scope.size && !scope.has(Number(tr.dataset.id))) return;
    tr.querySelectorAll("td[data-c]").forEach(td=>{
      if(td.dataset.c === "parityPct" || td.dataset.c === "be_move" || td.dataset.c === "ov_iv" || (td.dataset.c||"").startsWith("lr_") || (td.dataset.c||"").startsWith("cl_") || (td.dataset.c||"").startsWith("twq_") || (td.dataset.c||"").startsWith("dc_") || (td.dataset.c||"").startsWith("idb_") || (td.dataset.c||"").startsWith("cn_") || (td.dataset.c||"").startsWith("hn_")) return;   // client-computed / mirrored, not ours
      td.textContent = ""; td.classList.remove("ovd-on","pos","neg");
    });
  });
  for(const r of data.rows){
    const tr = document.querySelector(`#tbl tr[data-id="${r.secId}"]`);
    if(!tr) continue;
    const applied = (r.ovdSpot||r.ovdCbFx||r.ovdUndFx) ? true : false;
    const dv = applied ? r.ovdMktBid - r.liveMktBid : null;
    const vals = {
      nBid: fmt(r.nBid), nDeltaPct: (v=>v===""?"":v+"%")(fmt(r.nDelta*100,1)), nSpread: fmt(r.nSpread),
      nSpot: fmt(r.nSpot), nSpotFx: fmt(r.nSpotFx),
      liveMktBid: fmtBA(r.liveMktBid), liveMktAsk: fmtBA(r.liveMktAsk),
      liveSpot: fmt(r.liveSpot,2), liveCbFx: fmt(r.liveCbFx), liveUndFx: fmt(r.liveUndFx),
      eodMktBid: fmtBA(r.eodMktBid), eodMktAsk: fmtBA(r.eodMktAsk),
      eodSpot: fmt(r.eodSpot,2), eodCbFx: fmt(r.eodCbFx), eodUndFx: fmt(r.eodUndFx),
      ovdMktBid: applied ? fmtBA(r.ovdMktBid) : "",
      ovdMktAsk: applied ? fmtBA(r.ovdMktAsk) : "",
      dVsLive: dv===null ? "" : fmtBA(dv),
    };
    tr.querySelectorAll("td[data-c]").forEach(td=>{
      const c = td.dataset.c;
      if(c === "parityPct" || c === "be_move" || c === "ov_iv" || c.startsWith("lr_") || c.startsWith("cl_") || c.startsWith("twq_") || c.startsWith("dc_") || c.startsWith("idb_") || c.startsWith("cn_") || c.startsWith("hn_")) return;              // client-computed / mirrored, not ours
      td.textContent = vals[c];
      if(applied && (c==="ovdMktBid"||c==="ovdMktAsk"||c==="dVsLive"))
        td.classList.add("ovd-on");
      if(c==="dVsLive"){
        mvClass(td, (dv===null?NaN:dv), "pts");
      }
    });
  }
  const miss = data.missing.length ?
    ` &middot; <span class="warn">missing: ${data.missing.join(", ")}</span>` : "";
  const empty = !data.rows.length ?
    ' &middot; <span class="warn">empty payload &mdash; no live session or IDs not loaded</span>' : "";
  computeTheoAll();
  if(!quiet)
    setStatus(`<span class="ok">Nuked</span>` +
      (data.by ? ` by ${data.by}` : "") +
      ` &middot; host ${data.host} &middot; ${data.elapsed}s &middot; ` +
      `${data.returned}/${data.requested} returned${miss}${empty}`);
}

function rfxBanner(text){
  let b = document.getElementById("rfxban");
  if(!b){ b = document.createElement("div"); b.id = "rfxban";
    b.style.cssText = "position:sticky;top:0;z-index:50;background:#c62828;color:#fff;font:12px/1.4 Segoe UI,system-ui,sans-serif;padding:6px 12px;font-weight:600";
    document.body.insertBefore(b, document.body.firstChild); }
  b.textContent = text || ""; b.style.display = text ? "block" : "none";
}
function sprdChanged(el){
  const sid = Number(el.dataset.id), f = el.dataset.u, v = el.value.trim();
  if(!sid || !f) return;
  NS.rows[sid] = Object.assign({}, NS.rows[sid], {[f]: v});
  const _msg={secId: sid, [f]: v};
  if(f==="ovdUndFx"||f==="bond_type"){
    const bt=String((f==="bond_type"?v:
      (NS.rows[sid]||{}).bond_type)||"").toLowerCase();
    const uv=f==="ovdUndFx"?v:
      String((NS.rows[sid]||{}).ovdUndFx||"");
    if(bt.startsWith("vanil")&&uv){
      NS.rows[sid].ovdCbFx=uv; _msg.ovdCbFx=uv;
      const tr=el.closest("tr");
      const ci=tr&&tr.querySelector('input[data-u="ovdCbFx"]');
      if(ci) ci.value=uv;
    }
  }
  NS.send({type:"rows", list:[_msg]});
}

function quoteRows(mode){   // 'vs' | 'bid' | 'ask'
  const rows = rowSel.size ? [...rowSel].sort((a,b)=>a-b) : [];
  if(!rows.length){
    setStatus('<span class="err">Select rows first (tick the checkboxes), then quote.</span>');
    return;
  }
  const qesc = v => String(v ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;");
  const lines = [];
  for(const ri of rows){
    const tr = trAt(ri); if(!tr) continue;
    const txt = sel => { const el = tr.querySelector(sel);
                         return el ? el.textContent.trim() : ""; };
    const ivv = f => { const i = tr.querySelector(`input[data-f="${f}"]`);
                       return i ? i.value.trim() : ""; };
    const num = v => { const n = Number(String(v).replace(/[, ]/g, ""));
                       return (v === "" || v == null || isNaN(n)) ? null : n; };
    const name = (tr.querySelector('input[data-u="short_name"]') || {value:""})
                   .value.trim()
                 || txt("td.stick2") || String(tr.dataset.id);
    const uxv = f => { const i = tr.querySelector(`input[data-u="${f}"]`);
                       const n = parseFloat(i ? i.value : "");
                       return isFinite(n) ? n : 0; };
    const _ob = num(txt('td[data-c="ovdMktBid"]'));
    const _oa = num(txt('td[data-c="ovdMktAsk"]'));
    const bid = _ob == null ? null : _ob + uxv("x_bid") + uxv("x_both");
    const ask = _oa == null ? null : _oa + uxv("x_ask") + uxv("x_both");
    const spotN = num(ivv("ovdSpot")) ?? num(txt('td[data-c="nSpot"]'));
    const fxN   = num(ivv("ovdUndFx")) ?? num(txt('td[data-c="nSpotFx"]'));
    const dN    = num(txt('td[data-c="nDeltaPct"]').replace("%",""));
    const usd   = num(txt('td[data-r="usd_qty_live"]')) ?? 0;
    const uvv = f => { const i = tr.querySelector(`input[data-u="${f}"]`);
                       return i ? i.value.trim() : ""; };
    const sb = num(uvv("or_bid_sprd")) ?? 0;
    const sa = num(uvv("or_ask_sprd")) ?? 0;
    const p2 = v => v == null ? "?" : v.toFixed(2);
    const spotS = spotN == null ? "?" : String(+spotN.toFixed(4));
    const fxS   = fxN == null ? "?" : fxN.toFixed(4);
    const dS    = dN == null ? "" : " " + Math.round(dN) + "d";
    let q, priced = true;
    if(mode === "or" || mode === "orbid" || mode === "orask"){
      const ab = bid == null ? null : bid + sb;
      const aa = ask == null ? null : ask + sa;
      if(mode === "orask" && aa == null){ q = name + " no offer"; priced = false; }
      else if(mode === "orask"){
        q = name + " ref " + spotS + " fx " + fxS + " " + p2(aa) + " offer";
      }
      else{
        const core = mode === "or"
            ? (usd === 0 ? p2(ab) + " /"
               : (aa != null ? p2(ab) + " / " + p2(aa) : p2(ab)))
            : p2(ab) + " bid";
        q = name + " " + core + " ref " + spotS + " fx " + fxS;
      }
    }else{
      if(mode === "ask"){
        q = name + " vs " + spotS + " fx " + fxS + dS + " " +
            p2(ask != null ? ask : bid) + " offer";
      } else {
        const core = mode === "vs"
            ? (usd === 0 ? p2(bid) + " /"
               : (ask != null ? p2(bid) + " / " + p2(ask) : p2(bid)))
            : p2(bid) + " bid";
        q = name + " " + core + " vs " + spotS + " fx " + fxS + dS;
      }
    }
    if(priced){
      if(usd === 0)            q += " no offer";
      else if(usd < 1000000)   q += " scrappy " + fmt0(usd);
    }
    lines.push(q.replace(/ +/g, " ").trim());
  }
  const out = lines.join(String.fromCharCode(10));
  copyText(out).then(ok => setStatus(
      (ok ? '<span class="ok">copied:</span> '
          : '<span class="warn">copy blocked - string below:</span> ')
      + lines.map(qesc).join("<br>")));
}

function copyTable(){
  const heads = [...document.querySelectorAll("#tbl tr:nth-child(2) th")]
    .map(th=>th.textContent.trim() || "sel");
  const lines = [heads.join("\\t")];
  document.querySelectorAll("#tbl tr[data-id]").forEach((tr,ri)=>{
    const cells = [rowSel.has(ri) ? "x" : ""];
    tr.querySelectorAll("td").forEach(td=>{
      if(td.classList.contains("stick0")) return;
      const inp = td.querySelector("input");
      cells.push(inp ? inp.value : td.textContent.trim());
    });
    lines.push(cells.join("\\t"));
  });
  copyText(lines.join("\\n")).then(ok =>
    setStatus(ok ? "Table copied as TSV &mdash; paste into Excel."
      : '<span class="err">Copy blocked by the browser &mdash; ' +
        'use Ctrl+C on a selection instead.</span>'));
}

function download(){
  if(!lastResponse) return;
  const rows = lastResponse.rows.map(r=>{
    const tr = document.querySelector(`#tbl tr[data-id="${r.secId}"]`);
    return {
      ...r,
      company_name: (refCache[r.secId]||{}).company_name || "",
      ric:          (refCache[r.secId]||{}).ric          || "",
      short_name:   tr ? uVal(tr,"short_name") : "",
      und_fx:       tr ? uVal(tr,"und_fx")     : "",
      expiry_date:  (refCache[r.secId]||{}).expiry_date  || "",
      isin:         (refCache[r.secId]||{}).isin         || "",
    };
  });
  const blob = new Blob([JSON.stringify(rows,null,2)],{type:"application/json"});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  const ts = new Date().toISOString().replace(/[-:T]/g,"").slice(0,15);
  a.download = `cb_nuked_${ts}.json`;
  a.click();
}

initColPicker();
wireSideResizer();
sidePaint();
document.getElementById("ids").value = DEFAULT_IDS.join("\\n");
buildTable();
loadCfgForm();
NS.connect();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return (PAGE.replace("__DEFAULT_IDS__", json.dumps(STATE["ids"]))
                .replace("__IDS_SRC__", "shared"))


def preflight_port_check(port: int = None) -> None:
    """Fail fast with clear remediation if the port is already taken,
    BEFORE uvicorn's noisy startup/shutdown cycle and the browser open."""
    import socket as _socket
    port = PORT if port is None else port
    probe = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    try:
        probe.bind((HOST, port))
        probe.close()
        return
    except OSError as exc:
        probe.close()
        hint = "Another application is using this port."
        try:
            import urllib.request
            page = urllib.request.urlopen(
                f"http://127.0.0.1:{port}/", timeout=2).read(4000)
            if b"cb nuke station" in page.lower():
                hint = ("A CB Nuke Station instance is ALREADY serving on this "
                        "port -- probably an earlier session that never exited. "
                        "Your browser may be showing that OLD version.")
        except Exception:
            hint = ("Another process holds the port but does not answer HTTP "
                    "(could be a stuck/half-dead process).")
        logger.error("Cannot start: port %s is already in use (%s)", port, exc)
        logger.error(hint)
        logger.error("Find it:   netstat -ano | findstr :%s", port)
        logger.error("Kill it:   taskkill /PID <pid-from-netstat> /F")
        logger.error("Or run this instance on another port:  set APP_PORT=59998")
        if port >= 49152:
            logger.error(
                "NOTE: port %s is inside Windows' dynamic range (49152-65535), "
                "so ANY app can grab it as an ephemeral port at random.", port)
            logger.error(
                "Permanent fix (admin, while the port is free):  netsh int ipv4 "
                "add excludedportrange protocol=tcp startport=%s "
                "numberofports=1", port)
        raise SystemExit(1)


def rfx_diag(rics):
    """`python app.py --rfx-diag 6886.HK 3711.TW HKD= TWD=`
    Names the failing stage of the Refinitiv path without starting the
    server: session type/state, mixed get_data (what the poller calls),
    pricing pipe only, pricing snapshot endpoint, Datagrid closes."""
    import time as _t
    rics = rics or ["6886.HK", "3711.TW", "HKD=", "TWD="]
    import refinitiv.data as rd
    cfg = "refinitiv-data.config.json"
    print("cwd:", os.getcwd(), "| config json:",
          "FOUND" if os.path.exists(cfg) else "MISSING (default desktop session)")
    t0 = _t.time()
    try:
        rd.open_session(config_name=cfg) if os.path.exists(cfg) else rd.open_session()
    except TypeError:
        rd.open_session()
    s = rd.session.get_default()
    print("session:", type(s).__name__, "| state:", getattr(s, "open_state", "?"),
          "| opened in %.1fs" % (_t.time() - t0))

    def show(label, fn):
        try:
            df = fn()
            if df is None or df.empty:
                print(f"{label}: EMPTY  (shape={None if df is None else df.shape})")
            else:
                print(f"{label}: OK  {df.shape}")
                print(df.head(len(rics)).to_string())
        except Exception as e:
            print(f"{label}: ERROR {type(e).__name__}: {str(e)[:300]}")
    show("1) mixed get_data CF_*+TR.*  (what the poller calls)",
         lambda: rd.get_data(universe=rics, fields=REFINITIV_FIELDS))
    show("2) pricing pipe only CF_*",
         lambda: rd.get_data(universe=rics, fields=["CF_LAST", "CF_TIME", "CF_DATE", "CF_CLOSE"]))
    show("3) pricing snapshot endpoint",
         lambda: rd.content.pricing.Definition(universe=rics, fields=["CF_LAST", "CF_CLOSE"]).get_data().data.df)
    show("4) Datagrid closes TR.PriceClose",
         lambda: rd.get_data(universe=rics, fields=["TR.PriceClose", "TR.PriceClose.date"]))
    print("\nReading: 2 or 3 OK -> the poller will fill rfx. 2+3 EMPTY/502 with state Opened -> "
          "Workspace not serving data: exit it from the tray, check Task Manager, relaunch, sign in. "
          "4 ERROR alone -> Datagrid entitlement (closes then come from CF_CLOSE).")
    try:
        rd.close_session()
    except Exception:
        pass


if __name__ == "__main__":
    if "--rfx-diag" in sys.argv:
        rfx_diag([a for a in sys.argv[sys.argv.index("--rfx-diag") + 1:] if not a.startswith("-")])
        raise SystemExit(0)
    # Convenience launcher: `python app.py`.
    # Port/host come from APP_PORT / APP_HOST (defaults 59999 / 0.0.0.0).
    import webbrowser
    from threading import Timer

    preflight_port_check()
    browser_url = resolve_browser_url()
    share_url = f"http://{SERVER_FQDN}:{PORT}/" if _running_on_server() \
        else f"http://<this-machine>:{PORT}/"
    logger.info("Opening browser at: %s", browser_url)
    logger.info("Share with the desk: %s", share_url)

    def _open_browser() -> None:
        # Shortly after launch so the server has time to bind first.
        try:
            webbrowser.open_new(browser_url)
        except Exception as exc:
            logger.warning("Could not open browser: %s", exc)

    if os.environ.get("NUKE_NO_BROWSER"):
        logger.info("NUKE_NO_BROWSER set - not opening a browser tab "
                    "(started by Lagrange)")
    else:
        Timer(1.5, _open_browser).start()

    if os.environ.get("LAGRANGE_CHILD"):
        # Die with the parent: Lagrange holds our stdin pipe; when the
        # Lagrange process ends FOR ANY REASON the pipe closes, the read
        # returns EOF, and we exit. Works on Windows and POSIX alike.
        import threading as _th

        def _parent_watch():
            try:
                import sys as _sys
                _sys.stdin.buffer.read()
            except Exception:
                pass
            logger.info("Lagrange parent gone - shutting down Nuke Station")
            os._exit(0)

        _th.Thread(target=_parent_watch, daemon=True).start()

    # NOTE: reload must stay OFF (single process) so the in-memory
    # WebSocket hub works, and so the browser only opens once.
    print("=" * 62)
    print("  NUKE STATION  BUILD borrow.b63  \u00b7  %s"
          % os.path.abspath(__file__))
    print("  port %s \u00b7 if this banner is missing, an OLD file is\n  running \u2014 kill that process first." % PORT)
    print("=" * 62)
    uvicorn.run(app, host=HOST, port=PORT, workers=1)
