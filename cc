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
"""

import asyncio
import json
import logging
import os
import sys
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
BW_FIELDS = ("bw_dvb", "bw_dvs", "bw_brw", "bw_lo", "bw_hi",
             "bw_gap", "bw_util", "bw_d5", "bw_htb", "bw_evt",
             "bw_src", "bw_tnr")
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
    query = f"""
        SELECT sec_id, short_name, und_fx
        FROM {APP_DB}.{APP_TABLE}
        WHERE sec_id IN ({ph})
        ORDER BY saved_at DESC, id DESC
    """
    try:
        conn = _db()
    except Exception:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(query, sec_ids)
            rows = cur.fetchall()
    except Exception:
        return {}
    finally:
        conn.close()
    out: Dict[int, Dict[str, str]] = {}
    for sec_id, short_name, und_fx in rows:
        sid = int(sec_id)
        if sid not in out:
            out[sid] = {"short_name": short_name or "", "und_fx": und_fx or ""}
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
USER_FIELDS = ("short_name", "und_fx", "n_gamma",
               "or_bid_sprd", "or_ask_sprd",
               "x_bid", "x_ask", "x_both", "vol_flag", "bond_type",
               "bw_dvb", "bw_dvs", "bw_brw", "bw_lo", "bw_hi",
               "bw_gap", "bw_util", "bw_d5", "bw_htb", "bw_evt",
               "bw_src", "bw_tnr")
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


def _blank_row() -> Dict[str, Any]:
    return {"short_name": "", "und_fx": "", "n_gamma": "",
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


COLMAP = {"short_name": "short_name", "und_fx": "und_fx",
          "n_gamma": "n_gamma", "or_bid_sprd": "or_bid_sprd",
          "or_ask_sprd": "or_ask_sprd", "x_bid": "x_bid", "x_ask": "x_ask",
          "x_both": "x_both", "vol_flag": "vol_flag",
          "bond_type": "bond_type", "ovdSpot": "ovd_spot",
          "ovdCbFx": "ovd_cbfx", "ovdUndFx": "ovd_undfx",
          "bw_dvb": "bw_dvb",
          "bw_dvs": "bw_dvs",
          "bw_brw": "bw_brw",
          "bw_lo": "bw_lo",
          "bw_hi": "bw_hi",
          "bw_gap": "bw_gap",
          "bw_util": "bw_util",
          "bw_d5": "bw_d5",
          "bw_htb": "bw_htb",
          "bw_evt": "bw_evt",
          "bw_src": "bw_src",
          "bw_tnr": "bw_tnr"}


def load_state_rows(cur, into: Dict[int, Dict[str, Any]]) -> int:
    """Restore STATE rows from cb_state using whatever persisted columns
    exist (information_schema), so a missing column can never silently
    drop the extended fields (X, spreads, bond_type, vol_flag, borrow)."""
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
                     (rf.get("isin") or "")[:12] or None,
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


@app.post("/api/borrow/ingest")
async def api_borrow_ingest(request: Request):
    """Feed door for the borrow band. Body: {sec_id: {bw_*: v}}
    e.g. {"60426052": {"bw_brw": "1.35", "bw_src": "F",
    "bw_lo": "1.1", "bw_hi": "1.6"}}. Writes state, persists,
    broadcasts to every open window."""
    try:
        body = await request.json()
    except Exception:
        return {"ok": False, "error": "bad json"}
    changed = []
    for sid_s, kv in (body or {}).items():
        try:
            sid = int(sid_s)
        except (TypeError, ValueError):
            continue
        fields = {k: str(v) for k, v in (kv or {}).items()
                  if k in BW_FIELDS}
        if not fields:
            continue
        row = STATE["rows"].setdefault(sid, _blank_row())
        row.update(fields)
        changed.append(sid)
        await run_in_threadpool(db_upsert_state_fields, sid,
                                row, "borrow-feed",
                                list(fields.keys()))
    if changed:
        await broadcast({"type": "snapshot",
                         "state": snapshot(),
                         "online": len(CLIENTS)})
    return {"ok": True, "rows": len(changed)}


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
                    touched = [f for f in USER_FIELDS + OVD_FIELDS
                               if f in it]
                    for f in touched:
                        row[f] = str(it.get(f) or "")
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
#tbl.hb-brw th[data-band="brw"]:not(.bfirst),#tbl.hb-brw td[data-band="brw"]:not(.bfirst){display:none}
#tbl.hb-brw .bfirst[data-band="brw"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f1ecf6;border-left:1px solid #d8d4cc}
#tbl.hb-brw .bfirst[data-band="brw"] input{display:none}
.bw{background:#efe9f5}
input.bwv{width:52px}
th[data-key^="bw_"],td[data-band="brw"]{width:58px;min-width:58px;max-width:76px}
input.bwv.bwred{background:#fde7e5 !important;border-color:#b3261e !important;color:#8a1c12}
#tbl.hb-res th[data-band="res"]:not(.bfirst),#tbl.hb-res td[data-band="res"]:not(.bfirst){display:none}
#tbl.hb-cbnuke th[data-band="cbnuke"]:not(.bfirst),#tbl.hb-cbnuke td[data-band="cbnuke"]:not(.bfirst){display:none}
#tbl.hb-cbnuke .bfirst[data-band="cbnuke"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-cbnuke .bfirst[data-band="cbnuke"] input{display:none}
.cn{background:#e3eef9;color:#1e3a6e}td[data-band="cbnuke"].cnc{background:#eef3fb;font-weight:600}td[data-band="cbnuke"].cnt{background:#f4f7fb;font-size:9.5px;color:#555;text-align:left}
input.cnv{width:62px;text-align:center}td[data-c="cn_tk"].cn-err{color:#b91c1c}input.cnv.cn-bad{background:#fee2e2;color:#7f1d1d}
input.hnv{width:60px;text-align:right}input.hnv.hn-live{color:#1e3a6e;font-style:italic}input.hnv:not(.hn-live):not(:placeholder-shown){color:#111;font-style:normal}td[data-band="cbnuke"].hnc{background:#dcfce7;font-weight:700}
#tbl.hb-idb th[data-band="idb"]:not(.bfirst),#tbl.hb-idb td[data-band="idb"]:not(.bfirst){display:none}
#tbl.hb-idb .bfirst[data-band="idb"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
.ib{background:#dcd3f0;color:#3b2a6e}td[data-band="idb"].ichk{background:#f6f6f6}
/* IDB quotes: colour by SIDE (blue = bid, orange = offer; colour-blind safe), broker light / mine strong */
:root{--idb-ibBid-bg:#dbeafe;--idb-ibBid-fg:#111827;--idb-myBid-bg:#93c5fd;--idb-myBid-fg:#111827;--idb-ibOfr-bg:#ffedd5;--idb-ibOfr-fg:#111827;--idb-myOfr-bg:#fdba74;--idb-myOfr-fg:#111827}
td[data-band="idb"].ib-bid{background:var(--idb-ibBid-bg);color:var(--idb-ibBid-fg)}
td[data-band="idb"].my-bid{background:var(--idb-myBid-bg);color:var(--idb-myBid-fg);font-weight:700}
td[data-band="idb"].ib-ofr{background:var(--idb-ibOfr-bg);color:var(--idb-ibOfr-fg)}
td[data-band="idb"].my-ofr{background:var(--idb-myOfr-bg);color:var(--idb-myOfr-fg);font-weight:700}
td[data-band="idb"].ib-bidq{background:rgb(239,246,255);color:rgb(107,114,128);font-size:9.5px}
td[data-band="idb"].ib-ofrq{background:rgb(255,247,237);color:rgb(107,114,128);font-size:9.5px}
td[data-band="idb"].ib-refu{background:rgb(229,231,235);color:rgb(55,65,81);font-weight:700;border-left:2px solid rgb(156,163,175)}
td[data-c="idb_flag"]{text-align:left;white-space:nowrap}
.ip{display:inline-block;padding:0 5px;border-radius:9px;font-size:9px;font-weight:700;letter-spacing:.3px;line-height:14px;vertical-align:middle;font-family:'Segoe UI',system-ui,sans-serif}
.ip-x{background:#b91c1c;color:#fff}.ip-g1{background:#fde68a;color:#78350f}.ip-g2{background:#f59e0b;color:#3b1f00}.ip-g3{background:#c2410c;color:#fff}
.ip-grey{background:#e5e7eb;color:#374151}.ip-out{background:transparent;color:#6b7280;border:1px solid #cbd5e1;line-height:12px}.ip-blue{background:transparent;color:#1d4ed8;border:1px solid #93c5fd;line-height:12px}.ip-purple{background:#ede9fe;color:#5b21b6}
#tbl.hb-dcalc th[data-band="dcalc"]:not(.bfirst),#tbl.hb-dcalc td[data-band="dcalc"]:not(.bfirst){display:none}
#tbl.hb-dcalc .bfirst[data-band="dcalc"]{font-size:0;padding:0;width:14px;min-width:14px;max-width:14px;background:#f3f2ef;border-left:1px solid #d8d4cc}
#tbl.hb-dcalc .bfirst[data-band="dcalc"] input{display:none}
input.dcv{width:100%;box-sizing:border-box}
.dc{background:#f6f3ea}
td[data-c="dc_shares"],td[data-c="dc_usd"]{background:#f6f3ea;font-weight:600;text-align:right}
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
  <span class="sub">/GetNukedCBPrice &middot; wlb4 &middot; cbanalytics &middot; eqrms &middot; refinitiv &middot; cba_app &middot; <b style="color:#6b4b8a">borrow.b37</b></span>
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
      <p class="hint">
        short_name, und_fx, nGamma (teal, manual) and overrides (amber) all behave like a spreadsheet:
        drag select, <kbd>Ctrl</kbd>+<kbd>C</kbd>/<kbd>X</kbd>/<kbd>V</kbd>, corner drag-fill,
        <kbd>Ctrl</kbd>+<kbd>D</kbd>, <kbd>Esc</kbd> &middot;
        und_fx takes an Eikon FX RIC (<kbd>TWD=</kbd> <kbd>KRW=</kbd> <kbd>TWDKRW=R</kbd>, <kbd>1</kbd> = USD)
        and drives fx last/time/date/close &middot;
        toolbar buttons fill overrides (Live/EOD = all-or-selected, Last/Close = selected only) &middot;
        click eikon last/close cells to copy one value &middot; bands refresh on click
      </p>
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
              "dc_notl","dc_delta"];
const NCOLS = COLS.length;
const FIELDS = ["ovdSpot","ovdCbFx","ovdUndFx"];
const FSUF = {ovdSpot:"Spot", ovdCbFx:"CbFx", ovdUndFx:"UndFx"};
const zoneOf = c => c===0 ? 0 : (c===1 ? 1 : (c===2 ? 2 :
                       (c<=5 ? 3 : 4)));
const zoneBounds = z => z===0 ? [0,0] : (z===1 ? [1,1] :
                         (z===2 ? [2,2] : (z===3 ? [3,5] : [6,7])));
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
}
idbApplyColors();
const COL_DEFS = [
  ["secId","secId","stick1"],["company","company","ref stick2"],
  ["short_name","short_name","ref stick3"],
  ["bond_type","BOND_TYPE",""],["ric","ric","ref"],
  ["expiry","expiry","ref"],["isin","isin","ref"],["sec_fx","sec_fx","ref"],
  ["und_fx","und_fx","ref"],["quantity_live","qty_live","ref"],
  ["usd_qty_live","usd_qty_live","ref"],
  ["n_bid","nBid","grp"],["n_gamma","nGamma",""],
  ["n_spread","nSpread",""],
  ["n_spot","nSpot",""],["n_spotfx","nSpotFx",""],
  ["n_delta","nDelta%",""],["m_delta","MDelta%",""],
  ["parityPct","PARITY%",""],["cs_used","CSprd",""],
  ["bw_dvb","\u2202V/B","grp"],["bw_dvs","\u2202V/S",""],
  ["bw_brw","BRW",""],["bw_lo","B.LO",""],["bw_hi","B.HI",""],
  ["bw_gap","GAP",""],["bw_util","UTIL",""],["bw_d5","\u03945D",""],
  ["bw_htb","HTB",""],["bw_evt","EVT",""],["bw_src","SRC",""],["bw_tnr","TNR",""],
  ["x_bid","XBid","grp"],
  ["or_bid_sprd","OrBidSprd",""],["ovd_bid","bid",""],
  ["ovd_ask","ask",""],["or_ask_sprd","OrAskSprd",""],
  ["x_ask","XAsk",""],["x_both","X",""],
  ["quote_bid","QuoteBid",""],["quote_ask","QuoteAsk",""],
  ["ovdSpot","ovdSpot",""],["ovdCbFx","ovdCbFx",""],["ovdUndFx","ovdUndFx",""],
  ["be_move","BE move%",""],["stk_move","Stk%",""],["fx_move","FXbps",""],
  ["d_vs","\u0394 vs live bid",""],["mid_drift","MID DRIFT",""],
  ["f_call","CALL","grp"],["f_exp","EXP",""],["f_put","PUT",""],
  ["f_div","DIV",""],["f_move","MOVE",""],["f_nuke","NUKE",""],
  ["f_vol","VOL",""],
  ["v_iv","ImpVol","grp"],["v_10","V10",""],["v_30","V30",""],["v_90","V90",""],
  ["v_n",'VN <input id="volN" size="2" oninput="volNChanged(this)">',""],
  ["v_vega","Vega",""],
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
               ovdSpot:96, ovdCbFx:96, ovdUndFx:96, be_move:72,
               cf_last:78, cf_time:70, cf_date:80, cf_close:78, cf_closedt:80,
               dc_notl:96, dc_delta:64, dc_shares:92, dc_usd:96,
               cn_start:54, cn_end:54, cn_twap:72, cn_vwap:72, cn_vol:84, cn_tk:120,
               hn_delta:56, hn_astk:68, hn_afx:64, hn_abond:68, hn_cstk:68, hn_cfx:64, hn_dn:76,
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
    inp.addEventListener("change", ()=>syncRows([r]));

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
    `<td class="stick2"></td><td class="uin stick3" colspan="2">yours</td><td colspan="3"></td>` +
    `<td></td><td class="uin"></td><td colspan="2"></td>` +
    `<td colspan="9" class="gm grp bandhd" id="band-model" onclick="bandToggle('model')">model (last nuke) &#9662;</td>` +
    `<td colspan="12" class="bw grp bandhd" id="band-brw" onclick="bandToggle('brw')">borrow &#9662;</td>` +
    `<td colspan="17" class="go grp bandhd" id="band-res" onclick="bandToggle('res')">override result &#9662;</td>` +
    `<td colspan="7" class="fb grp bandhd" id="band-flags" onclick="bandToggle('flags')">flags &#9662;</td>` +
    `<td colspan="6" class="vb grp bandhd" id="band-vol" onclick="bandToggle('vol')">vol &#9662;</td>` +
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
      `<td class="ref rowclick" data-r="expiry_date"></td>` +
      `<td class="ref rowclick" data-r="isin"></td>` +
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
      `<td class="gc uinp grp bfirst" data-band="brw"><input class="gridcell bwv" data-u="bw_dvb" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="\u2202V/B \u00b7 value per +100bp borrow bump (engine bump-and-reprice) \u00b7 Bloomberg-only, build first"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_dvs" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="\u2202V/S \u00b7 value per +100bp credit-spread bump (same engine route)"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_brw" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="BRW \u00b7 borrow rate % \u00b7 SRC O=option-implied F=SSF-futures-implied (better where both) P=SecFin quoted W=web M=manual"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_lo" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="B.LO \u00b7 low of the implied-borrow band (route bid/ask)"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_hi" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="B.HI \u00b7 high of the implied-borrow band"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_gap" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="GAP = implied \u2212 SecFin quoted \u00b7 blank without Securities Finance \u00b7 the MPP lie-detector: the only column that says whether the rest of the block is lying"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_util" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="UTIL % \u00b7 TW exact (SBL balance/quota) \u00b7 KR balance/est. float (SEIBRO/KOFIA)"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_d5" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="\u03945D \u00b7 5-day change in balance/util (TW/KR scrape)"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_htb" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="HTB \u00b7 JP JSF \u54c1\u8cb8\u6599\u7387 annualised (\u00f7\u54c1\u8cb8\u65e5\u6570) \u00b7 auction ALARM, not the institutional level \u2014 prime finance for the level \u00b7 KR rate: no public print exists, prime only"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_evt" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="EVT \u00b7 days to next div/AGM record date or index review (CACS date-diff)"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_src" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="SRC \u00b7 O option / F futures / P SecFin / W web / M manual"></td>` +
      `<td class="gc uinp" data-band="brw"><input class="gridcell bwv" data-u="bw_tnr" data-id="${id}" onchange="bwChg(this)" autocomplete="off" placeholder="&#8212;" title="TNR \u00b7 tenor of the borrow quote"></td>` +
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
      ["f_call","f_exp","f_put","f_div","f_move","f_nuke"].map((f,i)=>
        `<td data-fl="${f}" data-band="flags" class="rowclick${i===0?" grp bfirst":""}"></td>`).join("") +
      `<td data-band="flags" class="gc"><select class="volsel"
         data-u="vol_flag" data-id="${id}" onchange="volChanged(this)">
         <option value=""></option><option>Cheap</option>
         <option>Rich</option></select></td>` +
      ["iv","10","30","90","n","vega"].map((n,i)=>
        `<td data-v="${n}" data-band="vol" class="rowclick${i===0?" grp bfirst":""}"
           title="realised vol, annualised (\u221a252), from daily closes"></td>`).join("") +
      `<td class="gc uinp grp bfirst" data-band="dcalc"><input class="gridcell dcv" data-dc="notl" data-id="${id}" data-row="${ri}" data-col="6" oninput="dcChg(this)" inputmode="decimal" autocomplete="off" placeholder="&#8212;" title="bond notional (face) - session only, blank at launch"></td>` +
      `<td class="gc uinp" data-band="dcalc"><input class="gridcell dcv" data-dc="delta" data-id="${id}" data-row="${ri}" data-col="7" oninput="dcChg(this)" inputmode="decimal" autocomplete="off" placeholder="&#8212;" title="delta % (e.g. 56)"></td>` +
      `<td data-c="dc_shares" data-band="dcalc" class="rowclick" title="notional x (ovdUndFx/ovdCbFx) x delta% x PARITY% / ovdSpot"></td>` +
      `<td data-c="dc_usd" data-band="dcalc" class="rowclick" title="notional x delta% x PARITY% / ovdCbFx"></td>` +
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
    .finally(()=>{ applyState(); applyNuke(); applyRfx(); if(typeof hnPaintAll==="function") hnPaintAll(); });   // and again after refdata (never skipped)
}

const BAND_FIRST = new Set(["n_bid","bw_dvb","x_bid","f_call","v_iv","dc_notl","cn_start",idbOrder()[0],
  "live_bid","eod_bid","t_m","stk_last","cf_last","fx_last"]);
const BANDS = {
  model:{label:"model (last nuke)",span:9},
  brw:{label:"borrow",span:12},
  res:{label:"override result",span:17},
  flags:{label:"flags",span:7},
  vol:{label:"vol",span:6,vr:true},
  dcalc:{label:"delta calc",span:4},
  cbnuke:{label:"cb nuke",span:13},
  idb:{label:"idb quotes",span:13},
  live:{label:"live",span:5}, eod:{label:"eod",span:5},
  theo:{label:"theo \u00b7 \u03b3-adj",span:6},
  stk:{label:"stock",span:5,ts:true}, cbfx:{label:"cb fx",span:5,rf:true}, fx:{label:"und fx",span:5,rf:true}};
function bandOf(k){
  if(k.startsWith("bw_")) return "brw";
  if(k.startsWith("dc_")) return "dcalc";
  if(k.startsWith("cn_")||k.startsWith("hn_")) return "cbnuke";
  if(k.startsWith("idb_")) return "idb";
  if(k==="parityPct"||k==="cs_used"||k==="m_delta"||k.startsWith("n_")) return "model";
  if(k==="stk_move"||k==="fx_move"||k==="x_bid"||k==="x_ask"||
     k==="quote_bid"||k==="quote_ask"||k==="mid_drift"||
     k==="x_both") return "res";
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
    td.innerHTML = BANDS[k].label + ts + rf + vr + " &#9662;";
    td.title = "Click to collapse";
  }
}
function bandToggle(k){
  if(k==="idb") idbMirrorRefresh();
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
    _yearsChip(g("f_call"), parseFloat(ref.years_to_call),
      "no years_to_call in lp_model_output");
    let ye = NaN;
    if(ref.expiry_date){
      const d = new Date(ref.expiry_date);
      if(!isNaN(d)) ye = (d - today) / (365.25*24*3600*1000);
    }
    _yearsChip(g("f_exp"), ye, "no expiry_date");
    _yearsChip(g("f_put"), parseFloat(ref.years_to_put),
      "no years_to_put in lp_model_output");
    const dv = g("f_div");
    if(dv){
      dv.classList.remove("fl-red","fl-amb","fl-dim");
      if(ref.next_div_date){
        const dd = new Date(ref.next_div_date);
        const days = (dd - today)/(24*3600*1000);
        if(isFinite(days) && days >= 0){
          dv.textContent = ref.next_div_date.slice(5);
          dv.classList.add(days <= FLAG_TH.divRed ? "fl-red"
                         : days <= FLAG_TH.divAmb ? "fl-amb" : "fl-dim");
          dv.title = "next ex-div " + ref.next_div_date;
        } else { dv.textContent=""; dv.title=""; }
      } else { dv.textContent=""; dv.title="no dividend data"; }
    }
    // break-even daily move from ImpVol (annualised): sigma / sqrt(252)
    const ivTxt = (tr.querySelector('td[data-v="iv"]')||{}).textContent || "";
    const ivPct = parseFloat(String(ivTxt).replace("%",""));
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
      if(ivTd){
        const iv = parseFloat(ref.implied_vol);
        ivTd.textContent = isFinite(iv) ? (iv*100).toFixed(1)+"%" : "";
        ivTd.title = "implied_vol (cbanalytics.lp_model_output)";
      }
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
  let m;
  if(t === "MKT BID > MY OFFER") return `<span class="ip ip-x" title="${esc(t)}">X BID</span>`;
  if(t === "MKT OFFER < MY BID") return `<span class="ip ip-x" title="${esc(t)}">X OFR</span>`;
  if((m = t.match(/^BID GAP ([+-]?[\\d.]+)/))) return `<span class="ip ${shade(num(m[1]))}" title="${esc(t)}">G.B ${num(m[1])>0?"+":""}${num(m[1]).toFixed(2)}</span>`;
  if((m = t.match(/^OFR GAP ([+-]?[\\d.]+)/))) return `<span class="ip ${shade(num(m[1]))}" title="${esc(t)}">G.O ${num(m[1])>0?"+":""}${num(m[1]).toFixed(2)}</span>`;
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
function idbPaintAll(){ document.querySelectorAll("#tbl tbody tr[data-id]").forEach(idbPaintRow); }
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
  updDeltaCalc(el.closest("tr"));
}
function updDeltaCalc(tr){
  if(!tr) return;
  const sid = Number(tr.dataset.id);
  const nI = tr.querySelector('input[data-dc="notl"]');
  const dI = tr.querySelector('input[data-dc="delta"]');
  if(!nI || !dI) return;
  const mem = DC[sid] || {};
  if(nI.value==="" && mem.notl) nI.value = mem.notl;
  if(dI.value==="" && mem.delta) dI.value = mem.delta;
  const notl = parseFloat(String(nI.value).replace(/,/g,""));
  const dl = parseFloat(dI.value) / 100;
  const par = parseFloat((tr.querySelector('td[data-c="parityPct"]')||{}).textContent) / 100;
  const sp = parseFloat((tr.querySelector('input[data-f="ovdSpot"]')||{}).value);
  const cb = parseFloat((tr.querySelector('input[data-f="ovdCbFx"]')||{}).value);
  const uf = parseFloat((tr.querySelector('input[data-f="ovdUndFx"]')||{}).value);
  const shT = tr.querySelector('td[data-c="dc_shares"]');
  const usT = tr.querySelector('td[data-c="dc_usd"]');
  const base = notl * dl * par;
  const fmt = v => isFinite(v) ? Math.round(v).toLocaleString("en-US") : "\u2014";
  const ok = isFinite(notl) && isFinite(dl) && isFinite(par);
  if(shT) shT.textContent = (ok && isFinite(sp) && sp>0 && isFinite(uf)
    && isFinite(cb) && cb>0) ? fmt(base*(uf/cb)/sp) : "\u2014";
  if(usT) usT.textContent = (ok && isFinite(cb) && cb>0) ? fmt(base/cb) : "\u2014";
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
    for(const f of ["short_name","und_fx","n_gamma","or_bid_sprd",
                    "or_ask_sprd","x_bid","x_ask","x_both","vol_flag","bond_type",
                    "bw_dvb","bw_dvs","bw_brw","bw_lo","bw_hi","bw_gap","bw_util","bw_d5","bw_htb","bw_evt","bw_src","bw_tnr"]){
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
  computeTheoAll();} finally{ if(typeof updParityAll==='function') updParityAll(); updMovesFlags(); if(typeof bwFlagAll==="function") bwFlagAll(); if(typeof cnPaintAll==="function") cnPaintAll(); }
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

function rowPayload(ri){
  const tr = trAt(ri); if(!tr) return null;
  const p = {secId: Number(tr.dataset.id)};
  for(const f of ["short_name","und_fx","n_gamma"]) p[f] = uVal(tr, f);
  for(const f of FIELDS){
    const i = tr.querySelector(`input[data-f="${f}"]`);
    p[f] = i ? i.value.trim() : "";
  }
  return p;
}

function syncRows(ris){
  [...new Set(ris)].forEach(ri=>{ const tr=trAt(ri); if(!tr) return;
    const sid=Number(tr.dataset.id); DC[sid]=DC[sid]||{};
    tr.querySelectorAll("input[data-dc]").forEach(i=>{
      DC[sid][i.dataset.dc]=i.value.trim(); });
    updDeltaCalc(tr); });
  const list = [...new Set(ris)].map(rowPayload).filter(Boolean);
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
        const tr = document.querySelector(`#tbl tr[data-id="${it.secId}"]`);
        if(!tr) continue;
        for(const f of ["short_name","und_fx","n_gamma","or_bid_sprd","or_ask_sprd","x_bid","x_ask","x_both","vol_flag","bond_type",
                        "bw_dvb","bw_dvs","bw_brw","bw_lo","bw_hi","bw_gap","bw_util","bw_d5","bw_htb","bw_evt","bw_src","bw_tnr"]){
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
      isin: (refCache[sid]||{}).isin || tv('td[data-r="isin"]'),
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
      bw_dvb: uVal(tr,"bw_dvb"),
      bw_dvs: uVal(tr,"bw_dvs"),
      bw_brw: uVal(tr,"bw_brw"),
      bw_lo: uVal(tr,"bw_lo"),
      bw_hi: uVal(tr,"bw_hi"),
      bw_gap: uVal(tr,"bw_gap"),
      bw_util: uVal(tr,"bw_util"),
      bw_d5: uVal(tr,"bw_d5"),
      bw_htb: uVal(tr,"bw_htb"),
      bw_evt: uVal(tr,"bw_evt"),
      bw_src: uVal(tr,"bw_src"),
      bw_tnr: uVal(tr,"bw_tnr"),

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
      if(td.dataset.c === "parityPct" || td.dataset.c === "be_move" || (td.dataset.c||"").startsWith("dc_") || (td.dataset.c||"").startsWith("idb_") || (td.dataset.c||"").startsWith("cn_") || (td.dataset.c||"").startsWith("hn_")) return;   // client-computed / mirrored, not ours
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
      if(c === "parityPct" || c === "be_move" || c.startsWith("dc_") || c.startsWith("idb_") || c.startsWith("cn_") || c.startsWith("hn_")) return;              // client-computed / mirrored, not ours
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

function bwChg(el){ bwFlag(el); sprdChanged(el); }
function bwFlag(el){
  const f=el.dataset.u,
    v=parseFloat(String(el.value).replace("%",""));
  let bad=false;
  if(f==="bw_htb"&&isFinite(v)) bad=v>=1;
  if(f==="bw_util"&&isFinite(v)) bad=v>=85;
  if(f==="bw_gap"&&isFinite(v)) bad=Math.abs(v)>=1;
  if(f==="bw_evt"&&isFinite(v)) bad=v>=0&&v<=3;
  el.classList.toggle("bwred",bad);
}
function bwFlagAll(){
  document.querySelectorAll('input[data-u^="bw_"]').forEach(bwFlag);
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
    print("  NUKE STATION  BUILD borrow.b37  \u00b7  %s"
          % os.path.abspath(__file__))
    print("  port %s \u00b7 if this banner is missing, an OLD file is\n  running \u2014 kill that process first." % PORT)
    print("=" * 62)
    uvicorn.run(app, host=HOST, port=PORT, workers=1)
