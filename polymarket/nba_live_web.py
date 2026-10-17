#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nba_live_web.py  --  Polymarket NBA Live Monitor  (Lagrange-style single-file web app)

  python nba_live_web.py              -> live Polymarket feeds, http://localhost:59955
  python nba_live_web.py --mock       -> offline simulated feed (UI / logic check off-hours)
  python nba_live_web.py --port 59956 --ca C:\\certs\\corp.pem

Python 3.9+, Windows-friendly.  pip install websocket-client  (required for push feeds;
without it the app degrades to REST polling: books every 2s, scores via Gamma every 5s).

Event-driven pipeline (r2):
  Sports WS  wss://sports-api.polymarket.com/ws                  score / period / clock   (push)
  CLOB WS    wss://ws-subscriptions-clob.polymarket.com/ws/market local order book        (push)
  CLOB REST  POST /books                                          resync every 10s / fallback 2s
  Gamma REST /events                                              discovery every 20s
  Browser    Server-Sent Events /api/stream                       pushed on every change (<=10/s)

Latency notes
  * Score / period / clock arrive only when the Sports WS emits (score change, period change,
    start / end).  Between those messages Polymarket sends nothing, so the clock is shown with
    its age ("as of Ns") instead of being extrapolated -- the NBA clock is stopped most of the
    real time, so a ticking guess would be wrong more often than right.
  * Polymarket labels its sports feed "informational, may be delayed".  For true in-play HFT
    use a direct league data feed; this tool shows feed ages so the lag is visible.

Price convention (polymarket.com display): mid = (bid+ask)/2, but if the spread is wider than
0.10 the last trade is shown instead (flag "L").  Prices are probabilities 0..1, shown as %.
"""
import argparse
import json
import sqlite3
import math
import os
import random
import re
import ssl
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

try:  # optional but strongly recommended
    import websocket  # websocket-client
except Exception:  # pragma: no cover
    websocket = None

VERSION = "nba.r6"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
SPORTS_WS = "wss://sports-api.polymarket.com/ws"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
NBA_SCOREBOARD = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nba_live_config.json")
MINUTE_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nba_minute.db")
DATA_API = "https://data-api.polymarket.com"
DEFAULT_NBA_SERIES = "10345"   # fallback if /sports lookup fails
GAME_TAG_ID = "100639"         # Gamma tag for single-game bets (excludes futures)
UA = "nba-live-monitor/" + VERSION

CFG: Dict[str, Any] = {
    "port": 59955,
    "push_min_ms": 100,        # max browser push rate = 10/s (coalesces bursts)
    "gamma_poll_s": 20,        # event / market discovery refresh
    "gamma_poll_fast_s": 5,    # ... when the score WS is down and games are live (fallback scores)
    "book_poll_s": 2.0,        # REST book loop tick (fallback when the CLOB WS is down)
    "book_resync_s": 10,       # full REST resync while the CLOB WS is up
    "book_window_min": 60,     # also stream books for games starting within N minutes
    "book_fresh_s": 45,        # quote older than this -> fall back to Gamma quote
    "score_ws_timeout_s": 12,  # server pings every 5s; silence > this -> reconnect
    "book_ws_stale_s": 25,     # no frame (incl. PONG) for this long -> reconnect
    "ws_backoff_max_s": 10,
    "score_order": "away_home",  # Polymarket feed: first number of "a-b" belongs to awayTeam (observed live)
    "score_source": "nba",     # quarter / clock / score source: polymarket | nba | espn   (CONFIG tab)
    "score_fallback": True,    # if the chosen source has no match for a game, use Polymarket WS data
    "nba_poll_s": 2.0,         # NBA.com live JSON poll interval (no push available)
    "espn_poll_s": 2.0,        # ESPN scoreboard poll interval
    "score_http_timeout": 3.0,
    "record_1m": True,         # write 1-minute bars of live games to nba_minute.db (CONFIG tab)
    "upcoming_hours": 48,
    "final_keep_min": 180,
    "alert_pts": 0.05,         # moneyline move alert threshold (0.05 = 5 pts)
    "alert_window_s": 60,
    "hist_keep_s": 3600,
    "hist_bucket_s": 1.0,      # history resolution (one point per second per market)
    "wide_spread": 0.10,
    "http_timeout": 10,
    "books_chunk": 50,
}

FINAL_STATUS = {"final", "f/ot", "finished", "ended"}
HALT_STATUS = {"suspended", "postponed", "delayed", "canceled", "cancelled", "forfeit", "notnecessary"}


# ----------------------------------------------------------------------------- helpers
def _f(x: Any) -> Optional[float]:
    """float or None (handles None, '', 'nan', bad strings)."""
    if x is None or x == "" or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _jlist(x: Any) -> list:
    """Gamma returns outcomes / clobTokenIds / outcomePrices as JSON *strings*; accept both."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            v = json.loads(x)
        except ValueError:
            return []
        return v if isinstance(v, list) else []
    return []


_ISO_RE = re.compile(r"^(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d(?::\d\d)?)(\.\d+)?\s*(Z|[+-]\d\d(?::?\d\d)?)?$")


def _ts(iso: Any) -> Optional[float]:
    """ISO-8601 -> epoch seconds. Python 3.9 fromisoformat is strict, so normalise first."""
    if not iso or not isinstance(iso, str):
        return None
    m = _ISO_RE.match(iso.strip())
    if not m:
        try:  # date only
            return datetime.strptime(iso.strip()[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            return None
    d, t, frac, tz = m.groups()
    if len(t) == 5:
        t += ":00"
    frac = (frac or ".0")[1:7].ljust(6, "0")
    if not tz or tz == "Z":
        tz = "+00:00"
    elif len(tz) == 3:
        tz += ":00"
    elif ":" not in tz:
        tz = tz[:3] + ":" + tz[3:]
    try:
        return datetime.fromisoformat("%sT%s.%s%s" % (d, t, frac, tz)).timestamp()
    except ValueError:
        return None


def _log_exc(tag: str) -> str:
    s = traceback.format_exc(limit=2).strip().splitlines()[-1]
    sys.stderr.write("[%s] %s %s\n" % (time.strftime("%H:%M:%S"), tag, s))
    return s


def split_score(score: Any, home: Any, away: Any, order: str) -> Optional[Tuple[str, int, str, int]]:
    """'73-82' -> (first_team, first_pts, second_team, second_pts) per CFG score_order.
    away_home: first number belongs to awayTeam (observed on the live feed, Oct 2026).
    home_away: first number belongs to homeTeam (what the docs describe)."""
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", str(score or ""))
    if not m:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    hn, an = str(home or "HOME"), str(away or "AWAY")
    if order == "home_away":
        return an, b, hn, a          # still display away on the left
    return an, a, hn, b


# ----------------------------------------------------------------------------- config (CONFIG tab)
EDITABLE: Dict[str, tuple] = {
    "score_source": ("choice", ("polymarket", "nba", "espn")),
    "score_fallback": ("bool",),
    "nba_poll_s": ("num", 1.0, 30.0),
    "espn_poll_s": ("num", 1.0, 30.0),
    "score_order": ("choice", ("away_home", "home_away")),
    "alert_pts": ("num", 0.005, 0.5),
    "push_min_ms": ("num", 50, 2000),
    "record_1m": ("bool",),
}


def validate_cfg(d: Any) -> Tuple[Dict[str, Any], List[str]]:
    """Whitelist + range check. Unknown keys / bad values are reported, never applied."""
    clean: Dict[str, Any] = {}
    errs: List[str] = []
    if not isinstance(d, dict):
        return clean, ["config must be a JSON object"]
    for k, v in d.items():
        spec = EDITABLE.get(k)
        if spec is None:
            errs.append("%s: not editable" % k)
            continue
        if spec[0] == "choice":
            if v in spec[1]:
                clean[k] = v
            else:
                errs.append("%s: must be one of %s" % (k, "/".join(spec[1])))
        elif spec[0] == "bool":
            if isinstance(v, bool):
                clean[k] = v
            else:
                errs.append("%s: must be true/false" % k)
        else:
            x = _f(v)
            if x is None or not (spec[1] <= x <= spec[2]):
                errs.append("%s: must be a number in [%s, %s]" % (k, spec[1], spec[2]))
            else:
                clean[k] = int(x) if isinstance(spec[1], int) and isinstance(spec[2], int) else x
    return clean, errs


def load_cfg_file(path: Optional[str] = None) -> List[str]:
    path = path or CONFIG_FILE
    try:
        with open(path, "r", encoding="utf-8") as fh:
            clean, errs = validate_cfg(json.load(fh))
    except FileNotFoundError:
        return []
    except Exception as e:
        return ["%s unreadable: %s" % (os.path.basename(path), e)]
    CFG.update(clean)
    return errs


def save_cfg_file(path: Optional[str] = None) -> Optional[str]:
    path = path or CONFIG_FILE
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({k: CFG[k] for k in EDITABLE}, fh, indent=2)
        os.replace(tmp, path)
        return None
    except Exception as e:
        return str(e)


# ----------------------------------------------------------------------------- teams / score sources
_TEAMS = [
    ("ATL", "Atlanta", "Hawks", ()), ("BOS", "Boston", "Celtics", ()), ("BKN", "Brooklyn", "Nets", ("BRK",)),
    ("CHA", "Charlotte", "Hornets", ("CHO",)), ("CHI", "Chicago", "Bulls", ()), ("CLE", "Cleveland", "Cavaliers", ()),
    ("DAL", "Dallas", "Mavericks", ()), ("DEN", "Denver", "Nuggets", ()), ("DET", "Detroit", "Pistons", ()),
    ("GSW", "Golden State", "Warriors", ("GS",)), ("HOU", "Houston", "Rockets", ()), ("IND", "Indiana", "Pacers", ()),
    ("LAC", "LA", "Clippers", ("LOS ANGELES CLIPPERS",)), ("LAL", "Los Angeles", "Lakers", ()),
    ("MEM", "Memphis", "Grizzlies", ()), ("MIA", "Miami", "Heat", ()), ("MIL", "Milwaukee", "Bucks", ()),
    ("MIN", "Minnesota", "Timberwolves", ()), ("NOP", "New Orleans", "Pelicans", ("NO", "NOR")),
    ("NYK", "New York", "Knicks", ("NY",)), ("OKC", "Oklahoma City", "Thunder", ()), ("ORL", "Orlando", "Magic", ()),
    ("PHI", "Philadelphia", "76ers", ("SIXERS",)), ("PHX", "Phoenix", "Suns", ("PHO",)),
    ("POR", "Portland", "Trail Blazers", ("BLAZERS",)), ("SAC", "Sacramento", "Kings", ()),
    ("SAS", "San Antonio", "Spurs", ("SA",)), ("TOR", "Toronto", "Raptors", ()), ("UTA", "Utah", "Jazz", ("UTAH",)),
    ("WAS", "Washington", "Wizards", ("WSH",)),
]
TEAM_ALIAS: Dict[str, str] = {}
for _tc, _city, _nick, _al in _TEAMS:
    for _k in (_tc, _nick, _city + " " + _nick) + tuple(_al):
        TEAM_ALIAS[_k.upper()] = _tc


def tri(x: Any) -> Optional[str]:
    """any team label (tricode, ESPN/Polymarket abbrev, nickname, full name) -> NBA tricode"""
    s = str(x or "").strip().upper().replace(".", "")
    return TEAM_ALIAS.get(s) if s else None


def game_pair(slug: str, teams: List[str]) -> Optional[frozenset]:
    """canonical {tricode, tricode} for a Polymarket event: slug first (nba-sas-phx-...), then outcome names."""
    m = re.match(r"^nba-([a-z0-9]+)-([a-z0-9]+)-\d{4}-\d\d-\d\d", str(slug or "").lower())
    if m:
        a, b = tri(m.group(1)), tri(m.group(2))
        if a and b and a != b:
            return frozenset((a, b))
    ts = [tri(t) for t in (teams or [])[:2]]
    if len(ts) == 2 and all(ts) and ts[0] != ts[1]:
        return frozenset(ts)
    return None


def period_label(p: int, reg: int = 4) -> str:
    if p <= 0:
        return ""
    return "Q%d" % p if p <= reg else "OT%d" % (p - reg)


def norm_clock(c: Any) -> str:
    """'PT04M10.00S' / '4:10' / '35.2' -> '04:10' / '04:10' / '00:35.2'"""
    s = str(c or "").strip()
    m = re.match(r"^PT(\d+)M([\d.]+)S$", s)
    if m:
        mm, ss = int(m.group(1)), float(m.group(2))
        if mm == 0 and ss < 60 and abs(ss - round(ss)) > 1e-6:
            return "00:%04.1f" % ss
        return "%02d:%02d" % (mm, int(ss))
    m = re.match(r"^(\d{1,2}):(\d{2}(?:\.\d+)?)$", s)
    if m:
        return "%02d:%s" % (int(m.group(1)), m.group(2))
    m = re.match(r"^(\d{1,2}(?:\.\d+)?)$", s)
    if m:
        return "00:%04.1f" % float(m.group(1))
    return s


def _score_rec(src: str, status: str, period: str, clock: str, away: str, home: str, ap: int, hp: int,
               start: Optional[float], ext_id: str) -> dict:
    a3, h3 = tri(away), tri(home)
    return {"src": src, "status": status, "period": period, "clock": clock, "away": away, "home": home,
            "ap": ap, "hp": hp, "start": start, "id": ext_id,
            "pair": frozenset((a3, h3)) if a3 and h3 and a3 != h3 else None,
            "text": "%s %d - %d %s" % (away, ap, hp, home), "rx": 0.0}


def parse_nba_scoreboard(data: Any) -> List[dict]:
    """cdn.nba.com liveData todaysScoreboard_00.json -> normalised score records"""
    out = []
    games = ((data or {}).get("scoreboard") or {}).get("games") if isinstance(data, dict) else None
    for g in games or []:
        if not isinstance(g, dict):
            continue
        h, a = g.get("homeTeam") or {}, g.get("awayTeam") or {}
        hn, an = str(h.get("teamTricode") or ""), str(a.get("teamTricode") or "")
        if not hn or not an:
            continue
        gs = int(_f(g.get("gameStatus")) or 0)
        txt = str(g.get("gameStatusText") or "").strip()
        tl = txt.lower()
        reg = int(_f(g.get("regulationPeriods")) or 4)
        p = int(_f(g.get("period")) or 0)
        per, clk = period_label(p, reg), norm_clock(g.get("gameClock"))
        if any(k in tl for k in ("ppd", "postpon", "cancel", "susp", "delay")):
            st = "HALT"
        elif gs == 3:
            st, per, clk = "FINAL", ("FT" if p <= reg else "FT/OT"), ""
        elif gs == 2:
            st = "LIVE"
            if "half" in tl:
                per, clk = "HALF", ""
            elif tl.startswith("end"):
                clk = "END"
        else:
            st, per, clk = "PRE", "", ""
        out.append(_score_rec("nba", st, per, clk, an, hn, int(_f(a.get("score")) or 0), int(_f(h.get("score")) or 0),
                              _ts(g.get("gameTimeUTC")), str(g.get("gameId") or "")))
    return out


def parse_espn_scoreboard(data: Any) -> List[dict]:
    """site.api.espn.com .../basketball/nba/scoreboard -> normalised score records"""
    out = []
    evs = data.get("events") if isinstance(data, dict) else None
    for ev in evs or []:
        if not isinstance(ev, dict):
            continue
        comps = ev.get("competitions") or [{}]
        comp = comps[0] if comps and isinstance(comps[0], dict) else {}
        stt = comp.get("status") or ev.get("status") or {}
        ty = stt.get("type") or {}
        side = {c.get("homeAway"): c for c in comp.get("competitors") or [] if isinstance(c, dict)}
        h, a = side.get("home") or {}, side.get("away") or {}
        hn, an = str((h.get("team") or {}).get("abbreviation") or ""), str((a.get("team") or {}).get("abbreviation") or "")
        if not hn or not an:
            continue
        state, name = str(ty.get("state") or "").lower(), str(ty.get("name") or "").upper()
        desc = str(ty.get("description") or "").lower()
        p = int(_f(stt.get("period")) or 0)
        per, clk = period_label(p), norm_clock(stt.get("displayClock"))
        if any(k in name for k in ("POSTPONED", "CANCELED", "CANCELLED", "SUSPENDED", "DELAYED")):
            st = "HALT"
        elif state == "post" or ty.get("completed") is True:
            st, per, clk = "FINAL", ("FT" if p <= 4 else "FT/OT"), ""
        elif state == "in":
            st = "LIVE"
            if "HALFTIME" in name or desc == "halftime":
                per, clk = "HALF", ""
            elif "END_PERIOD" in name or desc.startswith("end of"):
                clk = "END"
        else:
            st, per, clk = "PRE", "", ""
        out.append(_score_rec("espn", st, per, clk, an, hn, int(_f(a.get("score")) or 0), int(_f(h.get("score")) or 0),
                              _ts(ev.get("date")), str(ev.get("id") or "")))
    return out


SCORE_PARSERS = {"nba": parse_nba_scoreboard, "espn": parse_espn_scoreboard}


# ----------------------------------------------------------------------------- 1-minute bars (SQLite)
# Every price is in OUTCOME-A (token0) terms, 0..1.  ts = minute START, epoch seconds UTC;  ts_hkt = same minute in HKT.
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS book_1m (           -- live recorder: order book state per market per minute
  ts INTEGER, ts_hkt TEXT, game_id TEXT, slug TEXT, market_id TEXT, type TEXT, line REAL, outcome0 TEXT, token0 TEXT,
  mid_o REAL, mid_h REAL, mid_l REAL, mid_c REAL,            -- mid price path inside the minute
  bid REAL, ask REAL, bsz REAL, asz REAL, last REAL,         -- top of book at the minute close
  n_upd INTEGER,                                             -- book updates seen in the minute
  trd_o REAL, trd_h REAL, trd_l REAL, trd_c REAL, vol REAL, buy_vol REAL, sell_vol REAL, n_trd INTEGER,  -- trades (WS)
  quote_src TEXT, partial INTEGER, PRIMARY KEY (ts, market_id));
CREATE TABLE IF NOT EXISTS score_1m (          -- live recorder: game state at the minute close
  ts INTEGER, ts_hkt TEXT, game_id TEXT, slug TEXT, title TEXT, status TEXT, period TEXT, clock TEXT,
  away TEXT, home TEXT, away_pts INTEGER, home_pts INTEGER, score_src TEXT, PRIMARY KEY (ts, game_id));
CREATE TABLE IF NOT EXISTS hist_trade_1m (     -- backfill: 1-minute OHLCV built from every trade (data-api /v2/trades)
  ts INTEGER, ts_hkt TEXT, game_id TEXT, slug TEXT, market_id TEXT, type TEXT, line REAL, outcome0 TEXT,
  o REAL, h REAL, l REAL, c REAL, vol REAL, buy_vol REAL, sell_vol REAL, n INTEGER, PRIMARY KEY (ts, market_id));
CREATE TABLE IF NOT EXISTS hist_price_1m (     -- backfill: Polymarket's own 1-minute price series (/v2/prices-history)
  ts INTEGER, ts_hkt TEXT, game_id TEXT, market_id TEXT, price REAL, res_s INTEGER, PRIMARY KEY (ts, market_id));
CREATE TABLE IF NOT EXISTS hist_games (
  game_id TEXT PRIMARY KEY, slug TEXT, title TEXT, start INTEGER, start_hkt TEXT, n_markets INTEGER,
  n_trades INTEGER, n_trade_bars INTEGER, n_price_pts INTEGER, fetched INTEGER, note TEXT);
"""


def hkt_str(ts: float) -> str:
    return datetime.fromtimestamp(ts + 8 * 3600, timezone.utc).strftime("%Y-%m-%d %H:%M")


def open_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(DB_SCHEMA)
    return con


class Bar:
    """open/high/low/close (+ volume split) accumulator"""
    __slots__ = ("o", "h", "l", "c", "n", "vol", "buy", "sell")

    def __init__(self) -> None:
        self.o = self.h = self.l = self.c = None
        self.n, self.vol, self.buy, self.sell = 0, 0.0, 0.0, 0.0

    def add(self, px: float, size: float = 0.0, side: str = "") -> None:
        if self.o is None:
            self.o = self.h = self.l = px
        self.h, self.l, self.c = max(self.h, px), min(self.l, px), px
        self.n += 1
        if size:
            self.vol += size
            if side == "BUY":
                self.buy += size
            elif side == "SELL":
                self.sell += size


def _trade_fields(tr: dict) -> Optional[Tuple[float, str, Optional[int], float, float, str, str]]:
    """data-api trade row (v2 snake_case, v1 camelCase tolerated) -> (ts, token, outcome_idx, price, size, side, txid)"""
    if not isinstance(tr, dict):
        return None
    ts = tr.get("timestamp", tr.get("match_time"))
    t = _f(ts)
    if t is None:
        t = _ts(ts)
    if t is None:
        return None
    if t > 1e11:            # milliseconds
        t /= 1000.0
    px, sz = _f(tr.get("price")), _f(tr.get("size"))
    if px is None or sz is None or not (0 <= px <= 1):
        return None
    tok = str(tr.get("token_id") or tr.get("asset") or tr.get("asset_id") or "")
    oi = tr.get("outcome_index", tr.get("outcomeIndex"))
    oi = int(oi) if _f(oi) is not None else None
    side = str(tr.get("side") or "").upper()
    tx = str(tr.get("transaction_hash") or tr.get("transactionHash") or "")
    return t, tok, oi, px, sz, side, tx


def trades_to_bars(trades: List[dict], token0: str, token1: str, t0: float, t1: float) -> Tuple[Dict[int, "Bar"], int]:
    """1-minute bars in token0 terms. A trade in token1 at p is a token0 trade at 1-p with the side flipped
    (buying B == selling A). Duplicates (same tx/token/price/size/time) are dropped. Returns (bars, n_used)."""
    bars: Dict[int, Bar] = {}
    seen = set()
    used = 0
    rows = []
    for tr in trades or []:
        f = _trade_fields(tr)
        if not f:
            continue
        t, tok, oi, px, sz, side, tx = f
        if not (t0 <= t < t1):
            continue
        key = (tx, tok, oi, px, sz, int(t), side)
        if key in seen:
            continue
        seen.add(key)
        if tok == token0 or (not tok and oi == 0):
            p0, sd = px, side
        elif tok == token1 or (not tok and oi == 1):
            p0, sd = 1.0 - px, {"BUY": "SELL", "SELL": "BUY"}.get(side, side)
        else:
            continue
        rows.append((t, p0, sz, sd))
    rows.sort(key=lambda r: r[0])           # API is newest-first; bars need time order for open/close
    for t, p0, sz, sd in rows:
        b = bars.setdefault(int(t // 60) * 60, Bar())
        b.add(round(p0, 6), sz, sd)
        used += 1
    return bars, used


class MinuteRecorder:
    """Live 1-minute bars. Mid / trade accumulators are fed from the engine's hot path (O(1) each);
    at every minute boundary the finished minute is written to SQLite by this thread."""

    def __init__(self, eng: "Engine", path: Optional[str] = None):
        self.eng = eng
        self.path = path or MINUTE_DB
        # accumulators keyed by minute start, so updates arriving just after a boundary never leak into the old bar
        self.mid: Dict[int, Dict[str, Bar]] = {}
        self.trd: Dict[int, Dict[str, Bar]] = {}
        self.first_min = int(time.time() // 60) * 60      # bar of the start-up minute is partial
        self.h = {"state": "INIT", "rows": 0, "last": 0.0, "err": "", "path": self.path}

    def on_mid(self, t: str, mid: float, now: float) -> None:
        self.mid.setdefault(int(now // 60) * 60, {}).setdefault(t, Bar()).add(mid)

    def on_trade(self, t: str, px: Optional[float], size: Optional[float], side: str, now: float) -> None:
        if px is not None:
            self.trd.setdefault(int(now // 60) * 60, {}).setdefault(t, Bar()).add(px, size or 0.0, side)

    def collect(self, ts_min: int, now: float) -> Tuple[List[tuple], List[tuple]]:
        """rows for the minute starting ts_min (call with engine lock held). Resets accumulators."""
        eng = self.eng
        mids, trds = self.mid.pop(ts_min, {}), self.trd.pop(ts_min, {})
        for acc in (self.mid, self.trd):                   # drop anything older (missed flushes)
            for k in [k for k in acc if k < ts_min]:
                del acc[k]
        brows, srows = [], []
        hk = hkt_str(ts_min)
        partial = 1 if ts_min == self.first_min else 0
        for g in eng.games.values():
            st, per, clk, score, rec = eng._status(g, now)
            soon = g["start"] is not None and g["start"] - now <= CFG["book_window_min"] * 60
            if not (st in ("LIVE", "INPLAY?", "HALT") or (st == "PRE" and soon)):
                continue
            ap = hp = None
            aw = hm = None
            if rec:
                aw, hm, ap, hp = rec.get("away"), rec.get("home"), rec.get("ap"), rec.get("hp")
            srows.append((ts_min, hk, g["id"], g["slug"], g["title"], st, per, clk, aw, hm, ap, hp, rec["src"] if rec else ""))
            for m in g["markets"]:
                t = m["tokens"][0]
                q = eng.quote(m, now)
                mb, tb = mids.get(t), trds.get(t)
                if mb is None:                   # no update this minute: flat bar at the current mid
                    mo = mh = ml = mc = q["mid"]
                    nu = 0
                else:
                    mo, mh, ml, mc, nu = mb.o, mb.h, mb.l, mb.c, mb.n
                mo, mh, ml, mc = (None if x is None else round(x, 6) for x in (mo, mh, ml, mc))
                brows.append((ts_min, hk, g["id"], g["slug"], m["id"], m["type"], m["line"], m["outcomes"][0], t,
                              mo, mh, ml, mc, q["bid"], q["ask"], q["bsz"], q["asz"], q["last"], nu,
                              tb.o if tb else None, tb.h if tb else None, tb.l if tb else None, tb.c if tb else None,
                              tb.vol if tb else 0.0, tb.buy if tb else 0.0, tb.sell if tb else 0.0, tb.n if tb else 0,
                              q["src"], partial))
        return brows, srows

    def write(self, con: sqlite3.Connection, brows: List[tuple], srows: List[tuple]) -> None:
        with con:
            con.executemany("INSERT OR REPLACE INTO book_1m VALUES (%s)" % ",".join("?" * 29), brows)
            con.executemany("INSERT OR REPLACE INTO score_1m VALUES (%s)" % ",".join("?" * 13), srows)

    def run(self) -> None:
        con = None
        while not self.eng.stop.is_set():
            nxt = (int(time.time() // 60) + 1) * 60
            self.eng.stop.wait(max(0.0, nxt - time.time()) + 0.05)
            if self.eng.stop.is_set():
                break
            now = time.time()
            ts_min = int(now // 60) * 60 - 60
            with self.eng.lock:
                brows, srows = self.collect(ts_min, now)
                enabled = CFG["record_1m"]
            if not enabled:
                self.h["state"] = "OFF"
                continue
            if not brows and not srows:
                self.h["state"] = "IDLE"
                continue
            try:
                if con is None:
                    con = open_db(self.path)
                self.write(con, brows, srows)
                self.h.update(state="UP", rows=self.h["rows"] + len(brows) + len(srows), last=now, err="")
            except Exception as e:
                self.h.update(state="DOWN", err=("%s: %s" % (type(e).__name__, e))[:160])
                try:
                    if con is not None:
                        con.close()
                except Exception:
                    pass
                con = None


class Backfill:
    """Past NBA games -> 1-minute bars in SQLite.
    hist_trade_1m : OHLCV from every trade (data-api /v2/trades) -- works for older games
    hist_price_1m : Polymarket's 1-minute price series (/v2/prices-history) -- only kept ~7 days at 1-minute"""

    def __init__(self, get: Any, db_path: Optional[str] = None, log: Any = print, sleep_s: float = 0.15):
        self.get = get                       # get(url, params) -> parsed JSON
        self.series: Optional[str] = None
        self.db_path = db_path or MINUTE_DB
        self.log = log
        self.sleep_s = sleep_s

    def series_id(self) -> str:
        if not self.series:
            try:
                for x in self.get(GAMMA + "/sports", None) or []:
                    if isinstance(x, dict) and str(x.get("sport", "")).lower() == "nba":
                        self.series = str(x.get("series") or "").split(",")[0].strip() or None
                        break
            except Exception:
                pass
            self.series = self.series or DEFAULT_NBA_SERIES
        return self.series

    def closed_games(self, days: float, now: Optional[float] = None) -> List[dict]:
        now = now or time.time()
        lo = now - days * 86400
        out: List[dict] = []
        seen = set()
        for offset in range(0, 3000, 100):
            page = self.get(GAMMA + "/events", {"series_id": self.series_id(),
                                                "tag_id": GAME_TAG_ID, "closed": "true", "limit": 100, "offset": offset,
                                                "order": "startTime", "ascending": "false"})
            if not isinstance(page, list) or not page:
                break
            older = 0
            for ev in page:
                g = parse_event(dict(ev, markets=[dict(m, closed=False) for m in (ev.get("markets") or []) if isinstance(m, dict)])) \
                    if isinstance(ev, dict) else None
                if not g or g["id"] in seen or g["start"] is None:
                    continue
                seen.add(g["id"])
                if g["start"] < lo:
                    older += 1
                    continue
                if g["start"] <= now:
                    out.append(g)
            if older == len(page) or len(page) < 100:
                break
            time.sleep(self.sleep_s)
        out.sort(key=lambda g: g["start"])
        return out

    def _pages(self, url: str, params: dict, stop_before: Optional[float] = None, max_pages: int = 200) -> List[dict]:
        rows: List[dict] = []
        cur = None
        for _ in range(max_pages):
            q = dict(params)
            if cur:
                q["cursor"] = cur
            res = self.get(url, q)
            data = res.get("data") if isinstance(res, dict) else (res if isinstance(res, list) else None)
            if not data:
                break
            rows.extend(data)
            if stop_before is not None:
                f = _trade_fields(data[-1])
                if f and f[0] < stop_before:  # newest-first feed went past the window
                    break
            pg = res.get("pagination") if isinstance(res, dict) else None
            cur = (pg or {}).get("next_cursor")
            if not cur:
                break
            time.sleep(self.sleep_s)
        return rows

    def price_history(self, token: str, t0: float, t1: float) -> List[Tuple[int, float, int]]:
        res_rows = self._pages(DATA_API + "/v2/prices-history", {"token_id": token, "start": int(t0), "end": int(t1),
                                                                  "bucket_seconds": 60})
        out = []
        for p in res_rows:
            if not isinstance(p, dict):
                continue
            t = _f(p.get("timestamp", p.get("t")))
            px = _f(p.get("price", p.get("p")))
            if t is None or px is None:
                continue
            if t > 1e11:
                t /= 1000.0
            out.append((int(t // 60) * 60, px, int(_f(p.get("resolution_seconds")) or 60)))
        return out

    def run(self, days: float = 7, pre_min: int = 120, post_min: int = 240, types: Optional[List[str]] = None,
            now: Optional[float] = None) -> dict:
        con = open_db(self.db_path)
        games = self.closed_games(days, now)
        self.log("backfill: %d finished NBA games in the last %s days -> %s" % (len(games), days, self.db_path))
        tot = {"games": 0, "markets": 0, "trades": 0, "trade_bars": 0, "price_pts": 0, "errors": 0}
        for g in games:
            t0, t1 = g["start"] - pre_min * 60, g["start"] + post_min * 60
            n_tr = n_tb = n_pp = 0
            note = []
            mk = [m for m in g["markets"] if (not types or m["type"] in types)]
            for m in mk:
                tok0 = m["tokens"][0]
                tok1 = m["tokens"][1] if len(m["tokens"]) > 1 else ""
                try:
                    if m.get("cond"):
                        trs = self._pages(DATA_API + "/v2/trades", {"condition": m["cond"], "limit": 1000}, stop_before=t0)
                        bars, used = trades_to_bars(trs, tok0, tok1, t0, t1)
                        rows = [(ts, hkt_str(ts), g["id"], g["slug"], m["id"], m["type"], m["line"], m["outcomes"][0],
                                 b.o, b.h, b.l, b.c, round(b.vol, 6), round(b.buy, 6), round(b.sell, 6), b.n)
                                for ts, b in sorted(bars.items())]
                        with con:
                            con.executemany("INSERT OR REPLACE INTO hist_trade_1m VALUES (%s)" % ",".join("?" * 16), rows)
                        n_tr += used
                        n_tb += len(rows)
                    else:
                        note.append("%s: no conditionId" % m["id"])
                    pts = self.price_history(tok0, t0, t1)
                    with con:
                        con.executemany("INSERT OR REPLACE INTO hist_price_1m VALUES (?,?,?,?,?,?)",
                                        [(ts, hkt_str(ts), g["id"], m["id"], px, rs) for ts, px, rs in pts])
                    n_pp += len(pts)
                except Exception as e:
                    tot["errors"] += 1
                    note.append("%s: %s" % (m["id"], str(e)[:80]))
                time.sleep(self.sleep_s)
            with con:
                con.execute("INSERT OR REPLACE INTO hist_games VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            (g["id"], g["slug"], g["title"], int(g["start"]), hkt_str(g["start"]), len(mk), n_tr, n_tb, n_pp,
                             int(time.time()), "; ".join(note)[:500]))
            tot["games"] += 1
            tot["markets"] += len(mk)
            tot["trades"] += n_tr
            tot["trade_bars"] += n_tb
            tot["price_pts"] += n_pp
            self.log("  %s  %-40s markets %2d  trades %6d  trade-bars %4d  1m-price pts %4d%s" % (
                hkt_str(g["start"]), g["title"][:40], len(mk), n_tr, n_tb, n_pp, ("  [" + note[0] + "]") if note else ""))
        con.close()
        return tot


# ----------------------------------------------------------------------------- transport
class Http:
    def __init__(self, timeout: float, ca: Optional[str] = None):
        self.timeout = timeout
        self.ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()

    def _open(self, req: urllib.request.Request) -> Any:
        with urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx) as r:
            return json.loads(r.read().decode("utf-8"))

    def get(self, url: str, params: Optional[dict] = None) -> Any:
        if params:
            url += "?" + urllib.parse.urlencode(params)
        return self._open(urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"}))

    def raw(self, url: str, headers: Dict[str, str], timeout: Optional[float] = None) -> Tuple[int, bytes, Any]:
        """GET returning (status, body, headers); 304 Not Modified is returned, not raised."""
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout, context=self.ctx) as r:
                return r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return 304, b"", e.headers
            raise

    def post(self, url: str, body: Any) -> Any:
        data = json.dumps(body).encode("utf-8")
        return self._open(urllib.request.Request(url, data=data, method="POST", headers={
            "User-Agent": UA, "Accept": "application/json", "Content-Type": "application/json"}))


class LiveSource:
    """Real Polymarket endpoints."""
    name = "LIVE"

    def __init__(self, http: Http, ca: Optional[str] = None):
        self.http = http
        self.ca = ca
        self.series_id: Optional[str] = None
        nolib = "pip install websocket-client"
        self.ws_state, self.ws_err, self.ws_msgs, self.ws_ts = ("OFF", nolib, 0, 0.0) if websocket is None else ("INIT", "", 0, 0.0)
        self.mws_state, self.mws_err, self.mws_msgs, self.mws_ts = ("OFF", nolib, 0, 0.0) if websocket is None else ("INIT", "", 0, 0.0)
        self.mws_subs = 0
        self.etags: Dict[str, str] = {}

    def fetch_scores(self, src: str) -> Any:
        """NBA.com / ESPN scoreboard JSON, or None when unchanged (HTTP 304 via ETag)."""
        url = NBA_SCOREBOARD if src == "nba" else ESPN_SCOREBOARD
        hdr = {"User-Agent": BROWSER_UA, "Accept": "application/json", "Cache-Control": "no-cache"}
        if src == "nba":
            hdr.update(Referer="https://www.nba.com/", Origin="https://www.nba.com")
        if self.etags.get(url):
            hdr["If-None-Match"] = self.etags[url]
        code, body, h = self.http.raw(url, hdr, CFG["score_http_timeout"])
        if code == 304:
            return None
        et = h.get("ETag") if h is not None else None
        if et:
            self.etags[url] = et
        return json.loads(body.decode("utf-8"))

    # -- REST
    def nba_series(self) -> str:
        if self.series_id:
            return self.series_id
        sid = None
        try:
            for s in self.http.get(GAMMA + "/sports") or []:
                if str(s.get("sport", "")).lower() == "nba":
                    sid = str(s.get("series") or "").split(",")[0].strip() or None
                    break
        except Exception:
            _log_exc("sports lookup")
        self.series_id = sid or DEFAULT_NBA_SERIES
        return self.series_id

    def events(self) -> List[dict]:
        sid = self.nba_series()
        out: List[dict] = []
        seen = set()
        for offset in range(0, 500, 100):
            page = self.http.get(GAMMA + "/events", {
                "series_id": sid, "tag_id": GAME_TAG_ID, "active": "true", "closed": "false",
                "limit": 100, "offset": offset, "order": "startTime", "ascending": "true"})
            if not isinstance(page, list) or not page:
                break
            for ev in page:
                k = str(ev.get("id"))
                if k not in seen:
                    seen.add(k)
                    out.append(ev)
            if len(page) < 100:
                break
        return out

    def books(self, tokens: List[str]) -> List[dict]:
        out: List[dict] = []
        n = CFG["books_chunk"]
        for i in range(0, len(tokens), n):
            res = self.http.post(CLOB + "/books", [{"token_id": t} for t in tokens[i:i + n]])
            if isinstance(res, list):
                out.extend(res)
        return out

    # -- push feeds
    def start(self, eng: "Engine", stop: threading.Event) -> None:
        if websocket is None:
            return
        threading.Thread(target=self._score_loop, args=(eng, stop), daemon=True, name="sports-ws").start()
        threading.Thread(target=self._book_loop, args=(eng, stop), daemon=True, name="clob-ws").start()

    def _sslopt(self) -> dict:
        return {"ca_certs": self.ca} if self.ca else {}

    def _score_loop(self, eng: "Engine", stop: threading.Event) -> None:
        backoff = 0.5
        while not stop.is_set():
            ws = None
            try:
                self.ws_state = "CONN"
                ws = websocket.create_connection(SPORTS_WS, timeout=CFG["score_ws_timeout_s"], sslopt=self._sslopt())
                self.ws_state, self.ws_err, backoff = "UP", "", 0.5
                while not stop.is_set():
                    msg = ws.recv()               # raises on timeout -> reconnect (server pings every 5s)
                    self.ws_ts = time.time()
                    if not msg:
                        continue
                    if msg == "ping":             # app-level heartbeat: reply within 10s
                        ws.send("pong")
                        continue
                    try:
                        data = json.loads(msg)
                    except ValueError:
                        continue
                    for d in (data if isinstance(data, list) else [data]):
                        if isinstance(d, dict):
                            self.ws_msgs += 1
                            eng.on_score(d)
            except Exception as e:
                self.ws_state, self.ws_err = "DOWN", ("%s: %s" % (type(e).__name__, e))[:160]
            finally:
                try:
                    if ws is not None:
                        ws.close()
                except Exception:
                    pass
            stop.wait(backoff)
            backoff = min(backoff * 2, CFG["ws_backoff_max_s"])

    def _book_loop(self, eng: "Engine", stop: threading.Event) -> None:
        backoff = 0.5
        while not stop.is_set():
            want = eng.active_tokens(time.time())
            if not want:
                self.mws_state, self.mws_subs = "IDLE", 0
                stop.wait(1.0)
                continue
            ws = None
            try:
                self.mws_state = "CONN"
                ws = websocket.create_connection(CLOB_WS, timeout=10, sslopt=self._sslopt())
                ws.send(json.dumps({"assets_ids": want, "type": "market", "custom_feature_enabled": True}))
                subs = set(want)
                self.mws_state, self.mws_err, self.mws_subs, backoff = "UP", "", len(subs), 0.5
                ws.settimeout(1.0)
                last_rx = last_ping = last_sync = time.time()
                while not stop.is_set():
                    now = time.time()
                    if now - last_ping >= 10:          # app-level heartbeat for the market channel
                        ws.send("PING")
                        last_ping = now
                    if now - last_sync >= 1.0:         # follow the active game set without reconnecting
                        cur = set(eng.active_tokens(now))
                        add, rem = cur - subs, subs - cur
                        if add:
                            ws.send(json.dumps({"assets_ids": sorted(add), "operation": "subscribe", "custom_feature_enabled": True}))
                        if rem:
                            ws.send(json.dumps({"assets_ids": sorted(rem), "operation": "unsubscribe"}))
                        subs = cur
                        self.mws_subs = len(subs)
                        last_sync = now
                        if not subs:
                            break
                    try:
                        msg = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        if time.time() - last_rx > CFG["book_ws_stale_s"]:
                            raise RuntimeError("stale: no frame for %ds" % CFG["book_ws_stale_s"])
                        continue
                    last_rx = self.mws_ts = time.time()
                    if not msg or msg in ("PONG", "pong"):
                        continue
                    try:
                        data = json.loads(msg)
                    except ValueError:
                        continue
                    self.mws_msgs += 1
                    eng.on_market_msg(data)
            except Exception as e:
                self.mws_state, self.mws_err = "DOWN", ("%s: %s" % (type(e).__name__, e))[:160]
            finally:
                try:
                    if ws is not None:
                        ws.close()
                except Exception:
                    pass
            stop.wait(backoff)
            backoff = min(backoff * 2, CFG["ws_backoff_max_s"])


class MockSource:
    """Offline simulator. Emits Gamma / CLOB-WS / Sports-WS shaped payloads through the same
    engine entry points as the live feeds (on_market_msg / on_score), at realistic burst rates."""
    name = "MOCK"
    TEAMS = [("LAL", "Lakers", "BOS", "Celtics"), ("GSW", "Warriors", "DEN", "Nuggets"),
             ("NYK", "Knicks", "MIA", "Heat"), ("PHX", "Suns", "DAL", "Mavericks"),
             ("OKC", "Thunder", "MIN", "Timberwolves")]
    KEYS = ("ml", "spr", "spr2", "tot", "h1")

    def __init__(self, seed: int = 7):
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        now = time.time()
        offs = [-50 * 60, -95 * 60, 25 * 60, 6 * 3600, -150 * 60]   # 2 live, 1 soon, 1 later, 1 finished
        self.ws_state, self.ws_err, self.ws_msgs, self.ws_ts = "MOCK", "", 0, 0.0
        self.mws_state, self.mws_err, self.mws_msgs, self.mws_ts, self.mws_subs = "MOCK", "", 0, 0.0, 0
        self.games = []
        tid = 1000
        for i, (aa, an, ha, hn) in enumerate(self.TEAMS):
            el = max(0, min(-offs[i], 130 * 60)) / 60.0
            g = {"i": i, "aa": aa, "an": an, "ha": ha, "hn": hn, "start": now + offs[i],
                 "hs": int(el * 0.85), "as": int(el * 0.82), "spr_line": -round(self.rng.uniform(1, 8)) - 0.5,
                 "tot_line": 220.5 + 2 * i, "toks": {}, "px": {}, "q": {}, "per": ""}
            lead = g["as"] - g["hs"]
            g["px"]["ml"] = min(0.95, max(0.05, 1 / (1 + math.exp(-lead * 0.12)))) if el else self.rng.uniform(0.4, 0.6)
            for k in self.KEYS:
                g["toks"][k] = (str(tid), str(tid + 1))
                tid += 2
            g["px"].update(spr=0.5, spr2=0.3, tot=0.5, h1=g["px"]["ml"])
            for k in self.KEYS:
                g["q"][k] = self._mk_quote(g, k)
            self.games.append(g)

    def slug(self, g: dict) -> str:
        return "nba-%s-%s-%s" % (g["aa"].lower(), g["ha"].lower(),
                                 datetime.fromtimestamp(g["start"], timezone.utc).strftime("%Y-%m-%d"))

    def _mk_quote(self, g: dict, k: str) -> Tuple[float, float, int, int]:
        p = g["px"][k]
        sp = 0.14 if k == "spr2" else 0.01        # spr2 deliberately wide -> last-trade rule
        bid = round(max(0.001, p - sp / 2), 3)
        ask = round(min(0.999, p + sp / 2), 3)
        if ask <= bid:
            ask = round(bid + 0.001, 3)
        return bid, ask, self.rng.randint(50, 5000), self.rng.randint(50, 5000)

    def nba_series(self) -> str:
        return DEFAULT_NBA_SERIES

    def _mkt(self, g: dict, key: str, q: str, outs: List[str], smt: Optional[str], line: Optional[float]) -> dict:
        p = g["px"][key]
        bid, ask, _, _ = g["q"][key]
        m = {"id": "m%s%s" % (g["i"], key), "conditionId": "0xc%s%s" % (g["i"], key), "question": q,
             "outcomes": json.dumps(outs), "clobTokenIds": json.dumps(list(g["toks"][key])),
             "outcomePrices": json.dumps([str(round(p, 3)), str(round(1 - p, 3))]),
             "bestBid": bid, "bestAsk": ask, "lastTradePrice": round(p, 3),
             "volume": str(round(self.rng.uniform(5e4, 2e6), 2)), "liquidity": str(round(self.rng.uniform(1e4, 3e5), 2)),
             "gameStartTime": datetime.fromtimestamp(g["start"], timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"),
             "active": True, "closed": False}
        if smt:
            m["sportsMarketType"] = smt
        if line is not None:
            m["line"] = line
        return m

    def events(self) -> List[dict]:
        evs = []
        with self.lock:
            for g in self.games:
                mk = [self._mkt(g, "ml", "%s vs. %s" % (g["an"], g["hn"]), [g["an"], g["hn"]], "moneyline", None),
                      self._mkt(g, "spr", "Spread: %s (%.1f)" % (g["an"], g["spr_line"]), [g["an"], g["hn"]], "spreads", g["spr_line"]),
                      self._mkt(g, "spr2", "Spread: %s (%.1f)" % (g["an"], g["spr_line"] - 4), [g["an"], g["hn"]], None, None),
                      self._mkt(g, "tot", "%s vs. %s: O/U %.1f" % (g["an"], g["hn"], g["tot_line"]), ["Over", "Under"], "totals", g["tot_line"]),
                      self._mkt(g, "h1", "1H Moneyline", [g["an"], g["hn"]], "first_half_moneyline", None)]
                mk.append({"id": "bad%s" % g["i"], "question": "broken market (no tokens)", "outcomes": None})  # edge case
                evs.append({"id": str(9000 + g["i"]), "slug": self.slug(g), "title": "%s vs. %s" % (g["an"], g["hn"]),
                            "startTime": datetime.fromtimestamp(g["start"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "gameId": 5000 + g["i"], "volume": 1e6, "markets": mk})
        return evs

    def _book_msg(self, g: dict, k: str) -> dict:
        bid, ask, bsz, asz = g["q"][k]
        return {"event_type": "book", "asset_id": g["toks"][k][0], "market": "0x", "timestamp": str(int(time.time() * 1000)),
                "bids": [{"price": str(round(bid - 0.02, 3)), "size": "900"}, {"price": str(bid), "size": str(bsz)}],
                "asks": [{"price": str(round(ask + 0.02, 3)), "size": "900"}, {"price": str(ask), "size": str(asz)}],
                "last_trade_price": str(round(g["px"][k], 3))}

    def books(self, tokens: List[str]) -> List[dict]:
        want = set(tokens)
        out = []
        with self.lock:
            for g in self.games:
                for k in self.KEYS:
                    if g["toks"][k][0] in want:
                        out.append(self._book_msg(g, k))
        return out

    def start(self, eng: "Engine", stop: threading.Event) -> None:
        threading.Thread(target=self._sim, args=(eng, stop), daemon=True, name="mock-sim").start()

    ESPN_ABBR = {"GSW": "GS", "NYK": "NY", "SAS": "SA", "NOP": "NO", "UTA": "UTAH", "WAS": "WSH"}

    def _clock_state(self, g: dict, now: float) -> Tuple[str, int, float]:
        el = now - g["start"]
        st = "post" if el >= 140 * 60 else ("in" if el >= 0 else "pre")
        q = min(4, int(max(el, 0) // (30 * 60)) + 1)
        secs = max(0.0, 720.0 - (max(el, 0) % (30 * 60)) / 2.5)     # game clock runs 1s per 2.5s wall
        return st, q, secs

    def fetch_scores(self, src: str) -> Any:
        """NBA.com- / ESPN-shaped scoreboard from the same simulation state."""
        now = time.time()
        with self.lock:
            rows = []
            for g in self.games:
                st, q, secs = self._clock_state(g, now)
                rows.append((g, st, q, secs))
        if src == "nba":
            games = []
            for g, st, q, secs in rows:
                gs = {"pre": 1, "in": 2, "post": 3}[st]
                txt = {1: "7:30 pm ET", 3: "Final"}.get(gs, "Q%d %d:%02d" % (q, int(secs) // 60, int(secs) % 60))
                games.append({"gameId": "00226%05d" % g["i"], "gameStatus": gs, "gameStatusText": txt, "period": q if gs > 1 else 0,
                              "gameClock": "PT%02dM%05.2fS" % (int(secs) // 60, secs % 60) if gs == 2 else "",
                              "gameTimeUTC": datetime.fromtimestamp(g["start"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                              "regulationPeriods": 4,
                              "homeTeam": {"teamTricode": g["ha"], "teamName": g["hn"], "score": g["hs"]},
                              "awayTeam": {"teamTricode": g["aa"], "teamName": g["an"], "score": g["as"]}})
            games.append({"gameId": "0012600099", "gameStatus": 2, "gameStatusText": "Q2 5:00", "period": 2, "gameClock": "PT05M00.00S",
                          "gameTimeUTC": "2026-10-11T10:00:00Z", "homeTeam": {"teamTricode": "BKN", "score": 40},
                          "awayTeam": {"teamTricode": "MAC", "score": 38}})       # unmatched international preseason game
            return {"meta": {"code": 200}, "scoreboard": {"gameDate": "2026-10-11", "games": games}}
        evs = []
        for g, st, q, secs in rows:
            disp = ("%d:%02d" % (int(secs) // 60, int(secs) % 60)) if secs >= 60 else ("%.1f" % secs)
            evs.append({"id": "40180%04d" % g["i"], "date": datetime.fromtimestamp(g["start"], timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
                        "competitions": [{"competitors": [
                            {"homeAway": "home", "score": str(g["hs"]), "team": {"abbreviation": self.ESPN_ABBR.get(g["ha"], g["ha"])}},
                            {"homeAway": "away", "score": str(g["as"]), "team": {"abbreviation": self.ESPN_ABBR.get(g["aa"], g["aa"])}}],
                            "status": {"displayClock": disp if st == "in" else "0:00", "period": q if st != "pre" else 0,
                                       "type": {"state": st, "completed": st == "post",
                                                "name": {"pre": "STATUS_SCHEDULED", "in": "STATUS_IN_PROGRESS", "post": "STATUS_FINAL"}[st],
                                                "description": {"pre": "Scheduled", "in": "In Progress", "post": "Final"}[st]}}}]})
        return {"events": evs}

    def _score_msg(self, g: dict, now: float) -> dict:
        el = now - g["start"]
        live, ended = 0 <= el < 140 * 60, el >= 140 * 60
        q = min(4, int(max(el, 0) // (30 * 60)) + 1)
        clock = max(0, 12 * 60 - int((max(el, 0) % (30 * 60)) / 2.5))
        return {"gameId": 5000 + g["i"], "leagueAbbreviation": "nba", "slug": self.slug(g),
                "homeTeam": g["ha"], "awayTeam": g["aa"],
                "status": "Final" if ended else ("InProgress" if live else "Scheduled"),
                # mirrors the live feed: first number = awayTeam
                "score": "%d-%d" % (g["as"], g["hs"]),
                "period": ("Q%d" % q) if live else ("FT" if ended else ""),
                "elapsed": "%02d:%02d" % (clock // 60, clock % 60) if live else "", "live": live, "ended": ended}

    def _sim(self, eng: "Engine", stop: threading.Event) -> None:
        tick = 0.2
        with self.lock:
            snap = [self._book_msg(g, k) for g in self.games for k in self.KEYS]
        eng.on_market_msg(snap)                                  # initial snapshot, list form (as live)
        for g in self.games:
            eng.on_score(self._score_msg(g, time.time()))
        while not stop.is_set():
            now = time.time()
            msgs: List[dict] = []
            scores: List[dict] = []
            with self.lock:
                for g in self.games:
                    el = now - g["start"]
                    live = 0 <= el < 140 * 60
                    sm = self._score_msg(g, now)
                    if live and self.rng.random() < 0.06:        # a basket roughly every 3s of sim
                        side = self.rng.choice(("hs", "as"))
                        g[side] += self.rng.choice((1, 2, 2, 3))
                        sm = self._score_msg(g, now)
                        lead = g["as"] - g["hs"]                 # outcome 0 = away team
                        prog = min(el / (130 * 60), 0.98)
                        g["px"]["ml"] = min(0.99, max(0.01, 1 / (1 + math.exp(-lead * (0.05 + 0.25 * prog)))))
                        scores.append(sm)
                    elif sm["period"] != g["per"]:
                        scores.append(sm)
                    g["per"] = sm["period"]
                    for k in self.KEYS:
                        if self.rng.random() > (0.5 if live else 0.08):
                            continue
                        if k != "ml":
                            g["px"][k] = min(0.97, max(0.03, g["px"][k] + self.rng.gauss(0, 0.004)))
                        else:
                            g["px"][k] = min(0.99, max(0.01, g["px"][k] + self.rng.gauss(0, 0.002)))
                        ob, oa, _, _ = g["q"][k]
                        nb, na, bsz, asz = self._mk_quote(g, k)
                        g["q"][k] = (nb, na, bsz, asz)
                        t0 = g["toks"][k][0]
                        chg = [{"asset_id": t0, "price": str(nb), "size": str(bsz), "side": "BUY", "best_bid": str(nb), "best_ask": str(na)},
                               {"asset_id": t0, "price": str(na), "size": str(asz), "side": "SELL", "best_bid": str(nb), "best_ask": str(na)}]
                        if ob != nb:   # old best level emptied
                            chg.append({"asset_id": t0, "price": str(ob), "size": "0", "side": "BUY", "best_bid": str(nb), "best_ask": str(na)})
                        if oa != na:
                            chg.append({"asset_id": t0, "price": str(oa), "size": "0", "side": "SELL", "best_bid": str(nb), "best_ask": str(na)})
                        msgs.append({"event_type": "price_change", "market": "0x", "price_changes": chg,
                                     "timestamp": str(int(now * 1000))})
                        if self.rng.random() < 0.3:
                            msgs.append({"event_type": "last_trade_price", "asset_id": t0, "price": str(round(g["px"][k], 3)),
                                         "size": "100", "side": "BUY"})
            for sm in scores:
                self.ws_msgs += 1
                self.ws_ts = now
                eng.on_score(sm)
            eng.on_score({"gameId": 1, "leagueAbbreviation": "nfl", "slug": "nfl-x", "live": True})  # must be ignored
            eng.on_score({"garbage": True})                                                          # must be ignored
            if msgs:
                self.mws_msgs += len(msgs)
                self.mws_ts = now
                eng.on_market_msg(msgs)
            eng.on_market_msg({"event_type": "tick_size_change", "asset_id": "x"})                  # ignored type
            stop.wait(tick)


# ----------------------------------------------------------------------------- parsing
def classify(m: dict, q: str) -> str:
    t = str(m.get("sportsMarketType") or "").strip().lower()
    if t == "moneyline":
        return "moneyline"
    if t in ("spread", "spreads"):
        return "spreads"
    if t in ("total", "totals"):
        return "totals"
    if t:
        return t                       # e.g. first_half_moneyline, points, rebounds ...
    ql = q.lower()
    if "spread" in ql:
        return "spreads"
    if "o/u" in ql or "over/under" in ql:
        return "totals"
    if " vs" in ql:
        return "moneyline"
    return "other"


_LINE_PAREN = re.compile(r"\(\s*([+-]?\d+(?:\.\d+)?)\s*\)")
_LINE_OU = re.compile(r"o/u\s*([+-]?\d+(?:\.\d+)?)", re.I)


def parse_line(q: str) -> Optional[float]:
    m = _LINE_PAREN.search(q) or _LINE_OU.search(q)
    return _f(m.group(1)) if m else None


def parse_event(ev: dict) -> Optional[dict]:
    eid = str(ev.get("id") or "").strip()
    if not eid:
        return None
    markets = []
    starts = []
    for m in ev.get("markets") or []:
        if not isinstance(m, dict) or m.get("closed") is True:
            continue
        toks = [str(t) for t in _jlist(m.get("clobTokenIds")) if t is not None and str(t)]
        if not toks:
            continue
        outs = [str(o) for o in _jlist(m.get("outcomes"))] or ["Yes", "No"]
        q = str(m.get("question") or m.get("groupItemTitle") or "")
        mt = classify(m, q)
        line = _f(m.get("line"))
        if line is None and mt in ("spreads", "totals"):
            line = parse_line(q)
        px = [_f(p) for p in _jlist(m.get("outcomePrices"))]
        st = _ts(m.get("gameStartTime"))
        if st:
            starts.append(st)
        markets.append({"id": str(m.get("id") or toks[0]), "question": q, "type": mt, "line": line,
                        "outcomes": outs, "tokens": toks,
                        "cond": str(m.get("conditionId") or ""),
                        "g_bid": _f(m.get("bestBid")), "g_ask": _f(m.get("bestAsk")),
                        "g_last": _f(m.get("lastTradePrice")), "g_px": px[0] if px else None,
                        "volume": _f(m.get("volume")) or 0.0, "liquidity": _f(m.get("liquidity")) or 0.0})
    title = str(ev.get("title") or ev.get("slug") or eid)
    ml = next((x for x in markets if x["type"] == "moneyline"), None)
    if ml and len(ml["outcomes"]) >= 2:
        teams = ml["outcomes"][:2]
    else:
        parts = re.split(r"\s+vs\.?\s+", title, maxsplit=1)
        teams = parts if len(parts) == 2 else [title, ""]
    start = _ts(ev.get("startTime")) or (min(starts) if starts else None) or _ts(ev.get("startDate"))
    return {"id": eid, "slug": str(ev.get("slug") or ""), "title": title, "teams": teams, "start": start,
            "pair": game_pair(str(ev.get("slug") or ""), teams),
            "game_id": str(ev.get("gameId")) if ev.get("gameId") is not None else "",
            "g_live": bool(ev.get("live")), "g_ended": bool(ev.get("ended")),
            "g_score": ev.get("score"), "g_period": ev.get("period"), "g_elapsed": ev.get("elapsed"),
            "volume": _f(ev.get("volume")) or sum(x["volume"] for x in markets), "markets": markets}


def _levels(lst: Any) -> Dict[float, float]:
    out: Dict[float, float] = {}
    for l in lst or []:
        if isinstance(l, dict):
            p, s = _f(l.get("price")), _f(l.get("size"))
            if p is not None and s is not None and s > 0:
                out[p] = s
    return out


def best_from_book(b: dict) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """(bid, bid_sz, ask, ask_sz, last). Level order is not trusted -- take max bid / min ask."""
    bids, asks = _levels(b.get("bids")), _levels(b.get("asks"))
    bb = max(bids) if bids else None
    ba = min(asks) if asks else None
    return bb, (bids[bb] if bb is not None else None), ba, (asks[ba] if ba is not None else None), _f(b.get("last_trade_price"))


def mid_of(bid: Optional[float], ask: Optional[float], last: Optional[float], fallback: Optional[float],
           wide: float) -> Tuple[Optional[float], str]:
    """Polymarket display rule. Returns (mid, flag)  flag: ''=bid/ask mid, 'L'=last trade, 'G'=gamma px."""
    if bid is not None and ask is not None and ask >= bid:
        if ask - bid > wide + 1e-9 and last is not None:
            return last, "L"
        return (bid + ask) / 2.0, ""
    if last is not None:
        return last, "L"
    if fallback is not None:
        return fallback, "G"
    if bid is not None:
        return bid, "B"
    if ask is not None:
        return ask, "A"
    return None, ""


def _r4(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x, 4)


def side_quote(q: dict, flip: bool, d1: Optional[float] = None, d5: Optional[float] = None) -> dict:
    """Quote seen from outcome 0 (flip=False) or outcome 1 (flip=True) of a binary market.
    Polymarket books mirror: bid(B) = 1 - ask(A), ask(B) = 1 - bid(A), sizes swap, mid/last/deltas invert."""
    if not flip:
        return {"bid": q["bid"], "ask": q["ask"], "bsz": q["bsz"], "asz": q["asz"], "mid": _r4(q["mid"]),
                "last": q["last"], "flag": q["flag"], "d1": d1, "d5": d5}
    inv = lambda x: None if x is None else round(1.0 - x, 4)
    neg = lambda x: None if x is None else -x
    return {"bid": inv(q["ask"]), "ask": inv(q["bid"]), "bsz": q["asz"], "asz": q["bsz"], "mid": inv(q["mid"]),
            "last": inv(q["last"]), "flag": q["flag"], "d1": neg(d1), "d5": neg(d5)}


# ----------------------------------------------------------------------------- engine
class Engine:
    def __init__(self, source: Any):
        self.src = source
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.ver = 0                                      # bumped on every state change -> push
        self.stop = threading.Event()
        self.games: Dict[str, dict] = {}
        self.tok_index: Dict[str, Tuple[dict, dict]] = {}  # token0 -> (game, market)
        self.books: Dict[str, dict] = {}                  # token0 -> {"b":{px:sz}, "a":{px:sz}, "last", "ts"}
        self.quotes: Dict[str, dict] = {}                 # token0 -> top of book
        self.hist: Dict[str, deque] = {}                  # token0 -> deque[(ts, mid)]  1s buckets
        self.scores: Dict[str, dict] = {}                 # "slug:<slug>" / "gid:<id>" -> ws msg
        self.status_mem: Dict[str, Tuple[str, str]] = {}
        self.final_at: Dict[str, float] = {}
        self.last_alert: Dict[str, float] = {}
        self.log: deque = deque(maxlen=400)
        self._cache: Optional[Tuple[int, float, bytes]] = None
        self.ext: Dict[str, List[dict]] = {"nba": [], "espn": []}         # normalised score records per source
        self.ext_sig: Dict[tuple, Tuple[tuple, float]] = {}               # change detection -> rx = last change
        self.ext_h: Dict[str, dict] = {k: {"state": "IDLE", "ts": 0.0, "err": "", "n": 0, "ms": None, "polls": 0}
                                       for k in ("nba", "espn")}
        self.cfg_evt = threading.Event()
        self.rec = MinuteRecorder(self)
        self.h = {"gamma_ok": False, "gamma_ts": 0.0, "gamma_err": "", "gamma_n": 0, "gamma_seen": False,
                  "clob_ok": False, "clob_ts": 0.0, "clob_err": "", "clob_n": 0, "started": time.time()}

    # -- lifecycle
    def start(self) -> None:
        threading.Thread(target=self._gamma_loop, daemon=True, name="gamma").start()
        threading.Thread(target=self._rest_book_loop, daemon=True, name="books-rest").start()
        threading.Thread(target=self._ext_score_loop, daemon=True, name="ext-scores").start()
        threading.Thread(target=self.rec.run, daemon=True, name="minute-recorder").start()
        self.src.start(self, self.stop)

    # -- NBA.com / ESPN score polling (only the selected source is polled)
    def _ext_score_loop(self) -> None:
        while not self.stop.is_set():
            src = CFG["score_source"]
            t0 = time.time()
            if src in SCORE_PARSERS:
                self.poll_scores(src)
                wait = max(0.2, float(CFG[src + "_poll_s"]) - (time.time() - t0))
            else:
                wait = 1.0
            self.cfg_evt.wait(wait)          # a source switch in the CONFIG tab wakes this up at once
            self.cfg_evt.clear()

    def poll_scores(self, src: str) -> None:
        h = self.ext_h[src]
        t0 = time.time()
        try:
            data = self.src.fetch_scores(src)
        except Exception as e:
            with self.lock:
                h.update(state="DOWN", err=("%s: %s" % (type(e).__name__, e))[:160], ms=None)
                self._bump()
            return
        ms = (time.time() - t0) * 1000.0
        if data is None:                      # 304 not modified
            with self.lock:
                h.update(state="UP", ts=time.time(), err="", ms=ms, polls=h["polls"] + 1)
            return
        try:
            recs = SCORE_PARSERS[src](data)
        except Exception:
            with self.lock:
                h.update(state="DOWN", err="parse: " + _log_exc("parse " + src), ms=ms)
                self._bump()
            return
        self.apply_scores(src, recs, ms)

    def apply_scores(self, src: str, recs: List[dict], ms: Optional[float] = None) -> None:
        now = time.time()
        changed = False
        with self.lock:
            for r in recs:
                key = (src, r["id"] or (r["away"], r["home"], int((r["start"] or 0) // 3600)))
                sig = (r["status"], r["period"], r["clock"], r["ap"], r["hp"])
                prev = self.ext_sig.get(key)
                if prev and prev[0] == sig:
                    r["rx"] = prev[1]
                else:
                    r["rx"] = now
                    self.ext_sig[key] = (sig, now)
                    changed = True
            if len(recs) != len(self.ext[src]):
                changed = True
            self.ext[src] = recs
            h = self.ext_h[src]
            h.update(state="UP", ts=now, err="", n=len(recs), ms=ms, polls=h["polls"] + 1)
            if src == CFG["score_source"]:
                self.track_states()
            if changed:
                self._bump()

    def set_config(self, d: Any) -> Tuple[Dict[str, Any], List[str]]:
        clean, errs = validate_cfg(d)
        if clean:
            with self.lock:
                old = {k: CFG[k] for k in clean}
                CFG.update(clean)
                diff = ["%s %s -> %s" % (k, old[k], clean[k]) for k in clean if old[k] != clean[k]]
                if diff:
                    self.log.appendleft({"ts": time.time(), "kind": "CFG", "gid": "", "text": "; ".join(diff)})
                    if "score_source" in clean and old["score_source"] != clean["score_source"]:
                        self.status_mem.clear()      # don't log a burst of fake state changes on a source switch
                        self.track_states()
                self._bump()
            err = save_cfg_file()
            if err:
                errs.append("saved in memory only, file write failed: " + err)
            self.cfg_evt.set()
        return clean, errs

    def _bump(self) -> None:
        """call with self.lock held"""
        self.ver += 1
        self.cond.notify_all()

    def _push_up(self, attr: str) -> bool:
        return getattr(self.src, attr, "") in ("UP", "MOCK")

    def _gamma_loop(self) -> None:
        while not self.stop.is_set():
            self.refresh_events()
            fast = not self._push_up("ws_state") and any(
                self._status(g, time.time())[0] in ("LIVE", "INPLAY?") for g in list(self.games.values()))
            self.stop.wait(CFG["gamma_poll_fast_s"] if fast else CFG["gamma_poll_s"])

    def _rest_book_loop(self) -> None:
        last_full = 0.0
        while not self.stop.is_set():
            now = time.time()
            toks = self.active_tokens(now)
            fetch: List[str] = []
            if toks:
                if not self._push_up("mws_state") or now - last_full >= CFG["book_resync_s"]:
                    fetch, last_full = toks, now
                else:
                    with self.lock:
                        fetch = [t for t in toks if t not in self.quotes]   # new subs: seed immediately
            if fetch:
                self.refresh_books(fetch)
            with self.lock:
                if self.track_states():
                    self._bump()
            self.stop.wait(CFG["book_poll_s"])

    # -- logging
    def note(self, kind: str, text: str, gid: str = "") -> None:
        with self.lock:
            self.log.appendleft({"ts": time.time(), "kind": kind, "text": text, "gid": gid})
            self._bump()

    # -- discovery
    def refresh_events(self) -> None:
        try:
            evs = self.src.events()
        except Exception:
            with self.lock:
                self.h.update(gamma_ok=False, gamma_err=_log_exc("gamma"))
                self._bump()
            return
        parsed = {}
        for ev in evs or []:
            try:
                g = parse_event(ev) if isinstance(ev, dict) else None
            except Exception:
                _log_exc("parse_event")
                g = None
            if g:
                parsed[g["id"]] = g
        with self.lock:
            new = [g for k, g in parsed.items() if k not in self.games]
            self.games = parsed
            self.tok_index = {m["tokens"][0]: (g, m) for g in parsed.values() for m in g["markets"]}
            if new and self.h["gamma_seen"]:
                for g in new:
                    self.log.appendleft({"ts": time.time(), "kind": "NEW", "text": "listed: %s" % g["title"], "gid": g["id"]})
            self.h.update(gamma_ok=True, gamma_ts=time.time(), gamma_err="", gamma_n=len(parsed), gamma_seen=True)
            self.track_states()
            self._bump()

    def active_tokens(self, now: float) -> List[str]:
        out = []
        with self.lock:
            for g in self.games.values():
                st = self._status(g, now)[0]
                soon = g["start"] is not None and g["start"] - now <= CFG["book_window_min"] * 60
                if st in ("LIVE", "INPLAY?", "HALT") or (st == "PRE" and soon):
                    out.extend(m["tokens"][0] for m in g["markets"])
        return list(dict.fromkeys(out))   # de-dup, keep order

    # -- order book (push + REST share one code path)
    def _top(self, t: str, now: float, hint_bid: Optional[float] = None, hint_ask: Optional[float] = None) -> None:
        bk = self.books[t]
        bid = max(bk["b"]) if bk["b"] else None
        ask = min(bk["a"]) if bk["a"] else None
        if hint_bid is not None and hint_bid > 0:      # exchange-reported top wins over local book
            bid = hint_bid
        if hint_ask is not None and 0 < hint_ask < 1:
            ask = hint_ask
        self.quotes[t] = {"bid": bid, "ask": ask, "bsz": bk["b"].get(bid) if bid is not None else None,
                          "asz": bk["a"].get(ask) if ask is not None else None, "last": bk["last"], "ts": now}

    def _book(self, t: str, now: float) -> dict:
        bk = self.books.get(t)
        if bk is None:
            bk = self.books[t] = {"b": {}, "a": {}, "last": None, "ts": now}
        return bk

    def _set_book(self, t: str, bids: Any, asks: Any, last: Optional[float], now: float) -> None:
        bk = self._book(t, now)
        bk["b"], bk["a"], bk["ts"] = _levels(bids), _levels(asks), now
        if last is not None:
            bk["last"] = last
        self._top(t, now)

    def on_market_msg(self, data: Any) -> None:
        """CLOB market-channel frame(s): book / price_change / best_bid_ask / last_trade_price."""
        items = data if isinstance(data, list) else [data]
        now = time.time()
        touched = set()
        with self.lock:
            for d in items:
                if not isinstance(d, dict):
                    continue
                et = d.get("event_type") or d.get("type")
                if et == "book":
                    t = str(d.get("asset_id") or "")
                    if t in self.tok_index:
                        self._set_book(t, d.get("bids"), d.get("asks"), _f(d.get("last_trade_price")), now)
                        touched.add(t)
                elif et == "price_change":
                    chs = d.get("price_changes")
                    if chs is None and isinstance(d.get("changes"), list):          # legacy frame layout
                        chs = [dict(c, asset_id=d.get("asset_id")) for c in d["changes"] if isinstance(c, dict)]
                    hints: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
                    for c in chs or []:
                        if not isinstance(c, dict):
                            continue
                        t = str(c.get("asset_id") or "")
                        if t not in self.tok_index:
                            continue
                        bk = self._book(t, now)
                        px, sz, side = _f(c.get("price")), _f(c.get("size")), str(c.get("side") or "").upper()
                        if px is not None and sz is not None and side in ("BUY", "SELL"):
                            lv = bk["b"] if side == "BUY" else bk["a"]
                            if sz <= 0:
                                lv.pop(px, None)
                            else:
                                lv[px] = sz
                        bk["ts"] = now
                        hints[t] = (_f(c.get("best_bid")), _f(c.get("best_ask")))
                    for t, (hb, ha) in hints.items():                              # top once per frame
                        self._top(t, now, hb, ha)
                        touched.add(t)
                elif et == "best_bid_ask":
                    t = str(d.get("asset_id") or "")
                    if t in self.tok_index:
                        self._book(t, now)
                        self._top(t, now, _f(d.get("best_bid")), _f(d.get("best_ask")))
                        touched.add(t)
                elif et == "last_trade_price":
                    t = str(d.get("asset_id") or "")
                    if t in self.tok_index:
                        bk = self._book(t, now)
                        bk["last"] = _f(d.get("price"))
                        self.rec.on_trade(t, bk["last"], _f(d.get("size")), str(d.get("side") or "").upper(), now)
                        q = self.quotes.get(t)
                        if q:
                            q["last"], q["ts"] = bk["last"], now
                        else:
                            self._top(t, now)
                        touched.add(t)
            for t in touched:
                self._on_quote(t, now)
            if touched:
                self._bump()

    def refresh_books(self, tokens: Optional[List[str]] = None) -> None:
        toks = tokens if tokens is not None else self.active_tokens(time.time())
        if not toks:
            return
        try:
            books = self.src.books(toks)
        except Exception:
            with self.lock:
                self.h.update(clob_ok=False, clob_err=_log_exc("clob"))
                self._bump()
            return
        now = time.time()
        with self.lock:
            fresh = set()
            for b in books or []:
                if not isinstance(b, dict):
                    continue
                t = str(b.get("asset_id") or "")
                if t in self.tok_index:
                    self._set_book(t, b.get("bids"), b.get("asks"), _f(b.get("last_trade_price")), now)
                    fresh.add(t)
            for t in fresh:
                self._on_quote(t, now)
            self.h.update(clob_ok=True, clob_ts=now, clob_err="", clob_n=len(books or []))
            self._bump()

    def _on_quote(self, t: str, now: float) -> None:
        """history (1s buckets) + move alert. call with lock held."""
        ix = self.tok_index.get(t)
        if not ix:
            return
        g, m = ix
        q = self.quote(m, now)
        if q["mid"] is None:
            return
        self.rec.on_mid(t, q["mid"], now)
        dq = self.hist.setdefault(t, deque())
        if dq and now - dq[-1][0] < CFG["hist_bucket_s"]:
            dq[-1] = (dq[-1][0], q["mid"])
        else:
            dq.append((now, q["mid"]))
        while dq and dq[0][0] < now - CFG["hist_keep_s"]:
            dq.popleft()
        if m["type"] == "moneyline":
            d = self.delta(t, CFG["alert_window_s"], now)
            if d is not None and abs(d) >= CFG["alert_pts"] - 1e-9 and now - self.last_alert.get(t, 0) > CFG["alert_window_s"]:
                self.last_alert[t] = now
                self.log.appendleft({"ts": now, "kind": "MOVE", "gid": g["id"], "text": "%s %s %+.1f pts in %ds -> %.1f%%" % (
                    g["title"], m["outcomes"][0], d * 100, CFG["alert_window_s"], q["mid"] * 100)})

    # -- scores
    def on_score(self, msg: Any) -> None:
        if not isinstance(msg, dict) or str(msg.get("leagueAbbreviation", "")).lower() != "nba":
            return
        msg = dict(msg)
        msg["_rx"] = time.time()
        with self.lock:
            if msg.get("slug"):
                self.scores["slug:" + str(msg["slug"])] = msg
            if msg.get("gameId") is not None:
                self.scores["gid:" + str(msg["gameId"])] = msg
            self.track_states()
            self._bump()

    # -- derived values (call under lock)
    def score_for(self, g: dict) -> Optional[dict]:
        return self.scores.get("slug:" + g["slug"]) or (self.scores.get("gid:" + g["game_id"]) if g["game_id"] else None)

    def _pm_rec(self, g: dict) -> Optional[dict]:
        """Polymarket Sports-WS message for this game, normalised like the NBA/ESPN records."""
        sc = self.score_for(g)
        if not sc:
            return None
        raw = str(sc.get("status") or "")
        if sc.get("ended") or raw.lower() in FINAL_STATUS:
            st = "FINAL"
        elif raw.lower() in HALT_STATUS:
            st = "HALT"
        elif sc.get("live") or raw == "InProgress":
            st = "LIVE"
        else:
            st = "PRE"
        rec = {"src": "pm", "status": st, "period": str(sc.get("period") or ""), "clock": norm_clock(sc.get("elapsed")),
               "text": self._score_text(sc), "rx": sc["_rx"], "away": sc.get("awayTeam"), "home": sc.get("homeTeam"),
               "ap": None, "hp": None}
        sp = split_score(sc.get("score"), sc.get("homeTeam"), sc.get("awayTeam"), CFG["score_order"])
        if sp:
            rec["ap"], rec["hp"] = sp[1], sp[3]
        return rec

    def orient(self, g: dict) -> Tuple[Optional[int], Optional[str], Optional[dict]]:
        """(index of the HOME team among the moneyline outcomes, who said so, record).
        Home/away comes from NBA.com / ESPN records first (explicit), then Polymarket WS home/awayTeam.
        Returns (None, None, None) when unknown -- the UI then marks the side '?' instead of guessing."""
        ml = next((m for m in g["markets"] if m["type"] == "moneyline"), None)
        if not ml or len(ml["outcomes"]) < 2:
            return None, None, None
        oc = [tri(o) for o in ml["outcomes"][:2]]
        cands = [self._ext_match(k, g) for k in (CFG["score_source"], "nba", "espn") if k in SCORE_PARSERS]
        cands.append(self._pm_rec(g))
        for r in cands:
            if not r:
                continue
            h, a = tri(r.get("home")), tri(r.get("away"))
            if h and a and h in oc and a in oc and h != a:
                return oc.index(h), r["src"], r
        return None, None, None

    def _ext_match(self, src: str, g: dict) -> Optional[dict]:
        pair = g.get("pair")
        if not pair:
            return None
        best, bd = None, 9e18
        for r in self.ext.get(src, ()):
            if r["pair"] != pair:
                continue
            d = abs(r["start"] - g["start"]) if r["start"] and g["start"] else 0.0
            if d < bd:
                best, bd = r, d
        return best if best is not None and bd <= 18 * 3600 else None

    def score_rec(self, g: dict) -> Optional[dict]:
        sel = CFG["score_source"]
        if sel in SCORE_PARSERS:
            r = self._ext_match(sel, g)
            if r:
                return r
            if not CFG["score_fallback"]:
                return None
        return self._pm_rec(g)

    def _status(self, g: dict, now: float) -> Tuple[str, str, str, str, Optional[dict]]:
        """(status, period, clock, score_text, score_record)"""
        rec = self.score_rec(g)
        if rec:
            return rec["status"], rec["period"], rec["clock"], rec["text"], rec
        if g["g_ended"]:
            return "FINAL", str(g["g_period"] or ""), "", str(g["g_score"] or ""), None
        if g["g_live"]:
            return "LIVE", str(g["g_period"] or ""), str(g["g_elapsed"] or ""), str(g["g_score"] or ""), None
        if g["start"] is not None and now >= g["start"]:
            return "INPLAY?", "", "", "", None
        return "PRE", "", "", "", None

    @staticmethod
    def _score_text(sc: dict) -> str:
        p = split_score(sc.get("score"), sc.get("homeTeam"), sc.get("awayTeam"), CFG["score_order"])
        if not p:
            return str(sc.get("score") or "")
        return "%s %d - %d %s" % (p[0], p[1], p[3], p[2])

    def track_states(self) -> bool:
        """Log status / period changes. call with lock held. Returns True if anything was logged."""
        now = time.time()
        changed = False
        for g in self.games.values():
            st, per, _clk, score, _sc = self._status(g, now)
            if st == "FINAL":
                self.final_at.setdefault(g["id"], now)
            prev = self.status_mem.get(g["id"])
            self.status_mem[g["id"]] = (st, per)
            if prev is None or prev == (st, per):
                continue
            a = prev[0] + ((" " + prev[1]) if prev[1] else "")
            b = st + ((" " + per) if per else "")
            self.log.appendleft({"ts": now, "kind": "STATE", "gid": g["id"], "text": "%s  %s -> %s  %s" % (g["title"], a, b, score)})
            changed = True
        return changed

    def quote(self, m: dict, now: float) -> dict:
        t = m["tokens"][0]
        q = self.quotes.get(t)
        if q and now - q["ts"] <= CFG["book_fresh_s"]:
            bid, ask, bsz, asz, last, src = q["bid"], q["ask"], q["bsz"], q["asz"], q["last"], "BOOK"
            if last is None:
                last = m["g_last"]
        else:
            bid, ask, bsz, asz, last, src = m["g_bid"], m["g_ask"], None, None, m["g_last"], "GAMMA"
        mid, flag = mid_of(bid, ask, last, m["g_px"], CFG["wide_spread"])
        return {"bid": bid, "ask": ask, "bsz": bsz, "asz": asz, "last": last, "mid": mid, "flag": flag, "src": src,
                "age": (now - q["ts"]) if q else None}

    @staticmethod
    def _last_le(dq: deque, cut: float) -> int:
        """index of the last point with ts <= cut (caller guarantees dq[0].ts <= cut). O(log n)."""
        lo, hi = 0, len(dq) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if dq[mid][0] <= cut:
                lo = mid
            else:
                hi = mid - 1
        return lo

    def delta(self, token: str, window: float, now: float) -> Optional[float]:
        dq = self.hist.get(token)
        if not dq:
            return None
        cut = now - window
        if dq[0][0] > cut:
            return None                   # not enough history yet
        return dq[-1][1] - dq[self._last_le(dq, cut)][1]

    def spark(self, token: str, now: float, secs: float = 1800, n: int = 120) -> List[float]:
        dq = self.hist.get(token)
        if not dq:
            return []
        cut = now - secs
        i0 = 0 if dq[0][0] > cut else self._last_le(dq, cut)
        L = len(dq)
        step = max(1, (L - i0) // n)
        pts = [round(dq[i][1], 4) for i in range(i0, L, step)]
        if (L - 1 - i0) % step:
            pts.append(round(dq[-1][1], 4))
        return pts

    @staticmethod
    def _main_line(ms: List[dict], quotes: List[dict]) -> Optional[int]:
        """index of the line whose price is closest to 50/50 (the 'main' line)."""
        best, bi = 9.0, None
        for i, q in enumerate(quotes):
            if q["mid"] is not None and abs(q["mid"] - 0.5) < best:
                best, bi = abs(q["mid"] - 0.5), i
        if bi is None and ms:
            bi = 0
        return bi

    def _row(self, g: dict, now: float) -> dict:
        st, per, clk, score, sc = self._status(g, now)
        if st == "FINAL":
            self.final_at.setdefault(g["id"], now)
        row = {"id": g["id"], "slug": g["slug"], "title": g["title"], "teams": g["teams"], "start": g["start"],
               "status": st, "period": per, "clock": clk, "score": score, "volume": g["volume"],
               "score_age": (now - sc["rx"]) if sc and sc.get("rx") else None, "score_src": sc["src"] if sc else "", "ml": None, "spread": None, "total": None, "nmk": len(g["markets"])}
        ml = next((m for m in g["markets"] if m["type"] == "moneyline"), None)
        if ml:
            q = self.quote(ml, now)
            t = ml["tokens"][0]
            row["ml"] = {"a": ml["outcomes"][0], "b": ml["outcomes"][1] if len(ml["outcomes"]) > 1 else "",
                         "mid": q["mid"], "bid": q["bid"], "ask": q["ask"], "flag": q["flag"], "src": q["src"],
                         "age": q["age"], "d1": self.delta(t, 60, now), "d5": self.delta(t, 300, now),
                         "spark": self.spark(t, now)}
        mains = {}
        for key, typ in (("spread", "spreads"), ("total", "totals")):
            ms = [m for m in g["markets"] if m["type"] == typ]
            qs = [self.quote(m, now) for m in ms]
            i = self._main_line(ms, qs)
            if i is not None:
                mains[key] = (ms[i], qs[i])
                row[key] = {"line": ms[i]["line"], "out": ms[i]["outcomes"][0], "mid": qs[i]["mid"], "flag": qs[i]["flag"], "n": len(ms)}
        row["sides"], row["orient"] = self._sides(g, ml, mains, sc, now)
        return row

    def _sides(self, g: dict, ml: Optional[dict], mains: dict, sc: Optional[dict], now: float) -> Tuple[List[dict], Optional[str]]:
        """Two entries ordered [away, home] when known (else [outcome A, outcome B]), each seen from that team:
        ML bid/ask/sizes/mid/deltas, that team's spread line & price, total side (away row = Over, home row = Under
        - sportsbook convention), and that team's points."""
        if not ml or len(ml["outcomes"]) < 2:
            return [], None
        hidx, osrc, orec = self.orient(g)
        q = self.quote(ml, now)
        t = ml["tokens"][0]
        d1, d5 = self.delta(t, 60, now), self.delta(t, 300, now)
        order = [1 - hidx, hidx] if hidx is not None else [0, 1]
        pts = {}
        for r in (sc, orec):                                    # points by tricode from the displayed record
            if r and r.get("ap") is not None and tri(r.get("away")) and tri(r.get("home")):
                pts = {tri(r["away"]): r["ap"], tri(r["home"]): r["hp"]}
                break
        out = []
        for pos, i in enumerate(order):
            name = ml["outcomes"][i]
            tc = tri(name)
            sd = {"team": name, "tc": tc or "", "role": (("away", "home")[pos] if hidx is not None else None),
                  "idx": i, "flip": i == 1, "pts": pts.get(tc) if tc else None}
            sd.update(side_quote(q, i == 1, d1, d5))
            if "spread" in mains:
                m, sq = mains["spread"]
                o0 = tri(m["outcomes"][0]) if m["outcomes"] else None
                mine0 = (o0 == tc) if (o0 and tc) else (i == 0)          # unknown names -> same order as ML
                ln = m["line"]
                sd["spr"] = {"line": (ln if mine0 else (None if ln is None else -ln)),
                             "mid": _r4(sq["mid"]) if mine0 else (None if sq["mid"] is None else round(1 - sq["mid"], 4)),
                             "flag": sq["flag"]}
            if "total" in mains:
                m, tq = mains["total"]
                over_first = str(m["outcomes"][0]).lower().startswith("o") if m["outcomes"] else True
                p_over = tq["mid"] if over_first else (None if tq["mid"] is None else 1 - tq["mid"])
                ou = "O" if pos == 0 else "U"
                sd["tot"] = {"ou": ou, "line": m["line"],
                             "mid": _r4(p_over if ou == "O" else (None if p_over is None else 1 - p_over)), "flag": tq["flag"],
                             "over": _r4(p_over)}
            out.append(sd)
        return out, osrc

    def snapshot(self) -> dict:
        now = time.time()
        with self.lock:
            rows = [self._row(g, now) for g in self.games.values()]
            live, upcoming = [], []
            for r in rows:
                if r["status"] in ("LIVE", "INPLAY?", "HALT"):
                    live.append(r)
                elif r["status"] == "FINAL":
                    if now - self.final_at.get(r["id"], now) <= CFG["final_keep_min"] * 60:
                        live.append(r)
                elif r["start"] is None or r["start"] - now <= CFG["upcoming_hours"] * 3600:
                    upcoming.append(r)
            order = {"LIVE": 0, "HALT": 1, "INPLAY?": 2, "FINAL": 3}
            live.sort(key=lambda r: (order.get(r["status"], 9), r["start"] or 0))
            upcoming.sort(key=lambda r: r["start"] or 9e18)
            h = dict(self.h)
            for k in ("ws_state", "ws_err", "ws_msgs", "ws_ts", "mws_state", "mws_err", "mws_msgs", "mws_ts", "mws_subs"):
                h[k] = getattr(self.src, k, None)
            h.update(source=self.src.name, series=getattr(self.src, "series_id", None) or "")
            sel = CFG["score_source"]
            h["score_source"] = sel
            h["ext"] = {k: dict(v) for k, v in self.ext_h.items()}
            h["matched"] = {k: sum(1 for g in self.games.values() if self._ext_match(k, g)) for k in SCORE_PARSERS}
            h["pm_matched"] = sum(1 for g in self.games.values() if self.score_for(g))
            h["pairs"] = sum(1 for g in self.games.values() if g.get("pair"))
            h["rec"] = dict(self.rec.h)
            return {"now": now, "ver": self.ver, "version": VERSION, "cfg": CFG, "health": h, "live": live,
                    "upcoming": upcoming, "log": list(self.log)[:200]}

    def snapshot_json(self) -> Tuple[int, bytes]:
        """Cached per state version (and at most 1s old) so N browser streams cost one build."""
        with self.lock:
            c = self._cache
            if c and c[0] == self.ver and time.time() - c[1] < 1.0:
                return c[0], c[2]
            ver = self.ver
            body = json.dumps(self.snapshot(), default=str, allow_nan=False, separators=(",", ":")).encode("utf-8")
            self._cache = (ver, time.time(), body)
            return ver, body

    def game_detail(self, gid: str) -> Optional[dict]:
        now = time.time()
        with self.lock:
            g = self.games.get(gid)
            if not g:
                return None
            rank = {"moneyline": 0, "spreads": 1, "totals": 2}
            out = []
            for m in sorted(g["markets"], key=lambda m: (rank.get(m["type"], 3), m["type"], m["line"] if m["line"] is not None else 0)):
                q = self.quote(m, now)
                t = m["tokens"][0]
                d1, d5 = self.delta(t, 60, now), self.delta(t, 300, now)
                o = []
                for i, nm in enumerate(m["outcomes"][:2]):
                    x = {"name": nm, "tc": tri(nm) or "", "flip": i == 1,
                         "line": (m["line"] if i == 0 or m["type"] != "spreads" or m["line"] is None else -m["line"])}
                    x.update(side_quote(q, i == 1, d1, d5))
                    o.append(x)
                out.append({"o": o, "type": m["type"], "line": m["line"], "q": m["question"], "out0": m["outcomes"][0],
                            "out1": m["outcomes"][1] if len(m["outcomes"]) > 1 else "",
                            "bid": q["bid"], "ask": q["ask"], "bsz": q["bsz"], "asz": q["asz"], "last": q["last"],
                            "mid": q["mid"], "flag": q["flag"], "src": q["src"], "age": q["age"],
                            "d1": self.delta(t, 60, now), "d5": self.delta(t, 300, now),
                            "vol": m["volume"], "liq": m["liquidity"], "spark": self.spark(t, now, 1800, 90)})
            hidx, osrc, orec = self.orient(g)
            ml = next((m for m in g["markets"] if m["type"] == "moneyline"), None)
            home_tc = tri(ml["outcomes"][hidx]) if (ml and hidx is not None) else None
            away_tc = tri(ml["outcomes"][1 - hidx]) if (ml and hidx is not None) else None
            return {"id": gid, "title": g["title"], "slug": g["slug"], "markets": out,
                    "home_tc": home_tc or "", "away_tc": away_tc or "", "orient": osrc}


# ----------------------------------------------------------------------------- web
ENGINE: Optional[Engine] = None


class Handler(BaseHTTPRequestHandler):
    server_version = "NBALive/" + VERSION

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet
        pass

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj: Any, code: int = 200) -> None:
        self._send(code, json.dumps(obj, default=str, allow_nan=False).encode("utf-8"), "application/json")

    def _stream(self, detail_id: str) -> None:
        """Server-Sent Events: push on every state change (coalesced to push_min_ms), 1/s heartbeat."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        self.wfile.write(b"retry: 1000\n\n")
        self.wfile.flush()
        eng = ENGINE
        last_ver, last_sent = -1, 0.0
        while not eng.stop.is_set():
            with eng.cond:
                if eng.ver == last_ver:
                    eng.cond.wait(timeout=1.0)      # wake on change, or 1/s so ages/time-states refresh
            gap = CFG["push_min_ms"] / 1000.0 - (time.time() - last_sent)
            if gap > 0:
                time.sleep(gap)                      # coalesce bursts -> at most 1 push per push_min_ms
            ver, body = eng.snapshot_json()
            if detail_id:
                det = json.dumps(eng.game_detail(detail_id), default=str, allow_nan=False, separators=(",", ":")).encode("utf-8")
                payload = b'{"snap":' + body + b',"detail":' + det + b'}'
            else:
                payload = b'{"snap":' + body + b'}'
            self.wfile.write(b"data: " + payload + b"\n\n")
            self.wfile.flush()
            last_ver, last_sent = ver, time.time()

    def do_POST(self) -> None:  # noqa: N802
        u = urllib.parse.urlparse(self.path)
        try:
            if u.path != "/api/config":
                self._json({"error": "not found"}, 404)
                return
            n = int(self.headers.get("Content-Length") or 0)
            if n <= 0 or n > 10000:
                self._json({"ok": False, "errors": ["bad body size"]}, 400)
                return
            try:
                body = json.loads(self.rfile.read(n).decode("utf-8"))
            except ValueError:
                self._json({"ok": False, "errors": ["body is not JSON"]}, 400)
                return
            clean, errs = ENGINE.set_config(body)
            self._json({"ok": not errs, "applied": clean, "errors": errs, "cfg": {k: CFG[k] for k in EDITABLE}},
                       200 if clean or not errs else 400)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception:
            try:
                self._json({"error": _log_exc("http post")}, 500)
            except Exception:
                pass

    def do_GET(self) -> None:  # noqa: N802
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        try:
            if u.path in ("/", "/index.html"):
                self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
            elif u.path == "/api/stream":
                self._stream((qs.get("detail") or [""])[0])
            elif u.path == "/api/state":
                self._send(200, ENGINE.snapshot_json()[1], "application/json")
            elif u.path == "/api/game":
                d = ENGINE.game_detail((qs.get("id") or [""])[0])
                self._json(d if d else {"error": "unknown game id"}, 200 if d else 404)
            elif u.path == "/api/health":
                self._json(ENGINE.snapshot()["health"])
            else:
                self._json({"error": "not found"}, 404)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception:
            try:
                self._json({"error": _log_exc("http")}, 500)
            except Exception:
                pass


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NBA Live Monitor</title>
<style>
:root{--bg:#0b0e13;--panel:#121722;--panel2:#171d2a;--line:#232b3b;--fg:#d7dde8;--dim:#7d8799;--acc:#4fa3ff;
--up:#27c281;--dn:#ff5c6c;--warn:#f5b041;--live:#ff3b4e;--mono:Consolas,"Cascadia Mono","SF Mono",Menlo,monospace}
*{box-sizing:border-box}html,body{margin:0;background:var(--bg);color:var(--fg);font:12px/1.35 var(--mono)}
header{display:flex;align-items:center;gap:10px;padding:8px 14px;background:var(--panel);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:5;flex-wrap:wrap}
header b{color:var(--acc);letter-spacing:.08em}.pill{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border:1px solid var(--line);border-radius:10px;color:var(--dim)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--dim)}.ok .dot{background:var(--up)}.bad .dot{background:var(--dn)}.mid .dot{background:var(--warn)}
#clock{margin-left:auto;color:var(--dim)}
nav{display:flex;gap:2px;padding:6px 14px 0;background:var(--bg)}nav button{background:var(--panel);color:var(--dim);border:1px solid var(--line);border-bottom:none;padding:5px 14px;font:inherit;cursor:pointer;border-radius:4px 4px 0 0}
nav button.on{color:var(--fg);background:var(--panel2);border-color:var(--acc)}nav .n{color:var(--acc);margin-left:5px}
main{padding:0 14px 30px}.wrap{overflow-x:auto;border:1px solid var(--line);background:var(--panel2)}
table{border-collapse:collapse;width:100%;white-space:nowrap}th{position:sticky;top:0;background:#1b2231;color:var(--dim);font-weight:normal;text-align:right;padding:5px 8px;border-bottom:1px solid var(--line)}
td{padding:4px 8px;text-align:right;border-bottom:1px solid #1a2130}th.l,td.l{text-align:left}tr.g{cursor:pointer}tr.g:hover td{background:#1a2233}tr.sel td{background:#1d2840}
.st{display:inline-block;min-width:54px;text-align:center;padding:1px 4px;border-radius:3px;font-size:11px}
.LIVE{background:var(--live);color:#fff}.PRE{background:#26334a;color:#9fb3d1}.FINAL{background:#2a2f3a;color:var(--dim)}.INPLAY\?{background:#5a3b10;color:var(--warn)}.HALT{background:#5a3b10;color:var(--warn)}
.up{color:var(--up)}.dn{color:var(--dn)}.dim{color:var(--dim)}.big{font-size:13px;font-weight:bold}.fl{color:var(--warn);font-size:10px}
.age{color:var(--dim);font-size:10px;font-weight:normal;margin-left:4px}.age.old{color:var(--warn)}
td.fu{background:rgba(39,194,129,.30)!important}td.fd{background:rgba(255,92,108,.30)!important}
.det td{background:#0f141d;padding:8px 10px}.det table th{background:#141b28}.empty{padding:30px;text-align:center;color:var(--dim)}
.bar{display:inline-block;width:70px;height:6px;background:#2a3346;border-radius:3px;vertical-align:middle;margin-left:6px;overflow:hidden}.bar i{display:block;height:100%;background:var(--acc)}
.log td{text-align:left}.k-MOVE{color:var(--warn)}.k-STATE{color:var(--acc)}.k-NEW{color:var(--up)}.k-CFG{color:#c792ea}
.cfg td{text-align:left}.err{color:var(--dn)}.okc{color:var(--up)}
.tag{display:inline-block;font-size:9px;padding:0 4px;margin-left:5px;border-radius:3px;background:#26334a;color:#9fb3d1;vertical-align:middle}.tag.fb{background:#5a3b10;color:var(--warn)}
.form{padding:14px 16px;display:grid;gap:14px;max-width:980px}.form h3{margin:0 0 4px;font-size:12px;color:var(--acc);font-weight:normal;letter-spacing:.06em}
.srcs{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:8px}
.src{border:1px solid var(--line);border-radius:6px;padding:10px 12px;cursor:pointer;background:var(--panel)}.src:hover{border-color:#3a4760}.src.on{border-color:var(--acc);background:#14203a}
.src b.t{display:block;margin-bottom:3px}.src b{color:var(--fg)}.src small{color:var(--dim);display:block;line-height:1.4;white-space:normal}.src input{margin-right:6px}
.row{display:flex;gap:18px;flex-wrap:wrap;align-items:center}.row label{color:var(--dim)}.row select{width:auto!important}.row input[type=number],.row select{background:var(--panel);color:var(--fg);border:1px solid var(--line);border-radius:4px;padding:3px 6px;font:inherit;width:90px}
.btn{background:var(--acc);color:#071223;border:none;border-radius:4px;padding:6px 16px;font:inherit;font-weight:bold;cursor:pointer}.btn:disabled{opacity:.5;cursor:default}
#cfgmsg{min-height:16px}
.tb{display:flex;align-items:center;gap:4px;padding:6px 8px;border-bottom:1px solid var(--line);background:#141a26}.tb .hint{margin-left:10px}
.seg{background:var(--panel);color:var(--dim);border:1px solid var(--line);padding:3px 12px;font:inherit;cursor:pointer}.seg:first-of-type{border-radius:4px 0 0 4px}.seg:last-of-type{border-radius:0 4px 4px 0}
.seg.on{background:#1d2c4a;color:var(--fg);border-color:var(--acc)}
tbody.gb tr:first-child td{border-top:1px solid #2c3850}tbody.gb:hover td{background:#1a2233}tbody.gb.sel td{background:#1d2840}tbody.gb tr.det td{background:#0f141d}
td.tm{max-width:180px;overflow:hidden;text-overflow:ellipsis}.at{color:var(--dim)}
</style></head><body>
<header><b>NBA LIVE &middot; POLYMARKET</b><span id="src" class="pill"></span>
<span id="hP" class="pill"><span class="dot"></span>PUSH</span><span id="hW" class="pill"><span class="dot"></span>SCORES</span><span id="hB" class="pill"><span class="dot"></span>BOOKS</span><span id="hG" class="pill"><span class="dot"></span>GAMMA</span>
<span id="clock"></span></header>
<nav><button data-t="live" class="on">LIVE<span class="n" id="nL">0</span></button><button data-t="upcoming">UPCOMING<span class="n" id="nU">0</span></button><button data-t="log">LOG<span class="n" id="nG">0</span></button><button data-t="cfg">CONFIG</button></nav>
<main><div class="wrap" id="view"></div></main>
<script>
let VIEW=(()=>{try{return localStorage.getItem('nba_view')||'both'}catch(e){return 'both'}})(),CFGMSG=null,TAB='live',SEL=null,S=null,DET=null,ES=null,RECV=0,PEND=false;const PREV={},FL={},PT=[];
const $=s=>document.querySelector(s);
const nowMs=()=>performance.now();
const px=v=>v==null?'<span class="dim">-</span>':(v*100).toFixed(1);
const num=v=>v==null?'-':(v>=1e6?(v/1e6).toFixed(2)+'m':v>=1e3?(v/1e3).toFixed(0)+'k':v.toFixed(0));
const dl=v=>v==null?'<span class="dim">-</span>':`<span class="${v>0.0005?'up':v<-0.0005?'dn':'dim'}">${v>0?'+':''}${(v*100).toFixed(1)}</span>`;
const esc=s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
// Hong Kong time, independent of the PC's timezone setting (HK has no DST: always UTC+8)
const HK=t=>new Date(t*1000+8*3600*1000);const p2=n=>String(n).padStart(2,'0');const WD=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
const hkt=t=>{const d=HK(t);return `${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}:${p2(d.getUTCSeconds())}`};
const hkdt=t=>{const d=HK(t);return `${WD[d.getUTCDay()]} ${p2(d.getUTCDate())}/${p2(d.getUTCMonth()+1)} ${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}`};
function cd(t){const now=(S?S.now+el():Date.now()/1000),x=t-now;if(x<=0)return 'starting';const m=Math.floor(x/60),h=Math.floor(m/60),d=Math.floor(h/24);
 return d>0?`in ${d}d ${h%24}h`:h>0?`in ${h}h ${p2(m%60)}m`:`in ${m}m`}
const tm=t=>t?`${hkdt(t)} <span class="age ${t-(S?S.now+el():0)<3600?'old':''}">${cd(t)}</span>`:'-';
const ln=v=>v==null?'':(v>0?'+':'')+v;
const el=()=>(nowMs()-RECV)/1000;                       // seconds since this snapshot arrived
function age(a,warn){if(a==null)return '';const s=a+el();const t=s<60?s.toFixed(0)+'s':(s/60).toFixed(0)+'m';return `<span class="age ${warn&&s>warn?'old':''}">${t}</span>`}
function spark(a,w=110,h=22){if(!a||a.length<2)return '<span class="dim">·</span>';let mn=Math.min(...a),mx=Math.max(...a);if(mx-mn<0.01){const c=(mx+mn)/2;mn=c-0.005;mx=c+0.005}
const p=a.map((v,i)=>`${(i/(a.length-1)*w).toFixed(1)},${(h-2-(v-mn)/(mx-mn)*(h-4)).toFixed(1)}`).join(' ');const c=a[a.length-1]>=a[0]?'var(--up)':'var(--dn)';
return `<svg width="${w}" height="${h}" style="vertical-align:middle"><polyline fill="none" stroke="${c}" stroke-width="1.4" points="${p}"/></svg>`}
function pill(id,state,txt){const e=$(id);e.className='pill '+state;e.lastChild.textContent=' '+txt}
function fl(key,v){const p=PREV[key];PREV[key]=v;const t=nowMs();if(p!=null&&v!=null&&Math.abs(v-p)>=0.0005)FL[key]={c:v>p?'fu':'fd',u:t+900};const f=FL[key];return f&&f.u>t?f.c:''}
function hdr(){const h=S.health,n=S.now+el();const ag=t=>t?Math.max(0,n-t).toFixed(0)+'s':'never';
 $('#src').textContent=h.source+(h.series?' · series '+h.series:'')+' · '+S.version;
 const t=nowMs();while(PT.length&&PT[0]<t-5000)PT.shift();
 pill('#hP',ES&&ES.readyState===1?(el()<2.5?'ok':'mid'):'bad','PUSH '+(PT.length/5).toFixed(1)+'/s · '+(el()*1000).toFixed(0)+'ms');
 const ws=(st,ts)=>st==='UP'||st==='MOCK'?'ok':(st==='IDLE'||st==='OFF'||st==='CONN'||st==='INIT'?'mid':'bad');
 const ss=h.score_source;if(ss==='polymarket'){pill('#hW',ws(h.ws_state),'SCORES PM '+h.ws_state+(h.ws_ts?' '+ag(h.ws_ts):''));$('#hW').title=h.ws_err||''}
 else{const x=(h.ext||{})[ss]||{};pill('#hW',x.state==='UP'&&n-x.ts<Math.max(10,3*S.cfg[ss+'_poll_s'])?'ok':(x.state==='IDLE'?'mid':'bad'),'SCORES '+ss.toUpperCase()+' '+(x.ts?ag(x.ts):x.state)+(x.ms!=null?' · '+x.ms.toFixed(0)+'ms':''));$('#hW').title=x.err||''}
 pill('#hB',ws(h.mws_state),'BOOKS '+h.mws_state+(h.mws_subs?' ×'+h.mws_subs:'')+(h.mws_ts?' '+ag(h.mws_ts):''));
 pill('#hG',h.gamma_ok&&n-h.gamma_ts<90?'ok':'bad','GAMMA '+ag(h.gamma_ts));
 $('#hB').title=h.mws_err||h.clob_err||'';$('#hG').title=h.gamma_err||'';
 $('#nL').textContent=S.live.filter(r=>r.status!=='FINAL').length;$('#nU').textContent=S.upcoming.length;$('#nG').textContent=S.log.length}
function toolbar(){const H={both:'away row on top, home (@) below · each row = that team\'s book · recommended for market making',away:'one row per game · away team\'s book',home:'one row per game · home team\'s book'};
 return `<div class="tb"><span class="dim" style="margin-right:6px">VIEW</span>${['both','away','home'].map(v=>`<button class="seg ${VIEW===v?'on':''}" data-v="${v}">${v.toUpperCase()}</button>`).join('')}<span class="dim hint">${H[VIEW]}</span></div>`}
const SRCN={pm:'PM',nba:'NBA',espn:'ESPN'};
function srcTag(r){const want={polymarket:'pm',nba:'nba',espn:'espn'}[S.cfg.score_source];
 return r.score_src?`<span class="tag ${r.score_src!==want?'fb':''}" title="${r.score_src!==want?'fallback: chosen source has no match for this game':'score source'}">${SRCN[r.score_src]||r.score_src}</span>`:''}
function clkCell(r,live){const sa=live&&r.status==='LIVE'?age(r.score_age,null):'';
 return live?esc((r.period||'')+' '+(r.clock||''))+sa+srcTag(r):tm(r.start)}
function qcells(s,k){return `<td class="${fl('b'+k,s.bid)}">${px(s.bid)}</td><td class="big ${fl('m'+k,s.mid)}">${px(s.mid)}${s.flag?'<span class=fl>'+s.flag+'</span>':''}<span class="bar"><i style="width:${s.mid==null?0:(s.mid*100).toFixed(0)}%"></i></span></td><td class="${fl('a'+k,s.ask)}">${px(s.ask)}</td><td class="dim">${num(s.bsz)} × ${num(s.asz)}</td><td>${dl(s.d1)}</td><td>${dl(s.d5)}</td>`}
function sprCell(s){return s.spr&&s.spr.line!=null?`${ln(s.spr.line)} @ ${px(s.spr.mid)}${s.spr.flag?'<span class=fl>'+s.spr.flag+'</span>':''}`:'<span class=dim>-</span>'}
function totCell(s,ou){const t=s.tot;if(!t||t.line==null)return '<span class=dim>-</span>';ou=ou||t.ou;const m=t.over==null?null:(ou==='O'?t.over:1-t.over);return `${ou} ${t.line} @ ${px(m)}${t.flag?'<span class=fl>'+t.flag+'</span>':''}`}
function sideSpark(r,s,w){const a=(r.ml&&r.ml.spark)||[];return spark(s&&s.flip?a.map(v=>1-v):a,w)}
const UNK='<span class="tag fb" title="home/away unknown for this game - outcome order shown">?</span>';
function teamCell(r,s){return `${s.role==='home'?'<span class=at>@</span> ':''}${esc(s.team)}${r.orient?'':UNK}`}
function gameRows(rows,live){
 const tb=toolbar();
 if(!rows.length)return tb+`<div class="empty">${live?'No live NBA games right now.':'No NBA games listed in the next '+S.cfg.upcoming_hours+'h.'}</div>`;
 const both=VIEW==='both';const NC=both?14:15;
 let h=tb+`<table><thead><tr><th class="l">ST</th><th class="l">${live?'Q / CLK':'START (HKT)'}</th>${both?'<th class="l">TEAM</th><th>PTS</th>':'<th class="l">MATCHUP</th><th class="l">SCORE</th><th class="l">TEAM</th>'}
 <th>BID</th><th>MID</th><th>ASK</th><th>SIZE B×A</th><th>Δ1m</th><th>Δ5m</th><th class="l">30m</th><th class="l">SPREAD</th><th class="l">TOTAL</th><th>VOL</th></tr></thead>`;
 for(const r of rows){const sel=SEL===r.id;const sd=r.sides||[];
  h+=`<tbody class="gb ${sel?'sel':''}">`;
  const st=`<span class="st ${r.status}">${r.status}</span>`;
  const ck=`<td class="l ${fl('pc'+r.id,(r.period||'')+(r.clock||'')?hash((r.period||'')+(r.clock||'')):null)}" ${both&&sd.length?'rowspan="2"':''}>${clkCell(r,live)}</td>`;
  if(!sd.length){h+=`<tr class="g" data-id="${esc(r.id)}"><td class="l">${st}</td>${ck}<td class="l" colspan="${both?2:3}">${esc(r.title)} <span class=dim>(no moneyline)</span></td><td colspan="${NC-(both?5:6)}"></td><td>${num(r.volume)}</td></tr>`}
  else if(both){
   sd.forEach((s,k)=>{h+=`<tr class="g" data-id="${esc(r.id)}">`+(k===0?`<td class="l" rowspan="2">${st}</td>${ck}`:'')+
    `<td class="l tm" title="${esc(s.team)}">${teamCell(r,s)}</td><td class="big ${fl('sc'+r.id+k,s.pts)}">${s.pts==null?'<span class=dim>-</span>':s.pts}</td>${qcells(s,r.id+'_'+k)}
    <td class="l">${sideSpark(r,s,90)}</td><td class="l">${sprCell(s)}</td><td class="l">${totCell(s)}</td>`+(k===0?`<td rowspan="2">${num(r.volume)}</td>`:'')+'</tr>'})}
  else{const s=r.orient?sd.find(x=>x.role===VIEW):sd[VIEW==='away'?0:1];const aw=r.orient?sd[0]:null,hm=r.orient?sd[1]:null;
   h+=`<tr class="g" data-id="${esc(r.id)}"><td class="l">${st}</td>${ck}<td class="l">${aw?esc(aw.team)+' <span class=at>@</span> '+esc(hm.team):esc(r.title)}</td>
   <td class="l big ${fl('sc'+r.id,r.score?hash(r.score):null)}">${esc(r.score)||'<span class=dim>-</span>'}</td><td class="l tm">${teamCell(r,s)}</td>${qcells(s,r.id+'_'+VIEW)}
   <td class="l">${sideSpark(r,s,90)}</td><td class="l">${sprCell(s)}</td><td class="l">${totCell(s,'O')}</td><td>${num(r.volume)}</td></tr>`}
  if(sel)h+=`<tr class="det"><td colspan="${NC}">${detHtml()}</td></tr>`;
  h+='</tbody>'}
 return h+'</table>'}
function hash(s){let x=0;for(let i=0;i<s.length;i++)x=(x*31+s.charCodeAt(i))%1000003;return x/1000003*0.9+0.05}
function detPick(m){const o=m.o||[];if(o.length<2)return o;
 const isTot=m.type==='totals'||/^(over|under)$/i.test(o[0].name);
 if(VIEW==='both'){if(isTot)return /^o/i.test(o[0].name)?o:[o[1],o[0]];
  if(DET.away_tc&&o[1].tc===DET.away_tc)return [o[1],o[0]];return o}
 if(isTot)return [/^o/i.test(o[0].name)?o[0]:o[1]];
 const want=VIEW==='home'?DET.home_tc:DET.away_tc;const hit=want?o.find(x=>x.tc===want):null;
 return [hit||o[VIEW==='away'?0:1]]}
function detHtml(){if(!DET||DET.id!==SEL)return '<span class=dim>loading…</span>';
 let h=`<div style="margin-bottom:6px"><b>${esc(DET.title)}</b> <span class=dim>${esc(DET.slug)} · view ${VIEW.toUpperCase()} · each line = that outcome's book (B = 1 − A) · L = last trade (spread &gt; ${(S.cfg.wide_spread*100).toFixed(0)}c) · G = Gamma px${DET.orient?'':' · home/away unknown'}</span></div>
 <table><tr><th class="l">TYPE</th><th>LINE</th><th class="l">OUTCOME</th><th>BSZ</th><th>BID</th><th>MID</th><th>ASK</th><th>ASZ</th><th>LAST</th><th>Δ1m</th><th>Δ5m</th><th class="l">30m</th><th>VOL</th><th>LIQ</th><th class="l">SRC</th></tr>`;
 for(const m of DET.markets){const xs=detPick(m);const n=xs.length;
  xs.forEach((x,j)=>{const k=DET.id+m.type+m.line+x.name;const sp=x.flip?(m.spark||[]).map(v=>1-v):m.spark;
   const isHome=DET.home_tc&&x.tc===DET.home_tc;
   h+=`<tr>${j===0?`<td class="l" rowspan="${n}">${esc(m.type)}</td>`:''}<td>${x.line==null?'':(m.type==='spreads'?ln(x.line):x.line)}</td><td class="l">${isHome?'<span class=at>@</span> ':''}${esc(x.name)}</td>
   <td class=dim>${num(x.bsz)}</td><td class="${fl('db'+k,x.bid)}">${px(x.bid)}</td><td class="big ${fl('dm'+k,x.mid)}">${px(x.mid)}${x.flag?'<span class=fl>'+x.flag+'</span>':''}</td><td class="${fl('da'+k,x.ask)}">${px(x.ask)}</td><td class=dim>${num(x.asz)}</td><td>${px(x.last)}</td>
   <td>${dl(x.d1)}</td><td>${dl(x.d5)}</td><td class="l">${spark(sp,90,18)}</td>${j===0?`<td rowspan="${n}">${num(m.vol)}</td><td rowspan="${n}">${num(m.liq)}</td><td class="l dim" rowspan="${n}">${m.src}${age(m.age,30)}</td>`:''}</tr>`})}
 return h+'</table>'}
function logHtml(){if(!S.log.length)return '<div class="empty">No events yet. Moves ≥ '+(S.cfg.alert_pts*100).toFixed(0)+' pts / '+S.cfg.alert_window_s+'s on a moneyline and game state changes appear here.</div>';
 return '<table class="log"><tr><th class="l">TIME (HKT)</th><th class="l">KIND</th><th class="l">EVENT</th></tr>'+S.log.map(e=>`<tr><td>${hkt(e.ts)}</td><td class="k-${e.kind}">${e.kind}</td><td>${esc(e.text)}</td></tr>`).join('')+'</table>'}
const SRCDEF=[['nba','NBA.com live JSON','League\'s own feed behind NBA.com. Running game clock, quarter, score. Polled (no push). Free, unofficial endpoint.'],
 ['espn','ESPN scoreboard','ESPN public scoreboard API. Running clock, quarter, score. Polled. Free, unofficial endpoint. Good backup.'],
 ['polymarket','Polymarket Sports WS','Pushed instantly, but only on score / period change - clock does not tick between baskets.']];
function cfgForm(){const c=S.cfg;
 return `<div class="form" id="cfgform"><div><h3>QUARTER / CLOCK / SCORE SOURCE</h3><div class="srcs">${SRCDEF.map(([k,t,d])=>`<label class="src ${c.score_source===k?'on':''}"><b class="t"><input type="radio" name="src" value="${k}" ${c.score_source===k?'checked':''}>${t}</b><small>${d}</small><small id="st_${k}"></small></label>`).join('')}</div></div>
 <div><h3>OPTIONS</h3><div class="row">
 <label><input type="checkbox" id="c_fb" ${c.score_fallback?'checked':''}> fall back to Polymarket WS when the chosen source has no match</label>
 <label>NBA poll <input type="number" id="c_nba" min="1" max="30" step="0.5" value="${c.nba_poll_s}"> s</label>
 <label>ESPN poll <input type="number" id="c_espn" min="1" max="30" step="0.5" value="${c.espn_poll_s}"> s</label>
 <label>Polymarket score order <select id="c_ord"><option value="away_home" ${c.score_order==='away_home'?'selected':''}>first = away (observed)</option><option value="home_away" ${c.score_order==='home_away'?'selected':''}>first = home (docs)</option></select></label>
 <label>ML alert <input type="number" id="c_alert" min="0.5" max="50" step="0.5" value="${(c.alert_pts*100).toFixed(1)}"> pts</label>
 <label>Push max <input type="number" id="c_push" min="50" max="2000" step="10" value="${c.push_min_ms}"> ms</label>
 <label><input type="checkbox" id="c_rec" ${c.record_1m?'checked':''}> record 1-minute bars to nba_minute.db</label></div></div>
 <div class="row"><button class="btn" id="c_apply">APPLY &amp; SAVE</button><span id="cfgmsg" class="dim">Saved to nba_live_config.json next to the app; survives restarts.</span></div>
 <div><h3>STATUS</h3><div id="cfgstat"></div></div></div>`}
function cfgStat(){const h=S.health,n=S.now+el(),ag=t=>t?Math.max(0,n-t).toFixed(1)+'s ago':'never';
 for(const k of ['nba','espn']){const x=(h.ext||{})[k]||{},e=$('#st_'+k);if(e)e.innerHTML=k!==h.score_source&&!x.polls?'<span class=dim>idle (polled only when selected)</span>':`${x.state==='UP'?'<span class=okc>UP</span>':'<span class=err>'+esc(x.state)+'</span>'} · last poll ${ag(x.ts)} · ${x.ms!=null?x.ms.toFixed(0)+'ms':'-'} · ${x.n||0} games in feed · <b>${(h.matched||{})[k]||0}</b> matched${x.err?'<br><span class=err>'+esc(x.err)+'</span>':''}`}
 const e=$('#st_polymarket');if(e)e.innerHTML=`${h.ws_state==='UP'||h.ws_state==='MOCK'?'<span class=okc>'+h.ws_state+'</span>':'<span class=err>'+esc(h.ws_state)+'</span>'} · last msg ${ag(h.ws_ts)} · <b>${h.pm_matched||0}</b> matched${h.ws_err?'<br><span class=err>'+esc(h.ws_err)+'</span>':''}`;
 let r='';for(const[k,v]of Object.entries(S.cfg))r+=`<tr><td>${k}</td><td>${esc(v)}</td></tr>`;
 const er=[['gamma_err',h.gamma_err],['clob_err',h.clob_err],['ws_err',h.ws_err],['mws_err',h.mws_err]].filter(x=>x[1]).map(x=>`<tr><td>${x[0]}</td><td class=err>${esc(x[1])}</td></tr>`).join('');
 $('#cfgstat').innerHTML=`<table class="cfg"><tr><th class="l">KEY</th><th class="l">VALUE</th></tr><tr><td>1-minute recorder</td><td>${h.rec?`${h.rec.state==='UP'?'<span class=okc>UP</span>':esc(h.rec.state)} · ${h.rec.rows} rows · last write ${h.rec.last?hkt(h.rec.last)+' HKT':'never'} · ${esc(h.rec.path)}${h.rec.err?' <span class=err>'+esc(h.rec.err)+'</span>':''}`:'-'}</td></tr><tr><td>Polymarket games with a team pair</td><td>${h.pairs||0} of ${h.gamma_n}</td></tr><tr><td>book msgs / subs</td><td>${h.mws_msgs} / ${h.mws_subs}</td></tr>${er}${r}</table>`}
async function applyCfg(){const b=$('#c_apply'),m=$('#cfgmsg');const sel=document.querySelector('input[name=src]:checked');
 const body={score_source:sel?sel.value:S.cfg.score_source,score_fallback:$('#c_fb').checked,nba_poll_s:+$('#c_nba').value,espn_poll_s:+$('#c_espn').value,
  score_order:$('#c_ord').value,alert_pts:+$('#c_alert').value/100,push_min_ms:+$('#c_push').value,record_1m:$('#c_rec').checked};
 b.disabled=true;m.className='dim';m.textContent='applying…';
 try{const r=await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const j=await r.json();
  CFGMSG=j.errors&&j.errors.length?['err',j.errors.join(' · ')]:['okc','applied '+hkt(Date.now()/1000)+' HKT'+' · source = '+j.cfg.score_source]
  if(j.cfg)Object.assign(S.cfg,j.cfg);$('#view').innerHTML='';schedule()}
 catch(x){CFGMSG=['err','failed: '+x];m.className='err';m.textContent=CFGMSG[1]}finally{b.disabled=false}}
function render(){PEND=false;if(!S)return;hdr();const v=$('#view');
 if(TAB==='cfg'){if(!$('#cfgform')){v.innerHTML=cfgForm();
   if(CFGMSG){$('#cfgmsg').className=CFGMSG[0];$('#cfgmsg').textContent=CFGMSG[1]}
   $('#c_apply').onclick=applyCfg;v.querySelectorAll('input[name=src]').forEach(x=>x.onchange=()=>v.querySelectorAll('.src').forEach(l=>l.classList.toggle('on',l.contains(document.querySelector('input[name=src]:checked')))))}
  cfgStat();return}
 v.innerHTML=TAB==='live'?gameRows(S.live,true):TAB==='upcoming'?gameRows(S.upcoming,false):logHtml()}
function schedule(){if(!PEND){PEND=true;requestAnimationFrame(render)}}
function connect(){if(ES)ES.close();ES=new EventSource('/api/stream'+(SEL?'?detail='+encodeURIComponent(SEL):''));
 ES.onmessage=e=>{try{const p=JSON.parse(e.data);S=p.snap;DET=p.detail||null;RECV=nowMs();PT.push(RECV);schedule()}catch(x){}};
 ES.onerror=()=>{if(S)schedule()}}
document.querySelector('nav').onclick=e=>{const b=e.target.closest('button');if(!b)return;TAB=b.dataset.t;document.querySelectorAll('nav button').forEach(x=>x.classList.toggle('on',x===b));$('#view').innerHTML='';schedule()};
$('#view').onclick=e=>{const sb=e.target.closest('button.seg');if(sb){VIEW=sb.dataset.v;try{localStorage.setItem('nba_view',VIEW)}catch(x){};schedule();return}
 const tr=e.target.closest('tr.g');if(!tr)return;SEL=SEL===tr.dataset.id?null:tr.dataset.id;DET=null;schedule();connect()};
setInterval(()=>{$('#clock').textContent=hkdt(Date.now()/1000)+':'+p2(HK(Date.now()/1000).getUTCSeconds())+' HKT';if(S)schedule()},500);
connect();
</script></body></html>"""


# ----------------------------------------------------------------------------- main
def main(argv: Optional[List[str]] = None) -> None:
    global ENGINE
    ap = argparse.ArgumentParser(description="Polymarket NBA live monitor")
    ap.add_argument("--port", type=int, default=CFG["port"])
    ap.add_argument("--host", default="127.0.0.1", help="0.0.0.0 to share on the LAN")
    ap.add_argument("--mock", action="store_true", help="offline simulated feed")
    ap.add_argument("--ca", default=os.environ.get("NBA_CA_BUNDLE"), help="CA bundle for corporate TLS proxy")
    ap.add_argument("--alert-pts", type=float, default=None, help="ML move alert in prob units (0.05 = 5 pts)")
    ap.add_argument("--score-order", choices=("away_home", "home_away"), default=None,
                    help="which team the first number of Polymarket's 'a-b' score belongs to")
    ap.add_argument("--score-source", choices=("polymarket", "nba", "espn"), default=None,
                    help="quarter / clock / score source (also switchable live in the CONFIG tab)")
    ap.add_argument("--backfill", action="store_true", help="download 1-minute bars of finished games into nba_minute.db and exit")
    ap.add_argument("--days", type=float, default=7, help="backfill: how many days back (1-minute price history is kept ~7 days; trades longer)")
    ap.add_argument("--db", default=None, help="SQLite file (default nba_minute.db next to the app)")
    ap.add_argument("--types", default="", help="backfill: market types, e.g. moneyline,spreads,totals (default all)")
    a = ap.parse_args(argv)
    if a.backfill:
        http = Http(CFG["http_timeout"], a.ca)
        bf = Backfill(lambda url, params: http.get(url, params), a.db or MINUTE_DB)
        try:
            tot = bf.run(days=a.days, types=[x.strip() for x in a.types.split(",") if x.strip()] or None)
        except Exception as e:
            print("backfill FAILED: cannot list games from Polymarket (%s: %s)" % (type(e).__name__, str(e)[:160]))
            print("  check internet / proxy; behind a corporate TLS proxy add  --ca <bundle.pem>")
            sys.exit(1)
        print("done: %(games)d games, %(markets)d markets, %(trades)d trades -> %(trade_bars)d trade bars, "
              "%(price_pts)d 1-minute price points, %(errors)d errors" % tot)
        return
    for e in load_cfg_file():                 # CONFIG-tab choices persist in nba_live_config.json
        print("  config warning: " + e)
    CFG["port"] = a.port
    for k, v in (("alert_pts", a.alert_pts), ("score_order", a.score_order), ("score_source", a.score_source)):
        if v is not None:                     # explicit CLI flag beats the saved file
            CFG[k] = v
    src = MockSource() if a.mock else LiveSource(Http(CFG["http_timeout"], a.ca), a.ca)
    ENGINE = Engine(src)
    if a.db:
        ENGINE.rec.path = ENGINE.rec.h["path"] = a.db
    ENGINE.start()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    srv.daemon_threads = True
    print("NBA Live Monitor %s  [%s]  http://%s:%d   score source: %s   (Ctrl+C to quit)" % (
        VERSION, src.name, "localhost" if a.host in ("127.0.0.1", "0.0.0.0") else a.host, a.port, CFG["score_source"]))
    if not a.mock and websocket is None:
        print("  WARNING: websocket-client not installed -> no push feeds, REST polling only (pip install websocket-client)")
    try:
        srv.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        ENGINE.stop.set()
        srv.server_close()


if __name__ == "__main__":
    main()