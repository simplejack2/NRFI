"""
NRFI Predictor — all data fetching.

Every public function is SAFE: it never raises, always returns a typed default.
One shared requests.Session. Simple in-memory TTL cache (per-process lifetime).
"""

from __future__ import annotations

import csv
import io
import logging
import math
import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import requests

from config import MLB_API, SAVANT, LG

log = logging.getLogger(__name__)

# ── HTTP session ───────────────────────────────────────────────────────────────

_S = requests.Session()
_S.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (compatible; NRFI-Predictor/2.0; +research)"
    ),
    "Accept": "application/json, text/csv, */*",
})


# ── In-memory TTL cache ────────────────────────────────────────────────────────

_CACHE: dict[str, tuple[float, Any]] = {}


def _cached(key: str, ttl: int, fn) -> Any:
    entry = _CACHE.get(key)
    if entry and time.monotonic() < entry[0]:
        return entry[1]
    val = fn()
    _CACHE[key] = (time.monotonic() + ttl, val)
    return val


# ── Low-level GET helpers ──────────────────────────────────────────────────────

def _get(path: str, params: dict | None = None) -> dict | None:
    """GET from MLB Stats API. Returns None on any failure."""
    url = f"{MLB_API}{path}"
    for attempt in range(3):
        try:
            r = _S.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == 2:
                log.warning("MLB GET %s failed: %s", path, exc)
            else:
                time.sleep(2 ** attempt)
    return None


def _csv_get(url: str, params: dict | None = None) -> list[dict]:
    """GET a CSV endpoint. Returns empty list on any failure."""
    try:
        r = _S.get(url, params=params, timeout=30)
        r.raise_for_status()
        text = r.text
        if not text or text.strip().startswith("<"):
            log.warning("Non-CSV response from %s", url)
            return []
        return list(csv.DictReader(io.StringIO(text)))
    except Exception as exc:
        log.warning("CSV GET %s failed: %s", url, exc)
        return []


# ── Schedule ──────────────────────────────────────────────────────────────────

def schedule(game_date: str) -> list[dict]:
    """Return list of games for the date. Excludes cancelled/postponed."""
    return _cached(f"sched_{game_date}", 3600 * 4, lambda: _fetch_schedule(game_date))


def _fetch_schedule(game_date: str) -> list[dict]:
    # Use only the fields that the schedule endpoint reliably returns.
    # Venue coordinates come from the hardcoded VENUE_COORDS table in config.
    data = _get("/schedule", params={
        "sportId": 1,
        "date": game_date,
        "hydrate": "team,venue,probablePitcher(note)",
        "fields": (
            "dates,date,games,gamePk,gameDate,status,statusCode,"
            "teams,home,away,team,id,name,"
            "probablePitcher,id,fullName,pitchHand,"
            "venue,id,name"
        ),
    })
    if not data:
        return []

    from config import VENUE_COORDS
    games = []
    for de in data.get("dates", []):
        for g in de.get("games", []):
            sc = g.get("status", {}).get("statusCode", "")
            if sc in ("D", "DI", "CR", "CU"):          # cancelled / postponed
                continue
            home = g["teams"]["home"]
            away = g["teams"]["away"]
            venue      = g.get("venue", {})
            venue_name = venue.get("name", "")
            coords     = VENUE_COORDS.get(venue_name.lower(), {})

            games.append({
                "game_pk":        g["gamePk"],
                "game_date":      game_date,
                "game_time":      g.get("gameDate", ""),
                "status":         sc,
                "venue_id":       venue.get("id"),
                "venue_name":     venue_name,
                "lat":            coords.get("lat"),
                "lon":            coords.get("lon"),
                "home_team_id":   home["team"]["id"],
                "home_team_name": home["team"]["name"],
                "away_team_id":   away["team"]["id"],
                "away_team_name": away["team"]["name"],
                "home_probable":  _extract_probable(home.get("probablePitcher")),
                "away_probable":  _extract_probable(away.get("probablePitcher")),
            })

    log.info("Schedule: %d games for %s", len(games), game_date)
    return games


def _extract_probable(prob: dict | None) -> dict | None:
    if not prob:
        return None
    pid = prob.get("id")
    if not pid:
        return None
    return {
        "id":   pid,
        "name": prob.get("fullName", "TBD"),
        "hand": (prob.get("pitchHand") or {}).get("code", "R"),
    }


# ── Lineups ───────────────────────────────────────────────────────────────────

def lineups(game_pk: int) -> dict:
    """
    Return {"home": [...], "away": [...]} for a game.
    Tries three sources in order of reliability for pre-game data:
      1. /game/{pk}/lineups  — official lineup card endpoint
      2. /schedule?gamePk=   — schedule with lineup hydration (most reliable pre-game)
      3. /game/{pk}/boxscore — batting order (only available once game starts)
    """
    key = f"lineup_{game_pk}"
    cached = _CACHE.get(key)
    if cached and time.monotonic() < cached[0]:
        if _confirmed(cached[1]):
            return cached[1]

    result = _fetch_lineups_endpoint(game_pk)
    if not _confirmed(result):
        result = _fetch_lineups_schedule_hydrate(game_pk)
    if not _confirmed(result):
        result = _fetch_lineups_boxscore(game_pk)

    ttl = 7200 if _confirmed(result) else 300   # re-check every 5 min if unconfirmed
    _CACHE[key] = (time.monotonic() + ttl, result)
    log.info("Lineup game=%s confirmed=%s home=%d away=%d",
             game_pk, _confirmed(result),
             len(result.get("home", [])), len(result.get("away", [])))
    return result


def _fetch_lineups_endpoint(game_pk: int) -> dict:
    try:
        data = _get(f"/game/{game_pk}/lineups")
    except Exception:
        return {"home": [], "away": []}
    if not data:
        return {"home": [], "away": []}

    result: dict[str, list] = {"home": [], "away": []}
    # API returns homeTeamLineup/awayTeamLineup or homePlayers/awayPlayers
    for api_key, side in [
        ("homeTeamLineup", "home"), ("awayTeamLineup", "away"),
        ("homePlayers",    "home"), ("awayPlayers",    "away"),
    ]:
        if result[side]:          # already filled this side
            continue
        players = data.get(api_key, [])
        lineup = []
        for pos, p in enumerate(players, 1):
            person = p.get("person", p)
            pid = person.get("id") or p.get("id")
            if not pid:
                continue
            bat_side = (
                (p.get("batSide") or {}).get("code")
                or (person.get("batSide") or {}).get("code", "R")
            )
            lineup.append({
                "order":     pos,
                "player_id": pid,
                "name":      person.get("fullName", ""),
                "bat_side":  bat_side,
            })
        if lineup:
            result[side] = lineup
    return result


def _fetch_lineups_schedule_hydrate(game_pk: int) -> dict:
    """
    Fetch lineup via schedule endpoint with lineup hydration.
    This is the most reliable pre-game source — MLB posts lineups here
    as soon as the lineup card is submitted (~60-90 min before first pitch).
    """
    data = _get("/schedule", params={
        "sportId": 1,
        "gamePk":  game_pk,
        "hydrate": "lineups",
    })
    if not data:
        return {"home": [], "away": []}

    result: dict[str, list] = {"home": [], "away": []}
    for de in data.get("dates", []):
        for g in de.get("games", []):
            if g.get("gamePk") != game_pk:
                continue
            for api_key, side in [("homeTeamLineup", "home"), ("awayTeamLineup", "away")]:
                players = g.get(api_key, [])
                if not players:
                    continue
                lineup = []
                for pos, p in enumerate(players, 1):
                    person = p.get("person", p)
                    pid = person.get("id") or p.get("id")
                    if not pid:
                        continue
                    bat_side = (
                        (p.get("batSide") or {}).get("code")
                        or (person.get("batSide") or {}).get("code", "R")
                    )
                    lineup.append({
                        "order":     pos,
                        "player_id": pid,
                        "name":      person.get("fullName", ""),
                        "bat_side":  bat_side,
                    })
                if lineup:
                    result[side] = lineup
    return result


def _fetch_lineups_boxscore(game_pk: int) -> dict:
    data = _get(f"/game/{game_pk}/boxscore")
    if not data:
        return {"home": [], "away": []}

    result: dict[str, list] = {"home": [], "away": []}
    for side in ("home", "away"):
        team_data = data.get("teams", {}).get(side, {})
        order     = team_data.get("battingOrder", [])
        players   = team_data.get("players", {})
        lineup = []
        for pos, pid in enumerate(order, 1):
            p      = players.get(f"ID{pid}", {})
            person = p.get("person", {})
            lineup.append({
                "order":     pos,
                "player_id": pid,
                "name":      person.get("fullName", ""),
                "bat_side":  p.get("batSide", {}).get("code", "R"),
            })
        result[side] = lineup
    return result


def _confirmed(lu: dict) -> bool:
    return len(lu.get("home", [])) >= 9 and len(lu.get("away", [])) >= 9


# ── Linescore / first-inning result ──────────────────────────────────────────

def linescore(game_pk: int) -> dict:
    """
    Return first-inning result dict:
        away_r, home_r  — runs scored (None = not yet)
        nrfi_result     — 'NRFI'|'YRFI'|'bot_pending'|'top_pending'|'pending'
        game_status     — 'S'(scheduled)|'I'(in progress)|'F'(final)
    """
    key = f"linescore_{game_pk}"
    cached = _CACHE.get(key)
    if cached and time.monotonic() < cached[0]:
        # Only hard-cache final games; re-check live games every 2 min
        if cached[1].get("game_status") == "F":
            return cached[1]

    result = _fetch_linescore(game_pk)
    ttl = 86400 if result.get("game_status") == "F" else 120
    _CACHE[key] = (time.monotonic() + ttl, result)
    return result


def _fetch_linescore(game_pk: int) -> dict:
    data = _get(f"/game/{game_pk}/linescore")
    _pending = {"away_r": None, "home_r": None,
                "nrfi_result": "pending", "game_status": "S"}
    if not data:
        return _pending

    innings      = data.get("innings", [])
    cur_inning   = data.get("currentInning", 0)
    inning_state = data.get("inningState", "")  # Top/Middle/Bottom/End

    away_r = home_r = None
    if innings:
        first  = innings[0]
        away_r = first.get("away", {}).get("runs")
        home_r = first.get("home", {}).get("runs")

    top_done = away_r is not None
    bot_done = (
        home_r is not None
        and (cur_inning > 1 or inning_state in ("End", "Middle"))
    )

    if top_done and bot_done:
        nrfi   = "NRFI" if (away_r == 0 and home_r == 0) else "YRFI"
        status = "F" if cur_inning > 1 else "I"
    elif top_done:
        nrfi, status = "bot_pending", "I"
    elif not innings:
        nrfi, status = "pending", "S"
    else:
        nrfi, status = "top_pending", "I"

    return {"away_r": away_r, "home_r": home_r,
            "nrfi_result": nrfi, "game_status": status}


# ── Pitcher / batter stats (MLB Stats API) ────────────────────────────────────

def pitcher_stats(pid: int, season: int) -> dict:
    return _cached(f"pstats_{pid}_{season}", 86400,
                   lambda: _fetch_pitcher_stats(pid, season))


def _fetch_pitcher_stats(pid: int, season: int) -> dict:
    data = _get(f"/people/{pid}/stats", params={
        "stats": "season", "group": "pitching", "season": season, "sportId": 1,
    })
    s = _first_stat(data)
    if not s:
        return {}
    return {
        "era":    _f(s.get("era")),
        "whip":   _f(s.get("whip")),
        "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
        "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
        "gb_pct": _safe_pct(s.get("groundOuts"),
                            (s.get("groundOuts") or 0) + (s.get("airOuts") or 0)),
        "bf":     s.get("battersFaced") or 0,
        "k_per_9": _safe_div((s.get("strikeOuts") or 0) * 9,
                             _f(s.get("inningsPitched")) or None),
    }


def pitcher_career_stats(pid: int) -> dict:
    return _cached(f"pcareer_{pid}", 86400 * 7,
                   lambda: _fetch_pitcher_career(pid))


def _fetch_pitcher_career(pid: int) -> dict:
    data = _get(f"/people/{pid}/stats", params={
        "stats": "career", "group": "pitching", "sportId": 1,
    })
    s = _first_stat(data)
    if not s:
        return {}
    return {
        "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
        "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
        "gb_pct": _safe_pct(s.get("groundOuts"),
                            (s.get("groundOuts") or 0) + (s.get("airOuts") or 0)),
        "bf":     s.get("battersFaced") or 0,
    }


def pitcher_fi_split(pid: int, season: int) -> dict:
    """First-inning split for a pitcher."""
    return _cached(f"pfi_{pid}_{season}", 86400,
                   lambda: _fetch_pitcher_fi(pid, season))


def _fetch_pitcher_fi(pid: int, season: int) -> dict:
    data = _get(f"/people/{pid}/stats", params={
        "stats": "statSplits", "group": "pitching",
        "season": season, "sitCodes": "i1", "sportId": 1,
    })
    if not data:
        return {}
    for block in data.get("stats", []):
        for sp in block.get("splits", []):
            if sp.get("split", {}).get("code") == "i1":
                s = sp.get("stat", {})
                return {
                    "era":   _f(s.get("era")),
                    "k_pct": _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
                    "bb_pct":_safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
                    "bf":    s.get("battersFaced") or 0,
                }
    return {}


def pitcher_recent_form(pid: int, season: int, n: int = 3) -> dict:
    """Stats for pitcher's last N starts this season (game log)."""
    return _cached(f"pform_{pid}_{season}_{n}", 3600,
                   lambda: _fetch_pitcher_recent_form(pid, season, n))


def _fetch_pitcher_recent_form(pid: int, season: int, n: int) -> dict:
    data = _get(f"/people/{pid}/stats", params={
        "stats": "gameLog", "group": "pitching", "season": season, "sportId": 1,
    })
    if not data:
        return {}
    splits = []
    for block in data.get("stats", []):
        splits.extend(block.get("splits", []))
    # Sort by date descending, take last n
    splits = sorted(splits, key=lambda s: s.get("date", ""), reverse=True)[:n]
    if not splits:
        return {}
    total_er = total_ip = total_k = total_bb = total_bf = 0
    for sp in splits:
        s = sp.get("stat", {})
        total_er += (s.get("earnedRuns") or 0)
        total_k  += (s.get("strikeOuts") or 0)
        total_bb += (s.get("baseOnBalls") or 0)
        total_bf += (s.get("battersFaced") or 0)
        # inningsPitched is "6.1" where decimal = thirds
        ip_str = str(s.get("inningsPitched") or "0.0")
        try:
            parts = ip_str.split(".")
            total_ip += int(parts[0]) + int(parts[1] if len(parts) > 1 else 0) / 3.0
        except Exception:
            pass
    return {
        "era":    round(total_er / total_ip * 9, 2) if total_ip > 0 else None,
        "k_pct":  _safe_div(total_k, total_bf),
        "bb_pct": _safe_div(total_bb, total_bf),
        "n":      len(splits),
        "bf":     total_bf,
    }


def pitcher_platoon_stats(pid: int, season: int) -> dict:
    """Pitcher stats vs left-handed batters and vs right-handed batters."""
    return _cached(f"pplat_{pid}_{season}", 86400,
                   lambda: _fetch_pitcher_platoon(pid, season))


def _fetch_pitcher_platoon(pid: int, season: int) -> dict:
    result = {}
    for sit, label in [("vl", "vs_lhb"), ("vr", "vs_rhb")]:
        data = _get(f"/people/{pid}/stats", params={
            "stats": "statSplits", "group": "pitching",
            "season": season, "sitCodes": sit, "sportId": 1,
        })
        if not data:
            continue
        for block in data.get("stats", []):
            for sp in block.get("splits", []):
                if sp.get("split", {}).get("code") == sit:
                    s = sp.get("stat", {})
                    result[label] = {
                        "era":    _f(s.get("era")),
                        "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
                        "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
                        "bf":     s.get("battersFaced") or 0,
                    }
    return result


def pitcher_home_away(pid: int, season: int) -> dict:
    """Pitcher stats at home and on the road."""
    return _cached(f"pha_{pid}_{season}", 86400,
                   lambda: _fetch_pitcher_home_away(pid, season))


def _fetch_pitcher_home_away(pid: int, season: int) -> dict:
    result = {}
    for sit, label in [("h", "home"), ("a", "away")]:
        data = _get(f"/people/{pid}/stats", params={
            "stats": "statSplits", "group": "pitching",
            "season": season, "sitCodes": sit, "sportId": 1,
        })
        if not data:
            continue
        for block in data.get("stats", []):
            for sp in block.get("splits", []):
                if sp.get("split", {}).get("code") == sit:
                    s = sp.get("stat", {})
                    result[label] = {
                        "era":    _f(s.get("era")),
                        "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
                        "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
                        "bf":     s.get("battersFaced") or 0,
                    }
    return result


# ── NRFI odds (The Odds API) ──────────────────────────────────────────────────

# Priority bookmaker order — best odds are taken from the first available book.
# The Odds API key codes: draftkings, fanduel, betmgm, caesars, bovada, pointsbetus
_PREFERRED_BOOKS = [
    "draftkings", "fanduel", "betmgm", "caesars", "pointsbetus", "bovada",
]

# The Odds API market keys to try in order for NRFI (1st inning, no run).
# - totals_1st_1_innings: combined O/U 0.5 → "Under" = NRFI  (may need paid plan)
# - h2h_1st_1_innings:    YES/NO run moneyline → "No" = NRFI   (may need paid plan)
# - alternate_totals:     catch-all for alternate total markets (free or paid)
# We try them all and use whichever returns data.
_NRFI_MARKETS: list[tuple[str, str]] = [
    ("totals_1st_1_innings", "Under"),   # (market_key, outcome_name_for_NRFI)
    ("h2h_1st_1_innings",    "No"),
    ("alternate_totals",     "Under"),
]
# Keep the single-string alias for the cache key and logging
_NRFI_MARKET = _NRFI_MARKETS[0][0]


def nrfi_odds(game_date: str) -> dict[int, int]:
    """
    Fetch NRFI odds (American format, 1st-inning no-run) for all games on game_date.

    Source priority:
      1. The Odds API (requires ODDS_API_KEY env var; first-inning markets need paid plan)
         Tries: totals_1st_1_innings → h2h_1st_1_innings → alternate_totals
      2. DraftKings unofficial sportsbook API (no key; discovers 1st-inning category
         dynamically)

    Returns {game_pk: american_odds_int}.  Empty dict if no source returns data.
    Only called when a pick is missing its odds → ~1 API call per game-day.
    """
    api_key = os.environ.get("ODDS_API_KEY")

    # ── Primary: The Odds API ─────────────────────────────────────────────────
    if api_key:
        result = _cached(f"nrfi_odds_primary_{game_date}", 7200,
                         lambda: _fetch_nrfi_odds(game_date, api_key))
        if result:
            return result
        log.info("Odds API returned no NRFI data for %s", game_date)
    else:
        log.info("ODDS_API_KEY not set — skipping Odds API for %s", game_date)

    # ── Fallback: DraftKings (no key; works locally but blocked on cloud IPs) ─
    result = _cached(f"nrfi_odds_dk_{game_date}", 7200,
                     lambda: _fetch_nrfi_odds_draftkings(game_date))
    if result:
        return result

    log.warning(
        "NRFI odds unavailable for %s. Automated pulling requires The Odds API "
        "Standard plan ($9/mo at the-odds-api.com). Alternatively, click any "
        "odds cell on the Season Tracker page to enter odds manually.",
        game_date,
    )
    return {}


def _fetch_nrfi_odds(game_date: str, api_key: str) -> dict[int, int]:
    # Build team-name → game_pk lookup from cached schedule
    games = schedule(game_date)
    if not games:
        return {}

    def _norm(name: str) -> str:
        return name.lower().strip()

    sched_map: dict[tuple[str, str], int] = {
        (_norm(g["away_team_name"]), _norm(g["home_team_name"])): g["game_pk"]
        for g in games
    }

    # Accept games on game_date AND game_date+1 UTC (west-coast games roll over midnight)
    target  = date.fromisoformat(game_date)
    allowed = {game_date, (target + timedelta(days=1)).isoformat()}

    # Try each market key until one returns data
    events_by_market: list | None = None
    working_market: str = _NRFI_MARKETS[0][0]
    working_outcome: str = _NRFI_MARKETS[0][1]

    for mkt_key, outcome_name in _NRFI_MARKETS:
        try:
            r = _S.get(
                "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds",
                params={
                    "apiKey":     api_key,
                    "regions":    "us",
                    "markets":    mkt_key,
                    "oddsFormat": "american",
                    "dateFormat": "iso",
                },
                timeout=15,
            )
            remaining = r.headers.get("x-requests-remaining", "?")
            used      = r.headers.get("x-requests-used", "?")
            log.info("Odds API [%s]: HTTP %s  quota used=%s remaining=%s",
                     mkt_key, r.status_code, used, remaining)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("Odds API [%s] request failed: %s", mkt_key, exc)
            continue

        if not isinstance(data, list):
            log.warning("Odds API [%s] error: %s", mkt_key, str(data)[:200])
            continue

        # Check if any event actually carries this market from at least one bookmaker
        has_data = any(
            m.get("key") == mkt_key and m.get("outcomes")
            for ev in data
            for b in ev.get("bookmakers", [])
            for m in b.get("markets", [])
        )
        if has_data:
            events_by_market = data
            working_market   = mkt_key
            working_outcome  = outcome_name
            log.info("Odds API: using market '%s' (outcome name '%s')",
                     working_market, working_outcome)
            break
        else:
            log.info("Odds API [%s]: market not offered by any bookmaker (may need paid plan)",
                     mkt_key)

    if events_by_market is None:
        log.warning(
            "Odds API: none of the NRFI market keys returned data for %s. "
            "Check your plan at the-odds-api.com — first-inning markets may require "
            "a Standard or higher plan. Run test_odds.py to diagnose.", game_date)
        return {}

    result: dict[int, int] = {}
    for ev in events_by_market:
        ct_date = (ev.get("commence_time") or "")[:10]
        if ct_date not in allowed:
            continue

        away = _norm(ev.get("away_team", ""))
        home = _norm(ev.get("home_team", ""))

        # Exact match first, then substring fallback ("Athletics" ↔ "Oakland Athletics")
        game_pk = sched_map.get((away, home))
        if not game_pk:
            for (sa, sh), pk in sched_map.items():
                if (away in sa or sa in away) and (home in sh or sh in home):
                    game_pk = pk
                    break
        if not game_pk:
            log.debug("Odds API: no schedule match for %s @ %s", away, home)
            continue

        # Find NRFI price from preferred books first, then any available book
        books_by_key = {b["key"]: b for b in ev.get("bookmakers", [])}
        book_order = [books_by_key[k] for k in _PREFERRED_BOOKS if k in books_by_key]
        book_order += [b for b in ev.get("bookmakers", []) if b["key"] not in _PREFERRED_BOOKS]

        nrfi_price: int | None = None
        for book in book_order:
            for mkt in book.get("markets", []):
                if mkt.get("key") != working_market:
                    continue
                for outcome in mkt.get("outcomes", []):
                    if outcome.get("name") == working_outcome:
                        nrfi_price = outcome.get("price")
                        break
                if nrfi_price is not None:
                    break
            if nrfi_price is not None:
                break

        if nrfi_price is not None:
            result[game_pk] = int(nrfi_price)
            log.debug("Odds [game_pk=%s]: %s @ %s  NRFI=%s",
                      game_pk, ev.get("away_team"), ev.get("home_team"), nrfi_price)

    log.info("Odds API: %d/%d games have NRFI odds for %s",
             len(result), len(games), game_date)
    return result


# ── NRFI odds fallback: DraftKings unofficial API (no key required) ───────────
# NOTE: DraftKings blocks GitHub Actions / cloud IP ranges (HTTP 403).
# This only works when run locally. Kept as a convenience for local testing.

_DK_BASE = "https://sportsbook.draftkings.com/sites/US-SB/api/v5"

# DraftKings eventGroupId for MLB — try multiple in case DK changes the ID.
# 88808 and 84240 are the most commonly cited MLB IDs.
_DK_MLB_EG_IDS = [88808, 84240]

_DK_HEADERS = {
    "Accept":  "application/json",
    "Referer": "https://sportsbook.draftkings.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
}

# Category/subcategory name fragments to recognise 1st-inning total markets.
# Broad intentionally: DK naming varies by season and region.
_DK_CAT_KEYWORDS    = {"inning", "1st", "first"}
_DK_SUBCAT_KEYWORDS = {"total", "run", "score", "line"}


def _fetch_nrfi_odds_draftkings(game_date: str) -> dict[int, int]:
    """
    Fetch NRFI odds from DraftKings unofficial sportsbook API — no API key needed.

    Tries multiple MLB event-group IDs, logs ALL discovered categories so that
    workflow output always reveals what the API is actually returning.

    Returns {game_pk: american_odds_int} or {} on any failure.
    """
    games = schedule(game_date)
    if not games:
        log.warning("DraftKings: no schedule games found for %s", game_date)
        return {}

    def _norm(s: str) -> str:
        return (s or "").lower().strip()

    sched_map: dict[tuple[str, str], int] = {
        (_norm(g["away_team_name"]), _norm(g["home_team_name"])): g["game_pk"]
        for g in games
    }

    for eg_id in _DK_MLB_EG_IDS:
        result = _try_dk_event_group(eg_id, sched_map, _norm, game_date)
        if result:
            return result

    log.warning(
        "DraftKings: no NRFI odds found for %s after trying event group IDs %s",
        game_date, _DK_MLB_EG_IDS,
    )
    return {}


def _try_dk_event_group(
    eg_id: int,
    sched_map: dict,
    _norm,
    game_date: str,
) -> dict[int, int]:
    """Try one DraftKings MLB event-group ID. Returns {} if nothing useful found."""

    # ── Step 1: fetch event-group to discover category IDs ───────────────────
    try:
        r = _S.get(
            f"{_DK_BASE}/eventgroups/{eg_id}",
            params={"format": "json"},
            headers=_DK_HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        eg = r.json().get("eventGroup", {})
    except Exception as exc:
        log.warning("DraftKings: event-group %s fetch failed: %s", eg_id, exc)
        return {}

    eg_name = eg.get("name", "?")
    cats = eg.get("offerCategories", [])

    # Log ALL categories so workflow output reveals the API structure
    cat_names = [f"[{c.get('id')}] {c.get('name')}" for c in cats]
    log.info("DraftKings eventGroup %s ('%s'): %d categories: %s",
             eg_id, eg_name, len(cats), ", ".join(cat_names) or "(none)")

    # Find best matching category + subcategory
    cat_id: int | None    = None
    subcat_id: int | None = None
    cat_name = subcat_name = ""

    for cat in cats:
        cname = _norm(cat.get("name", ""))
        if not any(kw in cname for kw in _DK_CAT_KEYWORDS):
            continue
        sc_descs = cat.get("offerSubcategoryDescriptors", [])
        sc_names = [s.get("name", "") for s in sc_descs]
        log.info("DraftKings:   candidate cat '%s' → subcats: %s",
                 cat.get("name"), sc_names)
        for sc in sc_descs:
            scname = _norm(sc.get("name", ""))
            if any(kw in scname for kw in _DK_SUBCAT_KEYWORDS):
                cat_id     = cat.get("id")
                subcat_id  = sc.get("subcategoryId")
                cat_name   = cat.get("name", "")
                subcat_name = sc.get("name", "")
                break
        if cat_id is not None:
            break

    if cat_id is None:
        log.info("DraftKings: eventGroup %s — no matching 1st-inning category", eg_id)
        return {}

    log.info("DraftKings: eventGroup %s using '%s / %s' (cat=%s, subcat=%s)",
             eg_id, cat_name, subcat_name, cat_id, subcat_id)

    # ── Step 2: fetch offers for that subcategory ─────────────────────────────
    try:
        r = _S.get(
            f"{_DK_BASE}/eventgroups/{eg_id}"
            f"/categories/{cat_id}/subcategories/{subcat_id}",
            params={"format": "json"},
            headers=_DK_HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.warning("DraftKings: offers fetch failed for %s/%s/%s: %s",
                    eg_id, cat_id, subcat_id, exc)
        return {}

    eg2 = data.get("eventGroup", {})

    # Build eventId → event-dict lookup (source of team names)
    events_lut: dict[int, dict] = {}
    for ev in eg2.get("events", []):
        eid = ev.get("id") or ev.get("eventId")
        if eid is not None:
            events_lut[int(eid)] = ev

    log.info("DraftKings: offers response has %d events in lookup", len(events_lut))

    # Navigate: offerCategories[0] → offerSubcategoryDescriptors[0] → offerSubcategory → offers
    try:
        cats2 = eg2.get("offerCategories", [])
        scs2  = cats2[0].get("offerSubcategoryDescriptors", []) if cats2 else []
        offers_matrix = scs2[0].get("offerSubcategory", {}).get("offers", []) if scs2 else []
    except Exception as exc:
        log.warning("DraftKings: cannot navigate offers structure: %s", exc)
        return {}

    log.info("DraftKings: %d offer rows in matrix", len(offers_matrix))

    # ── Step 3: extract Under-0.5 prices and match to schedule ───────────────
    result: dict[int, int] = {}

    for offer_row in offers_matrix:
        items = offer_row if isinstance(offer_row, list) else [offer_row]
        for offer in items:
            under_price: int | None = None
            for oc in offer.get("outcomes", []):
                lbl = _norm(oc.get("label", ""))
                try:
                    line = float(oc.get("line") or 0)
                except (TypeError, ValueError):
                    line = 0.0
                if lbl == "under" and abs(line - 0.5) < 0.1:
                    try:
                        under_price = int(oc.get("oddsAmerican") or 0)
                    except (TypeError, ValueError):
                        pass
                    break

            if not under_price:
                continue

            eid = offer.get("eventId")
            ev  = events_lut.get(int(eid), {}) if eid is not None else {}

            home_name = _norm(
                ev.get("homeTeamName")
                or ev.get("homeName")
                or (ev.get("home") or {}).get("name", "")
            )
            away_name = _norm(
                ev.get("awayTeamName")
                or ev.get("awayName")
                or (ev.get("away") or {}).get("name", "")
            )

            if not home_name or not away_name:
                ev_name = ev.get("name", "")
                for sep in (" vs. ", " vs ", " @ "):
                    if sep in ev_name:
                        parts = ev_name.split(sep, 1)
                        away_name = _norm(parts[0])
                        home_name = _norm(parts[1])
                        break

            if not home_name and not away_name:
                log.debug("DraftKings: offer eventId=%s has no team names, skipping", eid)
                continue

            game_pk = sched_map.get((away_name, home_name))
            if not game_pk:
                for (sa, sh), pk in sched_map.items():
                    if (away_name and (away_name in sa or sa in away_name) and
                            home_name and (home_name in sh or sh in home_name)):
                        game_pk = pk
                        break

            if game_pk:
                result[game_pk] = under_price
                log.info("DraftKings: matched game_pk=%s  %s @ %s  NRFI=%+d",
                         game_pk, away_name, home_name, under_price)
            else:
                log.info("DraftKings: no schedule match for '%s' @ '%s' (Under=%+d)",
                         away_name, home_name, under_price)

    return result


def batter_stats(pid: int, season: int) -> dict:
    return _cached(f"bstats_{pid}_{season}", 86400,
                   lambda: _fetch_batter_stats(pid, season))


def _fetch_batter_stats(pid: int, season: int) -> dict:
    data = _get(f"/people/{pid}/stats", params={
        "stats": "season", "group": "hitting", "season": season, "sportId": 1,
    })
    s = _first_stat(data)
    if not s:
        return {}
    return {
        "obp":    _f(s.get("obp")),
        "slg":    _f(s.get("slg")),
        "ops":    _f(s.get("ops")),
        "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("plateAppearances")),
        "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("plateAppearances")),
        "pa":     s.get("plateAppearances") or 0,
    }


def batter_hand_splits(pid: int, season: int) -> dict:
    """Return {vs_lhp: {...}, vs_rhp: {...}}."""
    return _cached(f"bsplits_{pid}_{season}", 86400,
                   lambda: _fetch_batter_splits(pid, season))


def _fetch_batter_splits(pid: int, season: int) -> dict:
    result = {}
    for sit, label in [("vl", "vs_lhp"), ("vr", "vs_rhp")]:
        data = _get(f"/people/{pid}/stats", params={
            "stats": "statSplits", "group": "hitting",
            "season": season, "sitCodes": sit, "sportId": 1,
        })
        if not data:
            continue
        for block in data.get("stats", []):
            for sp in block.get("splits", []):
                if sp.get("split", {}).get("code") == sit:
                    s = sp.get("stat", {})
                    result[label] = {
                        "obp":    _f(s.get("obp")),
                        "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("plateAppearances")),
                        "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("plateAppearances")),
                        "pa":     s.get("plateAppearances") or 0,
                    }
    return result


# ── Baseball Savant leaderboards ──────────────────────────────────────────────

def savant_pitchers(season: int) -> dict[int, dict]:
    """
    Fetch Statcast pitcher leaderboard for a season.
    Returns {pitcher_id: {xwoba_against, k_pct, bb_pct, fps, hard_hit, barrel, gb, pa}}.
    """
    return _cached(f"sv_pitchers_{season}", 86400,
                   lambda: _fetch_savant_pitchers(season))


def _fetch_savant_pitchers(season: int) -> dict[int, dict]:
    rows = _csv_get(f"{SAVANT}/leaderboard/custom", params={
        "year":       season,
        "type":       "pitcher",
        "filter":     "",
        "selections": (
            "xwoba,xera,k_percent,bb_percent,whiff_percent,"
            "hard_hit_percent,f_strike_percent,o_swing_percent"
        ),
        "chart":      "false",
        "x":          "xwoba",
        "y":          "xera",
        "r":          "no",
        "csv":        "true",
    })
    result: dict[int, dict] = {}
    for row in rows:
        pid = _i(row.get("pitcher_id"))
        if not pid:
            continue
        result[pid] = {
            "xera":       _f(row.get("xera")),
            "k_pct":      _pct(row.get("k_percent")),
            "bb_pct":     _pct(row.get("bb_percent")),
            "fps":        _pct(row.get("f_strike_percent")),
            "whiff_pct":  _pct(row.get("whiff_percent")),
            "chase_rate": _pct(row.get("o_swing_percent")),
            "hard_hit":   _pct(row.get("hard_hit_percent")),
            "pa":         _i(row.get("pa")) or 0,
        }
    log.info("Savant pitchers: %d rows for %d", len(result), season)
    return result


def savant_batters(season: int) -> dict[int, dict]:
    """
    Fetch Statcast batter leaderboard.
    Returns {batter_id: {xwoba, bb_pct, hard_hit, barrel, sprint_speed, pa}}.
    """
    return _cached(f"sv_batters_{season}", 86400,
                   lambda: _fetch_savant_batters(season))


def _fetch_savant_batters(season: int) -> dict[int, dict]:
    rows = _csv_get(f"{SAVANT}/leaderboard/custom", params={
        "year":       season,
        "type":       "batter",
        "filter":     "",
        "selections": (
            "xwoba,xba,xslg,k_percent,bb_percent,"
            "hard_hit_percent,barrel_batted_rate,"
            "exit_velocity_avg,sprint_speed"
        ),
        "chart":      "false",
        "x":          "xwoba",
        "y":          "xba",
        "r":          "no",
        "csv":        "true",
    })
    result: dict[int, dict] = {}
    for row in rows:
        pid = _i(row.get("batter_id"))
        if not pid:
            continue
        result[pid] = {
            "xwoba":        _f(row.get("xwoba")),
            "k_pct":        _pct(row.get("k_percent")),
            "bb_pct":       _pct(row.get("bb_percent")),
            "hard_hit":     _pct(row.get("hard_hit_percent")),
            "barrel":       _pct(row.get("barrel_batted_rate")),
            "sprint_speed": _f(row.get("sprint_speed")),
            "pa":           _i(row.get("pa")) or 0,
        }
    log.info("Savant batters: %d rows for %d", len(result), season)
    return result


def sprint_speed(season: int) -> dict[int, float]:
    """Return {player_id: speed_ft_per_s}."""
    return _cached(f"sprint_{season}", 86400,
                   lambda: _fetch_sprint_speed(season))


def _fetch_sprint_speed(season: int) -> dict[int, float]:
    rows = _csv_get(f"{SAVANT}/leaderboard/sprint_speed", params={
        "year": season, "position": "", "team": "", "min": "10", "csv": "true",
    })
    result: dict[int, float] = {}
    for row in rows:
        pid = _i(row.get("player_id") or row.get("mlb_id"))
        spd = _f(row.get("r_sprint_speed_top50percent"))
        if pid and spd is not None:
            result[pid] = spd
    return result


def pop_time(season: int) -> dict[int, float]:
    """Return {catcher_player_id: pop_2b_seconds}."""
    return _cached(f"poptime_{season}", 86400,
                   lambda: _fetch_pop_time(season))


def _fetch_pop_time(season: int) -> dict[int, float]:
    rows = _csv_get(f"{SAVANT}/leaderboard/pop-time", params={
        "year": season, "minThrows": "10", "minOppSBA": "10", "csv": "true",
    })
    result: dict[int, float] = {}
    for row in rows:
        pid = _i(row.get("catcher_id") or row.get("player_id"))
        pt  = _f(row.get("pop_2b_sba_count_sr"))
        if pid and pt is not None:
            result[pid] = pt
    return result


def team_catchers(team_id: int, season: int) -> list[int]:
    """Return list of catcher player_ids on a team's active roster."""
    return _cached(f"catchers_{team_id}_{season}", 86400,
                   lambda: _fetch_team_catchers(team_id, season))


def _fetch_team_catchers(team_id: int, season: int) -> list[int]:
    data = _get(f"/teams/{team_id}/roster", params={
        "rosterType": "active", "season": season,
    })
    if not data:
        return []
    return [
        e["person"]["id"]
        for e in data.get("roster", [])
        if e.get("position", {}).get("abbreviation") == "C"
    ]


# ── Weather (wttr.in free API, no key required) ───────────────────────────────

def weather(lat: float | None, lon: float | None,
            venue_name: str = "") -> dict:
    """
    Return {temp_f, wind_mph, wind_deg, conditions, source}.
    Uses lat/lon when available, otherwise falls back to venue name for wttr.in.
    Returns neutral defaults only as a last resort.
    """
    _neutral = {"temp_f": 65.0, "wind_mph": 5.0, "wind_deg": 270.0,
                "conditions": "Unknown", "source": "default"}

    # Try OpenWeatherMap if key is set and we have coordinates
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if api_key and lat is not None and lon is not None:
        result = _fetch_openweather(lat, lon, api_key)
        if result:
            return result

    # wttr.in: prefer lat/lon, fall back to venue city name
    if lat is not None and lon is not None:
        cache_key = f"wx_{lat:.3f}_{lon:.3f}"
        query_arg = (lat, lon)
    elif venue_name:
        safe = venue_name.lower().replace(" ", "+")
        cache_key = f"wx_name_{safe}"
        query_arg = venue_name
    else:
        return _neutral

    return _cached(cache_key, 1800,
                   lambda: _fetch_wttr(*query_arg) if isinstance(query_arg, tuple)
                           else _fetch_wttr_city(query_arg) or _neutral)


def _fetch_openweather(lat: float, lon: float, api_key: str) -> dict | None:
    try:
        r = _S.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={"lat": lat, "lon": lon, "appid": api_key, "units": "imperial"},
            timeout=10,
        )
        r.raise_for_status()
        d = r.json()
        return {
            "temp_f":     float(d["main"].get("temp", 65)),
            "wind_mph":   float(d.get("wind", {}).get("speed", 5)),
            "wind_deg":   float(d.get("wind", {}).get("deg", 270)),
            "conditions": (d.get("weather") or [{}])[0].get("main", "Clear"),
            "source":     "openweathermap",
        }
    except Exception as exc:
        log.warning("OpenWeatherMap failed: %s", exc)
        return None


def _fetch_wttr(lat: float, lon: float) -> dict | None:
    return _wttr_query(f"{lat},{lon}")


def _fetch_wttr_city(city: str) -> dict | None:
    safe = city.replace(" ", "+")
    return _wttr_query(safe)


def _wttr_query(location: str) -> dict | None:
    try:
        r = _S.get(f"https://wttr.in/{location}",
                   params={"format": "j1"}, timeout=10)
        r.raise_for_status()
        d = r.json()
        c = d["current_condition"][0]
        return {
            "temp_f":     float(c.get("temp_F", 65)),
            "wind_mph":   float(c.get("windspeedMiles", 5)),
            "wind_deg":   float(c.get("winddirDegree", 270)),
            "conditions": (c.get("weatherDesc") or [{}])[0].get("value", "Clear"),
            "source":     "wttr.in",
        }
    except Exception as exc:
        log.warning("wttr.in failed for %s: %s", location, exc)
        return None


# ── Shared utilities ──────────────────────────────────────────────────────────

def _first_stat(data: dict | None) -> dict | None:
    if not data:
        return None
    splits = data.get("stats", [{}])[0].get("splits", [])
    return splits[0].get("stat") if splits else None


def _f(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _i(val) -> int | None:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _pct(val) -> float | None:
    """Convert '22.5' → 0.225, or 0.225 → 0.225."""
    v = _f(val)
    if v is None:
        return None
    return v / 100.0 if v > 1.0 else v


def _safe_pct(num, den) -> float | None:
    try:
        n, d = float(num), float(den)
        return n / d if d > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_div(num, den) -> float | None:
    try:
        n, d = float(num), float(den)
        return n / d if d > 0 else None
    except (TypeError, ValueError):
        return None
