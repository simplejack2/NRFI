"""
MLB Stats API fetcher.
Covers: daily schedule, probable pitchers, confirmed lineups, player stats/splits.
All data is returned as plain Python dicts/lists for easy downstream use.
"""

from __future__ import annotations

import os
import time
import logging
from datetime import date, datetime
from typing import Any

import requests

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import MLB_API_BASE, CACHE_TTL, CACHE_DIR
from fetchers._cache import cache_get, cache_set

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "NRFI-Predictor/1.0 (research)",
    "Accept": "application/json",
})


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get(path: str, params: dict | None = None, retries: int = 3) -> Any:
    """GET from MLB Stats API with retry logic."""
    url = f"{MLB_API_BASE}{path}"
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                logger.error("MLB API request failed: %s %s -> %s", url, params, exc)
                raise
            wait = 2 ** attempt
            logger.warning("MLB API attempt %d failed, retrying in %ds: %s", attempt + 1, wait, exc)
            time.sleep(wait)


def _today() -> str:
    return date.today().isoformat()


# ── Schedule / Games ──────────────────────────────────────────────────────────

def get_schedule(game_date: str | None = None) -> list[dict]:
    """
    Return list of games for a date (default: today).
    Each game dict contains game_pk, home/away team, venue, status, game_time.
    """
    game_date = game_date or _today()
    cache_key = f"mlb_schedule_{game_date}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    data = _get("/schedule", params={
        "sportId": 1,
        "date": game_date,
        "hydrate": "team,venue,probablePitcher(note),linescore",
        "fields": (
            "dates,date,games,gamePk,gameDate,status,statusCode,"
            "teams,home,away,team,id,name,"
            "probablePitcher,id,fullName,pitchHand,"
            "venue,id,name"
        ),
    })

    games = []
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            status_code = g.get("status", {}).get("statusCode", "")
            # Skip cancelled / postponed
            if status_code in ("D", "DI", "CR", "CU"):
                continue
            home = g["teams"]["home"]
            away = g["teams"]["away"]
            game = {
                "game_pk":   g["gamePk"],
                "game_date": game_date,
                "game_time": g.get("gameDate", ""),
                "status":    status_code,
                "venue_id":  g["venue"]["id"],
                "venue_name":g["venue"]["name"],
                "home_team_id":   home["team"]["id"],
                "home_team_name": home["team"]["name"],
                "away_team_id":   away["team"]["id"],
                "away_team_name": away["team"]["name"],
                "home_probable": _extract_probable(home.get("probablePitcher")),
                "away_probable": _extract_probable(away.get("probablePitcher")),
            }
            games.append(game)

    cache_set(cache_key, games, ttl=CACHE_TTL["schedule"])
    logger.info("Fetched %d games for %s", len(games), game_date)
    return games


def _extract_probable(prob: dict | None) -> dict | None:
    if not prob:
        return None
    return {
        "id":        prob.get("id"),
        "name":      prob.get("fullName"),
        "hand":      prob.get("pitchHand", {}).get("code", "R"),
    }


# ── Lineups ───────────────────────────────────────────────────────────────────

def get_lineups(game_pk: int) -> dict:
    """
    Return confirmed lineups for a game.
    Strategy:
      1. Try /game/{pk}/lineups  — official pre-game / in-game lineup endpoint
      2. Fall back to /game/{pk}/boxscore battingOrder  — populated once game starts
    Returns dict with 'home' and 'away' keys, each a list of player dicts.
    Empty lists if lineups not yet posted.
    """
    cache_key = f"mlb_lineups_{game_pk}"
    cached = cache_get(cache_key)
    if cached is not None:
        # Don't serve a cached "empty" result — always re-try if unconfirmed
        if lineups_confirmed(cached):
            return cached

    result = _fetch_lineups_endpoint(game_pk)

    # If the dedicated endpoint came back empty, try boxscore (works for started/final games)
    if not lineups_confirmed(result):
        result = _fetch_lineups_boxscore(game_pk) or result

    ttl = CACHE_TTL["lineups"] * 4 if lineups_confirmed(result) else CACHE_TTL["lineups"]
    cache_set(cache_key, result, ttl=ttl)
    return result


def _fetch_lineups_endpoint(game_pk: int) -> dict:
    """Use the dedicated /game/{pk}/lineups endpoint (pre-game lineups)."""
    try:
        data = _get(f"/game/{game_pk}/lineups")
    except Exception:
        return {"home": [], "away": []}

    result = {"home": [], "away": []}
    # MLB API uses homeTeamLineup/awayTeamLineup; some versions use homePlayers/awayPlayers
    key_pairs = [
        ("homeTeamLineup", "home"), ("awayTeamLineup", "away"),
        ("homePlayers",    "home"), ("awayPlayers",    "away"),
    ]
    filled = {"home": False, "away": False}
    for api_key, side in key_pairs:
        if filled[side]:
            continue
        players = data.get(api_key, [])
        if not players:
            continue
        lineup = []
        for pos, p in enumerate(players, start=1):
            person = p.get("person", p)
            bat_side = (p.get("batSide") or {}).get("code") or \
                       (person.get("batSide") or {}).get("code", "R")
            player_id = person.get("id") or p.get("id")
            if not player_id:
                continue
            lineup.append({
                "order":     pos,
                "player_id": player_id,
                "name":      person.get("fullName", ""),
                "bat_side":  bat_side,
            })
        if lineup:
            result[side] = lineup
            filled[side] = True
    return result


def _fetch_lineups_boxscore(game_pk: int) -> dict:
    """Fall back to boxscore battingOrder (only populated once game starts)."""
    try:
        data = _get(f"/game/{game_pk}/boxscore")
    except Exception:
        return {"home": [], "away": []}

    result = {"home": [], "away": []}
    for side in ("home", "away"):
        team_data = data.get("teams", {}).get(side, {})
        batting_order = team_data.get("battingOrder", [])
        players = team_data.get("players", {})
        lineup = []
        for pos, player_id in enumerate(batting_order, start=1):
            p = players.get(f"ID{player_id}", {})
            person = p.get("person", {})
            bat_side = p.get("batSide", {}).get("code", "R")
            lineup.append({
                "order":     pos,
                "player_id": player_id,
                "name":      person.get("fullName", ""),
                "bat_side":  bat_side,
            })
        result[side] = lineup
    return result


def lineups_confirmed(lineups: dict) -> bool:
    """Return True if both sides have at least 9 batters posted."""
    return len(lineups.get("home", [])) >= 9 and len(lineups.get("away", [])) >= 9


# ── Linescore / first-inning result ──────────────────────────────────────────

def get_first_inning_result(game_pk: int) -> dict:
    """
    Fetch the linescore and return the actual first-inning run totals.
    Returns dict with:
        away_runs_1st   - int or None (None = inning not yet complete)
        home_runs_1st   - int or None
        top_complete    - bool
        bot_complete    - bool
        nrfi_result     - 'NRFI' | 'YRFI' | 'top_pending' | 'bot_pending' | 'pending'
        game_status     - status code string ('S', 'P', 'I', 'F', etc.)
    """
    cache_key = f"mlb_linescore_{game_pk}"
    cached = cache_get(cache_key)
    # Only use cache if game is final (status F) — live games need fresh data
    if cached is not None and cached.get("game_status") == "F":
        return cached

    try:
        data = _get(f"/game/{game_pk}/linescore")
    except Exception:
        return _pending_result()

    innings   = data.get("innings", [])
    status    = data.get("currentInning", 0)
    inning_state = data.get("inningState", "")   # "Top", "Middle", "Bottom", "End"
    current_inning = data.get("currentInning", 0)

    # Try to get game status from the linescore itself
    game_status_code = "I" if innings else "S"

    away_1st = None
    home_1st = None

    if innings:
        first = innings[0]
        away_1st = first.get("away", {}).get("runs")
        home_1st = first.get("home", {}).get("runs")

    # Determine completion
    top_complete = away_1st is not None
    # Bottom complete if: inning > 1, OR (inning == 1 and state is "End"/"Middle")
    bot_complete = (
        home_1st is not None and
        (current_inning > 1 or inning_state in ("End", "Middle"))
    )

    # Derive NRFI result
    if top_complete and bot_complete:
        nrfi = "NRFI" if (away_1st == 0 and home_1st == 0) else "YRFI"
        game_status_code = "F" if current_inning > 1 else "I"
    elif top_complete:
        nrfi = "bot_pending"
        game_status_code = "I"
    elif not innings:
        nrfi = "pending"
        game_status_code = "S"
    else:
        nrfi = "top_pending"
        game_status_code = "I"

    result = {
        "away_runs_1st":  away_1st,
        "home_runs_1st":  home_1st,
        "top_complete":   top_complete,
        "bot_complete":   bot_complete,
        "nrfi_result":    nrfi,
        "game_status":    game_status_code,
    }

    ttl = CACHE_TTL["schedule"] if game_status_code == "F" else 120  # 2 min for live
    cache_set(cache_key, result, ttl=ttl)
    return result


def _pending_result() -> dict:
    return {
        "away_runs_1st": None, "home_runs_1st": None,
        "top_complete": False, "bot_complete": False,
        "nrfi_result": "pending", "game_status": "S",
    }


# ── Player stats ──────────────────────────────────────────────────────────────

def get_pitcher_season_stats(player_id: int, season: int | None = None) -> dict:
    """
    Fetch pitcher season stats from MLB Stats API.
    Returns dict of rate stats (ERA, K%, BB%, WHIP, etc.)
    Note: Statcast metrics are fetched separately from Savant.
    """
    season = season or date.today().year
    cache_key = f"mlb_pitcher_stats_{player_id}_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        data = _get(f"/people/{player_id}/stats", params={
            "stats":  "season",
            "group":  "pitching",
            "season": season,
            "sportId": 1,
        })
    except Exception:
        return {}

    splits = data.get("stats", [{}])[0].get("splits", [])
    if not splits:
        return {}

    s = splits[0].get("stat", {})
    result = {
        "era":         _f(s.get("era")),
        "whip":        _f(s.get("whip")),
        "k_per_9":     _f(s.get("strikeoutsPer9Inn")),
        "bb_per_9":    _f(s.get("walksPer9Inn")),
        "innings":     _f(s.get("inningsPitched")),
        "k_pct":       _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
        "bb_pct":      _safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
        "gb_pct":      _safe_pct(s.get("groundOuts"),
                                  (s.get("groundOuts", 0) or 0) + (s.get("airOuts", 0) or 0)),
        "batters_faced": s.get("battersFaced", 0),
    }
    cache_set(cache_key, result, ttl=CACHE_TTL["savant_season"])
    return result


def get_pitcher_career_stats(player_id: int) -> dict:
    """Fetch career aggregated pitching stats."""
    cache_key = f"mlb_pitcher_career_{player_id}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        data = _get(f"/people/{player_id}/stats", params={
            "stats":  "career",
            "group":  "pitching",
            "sportId": 1,
        })
    except Exception:
        return {}

    splits = data.get("stats", [{}])[0].get("splits", [])
    if not splits:
        return {}

    s = splits[0].get("stat", {})
    result = {
        "era":           _f(s.get("era")),
        "whip":          _f(s.get("whip")),
        "k_pct":         _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
        "bb_pct":        _safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
        "gb_pct":        _safe_pct(s.get("groundOuts"),
                                    (s.get("groundOuts", 0) or 0) + (s.get("airOuts", 0) or 0)),
        "batters_faced": s.get("battersFaced", 0),
    }
    cache_set(cache_key, result, ttl=CACHE_TTL["park_factors"])
    return result


def get_pitcher_splits(player_id: int, season: int | None = None) -> dict:
    """
    Fetch pitcher splits: vs LHB, vs RHB, first inning.
    Returns dict keyed by split type.
    """
    season = season or date.today().year
    cache_key = f"mlb_pitcher_splits_{player_id}_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    splits_result = {}
    for stat_type, label in [
        ("statSplits", "vsleft"),
        ("statSplits", "vsright"),
    ]:
        pass  # MLB API splits are limited; Savant handles the rich splits

    # First-inning split
    try:
        data = _get(f"/people/{player_id}/stats", params={
            "stats":  "statSplits",
            "group":  "pitching",
            "season": season,
            "sitCodes": "i1",
            "sportId": 1,
        })
        for stat_block in data.get("stats", []):
            for sp in stat_block.get("splits", []):
                if sp.get("split", {}).get("code") == "i1":
                    s = sp.get("stat", {})
                    splits_result["first_inning"] = {
                        "era":   _f(s.get("era")),
                        "k_pct": _safe_pct(s.get("strikeOuts"), s.get("battersFaced")),
                        "bb_pct":_safe_pct(s.get("baseOnBalls"), s.get("battersFaced")),
                        "whip":  _f(s.get("whip")),
                        "batters_faced": s.get("battersFaced", 0),
                    }
    except Exception as exc:
        logger.debug("First-inning split fetch failed for %d: %s", player_id, exc)

    cache_set(cache_key, splits_result, ttl=CACHE_TTL["savant_splits"])
    return splits_result


def get_batter_season_stats(player_id: int, season: int | None = None) -> dict:
    """Fetch batter season stats (OBP, SLG, OPS, BB%, K%)."""
    season = season or date.today().year
    cache_key = f"mlb_batter_stats_{player_id}_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        data = _get(f"/people/{player_id}/stats", params={
            "stats":  "season",
            "group":  "hitting",
            "season": season,
            "sportId": 1,
        })
    except Exception:
        return {}

    splits = data.get("stats", [{}])[0].get("splits", [])
    if not splits:
        return {}

    s = splits[0].get("stat", {})
    result = {
        "avg":    _f(s.get("avg")),
        "obp":    _f(s.get("obp")),
        "slg":    _f(s.get("slg")),
        "ops":    _f(s.get("ops")),
        "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("plateAppearances")),
        "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("plateAppearances")),
        "pa":     s.get("plateAppearances", 0),
    }
    cache_set(cache_key, result, ttl=CACHE_TTL["savant_season"])
    return result


def get_batter_vs_hand_splits(player_id: int, season: int | None = None) -> dict:
    """Return batter splits vs LHP and RHP."""
    season = season or date.today().year
    cache_key = f"mlb_batter_hand_splits_{player_id}_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    result = {}
    for sit_code, hand_label in [("vl", "vs_lhp"), ("vr", "vs_rhp")]:
        try:
            data = _get(f"/people/{player_id}/stats", params={
                "stats":    "statSplits",
                "group":    "hitting",
                "season":   season,
                "sitCodes": sit_code,
                "sportId":  1,
            })
            for stat_block in data.get("stats", []):
                for sp in stat_block.get("splits", []):
                    if sp.get("split", {}).get("code") == sit_code:
                        s = sp.get("stat", {})
                        result[hand_label] = {
                            "obp":    _f(s.get("obp")),
                            "slg":    _f(s.get("slg")),
                            "ops":    _f(s.get("ops")),
                            "k_pct":  _safe_pct(s.get("strikeOuts"), s.get("plateAppearances")),
                            "bb_pct": _safe_pct(s.get("baseOnBalls"), s.get("plateAppearances")),
                            "pa":     s.get("plateAppearances", 0),
                        }
        except Exception as exc:
            logger.debug("Hand split fetch failed for %d %s: %s", player_id, sit_code, exc)

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_splits"])
    return result


def get_player_info(player_id: int) -> dict:
    """Return basic player info (name, position, bat side, throw hand)."""
    cache_key = f"mlb_player_info_{player_id}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        data = _get(f"/people/{player_id}")
    except Exception:
        return {}

    people = data.get("people", [])
    if not people:
        return {}

    p = people[0]
    result = {
        "id":        p.get("id"),
        "name":      p.get("fullName"),
        "bat_side":  p.get("batSide", {}).get("code", "R"),
        "pitch_hand":p.get("pitchHand", {}).get("code", "R"),
        "position":  p.get("primaryPosition", {}).get("abbreviation", ""),
    }
    cache_set(cache_key, result, ttl=CACHE_TTL["park_factors"])
    return result


# ── Venue info ────────────────────────────────────────────────────────────────

def get_venues() -> dict[int, dict]:
    """Return dict of venue_id -> venue info (name, city, roof type, lat/lon)."""
    cache_key = "mlb_venues_all"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        data = _get("/venues", params={"sportId": 1})
    except Exception:
        return {}

    venues = {}
    for v in data.get("venues", []):
        venues[v["id"]] = {
            "name":     v.get("name", ""),
            "city":     v.get("location", {}).get("city", ""),
            "state":    v.get("location", {}).get("state", ""),
            "country":  v.get("location", {}).get("country", ""),
            "lat":      v.get("location", {}).get("defaultCoordinates", {}).get("latitude"),
            "lon":      v.get("location", {}).get("defaultCoordinates", {}).get("longitude"),
            "roof_type":v.get("fieldInfo", {}).get("roofType", "Open"),
            "capacity": v.get("fieldInfo", {}).get("capacity"),
        }
    cache_set(cache_key, venues, ttl=CACHE_TTL["park_factors"])
    return venues


# ── Utilities ─────────────────────────────────────────────────────────────────

def _f(val) -> float | None:
    """Safe float conversion."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_pct(numerator, denominator) -> float | None:
    try:
        n, d = float(numerator), float(denominator)
        return n / d if d > 0 else None
    except (TypeError, ValueError):
        return None
