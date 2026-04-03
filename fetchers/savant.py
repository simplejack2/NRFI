"""
Baseball Savant / Statcast fetcher.
Pulls pitcher and batter expected stats, contact quality, sprint speed, pop time.

Savant exposes public CSV endpoints for leaderboard data and custom searches.
We avoid heavy scraping by using the documented CSV export URLs.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
from datetime import date
from typing import Any

import requests

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import SAVANT_BASE, CACHE_TTL
from fetchers._cache import cache_get, cache_set

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://baseballsavant.mlb.com/",
})


def _csv_get(url: str, params: dict | None = None, retries: int = 3) -> list[dict]:
    """Fetch a CSV endpoint and return list of row dicts."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            resp.raise_for_status()
            content = resp.text
            if not content or content.strip().startswith("<"):
                logger.warning("Non-CSV response from %s", url)
                return []
            reader = csv.DictReader(io.StringIO(content))
            return [row for row in reader]
        except requests.RequestException as exc:
            if attempt == retries - 1:
                logger.error("Savant request failed: %s -> %s", url, exc)
                return []
            wait = 2 ** attempt
            logger.warning("Savant attempt %d failed, retrying in %ds", attempt + 1, wait)
            time.sleep(wait)
    return []


def _current_season() -> int:
    return date.today().year


# ── Pitcher Statcast Leaderboard ──────────────────────────────────────────────

def get_pitcher_statcast_season(season: int | None = None) -> dict[int, dict]:
    """
    Fetch Statcast pitching leaderboard for a full season.
    Returns dict keyed by pitcher_id with xwOBA, hard-hit%, barrel%, K%, BB%,
    xERA, first-pitch strike rate, etc.
    """
    season = season or _current_season()
    cache_key = f"savant_pitcher_season_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    url = f"{SAVANT_BASE}/leaderboard/custom"
    params = {
        "year":           season,
        "type":           "pitcher",
        "filter":         "",
        "selections":     (
            "xwoba,xera,k_percent,bb_percent,whiff_percent,"
            "hard_hit_percent,barrel_batted_rate,groundballs_percent,"
            "exit_velocity_avg,launch_angle_avg,sweet_spot_percent"
        ),
        "chart":          "false",
        "x":              "xwoba",
        "y":              "xera",
        "r":              "no",
        "exactNameMatch": "false",
        "csv":            "true",
    }

    rows = _csv_get(url, params)
    result = _index_by_id(rows, "pitcher_id", _parse_pitcher_statcast_row)

    # Supplement with pitch arsenal / first-pitch-strike data
    fps_data = get_pitcher_first_pitch_strike(season)
    for pid, fps in fps_data.items():
        if pid in result:
            result[pid].update(fps)
        else:
            result[pid] = fps

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_season"])
    logger.info("Fetched Statcast pitcher data for %d pitchers (%d)", len(result), season)
    return result


def _parse_pitcher_statcast_row(row: dict) -> dict:
    return {
        "name":              row.get("last_name, first_name", ""),
        "xwoba_allowed":     _f(row.get("xwoba")),
        "xera":              _f(row.get("xera")),
        "k_pct":             _pct(row.get("k_percent")),
        "bb_pct":            _pct(row.get("bb_percent")),
        "whiff_pct":         _pct(row.get("whiff_percent")),
        "hard_hit_pct":      _pct(row.get("hard_hit_percent")),
        "barrel_pct":        _pct(row.get("barrel_batted_rate")),
        "gb_pct":            _pct(row.get("groundballs_percent")),
        "exit_velo_avg":     _f(row.get("exit_velocity_avg")),
        "launch_angle_avg":  _f(row.get("launch_angle_avg")),
        "sweet_spot_pct":    _pct(row.get("sweet_spot_percent")),
        "pa":                _i(row.get("pa")),
    }


def get_pitcher_first_pitch_strike(season: int | None = None) -> dict[int, dict]:
    """
    Fetch first-pitch strike rate and chase rate from Savant pitch movement/arsenal.
    Uses the statcast batter discipline leaderboard filtered to pitchers.
    """
    season = season or _current_season()
    cache_key = f"savant_fps_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    url = f"{SAVANT_BASE}/leaderboard/custom"
    params = {
        "year":       season,
        "type":       "pitcher",
        "filter":     "",
        "selections": "f_strike_percent,oz_swing_percent,iz_contact_percent,meatball_percent",
        "csv":        "true",
    }

    rows = _csv_get(url, params)
    result = _index_by_id(rows, "pitcher_id", lambda r: {
        "first_pitch_strike_pct": _pct(r.get("f_strike_percent")),
        "chase_pct":              _pct(r.get("oz_swing_percent")),
        "iz_contact_pct":         _pct(r.get("iz_contact_percent")),
        "meatball_pct":           _pct(r.get("meatball_percent")),
    })

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_season"])
    return result


# ── Pitcher splits (vs LHB / vs RHB) ─────────────────────────────────────────

def get_pitcher_splits_statcast(pitcher_id: int, season: int | None = None) -> dict:
    """
    Fetch pitcher Statcast splits vs LHB and RHB via statcast_search CSV.
    Returns dict with 'vs_lhb' and 'vs_rhb' sub-dicts.
    """
    season = season or _current_season()
    cache_key = f"savant_pitcher_splits_{pitcher_id}_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    result = {}
    for stand, label in [("L", "vs_lhb"), ("R", "vs_rhb")]:
        rows = _statcast_search_pitcher(pitcher_id, season, batter_stands=stand)
        if rows:
            result[label] = _aggregate_pitcher_statcast(rows)

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_splits"])
    return result


def _statcast_search_pitcher(
    pitcher_id: int,
    season: int,
    batter_stands: str | None = None,
    inning: int | None = None,
) -> list[dict]:
    """Run a statcast_search query for a specific pitcher."""
    params: dict[str, Any] = {
        "hfPT":        "",
        "hfAB":        "",
        "hfGT":        "R%7C",   # Regular season
        "hfPR":        "",
        "hfZ":         "",
        "stadium":     "",
        "hfBBL":       "",
        "hfNewZones":  "",
        "hfPull":      "",
        "hfC":         "",
        "hfSea":       f"{season}%7C",
        "hfSit":       "",
        "player_type": "pitcher",
        "hfOuts":      "",
        "opponent":    "",
        "pitcher_throws":"",
        "batter_stands": batter_stands or "",
        "hfSA":        "",
        "game_date_gt":"",
        "game_date_lt":"",
        "hfInfield":   "",
        "team":        "",
        "position":    "",
        "hfOutfield":  "",
        "hfRO":        "",
        "home_road":   "",
        "pitchers_lookup[]": pitcher_id,
        "hfFlag":      "",
        "hfBBT":       "",
        "metric_1":    "",
        "hfInn":       f"{inning}%7C" if inning else "",
        "min_pitches": "0",
        "min_results": "0",
        "group_by":    "name",
        "sort_col":    "pitches",
        "player_event_sort":"h_launch_speed",
        "sort_order":  "desc",
        "min_pas":     "0",
        "type":        "details",
        "csv":         "true",
    }
    url = f"{SAVANT_BASE}/statcast_search/csv"
    return _csv_get(url, params)


def _aggregate_pitcher_statcast(rows: list[dict]) -> dict:
    """Aggregate a list of pitch-level rows into summary rates."""
    if not rows:
        return {}

    total = len(rows)
    hard_hit   = sum(1 for r in rows if _f(r.get("launch_speed")) is not None and
                     (_f(r.get("launch_speed")) or 0) >= 95)
    barrels    = sum(1 for r in rows if r.get("barrel") == "1")
    # xwOBA aggregation - average of available values
    xwoba_vals = [_f(r.get("estimated_woba_using_speedangle")) for r in rows
                  if _f(r.get("estimated_woba_using_speedangle")) is not None]
    strikes    = sum(1 for r in rows if r.get("type") in ("S", "X"))
    first_pitch_rows = [r for r in rows if r.get("pitch_number") == "1"]

    batted_balls = sum(1 for r in rows if r.get("type") == "X")

    return {
        "xwoba_allowed":   sum(xwoba_vals) / len(xwoba_vals) if xwoba_vals else None,
        "hard_hit_pct":    hard_hit / batted_balls if batted_balls else None,
        "barrel_pct":      barrels / batted_balls if batted_balls else None,
        "strike_pct":      strikes / total if total else None,
        "first_pitch_strike_pct": (
            sum(1 for r in first_pitch_rows if r.get("type") in ("S", "X")) /
            len(first_pitch_rows) if first_pitch_rows else None
        ),
        "sample_pitches": total,
    }


# ── Batter Statcast Leaderboard ───────────────────────────────────────────────

def get_batter_statcast_season(season: int | None = None) -> dict[int, dict]:
    """
    Fetch Statcast batting leaderboard.
    Returns dict keyed by batter_id with xwOBA, hard-hit%, barrel%, sprint speed.
    """
    season = season or _current_season()
    cache_key = f"savant_batter_season_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    url = f"{SAVANT_BASE}/leaderboard/custom"
    params = {
        "year":       season,
        "type":       "batter",
        "filter":     "",
        "selections": (
            "xwoba,xba,xslg,k_percent,bb_percent,"
            "hard_hit_percent,barrel_batted_rate,"
            "exit_velocity_avg,launch_angle_avg,sweet_spot_percent,"
            "oz_swing_percent,whiff_percent"
        ),
        "csv": "true",
    }

    rows = _csv_get(url, params)
    result = _index_by_id(rows, "batter_id", _parse_batter_statcast_row)

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_season"])
    logger.info("Fetched Statcast batter data for %d batters (%d)", len(result), season)
    return result


def _parse_batter_statcast_row(row: dict) -> dict:
    return {
        "name":          row.get("last_name, first_name", ""),
        "xwoba":         _f(row.get("xwoba")),
        "xba":           _f(row.get("xba")),
        "xslg":          _f(row.get("xslg")),
        "k_pct":         _pct(row.get("k_percent")),
        "bb_pct":        _pct(row.get("bb_percent")),
        "hard_hit_pct":  _pct(row.get("hard_hit_percent")),
        "barrel_pct":    _pct(row.get("barrel_batted_rate")),
        "exit_velo_avg": _f(row.get("exit_velocity_avg")),
        "sweet_spot_pct":_pct(row.get("sweet_spot_percent")),
        "chase_pct":     _pct(row.get("oz_swing_percent")),
        "whiff_pct":     _pct(row.get("whiff_percent")),
        "pa":            _i(row.get("pa")),
    }


def get_batter_splits_statcast(batter_id: int, season: int | None = None) -> dict:
    """
    Fetch batter Statcast splits vs LHP and RHP.
    """
    season = season or _current_season()
    cache_key = f"savant_batter_splits_{batter_id}_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    result = {}
    url = f"{SAVANT_BASE}/leaderboard/custom"

    for throws, label in [("L", "vs_lhp"), ("R", "vs_rhp")]:
        params = {
            "year":           season,
            "type":           "batter",
            "filter":         "",
            "selections":     "xwoba,k_percent,bb_percent,hard_hit_percent,barrel_batted_rate",
            "pitcher_throws": throws,
            "player_lookup[]": batter_id,
            "csv":            "true",
        }
        rows = _csv_get(url, params)
        if rows:
            r = rows[0]
            result[label] = {
                "xwoba":        _f(r.get("xwoba")),
                "k_pct":        _pct(r.get("k_percent")),
                "bb_pct":       _pct(r.get("bb_percent")),
                "hard_hit_pct": _pct(r.get("hard_hit_percent")),
                "barrel_pct":   _pct(r.get("barrel_batted_rate")),
                "pa":           _i(r.get("pa")),
            }

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_splits"])
    return result


# ── Sprint Speed Leaderboard ──────────────────────────────────────────────────

def get_sprint_speed(season: int | None = None) -> dict[int, dict]:
    """
    Fetch Statcast sprint speed leaderboard.
    Returns dict keyed by player_id with sprint_speed (ft/s).
    """
    season = season or _current_season()
    cache_key = f"savant_sprint_speed_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    url = f"{SAVANT_BASE}/leaderboard/sprint_speed"
    params = {
        "year":     season,
        "position": "",
        "team":     "",
        "min":      "10",
        "csv":      "true",
    }

    rows = _csv_get(url, params)
    result: dict[int, dict] = {}
    for row in rows:
        pid = _i(row.get("player_id") or row.get("mlb_id"))
        if pid:
            result[pid] = {
                "name":         row.get("last_name, first_name", row.get("player_name", "")),
                "sprint_speed": _f(row.get("r_sprint_speed_top50percent")),
                "hp_to_1b":     _f(row.get("hp_to_1b")),
            }

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_season"])
    logger.info("Fetched sprint speed for %d players (%d)", len(result), season)
    return result


# ── Catcher Pop Time ──────────────────────────────────────────────────────────

def get_pop_time(season: int | None = None) -> dict[int, dict]:
    """
    Fetch catcher pop time leaderboard.
    Returns dict keyed by catcher player_id with pop_time (seconds to 2B).
    """
    season = season or _current_season()
    cache_key = f"savant_pop_time_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    url = f"{SAVANT_BASE}/leaderboard/pop-time"
    params = {
        "year":       season,
        "minThrows":  "10",
        "minOppSBA":  "10",
        "csv":        "true",
    }

    rows = _csv_get(url, params)
    result: dict[int, dict] = {}
    for row in rows:
        pid = _i(row.get("catcher_id") or row.get("player_id"))
        if pid:
            result[pid] = {
                "name":         row.get("catcher_name", ""),
                "pop_2b_sba":   _f(row.get("pop_2b_sba_count_sr")),   # exchange + throw time
                "pop_2b_cs":    _f(row.get("pop_2b_cs_count_sr")),
                "throws_2b":    _i(row.get("throws_2b_sba")),
                "cs_pct":       _pct(row.get("cs_pct")),
            }

    cache_set(cache_key, result, ttl=CACHE_TTL["savant_season"])
    logger.info("Fetched pop time for %d catchers (%d)", len(result), season)
    return result


# ── Prior-season fallback ─────────────────────────────────────────────────────

def get_pitcher_statcast_prior_season(player_id: int) -> dict:
    """Get last year's Statcast stats for a pitcher (regression anchor)."""
    prior = _current_season() - 1
    season_data = get_pitcher_statcast_season(prior)
    return season_data.get(player_id, {})


def get_batter_statcast_prior_season(player_id: int) -> dict:
    prior = _current_season() - 1
    season_data = get_batter_statcast_season(prior)
    return season_data.get(player_id, {})


# ── Roster catcher lookup helper ─────────────────────────────────────────────

def get_team_catchers(team_id: int, season: int | None = None) -> list[dict]:
    """
    Return active catchers on a team roster via MLB Stats API.
    Used to cross-reference pop-time data.
    """
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from fetchers.mlb_api import _get as mlb_get

    season = season or _current_season()
    cache_key = f"mlb_roster_catchers_{team_id}_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        data = mlb_get(f"/teams/{team_id}/roster", params={
            "rosterType": "active",
            "season":     season,
        })
    except Exception:
        return []

    catchers = []
    for entry in data.get("roster", []):
        pos = entry.get("position", {}).get("abbreviation", "")
        if pos == "C":
            catchers.append({
                "player_id": entry["person"]["id"],
                "name":      entry["person"]["fullName"],
            })

    cache_set(cache_key, catchers, ttl=CACHE_TTL["savant_season"])
    return catchers


# ── Shared utilities ──────────────────────────────────────────────────────────

def _index_by_id(rows: list[dict], id_field: str, parser) -> dict[int, dict]:
    result: dict[int, dict] = {}
    for row in rows:
        pid = _i(row.get(id_field))
        if pid:
            result[pid] = parser(row)
    return result


def _f(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _pct(val) -> float | None:
    """Convert a percentage string like '22.5' to 0.225."""
    v = _f(val)
    if v is None:
        return None
    return v / 100.0 if v > 1.0 else v


def _i(val) -> int | None:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None
