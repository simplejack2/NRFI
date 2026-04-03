"""
Contact damage + baserunner speed modifier (10% of composite score).

Two sub-components:
  A. Contact damage ceiling (5%)
     Pitcher's hard-hit suppression + barrel suppression vs top-of-lineup.
     Already partially captured in pitcher_score, but here we look at the
     matchup-level interaction: does this specific top-4 have exceptional
     exit velocity upside against this specific pitcher type?

  B. Baserunner speed risk (5%)
     How fast is the top of the lineup? If they can steal bases after reaching,
     the run-scoring probability from a single leadoff hit goes up substantially.
     Measured by Statcast Sprint Speed vs catcher Pop Time.

Score: 1.0 = minimal damage ceiling + slow lineup + good catcher (NRFI-friendly)
       0.0 = high damage ceiling + blazing lineup + weak catcher arm
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import LEAGUE_AVG
from fetchers.savant import (
    get_sprint_speed,
    get_pop_time,
    get_batter_statcast_season,
    get_pitcher_statcast_season,
    get_team_catchers,
)

logger = logging.getLogger(__name__)

# League-average sprint speed (ft/s) – Statcast average ~27 ft/s
LEAGUE_AVG_SPRINT = 27.0
# League-average pop time to 2B (seconds) – ~2.02s
LEAGUE_AVG_POP_TIME = 2.02


def score_damage_speed(
    batters: list[dict],
    pitcher_id: int,
    defending_team_id: int,
    top_n: int = 4,
) -> dict:
    """
    Score the contact-damage ceiling and baserunner speed risk.

    Parameters
    ----------
    batters : list[dict]
        Batting order from mlb_api.get_lineups() (attacking team's lineup).
    pitcher_id : int
        Starting pitcher MLB ID.
    defending_team_id : int
        Team ID of the fielding/catching team (to look up their catcher).
    top_n : int
        How many top batters to consider.

    Returns
    -------
    dict with score, grade, damage_score, speed_score, components.
    """
    season = date.today().year
    ordered = sorted(batters, key=lambda b: b.get("order", 99))[:top_n]

    damage_score = _score_damage(ordered, pitcher_id, season)
    speed_score  = _score_speed(ordered, defending_team_id, season)

    # Equal split (5% + 5% = 10% total weight)
    combined = 0.5 * damage_score + 0.5 * speed_score

    return {
        "score":         round(combined, 4),
        "grade":         _grade(combined),
        "damage_score":  round(damage_score, 4),
        "speed_score":   round(speed_score, 4),
        "components": {
            "damage_ceiling": round(damage_score, 4),
            "baserunner_speed": round(speed_score, 4),
        },
    }


# ── Contact damage ceiling ────────────────────────────────────────────────────

def _score_damage(batters: list[dict], pitcher_id: int, season: int) -> float:
    """
    Score = 1.0 if the pitcher suppresses hard contact against these hitters.
    Uses exit velocity and barrel rate as proxies.
    """
    # Pitcher's hard-hit and barrel suppression
    pitcher_savant = get_pitcher_statcast_season(season).get(pitcher_id, {})
    pitcher_hh     = pitcher_savant.get("hard_hit_pct", LEAGUE_AVG["hard_hit_pct_avg"])
    pitcher_barrel = pitcher_savant.get("barrel_pct",   LEAGUE_AVG["barrel_pct_avg"])

    # Average hard-hit and barrel rate across top batters
    batter_savant = get_batter_statcast_season(season)
    batter_hh_vals, batter_barrel_vals = [], []
    for b in batters:
        bs = batter_savant.get(b["player_id"], {})
        hh = bs.get("hard_hit_pct")
        brl = bs.get("barrel_pct")
        if hh is not None:    batter_hh_vals.append(hh)
        if brl is not None:   batter_barrel_vals.append(brl)

    avg_batter_hh     = (sum(batter_hh_vals) / len(batter_hh_vals)
                         if batter_hh_vals else LEAGUE_AVG["batter_hard_hit_avg"])
    avg_batter_barrel = (sum(batter_barrel_vals) / len(batter_barrel_vals)
                         if batter_barrel_vals else LEAGUE_AVG["batter_barrel_avg"])

    # Matchup damage estimate: average of pitcher suppression and batter output
    matchup_hh     = (pitcher_hh     + avg_batter_hh)     / 2.0
    matchup_barrel = (pitcher_barrel + avg_batter_barrel)  / 2.0

    # Score: lower matchup hard-hit/barrel → higher score
    hh_score     = _sigmoid_inverse(matchup_hh,     0.370, low=0.25, high=0.52)
    barrel_score = _sigmoid_inverse(matchup_barrel, 0.080, low=0.03, high=0.14)

    return 0.6 * hh_score + 0.4 * barrel_score


# ── Baserunner speed risk ─────────────────────────────────────────────────────

def _score_speed(batters: list[dict], defending_team_id: int, season: int) -> float:
    """
    Score = 1.0 if the top of the lineup is slow AND catcher has elite pop time.
    Score = 0.0 if lineup is blazing fast AND catcher can't throw.
    """
    sprint_data = get_sprint_speed(season)
    pop_data    = get_pop_time(season)

    # Average sprint speed of top batters
    speeds = []
    for b in batters:
        sp = sprint_data.get(b["player_id"], {})
        spd = sp.get("sprint_speed")
        if spd is not None:
            speeds.append(spd)

    avg_speed = sum(speeds) / len(speeds) if speeds else LEAGUE_AVG_SPRINT

    # Catcher pop time for the defending team
    catchers  = get_team_catchers(defending_team_id, season)
    pop_times = []
    for c in catchers:
        pt = pop_data.get(c["player_id"], {})
        pt_val = pt.get("pop_2b_sba")
        if pt_val is not None:
            pop_times.append(pt_val)

    # Use best catcher (lowest pop time = fastest)
    best_pop_time = min(pop_times) if pop_times else LEAGUE_AVG_POP_TIME

    # Sprint speed: higher speed → lower NRFI score
    # Range: ~24 ft/s (slow) to ~30 ft/s (elite speed)
    speed_score = _sigmoid_inverse(avg_speed, LEAGUE_AVG_SPRINT, low=24.0, high=30.0)

    # Pop time: lower pop time → higher NRFI score (catcher can shut down running game)
    # Range: ~1.85s (elite) to ~2.20s (poor)
    pop_score = _sigmoid_inverse(best_pop_time, LEAGUE_AVG_POP_TIME, low=1.85, high=2.25)

    # Combined: average of the two
    return 0.55 * speed_score + 0.45 * pop_score


# ── Math helpers ──────────────────────────────────────────────────────────────

def _sigmoid(val: float, avg: float, low: float, high: float) -> float:
    if high <= low:
        return 0.5
    normalized = (val - avg) / ((high - low) / 2.0)
    return 1.0 / (1.0 + 2.71828 ** (-3.0 * normalized))


def _sigmoid_inverse(val: float, avg: float, low: float, high: float) -> float:
    return _sigmoid(-val, -avg, -high, -low)


def _grade(score: float) -> str:
    if score >= 0.80: return "A+"
    if score >= 0.72: return "A"
    if score >= 0.65: return "B+"
    if score >= 0.57: return "B"
    if score >= 0.50: return "C+"
    if score >= 0.43: return "C"
    if score >= 0.35: return "D+"
    return "D"
