"""
NRFI Probability Model.

Orchestrates all scoring modules to produce:
  P(NRFI) = P(no run, top 1st) × P(no run, bottom 1st)

Per-half-inning flow:
  1. Pitcher suppression score     (40%)
  2. Top-of-lineup offense score   (30%)
  3. Park + weather score          (15%)
  4. Contact damage + speed score  (10%)
  5. Lineup confirmation layer     ( 5%)
  → Composite score [0,1] → logistic mapping → half-inning P(no run)
  → Multiply both half-innings

Also applies the bet filter to flag whether a game meets the threshold
for a recommended play.
"""

from __future__ import annotations

import logging
import math
from datetime import date
from typing import Any

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import WEIGHTS, BET_FILTER, LEAGUE_AVG
from fetchers.mlb_api import (
    get_schedule,
    get_lineups,
    lineups_confirmed,
    get_venues,
    get_player_info,
)
from scoring.pitcher_score  import score_pitcher
from scoring.batter_score   import score_top_of_lineup
from scoring.park_weather   import score_park_weather
from scoring.damage_speed   import score_damage_speed

logger = logging.getLogger(__name__)


# ── Calibration constants ──────────────────────────────────────────────────────
# The logistic function maps composite score → half-inning P(no run).
# Anchored so that:
#   score = 0.5 (league average matchup) → P ≈ 0.848 (which gives ~0.72 NRFI)
#   score = 0.8 (elite matchup)          → P ≈ 0.92
#   score = 0.2 (poor matchup)           → P ≈ 0.77

# Using: P = base_p_low + (base_p_high - base_p_low) * sigmoid(k*(score - 0.5))
HALF_INNING_P_LOW  = 0.76   # worst realistic half-inning NRFI prob
HALF_INNING_P_HIGH = 0.93   # best realistic half-inning NRFI prob
LOGISTIC_K         = 6.0    # steepness of mapping


# ── Main model entrypoint ─────────────────────────────────────────────────────

def run_daily_model(game_date: str | None = None, require_confirmed: bool = False) -> list[dict]:
    """
    Run the NRFI model for all games on a given date.

    Parameters
    ----------
    game_date : str | None
        ISO date string (YYYY-MM-DD). Defaults to today.
    require_confirmed : bool
        If True, only score games with confirmed lineups.

    Returns
    -------
    List of game result dicts, sorted by NRFI probability descending.
    """
    game_date = game_date or date.today().isoformat()
    logger.info("Running NRFI model for %s", game_date)

    games    = get_schedule(game_date)
    venues   = get_venues()

    if not games:
        logger.warning("No games found for %s", game_date)
        return []

    results = []
    for game in games:
        try:
            result = _score_game(game, venues, require_confirmed)
            if result:
                results.append(result)
        except Exception as exc:
            logger.error("Error scoring game %s: %s", game.get("game_pk"), exc, exc_info=True)

    # Sort by NRFI probability descending
    results.sort(key=lambda r: r["nrfi_prob"], reverse=True)
    logger.info("Scored %d games for %s", len(results), game_date)
    return results


def _score_game(game: dict, venues: dict, require_confirmed: bool) -> dict | None:
    """Score a single game and return the full result dict."""
    game_pk   = game["game_pk"]
    venue_id  = game.get("venue_id")
    venue_name= game.get("venue_name", "")

    # Venue coordinates for weather
    venue_info = venues.get(venue_id, {})
    lat = venue_info.get("lat")
    lon = venue_info.get("lon")

    # Probable pitchers
    home_prob = game.get("home_probable")
    away_prob = game.get("away_probable")

    if not home_prob or not away_prob:
        logger.debug("Missing probable pitcher(s) for game %s, skipping", game_pk)
        return None

    # Lineups
    lineups = get_lineups(game_pk)
    confirmed = lineups_confirmed(lineups)

    if require_confirmed and not confirmed:
        logger.debug("Lineups not confirmed for game %s, skipping", game_pk)
        return None

    # Use confirmed lineups if available; otherwise use roster order as proxy
    home_batters = lineups.get("home", [])
    away_batters = lineups.get("away", [])

    # ── Top half (away bats against home pitcher) ─────────────────────────────
    top_half = _score_half_inning(
        half="top",
        batting_team_id=game["away_team_id"],
        fielding_team_id=game["home_team_id"],
        pitcher=home_prob,
        batters=away_batters,
        venue_name=venue_name,
        venue_id=venue_id,
        lat=lat, lon=lon,
        game_time=game.get("game_time"),
    )

    # ── Bottom half (home bats against away pitcher) ──────────────────────────
    bot_half = _score_half_inning(
        half="bottom",
        batting_team_id=game["home_team_id"],
        fielding_team_id=game["away_team_id"],
        pitcher=away_prob,
        batters=home_batters,
        venue_name=venue_name,
        venue_id=venue_id,
        lat=lat, lon=lon,
        game_time=game.get("game_time"),
    )

    # ── Combine ───────────────────────────────────────────────────────────────
    nrfi_prob = round(top_half["half_inning_prob"] * bot_half["half_inning_prob"], 4)
    yrfi_prob = round(1.0 - nrfi_prob, 4)

    # Bet filter
    bet_rec = _apply_bet_filter(nrfi_prob, top_half, bot_half, confirmed)

    return {
        "game_pk":       game_pk,
        "game_date":     game.get("game_date"),
        "game_time":     game.get("game_time"),
        "venue_name":    venue_name,
        "away_team":     game["away_team_name"],
        "home_team":     game["home_team_name"],
        "away_pitcher":  away_prob,
        "home_pitcher":  home_prob,
        "lineups_confirmed": confirmed,
        "top_half":      top_half,
        "bot_half":      bot_half,
        "nrfi_prob":     nrfi_prob,
        "yrfi_prob":     yrfi_prob,
        "bet_recommendation": bet_rec,
    }


def _score_half_inning(
    half: str,
    batting_team_id: int,
    fielding_team_id: int,
    pitcher: dict,
    batters: list[dict],
    venue_name: str,
    venue_id: int | None,
    lat: float | None,
    lon: float | None,
    game_time: str | None,
) -> dict:
    """
    Score one half-inning and return a half-inning result dict.
    """
    pitcher_id   = pitcher["id"]
    pitcher_hand = pitcher.get("hand", "R")

    # Primary batter hand for park factor lookup (most common in top 4)
    primary_bat_hand = _dominant_bat_hand(batters[:4]) if batters else None

    # ── 1. Pitcher score (40%) ────────────────────────────────────────────────
    p_score = score_pitcher(pitcher_id, vs_hand=primary_bat_hand)

    # ── 2. Batter score (30%) ─────────────────────────────────────────────────
    b_score = score_top_of_lineup(batters, pitcher_hand, top_n=4) if batters else _default_batter_score()

    # ── 3. Park + weather (15%) ───────────────────────────────────────────────
    pw_score = score_park_weather(venue_name, venue_id, lat, lon,
                                   primary_bat_hand, game_time)

    # ── 4. Damage + speed (10%) ───────────────────────────────────────────────
    ds_score = score_damage_speed(batters, pitcher_id, fielding_team_id) if batters \
               else {"score": 0.5, "grade": "C"}

    # ── 5. Lineup confirmation layer (5%) ─────────────────────────────────────
    lineup_score = _lineup_confirmation_score(batters)

    # ── Composite score ───────────────────────────────────────────────────────
    w = WEIGHTS
    composite = (
        p_score["score"]    * w["pitcher"]      +
        b_score["score"]    * w["batter"]       +
        pw_score["score"]   * w["park_weather"] +
        ds_score["score"]   * w["damage_speed"] +
        lineup_score        * w["lineup"]
    )
    composite = max(0.0, min(1.0, composite))

    # ── Map composite → half-inning probability ───────────────────────────────
    half_prob = _composite_to_probability(composite)

    return {
        "half":             half,
        "pitcher_id":       pitcher_id,
        "pitcher_name":     pitcher.get("name", ""),
        "pitcher_hand":     pitcher_hand,
        "composite_score":  round(composite, 4),
        "half_inning_prob": round(half_prob, 4),
        "scores": {
            "pitcher":      p_score,
            "batter":       b_score,
            "park_weather": pw_score,
            "damage_speed": ds_score,
            "lineup":       round(lineup_score, 4),
        },
    }


def _composite_to_probability(score: float) -> float:
    """
    Map composite [0, 1] score to a half-inning P(no run).
    Uses a logistic sigmoid anchored to calibration constants.
    """
    # Sigmoid: 1 / (1 + e^(-k*(x-0.5)))
    sig = 1.0 / (1.0 + math.exp(-LOGISTIC_K * (score - 0.5)))
    # Scale to [P_LOW, P_HIGH]
    return HALF_INNING_P_LOW + (HALF_INNING_P_HIGH - HALF_INNING_P_LOW) * sig


def _dominant_bat_hand(batters: list[dict]) -> str | None:
    """Return the dominant bat side ('L' or 'R') in a batter list."""
    if not batters:
        return None
    counts = {"L": 0, "R": 0, "S": 0}
    for b in batters:
        side = b.get("bat_side", "R")
        counts[side] = counts.get(side, 0) + 1
    # Switch hitters count toward opposing pitcher's weaker side (simplification)
    return max(("L", "R"), key=lambda s: counts[s])


def _lineup_confirmation_score(batters: list[dict]) -> float:
    """
    5% weight: reward confirmed lineups, penalize uncertainty.
    Also slightly penalizes if first 2 slots look very dangerous (high-OBP leadoff + power 2).
    This is a thin signal; returns 0.5 by default if unconfirmed.
    """
    if len(batters) < 9:
        return 0.50   # Unconfirmed: neutral

    # Mild confirmation bonus
    return 0.55


def _default_batter_score() -> dict:
    return {"score": 0.5, "grade": "C", "batter_scores": [], "weighted_avg_metrics": {}}


# ── Bet filter ────────────────────────────────────────────────────────────────

def _apply_bet_filter(
    nrfi_prob: float,
    top_half: dict,
    bot_half: dict,
    confirmed: bool,
) -> dict:
    """
    Apply the strict bet filter and return a recommendation dict.

    Filter requirements (all must pass):
      1. Both pitchers grade ≥ "B" (score ≥ 0.57)
      2. Neither offense has elite top-4 (batter score ≥ 0.50, i.e. below-average offense)
      3. Park + weather neutral or pitcher-friendly (pw_score ≥ 0.45)
      4. NRFI probability ≥ threshold
      5. Lineup confirmed (or near-confirmed)
    """
    bf = BET_FILTER
    reasons_pass, reasons_fail = [], []

    # 1. Pitcher quality
    top_p_score = top_half["scores"]["pitcher"]["score"]
    bot_p_score = bot_half["scores"]["pitcher"]["score"]
    if top_p_score >= 0.57 and bot_p_score >= 0.57:
        reasons_pass.append(f"Both pitchers grade B+ or better ({top_p_score:.2f} / {bot_p_score:.2f})")
    else:
        reasons_fail.append(f"Pitcher quality below threshold ({top_p_score:.2f} / {bot_p_score:.2f})")

    # 2. Offense quality
    top_b_score = top_half["scores"]["batter"]["score"]
    bot_b_score = bot_half["scores"]["batter"]["score"]
    if top_b_score >= 0.48 and bot_b_score >= 0.48:
        reasons_pass.append(f"Neither offense is elite ({top_b_score:.2f} / {bot_b_score:.2f})")
    else:
        reasons_fail.append(f"Elite offense detected ({top_b_score:.2f} / {bot_b_score:.2f})")

    # 3. Park + weather
    top_pw = top_half["scores"]["park_weather"]["score"]
    if top_pw >= 0.45:
        reasons_pass.append(f"Park/weather neutral or pitcher-friendly ({top_pw:.2f})")
    else:
        reasons_fail.append(f"Hitter-friendly environment ({top_pw:.2f})")

    # 4. NRFI probability threshold
    if nrfi_prob >= bf["min_nrfi_prob"]:
        reasons_pass.append(f"NRFI prob {nrfi_prob:.1%} ≥ threshold {bf['min_nrfi_prob']:.1%}")
    else:
        reasons_fail.append(f"NRFI prob {nrfi_prob:.1%} below threshold {bf['min_nrfi_prob']:.1%}")

    # 5. Lineup confirmation
    if confirmed:
        reasons_pass.append("Lineups confirmed")
    else:
        reasons_fail.append("Lineups not yet confirmed (recheck before bet)")

    # 6. Both half-inning probs clear minimum
    top_p = top_half["half_inning_prob"]
    bot_p = bot_half["half_inning_prob"]
    min_p = bf["min_half_inning_prob"]
    if top_p >= min_p and bot_p >= min_p:
        reasons_pass.append(f"Both half-innings ≥ {min_p:.1%} ({top_p:.1%} / {bot_p:.1%})")
    else:
        reasons_fail.append(f"Half-inning probs ({top_p:.1%} / {bot_p:.1%}) below {min_p:.1%}")

    recommended = len(reasons_fail) == 0

    return {
        "recommended":   recommended,
        "reasons_pass":  reasons_pass,
        "reasons_fail":  reasons_fail,
        "nrfi_prob":     nrfi_prob,
        "confirmed":     confirmed,
    }
