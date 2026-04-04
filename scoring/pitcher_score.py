"""
Pitcher suppression scorer (40% of composite score).

Produces a score in [0, 1] where:
  1.0 = elite first-inning suppressor (maximum NRFI-friendly)
  0.0 = historically allows runs in the first (minimum NRFI-friendly)

Metric stack (per the design spec):
  - xwOBA allowed           25%
  - K%                      20%
  - BB%                     15%
  - First-pitch strike rate 15%
  - Hard-hit %              10%
  - Barrel %                10%
  - GB %                     5%

Data blending (career + prior year + rolling + current season) is applied
before scoring so small early-season samples don't dominate.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import (
    PITCHER_SUB_WEIGHTS, BLEND, LEAGUE_AVG, WEIGHTS,
    HAND_LEFT, HAND_RIGHT,
)
from fetchers.mlb_api import (
    get_pitcher_season_stats,
    get_pitcher_career_stats,
    get_pitcher_splits,
    get_player_info,
)
from fetchers.savant import (
    get_pitcher_statcast_season,
    get_pitcher_splits_statcast,
    get_pitcher_statcast_prior_season,
)

logger = logging.getLogger(__name__)


# ── Public entry point ────────────────────────────────────────────────────────

def score_pitcher(pitcher_id: int | None, vs_hand: str | None = None) -> dict:
    """
    Compute the full pitcher suppression score for a given starter.

    Parameters
    ----------
    pitcher_id : int
        MLB player ID for the starting pitcher.
    vs_hand : str | None
        Batting handedness of the opposing lineup ('L', 'R', or None for mixed).

    Returns
    -------
    dict with keys:
        score            - float [0, 1], pitcher-friendliness
        grade            - letter grade string
        components       - per-metric breakdown
        blended_metrics  - the blended stat line used for scoring
        sample_warning   - bool, True if sample size is thin
    """
    # Unknown pitcher — return league-average score
    if not pitcher_id:
        return {
            "pitcher_id": None, "score": 0.5, "grade": "C",
            "components": {}, "blended_metrics": {}, "sample_warning": True,
            "vs_hand": vs_hand,
        }
    season = date.today().year
    blended = _blend_pitcher_metrics(pitcher_id, season, vs_hand)
    components = _score_components(blended)
    score = _weighted_sum(components, PITCHER_SUB_WEIGHTS)

    return {
        "pitcher_id":     pitcher_id,
        "score":          round(score, 4),
        "grade":          _grade(score),
        "components":     components,
        "blended_metrics":blended,
        "sample_warning": blended.get("_thin_sample", False),
        "vs_hand":        vs_hand,
    }


# ── Data blending ─────────────────────────────────────────────────────────────

def _blend_pitcher_metrics(pitcher_id: int, season: int, vs_hand: str | None) -> dict:
    """
    Build a blended metric dict using career/prior/rolling/current weights.
    Sources: MLB Stats API + Baseball Savant.
    Falls back to league-average when a metric is missing.
    """
    # 1. Savant season leaderboard (current season)
    savant_current = get_pitcher_statcast_season(season).get(pitcher_id, {})
    savant_prior   = get_pitcher_statcast_prior_season(pitcher_id)

    # 2. MLB Stats API season + career
    mlb_season  = get_pitcher_season_stats(pitcher_id, season)
    mlb_career  = get_pitcher_career_stats(pitcher_id)
    mlb_splits  = get_pitcher_splits(pitcher_id, season)

    # 3. Savant splits (vs specific hand)
    savant_splits = {}
    if vs_hand:
        split_key = "vs_lhb" if vs_hand == HAND_LEFT else "vs_rhb"
        savant_splits = get_pitcher_splits_statcast(pitcher_id, season).get(split_key, {})

    # 4. First-inning specific from MLB splits
    fi_split = mlb_splits.get("first_inning", {})

    # ── Blend each metric ─────────────────────────────────────────────────────
    def blend(metric: str, current_val, prior_val, career_val,
              league_avg: float, hand_split_val=None) -> float:
        """Weighted blend with regression to league average."""
        vals = []
        weights = []

        if current_val is not None:
            vals.append(current_val)
            weights.append(BLEND["current_season"])
        if prior_val is not None:
            vals.append(prior_val)
            weights.append(BLEND["prior_year_weight"])
        if career_val is not None:
            vals.append(career_val)
            weights.append(BLEND["career_weight"])

        # If we have a hand-specific split, give it some weight
        if hand_split_val is not None:
            vals.append(hand_split_val)
            weights.append(0.15)

        if not vals:
            return league_avg

        total_w = sum(weights)
        blended_val = sum(v * w for v, w in zip(vals, weights)) / total_w

        # Light regression toward league average based on sample size
        bf = savant_current.get("pa") or mlb_season.get("batters_faced") or 0
        regress_factor = max(0.0, 1.0 - bf / 400.0)  # full regression at 0 PA, none at 400+
        return blended_val * (1 - regress_factor) + league_avg * regress_factor

    la = LEAGUE_AVG
    blended: dict[str, Any] = {}

    blended["xwoba_allowed"] = blend(
        "xwoba_allowed",
        savant_current.get("xwoba_allowed"),
        savant_prior.get("xwoba_allowed"),
        None,
        la["xwoba_allowed_avg"],
        savant_splits.get("xwoba_allowed"),
    )
    blended["k_pct"] = blend(
        "k_pct",
        savant_current.get("k_pct") or mlb_season.get("k_pct"),
        savant_prior.get("k_pct"),
        mlb_career.get("k_pct"),
        la["k_pct_avg"],
        savant_splits.get("k_pct"),
    )
    blended["bb_pct"] = blend(
        "bb_pct",
        savant_current.get("bb_pct") or mlb_season.get("bb_pct"),
        savant_prior.get("bb_pct"),
        mlb_career.get("bb_pct"),
        la["bb_pct_avg"],
        savant_splits.get("bb_pct"),
    )
    blended["first_pitch_strike"] = blend(
        "first_pitch_strike",
        savant_current.get("first_pitch_strike_pct"),
        savant_prior.get("first_pitch_strike_pct"),
        None,
        la["first_pitch_strike_avg"],
        None,
    )
    blended["hard_hit_pct"] = blend(
        "hard_hit_pct",
        savant_current.get("hard_hit_pct"),
        savant_prior.get("hard_hit_pct"),
        None,
        la["hard_hit_pct_avg"],
        savant_splits.get("hard_hit_pct"),
    )
    blended["barrel_pct"] = blend(
        "barrel_pct",
        savant_current.get("barrel_pct"),
        savant_prior.get("barrel_pct"),
        None,
        la["barrel_pct_avg"],
        savant_splits.get("barrel_pct"),
    )
    blended["gb_pct"] = blend(
        "gb_pct",
        savant_current.get("gb_pct") or mlb_season.get("gb_pct"),
        savant_prior.get("gb_pct"),
        mlb_career.get("gb_pct"),
        la["gb_pct_avg"],
        None,
    )

    # First-inning ERA boost: if pitcher has solid first-inning ERA, nudge the score
    fi_era = fi_split.get("era")
    blended["first_inning_era"] = fi_era
    blended["first_inning_k_pct"] = fi_split.get("k_pct")
    blended["first_inning_bb_pct"] = fi_split.get("bb_pct")

    # Sample size flag
    bf_total = savant_current.get("pa") or mlb_season.get("batters_faced") or 0
    blended["_thin_sample"] = bf_total < 50
    blended["_batters_faced"] = bf_total

    return blended


# ── Scoring components ────────────────────────────────────────────────────────

def _score_components(m: dict) -> dict[str, float]:
    """
    Convert each blended metric to a [0, 1] component score.
    For pitcher metrics, lower xwOBA/BB%/hard-hit% → higher score (better for NRFI).
    Higher K%/FPS/GB% → higher score.
    """
    la = LEAGUE_AVG

    # xwOBA allowed: lower is better. Range ~0.20 (elite) to ~0.40 (poor)
    xwoba_score = _sigmoid_inverse(m["xwoba_allowed"], la["xwoba_allowed_avg"],
                                    low=0.200, high=0.400)

    # K%: higher is better. Range ~0.10 to ~0.40
    k_score = _sigmoid(m["k_pct"], la["k_pct_avg"], low=0.10, high=0.40)

    # BB%: lower is better. Range ~0.03 to ~0.16
    bb_score = _sigmoid_inverse(m["bb_pct"], la["bb_pct_avg"], low=0.03, high=0.16)

    # First-pitch strike: higher is better. Range ~0.50 to ~0.75
    fps_score = _sigmoid(m["first_pitch_strike"], la["first_pitch_strike_avg"],
                          low=0.50, high=0.75)

    # Hard-hit%: lower is better. Range ~0.25 to ~0.50
    hh_score = _sigmoid_inverse(m["hard_hit_pct"], la["hard_hit_pct_avg"],
                                  low=0.25, high=0.50)

    # Barrel%: lower is better. Range ~0.02 to ~0.15
    barrel_score = _sigmoid_inverse(m["barrel_pct"], la["barrel_pct_avg"],
                                     low=0.02, high=0.15)

    # GB%: higher is better (fewer fly balls). Range ~0.30 to ~0.60
    gb_score = _sigmoid(m["gb_pct"], la["gb_pct_avg"], low=0.30, high=0.60)

    # ── First-inning specific adjustment ─────────────────────────────────────
    fi_adj = 0.0
    fi_era = m.get("first_inning_era")
    if fi_era is not None and m.get("_batters_faced", 0) >= 50:
        # First-inning ERA: league avg ~4.50; add a small bonus/penalty
        fi_adj = _sigmoid_inverse(fi_era / 9.0, 0.50, low=0.20, high=0.80)
        fi_adj = (fi_adj - 0.5) * 0.10   # Scale to [-0.05, +0.05]

    return {
        "xwoba_allowed": round(xwoba_score + fi_adj, 4),
        "k_pct":         round(k_score, 4),
        "bb_pct":        round(bb_score, 4),
        "first_pitch_strike": round(fps_score, 4),
        "hard_hit_pct":  round(hh_score, 4),
        "barrel_pct":    round(barrel_score, 4),
        "gb_pct":        round(gb_score, 4),
    }


# ── Math helpers ──────────────────────────────────────────────────────────────

def _sigmoid(val: float, avg: float, low: float, high: float) -> float:
    """
    Map a value to [0, 1] where val == avg → 0.5.
    val == high → ~0.85; val == low → ~0.15.
    Higher val = better (more NRFI-friendly).
    """
    if high <= low:
        return 0.5
    # Normalize to [-1, 1] centered on avg
    normalized = (val - avg) / ((high - low) / 2.0)
    return 1.0 / (1.0 + 2.71828 ** (-3.0 * normalized))


def _sigmoid_inverse(val: float, avg: float, low: float, high: float) -> float:
    """
    Like _sigmoid but inverted: lower val = higher score.
    """
    return _sigmoid(-val, -avg, -high, -low)


def _weighted_sum(components: dict[str, float], weights: dict[str, float]) -> float:
    total_w = sum(weights.values())
    return sum(components.get(k, 0.5) * w for k, w in weights.items()) / total_w


def _grade(score: float) -> str:
    if score >= 0.80: return "A+"
    if score >= 0.72: return "A"
    if score >= 0.65: return "B+"
    if score >= 0.57: return "B"
    if score >= 0.50: return "C+"
    if score >= 0.43: return "C"
    if score >= 0.35: return "D+"
    return "D"
