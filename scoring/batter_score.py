"""
Top-of-lineup offensive quality scorer (30% of composite score).

Produces a score in [0, 1] where:
  1.0 = weak top-4 lineup vs pitcher's hand (NRFI-friendly)
  0.0 = elite top-4 lineup vs pitcher's hand (NRFI-unfriendly)

Key principle from the spec:
  "A walk or single from a leadoff hitter is dangerous because run expectancy
   jumps quickly with a runner on and no outs. OBP/xwOBA/BB% deserve heavier
   weight than season-long slugging alone."

Metric weights (within 30% block):
  xwOBA           30%
  OBP             25%
  BB%             20%
  Hard-hit %      15%
  Barrel %        10%

Only top-4 batters in the confirmed/projected lineup are scored.
Positions 1–4 are weighted: leadoff gets the most weight.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import BATTER_SUB_WEIGHTS, BLEND, LEAGUE_AVG, HAND_LEFT, HAND_RIGHT
from fetchers.mlb_api import (
    get_batter_season_stats,
    get_batter_vs_hand_splits,
    get_player_info,
)
from fetchers.savant import (
    get_batter_statcast_season,
    get_batter_splits_statcast,
    get_batter_statcast_prior_season,
)

logger = logging.getLogger(__name__)

# Batting order position weights (positions 1–4)
# Leadoff = most dangerous for first-inning run scoring
LINEUP_POS_WEIGHTS = {1: 0.35, 2: 0.28, 3: 0.22, 4: 0.15}


# ── Public entry point ────────────────────────────────────────────────────────

def score_top_of_lineup(
    batters: list[dict],
    pitcher_hand: str,
    top_n: int = 4,
) -> dict:
    """
    Score the top N batters in a lineup against a specific pitcher handedness.

    Parameters
    ----------
    batters : list[dict]
        Batting order list from mlb_api.get_lineups(), each with:
        {'order': int, 'player_id': int, 'name': str, 'bat_side': str}
    pitcher_hand : str
        Pitcher throwing hand ('L' or 'R').
    top_n : int
        How many top batters to evaluate (default 4).

    Returns
    -------
    dict with:
        score            - float [0, 1], NRFI-friendliness (1 = weak lineup)
        grade            - letter grade
        batter_scores    - per-batter breakdown
        weighted_avg_metrics - blended lineup metric summary
    """
    # Sort by batting order and take top N
    ordered = sorted(batters, key=lambda b: b.get("order", 99))[:top_n]
    if not ordered:
        return {"score": 0.5, "grade": "C", "batter_scores": [], "weighted_avg_metrics": {}}

    season = date.today().year
    batter_results = []
    weighted_metrics: dict[str, list[tuple[float, float]]] = {
        "xwoba": [], "obp": [], "bb_pct": [], "hard_hit_pct": [], "barrel_pct": []
    }

    for batter in ordered:
        pos      = batter.get("order", 5)
        pos_w    = LINEUP_POS_WEIGHTS.get(pos, 0.10)
        player_id= batter["player_id"]
        bat_side = batter.get("bat_side", "R")

        blended  = _blend_batter_metrics(player_id, season, pitcher_hand, bat_side)
        comp     = _score_batter_components(blended)
        b_score  = _weighted_sum(comp, BATTER_SUB_WEIGHTS)

        batter_results.append({
            "order":         pos,
            "player_id":     player_id,
            "name":          batter.get("name", ""),
            "bat_side":      bat_side,
            "score":         round(b_score, 4),
            "grade":         _grade(b_score),
            "components":    comp,
            "blended":       {k: v for k, v in blended.items() if not k.startswith("_")},
            "sample_warning":blended.get("_thin_sample", False),
            "pos_weight":    pos_w,
        })

        for metric_key in weighted_metrics:
            val = blended.get(metric_key)
            if val is not None:
                weighted_metrics[metric_key].append((val, pos_w))

    # Weighted-average lineup score
    total_pos_w = sum(b["pos_weight"] for b in batter_results)
    lineup_score = (
        sum(b["score"] * b["pos_weight"] for b in batter_results) / total_pos_w
        if total_pos_w > 0 else 0.5
    )

    # Summary weighted-average metrics for reporting
    avg_metrics = {}
    for key, vals in weighted_metrics.items():
        if vals:
            total_w = sum(w for _, w in vals)
            avg_metrics[key] = sum(v * w for v, w in vals) / total_w

    return {
        "score":                round(lineup_score, 4),
        "grade":                _grade(lineup_score),
        "batter_scores":        batter_results,
        "weighted_avg_metrics": avg_metrics,
        "pitcher_hand":         pitcher_hand,
        "top_n":                top_n,
    }


# ── Data blending ─────────────────────────────────────────────────────────────

def _blend_batter_metrics(
    player_id: int,
    season: int,
    pitcher_hand: str,
    bat_side: str,
) -> dict:
    """
    Build a blended metric dict for a single batter.
    Priority: Statcast splits vs pitcher hand > overall season > prior season.
    """
    # Savant season-level
    savant_all   = get_batter_statcast_season(season).get(player_id, {})
    savant_prior = get_batter_statcast_prior_season(player_id)

    # MLB Stats API (OBP, BB%)
    mlb_season   = get_batter_season_stats(player_id, season)
    mlb_hand     = get_batter_vs_hand_splits(player_id, season)

    # Savant splits vs pitcher hand
    split_label  = "vs_lhp" if pitcher_hand == HAND_LEFT else "vs_rhp"
    savant_split = get_batter_splits_statcast(player_id, season).get(split_label, {})
    mlb_split    = mlb_hand.get(split_label.replace("lhp", "lhp").replace("rhp", "rhp"), {})
    # MLB API key is vs_lhp or vs_rhp already

    pa = savant_all.get("pa") or mlb_season.get("pa") or 0
    regress_factor = max(0.0, 1.0 - pa / 200.0)  # full at 0 PA, none at 200+

    def blend(current, split, prior, league_avg: float) -> float:
        vals, wts = [], []
        if split is not None:
            vals.append(split); wts.append(0.40)
        if current is not None:
            vals.append(current); wts.append(0.30)
        if prior is not None:
            vals.append(prior); wts.append(0.20)
        if not vals:
            return league_avg
        total_w = sum(wts)
        blended_val = sum(v * w for v, w in zip(vals, wts)) / total_w
        return blended_val * (1 - regress_factor) + league_avg * regress_factor

    la = LEAGUE_AVG
    blended: dict[str, Any] = {}

    blended["xwoba"] = blend(
        savant_all.get("xwoba"),
        savant_split.get("xwoba"),
        savant_prior.get("xwoba"),
        la["batter_xwoba_avg"],
    )
    blended["obp"] = blend(
        mlb_season.get("obp"),
        mlb_split.get("obp"),
        None,
        la["batter_obp_avg"],
    )
    blended["bb_pct"] = blend(
        savant_all.get("bb_pct") or mlb_season.get("bb_pct"),
        savant_split.get("bb_pct") or mlb_split.get("bb_pct"),
        savant_prior.get("bb_pct"),
        la["batter_bb_pct_avg"],
    )
    blended["hard_hit_pct"] = blend(
        savant_all.get("hard_hit_pct"),
        savant_split.get("hard_hit_pct"),
        savant_prior.get("hard_hit_pct"),
        la["batter_hard_hit_avg"],
    )
    blended["barrel_pct"] = blend(
        savant_all.get("barrel_pct"),
        savant_split.get("barrel_pct"),
        savant_prior.get("barrel_pct"),
        la["batter_barrel_avg"],
    )

    blended["_thin_sample"] = pa < 30
    blended["_pa"] = pa
    return blended


# ── Scoring components ────────────────────────────────────────────────────────

def _score_batter_components(m: dict) -> dict[str, float]:
    """
    Convert blended metrics to [0, 1] component scores.
    For NRFI, higher batter output = lower score (1 = very weak batter).
    """
    la = LEAGUE_AVG

    # xwOBA: higher batter xwOBA → lower (worse for NRFI). Range ~0.250–0.420
    xwoba_score = _sigmoid_inverse(m["xwoba"], la["batter_xwoba_avg"],
                                    low=0.220, high=0.420)

    # OBP: higher OBP → lower score. Range ~0.260–0.430
    obp_score = _sigmoid_inverse(m["obp"], la["batter_obp_avg"],
                                   low=0.250, high=0.430)

    # BB%: higher BB% → lower score (better at reaching base). Range ~0.03–0.18
    bb_score = _sigmoid_inverse(m["bb_pct"], la["batter_bb_pct_avg"],
                                  low=0.030, high=0.180)

    # Hard-hit%: higher → lower score. Range ~0.25–0.55
    hh_score = _sigmoid_inverse(m["hard_hit_pct"], la["batter_hard_hit_avg"],
                                  low=0.250, high=0.550)

    # Barrel%: higher → lower score. Range ~0.02–0.20
    barrel_score = _sigmoid_inverse(m["barrel_pct"], la["batter_barrel_avg"],
                                     low=0.020, high=0.200)

    return {
        "xwoba":        round(xwoba_score, 4),
        "obp":          round(obp_score, 4),
        "bb_pct":       round(bb_score, 4),
        "hard_hit_pct": round(hh_score, 4),
        "barrel_pct":   round(barrel_score, 4),
    }


# ── Math helpers (duplicated from pitcher_score to keep modules self-contained) ─

def _sigmoid(val: float, avg: float, low: float, high: float) -> float:
    if high <= low:
        return 0.5
    normalized = (val - avg) / ((high - low) / 2.0)
    return 1.0 / (1.0 + 2.71828 ** (-3.0 * normalized))


def _sigmoid_inverse(val: float, avg: float, low: float, high: float) -> float:
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
