"""
Park + weather environment scorer (15% of composite score).

Produces a score in [0, 1] where:
  1.0 = maximum pitcher-friendly environment (cold, wind-in, pitcher's park)
  0.0 = maximum hitter-friendly environment (hot, wind-out, hitter's park)

Park factor is the primary driver; weather adds a secondary layer.
"""

from __future__ import annotations

import logging
from typing import Any

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import PARK_FACTOR_NEUTRAL
from fetchers.fangraphs import get_park_factor_for_venue
from fetchers.weather import get_weather_for_venue

logger = logging.getLogger(__name__)


def score_park_weather(
    venue_name: str,
    venue_id: int | None,
    lat: float | None,
    lon: float | None,
    primary_batter_hand: str | None = None,
    game_time_utc: str | None = None,
) -> dict:
    """
    Score the park + weather environment for a half-inning.

    Parameters
    ----------
    venue_name : str
        Official ballpark name.
    venue_id : int | None
        MLB venue ID (not used directly but kept for extensibility).
    lat, lon : float | None
        Coordinates for weather lookup.
    primary_batter_hand : str | None
        Dominant bat side of the opposing lineup ('L', 'R', or None).
    game_time_utc : str | None
        ISO datetime string for game time (used for forecast lookups, future use).

    Returns
    -------
    dict with:
        score               - float [0, 1]
        grade               - letter grade
        park_factor         - raw FanGraphs park factor (100 = neutral)
        park_adjustment     - float from park factor centered on 0
        weather_adjustment  - float from weather module
        weather_detail      - raw weather conditions dict
        combined_adjustment - sum of park + weather adjustments
    """
    # ── Park factor ───────────────────────────────────────────────────────────
    pf_data = get_park_factor_for_venue(venue_name, primary_batter_hand)
    park_factor   = pf_data.get("relevant", PARK_FACTOR_NEUTRAL)
    park_adj      = pf_data.get("adjustment", 0.0)    # centered on 0; positive = hitter-friendly

    # ── Weather ───────────────────────────────────────────────────────────────
    wx_data  = get_weather_for_venue(venue_name, lat, lon, game_time_utc)
    wx_adj   = wx_data.get("weather_adjustment", 0.0)  # positive = hitter-friendly

    # ── Combined environment adjustment ──────────────────────────────────────
    # Scale: park factor contributes more (2/3) than weather (1/3)
    combined_adj = park_adj * (2.0 / 3.0) + wx_adj * (1.0 / 3.0)

    # ── Convert to [0, 1] score ───────────────────────────────────────────────
    # combined_adj in roughly [-0.20, +0.20]
    # Positive adj = hitter-friendly → lower NRFI score
    # Negative adj = pitcher-friendly → higher NRFI score
    #
    # Map: adj = -0.20 → 1.0 (very pitcher-friendly)
    #      adj =  0.00 → 0.5 (neutral)
    #      adj = +0.20 → 0.0 (very hitter-friendly)
    score = 0.5 - (combined_adj / 0.40)
    score = max(0.0, min(1.0, score))

    return {
        "score":               round(score, 4),
        "grade":               _grade(score),
        "venue_name":          venue_name,
        "park_factor":         park_factor,
        "park_adjustment":     round(park_adj, 4),
        "weather_adjustment":  round(wx_adj, 4),
        "combined_adjustment": round(combined_adj, 4),
        "weather_detail":      wx_data,
        "roof_type":           wx_data.get("roof_type", "open"),
    }


def _grade(score: float) -> str:
    if score >= 0.80: return "A+"
    if score >= 0.72: return "A"
    if score >= 0.65: return "B+"
    if score >= 0.57: return "B"
    if score >= 0.50: return "C+"
    if score >= 0.43: return "C"
    if score >= 0.35: return "D+"
    return "D"
