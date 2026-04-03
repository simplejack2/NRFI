"""
NRFI Predictor - Configuration
Central constants, weights, thresholds, and API endpoints.
"""

import os
# Repo root = directory containing this file, works locally and in CI
_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── Scoring weights (must sum to 1.0) ─────────────────────────────────────────
WEIGHTS = {
    "pitcher":      0.40,   # Pitcher first-inning / first-time-through profile
    "batter":       0.30,   # Projected top-4 hitters vs handedness
    "park_weather": 0.15,   # Park factor + weather environment
    "damage_speed": 0.10,   # Hard-hit/barrel suppression + baserunner speed
    "lineup":       0.05,   # Late lineup-confirmation / adjustment layer
}

# ── Pitcher sub-weights (within 40% block) ────────────────────────────────────
PITCHER_SUB_WEIGHTS = {
    "xwoba_allowed":        0.25,
    "k_pct":                0.20,
    "bb_pct":               0.15,
    "first_pitch_strike":   0.15,
    "hard_hit_pct":         0.10,
    "barrel_pct":           0.10,
    "gb_pct":               0.05,
}

# ── Batter sub-weights (within 30% block) ─────────────────────────────────────
BATTER_SUB_WEIGHTS = {
    "xwoba":        0.30,
    "obp":          0.25,
    "bb_pct":       0.20,
    "hard_hit_pct": 0.15,
    "barrel_pct":   0.10,
}

# ── Bet filter thresholds ─────────────────────────────────────────────────────
BET_FILTER = {
    "min_nrfi_prob":        0.62,   # Minimum combined NRFI probability
    "min_edge_over_book":   0.03,   # Minimum edge vs implied book probability
    "max_plays_per_day":    2,      # Hard cap on recommended plays
    "min_half_inning_prob": 0.78,   # Each half-inning must clear this
}

# ── Regression / blending constants ───────────────────────────────────────────
BLEND = {
    "career_weight":    0.40,
    "prior_year_weight":0.35,
    "rolling_30d":      0.15,
    "current_season":   0.10,
}

# League-average baseline probabilities (calibrated to ~72% historical NRFI rate)
LEAGUE_AVG = {
    "half_inning_nrfi_prob": 0.848,  # sqrt(0.72) ≈ 0.848 each half
    "xwoba_allowed_avg":     0.315,
    "k_pct_avg":             0.228,
    "bb_pct_avg":            0.085,
    "first_pitch_strike_avg":0.620,
    "hard_hit_pct_avg":      0.370,
    "barrel_pct_avg":        0.080,
    "gb_pct_avg":            0.440,
    "batter_xwoba_avg":      0.315,
    "batter_obp_avg":        0.320,
    "batter_bb_pct_avg":     0.085,
    "batter_hard_hit_avg":   0.370,
    "batter_barrel_avg":     0.080,
}

# ── Park factor league-average ────────────────────────────────────────────────
PARK_FACTOR_NEUTRAL = 100   # FanGraphs scale: 100 = league average

# ── Weather thresholds ────────────────────────────────────────────────────────
WEATHER = {
    "cold_temp_f":          50,     # Below this is pitcher-friendly
    "warm_temp_f":          80,     # Above this boosts offense
    "wind_out_threshold":   10,     # mph blowing out starts to matter
    "wind_in_threshold":    10,     # mph blowing in is pitcher-friendly
}

# ── Cache settings ────────────────────────────────────────────────────────────
CACHE_DIR = os.path.join(_ROOT, ".cache")
CACHE_TTL = {
    "schedule":         3600 * 4,   # 4 hours
    "lineups":          1800,       # 30 minutes
    "savant_season":    3600 * 24,  # 24 hours (season stats)
    "savant_splits":    3600 * 24,
    "park_factors":     3600 * 24 * 7,  # 1 week
    "weather":          1800,       # 30 minutes
}

# ── API endpoints ─────────────────────────────────────────────────────────────
MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
SAVANT_BASE  = "https://baseballsavant.mlb.com"
FANGRAPHS_BASE = "https://www.fangraphs.com"

# OpenWeatherMap (set API key via env var OPENWEATHER_API_KEY)
OPENWEATHER_API = "https://api.openweathermap.org/data/2.5/weather"

# ── Pitch handedness labels ───────────────────────────────────────────────────
HAND_LEFT  = "L"
HAND_RIGHT = "R"
HAND_SWITCH = "S"

# ── Score normalization range ─────────────────────────────────────────────────
# Raw composite scores are mapped to [0, 1] where:
#   1.0 = maximum pitcher-friendly (high NRFI probability)
#   0.0 = maximum offense-friendly (low NRFI probability)
SCORE_MIN = 0.0
SCORE_MAX = 1.0

# ── Wind direction mapping ────────────────────────────────────────────────────
# Park-specific wind "blowing out" compass degrees (approximate)
# Key = venue_id, value = dict with "out_direction" bearing in degrees
WIND_DIRECTIONS: dict = {}   # Populated at runtime from venue data

# ── Output ────────────────────────────────────────────────────────────────────
REPORT_WIDTH = 100
