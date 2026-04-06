"""NRFI Predictor — central constants. No imports except os."""
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

# ── Scoring weights (must sum to 1.0) ─────────────────────────────────────────
WEIGHTS = {
    "pitcher":      0.40,
    "batter":       0.30,
    "park_weather": 0.15,
    "damage_speed": 0.10,
    "lineup":       0.05,
}

# ── Sub-weights within each block ─────────────────────────────────────────────
P_WEIGHTS = {           # pitcher block
    "xwoba":    0.25,
    "k_pct":    0.20,
    "bb_pct":   0.15,
    "fps":      0.15,   # first-pitch strike rate
    "hard_hit": 0.10,
    "barrel":   0.10,
    "gb":       0.05,
}

B_WEIGHTS = {           # batter block
    "xwoba":    0.30,
    "obp":      0.25,
    "bb_pct":   0.20,
    "hard_hit": 0.15,
    "barrel":   0.10,
}

# ── Bet filter ─────────────────────────────────────────────────────────────────
BET_FILTER = {
    "min_nrfi_prob":  0.62,
    "min_half_prob":  0.78,
    "max_plays":      2,
}

# ── League-average baselines (regression anchors) ──────────────────────────────
LG = {
    # pitcher metrics (allow)
    "xwoba_against": 0.315,
    "k_pct":         0.228,
    "bb_pct":        0.085,
    "fps":           0.620,
    "hard_hit":      0.370,
    "barrel":        0.080,
    "gb":            0.440,
    # batter metrics
    "xwoba":         0.315,
    "obp":           0.320,
    "batter_bb_pct": 0.085,
    "batter_hh":     0.370,
    "batter_barrel": 0.080,
    # speed/field
    "sprint":        27.0,   # ft/s
    "pop_time":      2.02,   # seconds catcher 2B pop time
}

# ── Half-inning P(no-run) calibration ─────────────────────────────────────────
# Logistic mapping: composite [0,1] → P(no run per half) in [0.76, 0.93]
# Anchored: score=0.5 → P≈0.848 so that P(NRFI) = 0.848² ≈ 0.72 (historical avg)
HALF_P_LOW  = 0.76
HALF_P_HIGH = 0.93
LOGISTIC_K  = 6.0

# ── Park factors (FanGraphs 100-scale; 100 = neutral) ─────────────────────────
# r=runs, lhb=vs left-handed batter, rhb=vs right-handed batter
PARK_FACTORS: dict[str, dict] = {
    "coors field":              {"r": 115, "lhb": 113, "rhb": 117},
    "great american ball park": {"r": 108, "lhb": 106, "rhb": 110},
    "yankee stadium":           {"r": 107, "lhb": 112, "rhb": 103},
    "fenway park":              {"r": 106, "lhb": 110, "rhb": 102},
    "wrigley field":            {"r": 104, "lhb": 103, "rhb": 105},
    "globe life field":         {"r": 103, "lhb": 104, "rhb": 102},
    "citizens bank park":       {"r": 103, "lhb": 103, "rhb": 103},
    "guaranteed rate field":    {"r": 104, "lhb": 105, "rhb": 103},
    "camden yards":             {"r": 104, "lhb": 105, "rhb": 103},
    "truist park":              {"r": 103, "lhb": 103, "rhb": 103},
    "nationals park":           {"r": 101, "lhb": 100, "rhb": 102},
    "american family field":    {"r": 101, "lhb": 101, "rhb": 101},
    "chase field":              {"r": 100, "lhb": 100, "rhb": 100},
    "minute maid park":         {"r":  99, "lhb": 101, "rhb":  97},
    "target field":             {"r":  99, "lhb":  99, "rhb":  99},
    "progressive field":        {"r":  98, "lhb":  98, "rhb":  98},
    "angel stadium":            {"r":  97, "lhb":  98, "rhb":  96},
    "busch stadium":            {"r":  97, "lhb":  97, "rhb":  97},
    "citi field":               {"r":  97, "lhb":  97, "rhb":  97},
    "dodger stadium":           {"r":  96, "lhb":  96, "rhb":  96},
    "kauffman stadium":         {"r":  96, "lhb":  96, "rhb":  96},
    "comerica park":            {"r":  96, "lhb":  95, "rhb":  97},
    "loandepot park":           {"r":  96, "lhb":  96, "rhb":  96},
    "t-mobile park":            {"r":  95, "lhb":  95, "rhb":  95},
    "pnc park":                 {"r":  95, "lhb":  93, "rhb":  97},
    "oracle park":              {"r":  94, "lhb":  92, "rhb":  96},
    "petco park":               {"r":  93, "lhb":  94, "rhb":  92},
    "tropicana field":          {"r":  93, "lhb":  92, "rhb":  94},
    "rogers centre":            {"r":  99, "lhb":  99, "rhb":  99},
}

# ── Dome / fully-enclosed stadiums (weather irrelevant) ───────────────────────
DOME_VENUES = {
    "tropicana field",
}

# ── Retractable roof (weather conditionally irrelevant) ───────────────────────
RETRACTABLE_VENUES = {
    "minute maid park", "american family field", "chase field",
    "loandepot park", "t-mobile park", "globe life field", "rogers centre",
}

# ── API endpoints ─────────────────────────────────────────────────────────────
MLB_API = "https://statsapi.mlb.com/api/v1"
SAVANT  = "https://baseballsavant.mlb.com"
