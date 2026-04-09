"""NRFI Predictor — central constants. No imports except os."""
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

# ── Scoring weights (must sum to 1.0) ─────────────────────────────────────────
WEIGHTS = {
    "pitcher":      0.42,   # pitcher is the dominant factor in first-inning outcomes
    "batter":       0.28,
    "park_weather": 0.14,
    "damage_speed": 0.10,
    "lineup":       0.06,
}

# ── Sub-weights within each block ─────────────────────────────────────────────
P_WEIGHTS = {               # pitcher block
    "k_pct":      0.22,   # strikeouts = most reliable outs, no contact risk
    "fps":        0.20,   # first-pitch strike rate — strongest leading indicator
    "xera":       0.20,   # park-neutral expected ERA — comprehensive quality signal
    "bb_pct":     0.13,   # walks guarantee baserunners
    "chase_rate": 0.12,   # o-swing% — batters chasing = weak contact / K's
    "whiff_pct":  0.08,   # swing-and-miss rate per swing — measures raw stuff
    "hard_hit":   0.05,   # hard contact rate (residual signal beyond xERA)
}

B_WEIGHTS = {           # batter block
    "xwoba":    0.25,
    "k_pct":    0.20,   # high K% batters = easier outs = good for NRFI
    "obp":      0.20,
    "bb_pct":   0.15,
    "hard_hit": 0.12,
    "barrel":   0.08,
}

# ── Bet filter ─────────────────────────────────────────────────────────────────
BET_FILTER = {
    "min_nrfi_prob":  0.725,
    "min_half_prob":  0.79,
    "max_plays":      2,
}

# ── League-average baselines (regression anchors) ──────────────────────────────
LG = {
    # pitcher metrics (allow)
    "xera":          4.25,   # MLB avg xERA ~4.20-4.35
    "k_pct":         0.228,
    "bb_pct":        0.085,
    "fps":           0.620,
    "whiff_pct":     0.245,
    "chase_rate":    0.310,  # MLB avg o-swing% ~30-31%
    "hard_hit":      0.370,
    "barrel":        0.080,  # fallback default used in damage_score
    # batter metrics
    "xwoba":         0.315,
    "obp":           0.320,
    "batter_bb_pct": 0.085,
    "batter_k_pct":  0.228,  # mirror of pitcher K% — symmetric
    "batter_hh":     0.370,
    "batter_barrel": 0.080,
    # speed/field
    "sprint":        27.0,   # ft/s
    "pop_time":      2.02,   # seconds catcher 2B pop time
}

# ── Half-inning P(no-run) calibration ─────────────────────────────────────────
# Logistic mapping: composite [0,1] → P(no run per half) in [0.75, 0.94]
# Anchored: score=0.5 → P≈0.845 so that P(NRFI) = 0.845² ≈ 0.714 (historical avg)
# K=6.5 gives steeper sigmoid → more differentiation between good/bad matchups
HALF_P_LOW  = 0.75
HALF_P_HIGH = 0.94
LOGISTIC_K  = 6.5

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

# ── Venue coordinates (for weather lookups) ───────────────────────────────────
# Keys are lowercase venue names matching the MLB Stats API
VENUE_COORDS: dict[str, dict] = {
    "angel stadium":              {"lat": 33.8003, "lon": -117.8827},
    "busch stadium":              {"lat": 38.6226, "lon": -90.1928},
    "camden yards":               {"lat": 39.2838, "lon": -76.6218},
    "chase field":                {"lat": 33.4453, "lon": -112.0667},
    "citi field":                 {"lat": 40.7571, "lon": -73.8458},
    "citizens bank park":         {"lat": 39.9061, "lon": -75.1665},
    "comerica park":              {"lat": 42.3390, "lon": -83.0485},
    "coors field":                {"lat": 39.7559, "lon": -104.9942},
    "dodger stadium":             {"lat": 34.0739, "lon": -118.2400},
    "fenway park":                {"lat": 42.3467, "lon": -71.0972},
    "globe life field":           {"lat": 32.7473, "lon": -97.0822},
    "great american ball park":   {"lat": 39.0974, "lon": -84.5082},
    "guaranteed rate field":      {"lat": 41.8299, "lon": -87.6338},
    "kauffman stadium":           {"lat": 39.0517, "lon": -94.4803},
    "loandepot park":             {"lat": 25.7781, "lon": -80.2197},
    "minute maid park":           {"lat": 29.7573, "lon": -95.3555},
    "nationals park":             {"lat": 38.8731, "lon": -77.0075},
    "oracle park":                {"lat": 37.7786, "lon": -122.3893},
    "petco park":                 {"lat": 32.7076, "lon": -117.1570},
    "pnc park":                   {"lat": 40.4469, "lon": -80.0057},
    "progressive field":          {"lat": 41.4962, "lon": -81.6852},
    "rogers centre":              {"lat": 43.6414, "lon": -79.3894},
    "t-mobile park":              {"lat": 47.5914, "lon": -122.3325},
    "target field":               {"lat": 44.9817, "lon": -93.2781},
    "tropicana field":            {"lat": 27.7683, "lon": -82.6534},
    "truist park":                {"lat": 33.8908, "lon": -84.4678},
    "wrigley field":              {"lat": 41.9484, "lon": -87.6553},
    "yankee stadium":             {"lat": 40.8296, "lon": -73.9262},
    "american family field":      {"lat": 43.0280, "lon": -87.9712},
}

# ── API endpoints ─────────────────────────────────────────────────────────────
MLB_API = "https://statsapi.mlb.com/api/v1"
SAVANT  = "https://baseballsavant.mlb.com"
