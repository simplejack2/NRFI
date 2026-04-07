"""
NRFI Probability Model.

Flow per half-inning:
  pitcher score (40%) + batter score (30%) + park/weather (15%)
  + damage/speed (10%) + lineup confirmation (5%)
  → composite [0,1]
  → logistic sigmoid → P(no run this half)

P(NRFI) = P(no run, top 1st) × P(no run, bottom 1st)
"""

from __future__ import annotations

import logging
import math
from datetime import date

import fetcher as F
from config import (
    WEIGHTS, P_WEIGHTS, B_WEIGHTS, BET_FILTER,
    LG, HALF_P_LOW, HALF_P_HIGH, LOGISTIC_K,
    PARK_FACTORS, DOME_VENUES, RETRACTABLE_VENUES,
    VENUE_COORDS,
)


log = logging.getLogger(__name__)

# Batting-order position weights derived from expected plate appearances in
# the 1st inning.  Slot 1 always bats; each subsequent slot has a lower
# probability of reaching the plate before 3 outs are recorded.
# Empirical estimates: ~1.00, 0.95, 0.85, 0.68, 0.50 per inning.
_POS_W = {1: 1.00, 2: 0.95, 3: 0.85, 4: 0.68, 5: 0.50}

# Wind-out bearing (degrees) for known parks
_WIND_OUT = {
    "wrigley field": 225.0,
    "fenway park":   270.0,
    "yankee stadium":270.0,
    "coors field":   270.0,
    "oracle park":   315.0,
}


# ── Main entrypoint ────────────────────────────────────────────────────────────

def run(game_date: str | None = None) -> list[dict]:
    """
    Score all games for a date and return results sorted by NRFI probability.
    Never raises — each game is wrapped in its own try/except.
    """
    game_date = game_date or date.today().isoformat()
    season    = int(game_date[:4])
    log.info("Running NRFI model for %s", game_date)

    games = F.schedule(game_date)
    if not games:
        log.warning("No games found for %s", game_date)
        return []

    # Pre-fetch leaderboards once for all games
    sv_pit = F.savant_pitchers(season)
    sv_bat = F.savant_batters(season)
    sprints = F.sprint_speed(season)
    pops    = F.pop_time(season)

    ctx = {
        "season":   season,
        "sv_pit":   sv_pit,
        "sv_bat":   sv_bat,
        "sprints":  sprints,
        "pops":     pops,
    }

    results = []
    for game in games:
        try:
            r = _score_game(game, ctx)
            results.append(r)
        except Exception as exc:
            log.error("Failed to score game %s: %s", game.get("game_pk"), exc, exc_info=True)

    results.sort(key=lambda r: r["nrfi_prob"], reverse=True)
    log.info("Scored %d / %d games", len(results), len(games))
    return results


# ── Game scoring ───────────────────────────────────────────────────────────────

def _score_game(game: dict, ctx: dict) -> dict:
    game_pk    = game["game_pk"]
    venue_name = game.get("venue_name", "")
    lat        = game.get("lat")
    lon        = game.get("lon")
    game_time  = game.get("game_time")

    home_prob = game.get("home_probable") or {"id": None, "name": "TBD", "hand": "R"}
    away_prob = game.get("away_probable") or {"id": None, "name": "TBD", "hand": "R"}

    lu        = F.lineups(game_pk)
    confirmed = F._confirmed(lu)
    fi        = F.linescore(game_pk)

    # Game state
    gs = fi.get("game_status", "S")
    if gs == "F":
        game_state = "final"
    elif gs == "I":
        game_state = "live"
    else:
        game_state = "pregame"

    # Top half: away bats vs home pitcher
    top = _score_half(
        pitcher=home_prob,
        batters=lu.get("away", []),
        batting_team_id=game["away_team_id"],
        defending_team_id=game["home_team_id"],
        venue_name=venue_name,
        lat=lat, lon=lon,
        game_time=game_time,
        confirmed=confirmed,
        ctx=ctx,
    )

    # Bottom half: home bats vs away pitcher
    bot = _score_half(
        pitcher=away_prob,
        batters=lu.get("home", []),
        batting_team_id=game["home_team_id"],
        defending_team_id=game["away_team_id"],
        venue_name=venue_name,
        lat=lat, lon=lon,
        game_time=game_time,
        confirmed=confirmed,
        ctx=ctx,
    )

    nrfi_prob = round(top["half_prob"] * bot["half_prob"], 4)
    bet       = _bet_filter(nrfi_prob, top, bot, confirmed, game_state)

    return {
        "game_pk":           game_pk,
        "game_date":         game.get("game_date"),
        "game_time":         game_time,
        "venue_name":        venue_name,
        "away_team":         game["away_team_name"],
        "home_team":         game["home_team_name"],
        "away_pitcher":      away_prob,
        "home_pitcher":      home_prob,
        "lineups_confirmed": confirmed,
        "game_state":        game_state,
        "first_inning":      fi,
        "top_half":          top,
        "bot_half":          bot,
        "nrfi_prob":         nrfi_prob,
        "yrfi_prob":         round(1.0 - nrfi_prob, 4),
        "bet_recommendation":bet,
    }


# ── Half-inning scoring ────────────────────────────────────────────────────────

def _score_half(
    pitcher: dict,
    batters: list[dict],
    batting_team_id: int,
    defending_team_id: int,
    venue_name: str,
    lat, lon,
    game_time: str | None,
    confirmed: bool,
    ctx: dict,
) -> dict:
    season = ctx["season"]
    pid    = pitcher.get("id")
    phand  = pitcher.get("hand", "R")

    p_score  = _pitcher_score(pid, phand, season, ctx)
    b_score  = _lineup_score(batters[:5], phand, season, ctx)
    pw_score = _park_weather_score(venue_name, lat, lon, game_time)
    ds_score = _damage_speed_score(batters[:5], pid, defending_team_id, season, ctx)
    lu_score = 0.55 if confirmed else 0.45   # small adjustment for lineup status

    composite = (
        p_score  * WEIGHTS["pitcher"]
        + b_score  * WEIGHTS["batter"]
        + pw_score * WEIGHTS["park_weather"]
        + ds_score * WEIGHTS["damage_speed"]
        + lu_score * WEIGHTS["lineup"]
    )

    half_prob = _composite_to_prob(composite)

    return {
        "pitcher_name":   pitcher.get("name", "TBD"),
        "pitcher_hand":   phand,
        "composite":      round(composite, 4),
        "half_prob":      round(half_prob, 4),
        "scores": {
            "pitcher":     round(p_score, 4),
            "batter":      round(b_score, 4),
            "park_weather":round(pw_score, 4),
            "damage_speed":round(ds_score, 4),
            "lineup":      round(lu_score, 4),
        },
        "batters": _summarize_batters(batters[:5], phand, season, ctx),
    }


def _composite_to_prob(composite: float) -> float:
    """Logistic sigmoid: composite [0,1] → P(no run per half) in [0.76, 0.93]."""
    sig = 1.0 / (1.0 + math.exp(-LOGISTIC_K * (composite - 0.5)))
    return HALF_P_LOW + (HALF_P_HIGH - HALF_P_LOW) * sig


# ── Pitcher scoring ────────────────────────────────────────────────────────────

def _pitcher_score(pid: int | None, vs_hand: str, season: int, ctx: dict) -> float:
    if not pid:
        return 0.50   # league-average for TBD pitcher

    sv   = ctx["sv_pit"].get(pid, {})
    sv_p = F.savant_pitchers(season - 1).get(pid, {})    # prior season
    mlb  = F.pitcher_stats(pid, season)
    car  = F.pitcher_career_stats(pid)
    fi   = F.pitcher_fi_split(pid, season)

    bf = sv.get("pa") or mlb.get("bf") or 0

    def blend(cur, prior, career, lg_avg):
        return _blend3(cur, prior, career, lg_avg, bf, full_at=400)

    m = {
        "xera":       blend(sv.get("xera"),
                            sv_p.get("xera"), None, LG["xera"]),
        "k_pct":      blend(sv.get("k_pct") or mlb.get("k_pct"),
                            sv_p.get("k_pct"), car.get("k_pct"), LG["k_pct"]),
        "bb_pct":     blend(sv.get("bb_pct") or mlb.get("bb_pct"),
                            sv_p.get("bb_pct"), car.get("bb_pct"), LG["bb_pct"]),
        "fps":        blend(sv.get("fps"), sv_p.get("fps"), None, LG["fps"]),
        "whiff_pct":  blend(sv.get("whiff_pct"), sv_p.get("whiff_pct"), None, LG["whiff_pct"]),
        "chase_rate": blend(sv.get("chase_rate"), sv_p.get("chase_rate"), None, LG["chase_rate"]),
        "hard_hit":   blend(sv.get("hard_hit"), sv_p.get("hard_hit"), None, LG["hard_hit"]),
    }

    # First-inning split: ERA + K% nudge when we have real first-inning data
    fi_adj = 0.0
    if fi.get("bf", 0) >= 15:
        if fi.get("era") is not None:
            # Convert first-inning ERA to a per-inning run rate for comparison
            fi_era_per_inn = fi["era"] / 9.0
            fi_adj += (_sig_inv(fi_era_per_inn, 0.50, 0.20, 0.80) - 0.5) * 0.12
        if fi.get("k_pct") is not None:
            fi_adj += (_sig(fi["k_pct"], LG["k_pct"], 0.10, 0.40) - 0.5) * 0.05

    components = {
        "k_pct":      _sig(m["k_pct"],       LG["k_pct"],     0.10,  0.40),
        "fps":        _sig(m["fps"],          LG["fps"],        0.50,  0.75),
        "xera":       _sig_inv(m["xera"],     LG["xera"],       3.00,  5.80) + fi_adj,
        "bb_pct":     _sig_inv(m["bb_pct"],   LG["bb_pct"],     0.03,  0.16),
        "chase_rate": _sig(m["chase_rate"],   LG["chase_rate"], 0.22,  0.40),
        "whiff_pct":  _sig(m["whiff_pct"],    LG["whiff_pct"],  0.15,  0.40),
        "hard_hit":   _sig_inv(m["hard_hit"], LG["hard_hit"],   0.25,  0.50),
    }

    return _wsum(components, P_WEIGHTS)


# ── Batter/lineup scoring ──────────────────────────────────────────────────────

def _lineup_score(batters: list[dict], pitcher_hand: str,
                  season: int, ctx: dict) -> float:
    """Score [0,1] — 1.0 = weak top-4 (good for NRFI), 0.0 = elite top-4."""
    if not batters:
        return 0.50

    total_w = score_w = 0.0
    for b in sorted(batters, key=lambda x: x.get("order", 99))[:4]:
        pos = b.get("order", 4)
        pw  = _POS_W.get(pos, 0.10)
        total_w += pw
        score_w += _batter_score(b["player_id"], pitcher_hand, season, ctx) * pw

    return score_w / total_w if total_w else 0.50


def _batter_score(pid: int, pitcher_hand: str, season: int, ctx: dict) -> float:
    """Score [0,1] — 1.0 = weak batter (good for NRFI)."""
    sv   = ctx["sv_bat"].get(pid, {})
    sv_p = F.savant_batters(season - 1).get(pid, {})
    mlb  = F.batter_stats(pid, season)
    mlb_s = F.batter_hand_splits(pid, season)

    split_key = "vs_lhp" if pitcher_hand == "L" else "vs_rhp"
    spl = mlb_s.get(split_key, {})

    pa = sv.get("pa") or mlb.get("pa") or 0

    def blend(cur, split, prior, lg_avg):
        return _blend_batter(cur, split, prior, lg_avg, pa)

    m = {
        "xwoba":    blend(sv.get("xwoba"),    None,              sv_p.get("xwoba"),    LG["xwoba"]),
        "k_pct":    blend(sv.get("k_pct") or mlb.get("k_pct"),
                          spl.get("k_pct"),   sv_p.get("k_pct"), LG["batter_k_pct"]),
        "obp":      blend(mlb.get("obp"),     spl.get("obp"),    None,                 LG["obp"]),
        "bb_pct":   blend(sv.get("bb_pct") or mlb.get("bb_pct"),
                          spl.get("bb_pct"),  sv_p.get("bb_pct"), LG["batter_bb_pct"]),
        "hard_hit": blend(sv.get("hard_hit"), None,              sv_p.get("hard_hit"), LG["batter_hh"]),
        "barrel":   blend(sv.get("barrel"),   None,              sv_p.get("barrel"),   LG["batter_barrel"]),
    }

    components = {
        "xwoba":    _sig_inv(m["xwoba"],    LG["xwoba"],          0.220, 0.420),
        "k_pct":    _sig(m["k_pct"],        LG["batter_k_pct"],   0.100, 0.380),
        "obp":      _sig_inv(m["obp"],      LG["obp"],            0.250, 0.430),
        "bb_pct":   _sig_inv(m["bb_pct"],   LG["batter_bb_pct"],  0.030, 0.180),
        "hard_hit": _sig_inv(m["hard_hit"], LG["batter_hh"],      0.250, 0.550),
        "barrel":   _sig_inv(m["barrel"],   LG["batter_barrel"],  0.020, 0.200),
    }

    return _wsum(components, B_WEIGHTS)


def _summarize_batters(batters: list[dict], pitcher_hand: str,
                       season: int, ctx: dict) -> list[dict]:
    out = []
    sv_prev = F.savant_batters(season - 1)
    for b in sorted(batters, key=lambda x: x.get("order", 99))[:4]:
        pid = b["player_id"]
        sv  = ctx["sv_bat"].get(pid, {})
        sv_p = sv_prev.get(pid, {})
        mlb  = F.batter_stats(pid, season)
        # Show current xwoba; fall back to prior season when current is null (early season)
        xwoba = sv.get("xwoba") or sv_p.get("xwoba")
        k_pct = sv.get("k_pct") or sv_p.get("k_pct")
        out.append({
            "order":    b.get("order"),
            "name":     b.get("name", ""),
            "bat_side": b.get("bat_side", "R"),
            "score":    round(_batter_score(pid, pitcher_hand, season, ctx), 3),
            "xwoba":    xwoba,
            "k_pct":    k_pct,
            "obp":      mlb.get("obp"),
        })
    return out


# ── Park + weather scoring ─────────────────────────────────────────────────────

def _park_weather_score(venue_name: str, lat, lon, game_time: str | None) -> float:
    vn = venue_name.lower().strip()

    # Park factor
    pf = _lookup_park_factor(vn)
    park_adj = (pf - 100) / 100.0          # >0 = hitter-friendly

    # Weather: skip domes; partial for retractable (roof likely closed in bad wx)
    wx_adj = 0.0
    if vn not in DOME_VENUES:
        wx = F.weather(lat, lon, venue_name)
        if vn in RETRACTABLE_VENUES:
            temp = wx.get("temp_f", 65)
            cond = (wx.get("conditions") or "").lower()
            if temp < 55 or any(w in cond for w in ("rain", "drizzle", "shower", "thunder")):
                wx_adj = 0.0   # roof almost certainly closed
            else:
                wx_adj = _weather_adjustment(wx, vn) * 0.5  # roof may be open
        else:
            wx_adj = _weather_adjustment(wx, vn)

    combined = park_adj * (2.0 / 3.0) + wx_adj * (1.0 / 3.0)
    score = max(0.0, min(1.0, 0.5 - combined / 0.40))
    return score


def _lookup_park_factor(vn: str) -> int:
    pf = PARK_FACTORS.get(vn)
    if pf is None:
        for k, v in PARK_FACTORS.items():
            if k in vn or vn in k:
                pf = v
                break
    return (pf or {}).get("r", 100)


def _weather_adjustment(wx: dict, vn: str) -> float:
    """Return float in [-0.20, +0.20]: positive = hitter-friendly."""
    adj = 0.0
    temp = wx.get("temp_f", 65)
    wind = wx.get("wind_mph", 5)
    wdeg = wx.get("wind_deg", 270)
    cond = (wx.get("conditions") or "").lower()

    # Temperature
    if temp >= 80:
        adj += 0.04 * min((temp - 80) / 20.0, 1.0)
    elif temp <= 50:
        adj -= 0.06 * min((50 - temp) / 20.0, 1.0)

    # Wind
    if wind >= 10:
        out_bearing = _WIND_OUT.get(vn, 270.0)
        angle = abs((wdeg - out_bearing + 180) % 360 - 180)
        cos_comp = math.cos(math.radians(angle)) * min(wind / 20.0, 1.5)
        adj += 0.08 * cos_comp

    # Rain
    if any(w in cond for w in ("rain", "drizzle", "shower", "thunder")):
        adj -= 0.05

    return max(-0.20, min(0.20, adj))


# ── Damage / speed scoring ─────────────────────────────────────────────────────

def _damage_speed_score(batters: list[dict], pitcher_id: int | None,
                        defending_team_id: int, season: int, ctx: dict) -> float:
    damage = _damage_score(batters, pitcher_id, season, ctx)
    speed  = _speed_score(batters, defending_team_id, season, ctx)
    return 0.5 * damage + 0.5 * speed


def _damage_score(batters: list[dict], pid: int | None,
                  season: int, ctx: dict) -> float:
    sv_pit = ctx["sv_pit"].get(pid, {}) if pid else {}
    pit_hh = sv_pit.get("hard_hit", LG["hard_hit"])
    pit_br = sv_pit.get("barrel",   LG["barrel"])

    hh_vals, br_vals = [], []
    for b in batters:
        bsv = ctx["sv_bat"].get(b["player_id"], {})
        if bsv.get("hard_hit") is not None: hh_vals.append(bsv["hard_hit"])
        if bsv.get("barrel")   is not None: br_vals.append(bsv["barrel"])

    avg_hh = sum(hh_vals) / len(hh_vals) if hh_vals else LG["batter_hh"]
    avg_br = sum(br_vals) / len(br_vals) if br_vals else LG["batter_barrel"]

    matchup_hh = (pit_hh + avg_hh) / 2.0
    matchup_br = (pit_br + avg_br) / 2.0

    return 0.6 * _sig_inv(matchup_hh, 0.370, 0.25, 0.52) \
         + 0.4 * _sig_inv(matchup_br, 0.080, 0.03, 0.14)


def _speed_score(batters: list[dict], defending_team_id: int,
                 season: int, ctx: dict) -> float:
    speeds = [ctx["sprints"][b["player_id"]]
              for b in batters if b["player_id"] in ctx["sprints"]]
    avg_speed = sum(speeds) / len(speeds) if speeds else LG["sprint"]

    cat_ids  = F.team_catchers(defending_team_id, season)
    pop_vals = [ctx["pops"][c] for c in cat_ids if c in ctx["pops"]]
    best_pop = min(pop_vals) if pop_vals else LG["pop_time"]

    spd_score = _sig_inv(avg_speed, LG["sprint"],   24.0, 30.0)
    pop_score = _sig_inv(best_pop,  LG["pop_time"], 1.85, 2.25)
    return 0.55 * spd_score + 0.45 * pop_score


# ── Bet filter ─────────────────────────────────────────────────────────────────

def _bet_filter(nrfi_prob: float, top: dict, bot: dict,
                confirmed: bool, game_state: str) -> dict:
    reasons_pass, reasons_fail = [], []

    if game_state in ("live", "final"):
        return {
            "recommended":   False,
            "reasons_pass":  [],
            "reasons_fail":  [f"Game already {game_state}"],
        }

    if nrfi_prob >= BET_FILTER["min_nrfi_prob"]:
        reasons_pass.append(f"NRFI prob {nrfi_prob:.1%} ≥ {BET_FILTER['min_nrfi_prob']:.0%}")
    else:
        reasons_fail.append(f"NRFI prob {nrfi_prob:.1%} < {BET_FILTER['min_nrfi_prob']:.0%}")

    top_p = top["half_prob"]
    bot_p = bot["half_prob"]
    if top_p >= BET_FILTER["min_half_prob"]:
        reasons_pass.append(f"Top 1st P={top_p:.1%} ≥ {BET_FILTER['min_half_prob']:.0%}")
    else:
        reasons_fail.append(f"Top 1st P={top_p:.1%} < {BET_FILTER['min_half_prob']:.0%}")

    if bot_p >= BET_FILTER["min_half_prob"]:
        reasons_pass.append(f"Bot 1st P={bot_p:.1%} ≥ {BET_FILTER['min_half_prob']:.0%}")
    else:
        reasons_fail.append(f"Bot 1st P={bot_p:.1%} < {BET_FILTER['min_half_prob']:.0%}")

    if not confirmed:
        reasons_fail.append("Lineups not yet confirmed")

    return {
        "recommended":  not reasons_fail,
        "reasons_pass": reasons_pass,
        "reasons_fail": reasons_fail,
    }


# ── Math helpers ───────────────────────────────────────────────────────────────

def _sig(val: float, avg: float, lo: float, hi: float) -> float:
    """Map val → [0,1]: avg→0.5, hi→~0.85, lo→~0.15. Higher = better."""
    if hi <= lo:
        return 0.5
    n = (val - avg) / ((hi - lo) / 2.0)
    return 1.0 / (1.0 + math.exp(-3.0 * n))


def _sig_inv(val: float, avg: float, lo: float, hi: float) -> float:
    """Inverted: lower val = higher score."""
    return _sig(-val, -avg, -hi, -lo)


def _wsum(components: dict, weights: dict) -> float:
    total_w = sum(weights.values())
    return sum(components.get(k, 0.5) * w for k, w in weights.items()) / total_w


def _blend3(cur, prior, career, lg_avg: float, bf: int, full_at: int = 400) -> float:
    """
    Weighted blend of current/prior/career with regression to lg_avg.

    Key fix: when historical data (prior/career) exists, cap regression at 35%
    so prior-year track record still matters at bf=0 (start of season).
    Only fully regress to lg_avg when there is truly no data at all.
    """
    vals, wts = [], []
    if cur     is not None: vals.append(cur);    wts.append(0.45)
    if prior   is not None: vals.append(prior);  wts.append(0.35)
    if career  is not None: vals.append(career); wts.append(0.20)
    if not vals:
        return lg_avg
    blended = sum(v * w for v, w in zip(vals, wts)) / sum(wts)
    raw_reg = max(0.0, 1.0 - bf / full_at)
    # With historical data available, cap regression: deGrom still looks like deGrom
    # in April even with bf=0. Full regression only when current-season only.
    has_history = (prior is not None or career is not None)
    reg = raw_reg * (0.35 if has_history else 1.0)
    return blended * (1 - reg) + lg_avg * reg


def _blend_batter(cur, split, prior, lg_avg: float, pa: int) -> float:
    vals, wts = [], []
    if split is not None: vals.append(split); wts.append(0.40)
    if cur   is not None: vals.append(cur);   wts.append(0.35)
    if prior is not None: vals.append(prior); wts.append(0.25)
    if not vals:
        return lg_avg
    blended = sum(v * w for v, w in zip(vals, wts)) / sum(wts)
    raw_reg = max(0.0, 1.0 - pa / 200.0)
    has_history = (prior is not None or split is not None)
    reg = raw_reg * (0.35 if has_history else 1.0)
    return blended * (1 - reg) + lg_avg * reg


def grade(score: float) -> str:
    if score >= 0.80: return "A+"
    if score >= 0.72: return "A"
    if score >= 0.65: return "B+"
    if score >= 0.57: return "B"
    if score >= 0.50: return "C+"
    if score >= 0.43: return "C"
    if score >= 0.35: return "D+"
    return "D"
