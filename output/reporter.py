"""
NRFI Daily Report Generator.
Formats model output as a clean terminal report with color coding.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import BET_FILTER, REPORT_WIDTH

logger = logging.getLogger(__name__)

# ── Color helpers ──────────────────────────────────────────────────────────────

def _green(s: str) -> str:
    return (Fore.GREEN + s + Style.RESET_ALL) if HAS_COLOR else s

def _yellow(s: str) -> str:
    return (Fore.YELLOW + s + Style.RESET_ALL) if HAS_COLOR else s

def _red(s: str) -> str:
    return (Fore.RED + s + Style.RESET_ALL) if HAS_COLOR else s

def _cyan(s: str) -> str:
    return (Fore.CYAN + s + Style.RESET_ALL) if HAS_COLOR else s

def _bold(s: str) -> str:
    return (Style.BRIGHT + s + Style.RESET_ALL) if HAS_COLOR else s

def _dim(s: str) -> str:
    return (Style.DIM + s + Style.RESET_ALL) if HAS_COLOR else s


# ── Main report function ───────────────────────────────────────────────────────

def print_daily_report(results: list[dict], game_date: str) -> None:
    """Print a formatted daily NRFI report to stdout."""
    W = REPORT_WIDTH
    sep = "─" * W

    # Header
    print()
    print(_bold("=" * W))
    print(_bold(f"  NRFI PREDICTOR  |  {game_date}  |  {len(results)} games scored"))
    print(_bold("=" * W))
    print()

    if not results:
        print(_yellow("  No games found for this date."))
        return

    # ── Recommended plays ────────────────────────────────────────────────────
    recs = [r for r in results if r["bet_recommendation"]["recommended"]]
    if recs:
        print(_bold(_green(f"  ★  RECOMMENDED PLAYS ({len(recs)})  ★")))
        print(sep)
        for r in recs[:BET_FILTER["max_plays_per_day"]]:
            _print_game_card(r, highlight=True)
            print(sep)
    else:
        print(_yellow("  No games cleared the full bet filter today."))
        print(_dim("  (Showing all games ranked by NRFI probability)"))
        print(sep)

    # ── All games ranked ─────────────────────────────────────────────────────
    print()
    print(_bold("  ALL GAMES  (ranked by NRFI probability)"))
    print(sep)
    _print_summary_table(results)
    print()

    # ── Detailed breakdown for top 5 ─────────────────────────────────────────
    print(_bold("  DETAILED BREAKDOWN  (top 5 by NRFI probability)"))
    for r in results[:5]:
        print(sep)
        _print_game_card(r, highlight=r["bet_recommendation"]["recommended"])
    print(sep)
    print()


def _print_summary_table(results: list[dict]) -> None:
    """Print a compact ranked table of all games."""
    header = (
        f"  {'#':>2}  {'Matchup':<32}  {'Pitchers':<30}  "
        f"{'NRFI%':>6}  {'Top1':>5}  {'Bot1':>5}  {'Conf':>5}  {'Rec':>4}"
    )
    print(_dim(header))
    print(_dim("  " + "─" * (REPORT_WIDTH - 2)))

    for i, r in enumerate(results, 1):
        matchup  = f"{r['away_team'][:14]} @ {r['home_team'][:14]}"
        ap_name  = r["away_pitcher"].get("name", "TBD")[:14]
        hp_name  = r["home_pitcher"].get("name", "TBD")[:14]
        pitchers = f"{ap_name} / {hp_name}"
        nrfi     = f"{r['nrfi_prob']:.1%}"
        top1     = f"{r['top_half']['half_inning_prob']:.1%}"
        bot1     = f"{r['bot_half']['half_inning_prob']:.1%}"
        conf     = "YES" if r["lineups_confirmed"] else "no"
        rec      = _bold(_green("YES")) if r["bet_recommendation"]["recommended"] else _dim("no")

        nrfi_colored = (
            _green(nrfi) if r["nrfi_prob"] >= BET_FILTER["min_nrfi_prob"]
            else _yellow(nrfi) if r["nrfi_prob"] >= 0.60
            else _red(nrfi)
        )

        print(
            f"  {i:>2}  {matchup:<32}  {pitchers:<30}  "
            f"{nrfi_colored:>6}  {top1:>5}  {bot1:>5}  {conf:>5}  {rec:>4}"
        )


def _print_game_card(r: dict, highlight: bool = False) -> None:
    """Print a full game breakdown card."""
    indent = "  "
    W = REPORT_WIDTH - 4

    # Title row
    away = r["away_team"]
    home = r["home_team"]
    date_str = _format_game_time(r.get("game_time", ""))
    matchup_str = f"{away} @ {home}  {date_str}"
    nrfi_str    = f"NRFI: {r['nrfi_prob']:.1%}"
    conf_str    = "✓ Lineups Confirmed" if r["lineups_confirmed"] else "⚠ Lineups Pending"

    title = f"{matchup_str:<50}  {nrfi_str}  |  {conf_str}"
    print(indent + (_bold(_green(title)) if highlight else _bold(title)))
    print()

    # Pitchers row
    ap = r["away_pitcher"]
    hp = r["home_pitcher"]
    print(indent + f"  Away SP: {ap.get('name','TBD'):<20} ({ap.get('hand','?')}HP)  "
          f"Home SP: {hp.get('name','TBD'):<20} ({hp.get('hand','?')}HP)")
    print()

    # Half-inning breakdown
    _print_half_summary(r["top_half"], "TOP 1st", indent + "  ")
    print()
    _print_half_summary(r["bot_half"], "BOT 1st", indent + "  ")
    print()

    # Bet filter
    bet = r["bet_recommendation"]
    print(indent + "  Bet Filter:")
    for msg in bet["reasons_pass"]:
        print(indent + _green(f"    ✓ {msg}"))
    for msg in bet["reasons_fail"]:
        print(indent + _red(f"    ✗ {msg}"))
    print()


def _print_half_summary(half: dict, label: str, indent: str) -> None:
    """Print a single half-inning summary."""
    comp  = half["composite_score"]
    prob  = half["half_inning_prob"]
    scores= half["scores"]

    prob_str = _green(f"{prob:.1%}") if prob >= BET_FILTER["min_half_inning_prob"] else _yellow(f"{prob:.1%}")

    print(indent + _bold(f"{label}  →  P(no run): {prob_str}  [composite: {comp:.3f}]"))
    print(indent + f"  Pitcher:      {half['pitcher_name']:<20} ({half['pitcher_hand']}HP)")

    p_s  = scores["pitcher"]
    b_s  = scores["batter"]
    pw_s = scores["park_weather"]
    ds_s = scores["damage_speed"]

    def score_str(s: dict) -> str:
        sc = s.get("score", s) if isinstance(s, dict) else s
        grade = s.get("grade", "") if isinstance(s, dict) else ""
        col = (_green if sc >= 0.65 else _yellow if sc >= 0.50 else _red)
        return col(f"{sc:.3f} [{grade}]")

    print(indent + f"  Pitcher suppression:  {score_str(p_s):<20}  (40% weight)")
    print(indent + f"  Top-4 offense:        {score_str(b_s):<20}  (30% weight)")
    print(indent + f"  Park / weather:       {score_str(pw_s):<20}  (15% weight)")
    print(indent + f"  Damage / speed:       {score_str(ds_s):<20}  (10% weight)")

    # Weather detail
    wx = pw_s.get("weather_detail", {}) if isinstance(pw_s, dict) else {}
    if wx and wx.get("conditions") != "Dome":
        temp  = wx.get("temperature_f", "?")
        wind  = wx.get("wind_mph", "?")
        wdir  = wx.get("wind_direction_str", "?")
        cond  = wx.get("conditions", "?")
        roof  = wx.get("roof_type", "open")
        print(indent + f"  Weather:  {temp}°F, {wind}mph {wdir}, {cond}  [roof: {roof}]")

    # Batter detail
    if isinstance(b_s, dict) and b_s.get("batter_scores"):
        print(indent + "  Top batters:")
        for b in b_s["batter_scores"]:
            warn = " ⚠" if b.get("sample_warning") else ""
            bl = b.get("blended", {})
            xwoba = f"{bl.get('xwoba', 0):.3f}" if bl.get("xwoba") else "N/A"
            obp   = f"{bl.get('obp', 0):.3f}"   if bl.get("obp")   else "N/A"
            name  = b.get("name", "")[:18]
            sc    = b.get("score", 0.5)
            col   = (_green if sc >= 0.65 else _yellow if sc >= 0.50 else _red)
            print(indent + f"    {b['order']}. {name:<18}  {col(f'{sc:.3f}')}"
                  f"  xwOBA:{xwoba}  OBP:{obp}{warn}")


def _format_game_time(game_time: str) -> str:
    """Format ISO game time to a human-readable string."""
    if not game_time:
        return ""
    try:
        dt = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
        return dt.strftime("%-I:%M %p ET") if hasattr(dt, "strftime") else game_time
    except (ValueError, TypeError):
        return game_time[:16]


def save_report_html(results: list[dict], game_date: str,
                     template_path: str = "/home/user/NRFI/index.html") -> str:
    """
    Inject the report JSON into index.html so GitHub Pages serves a live report.
    Replaces the placeholder line `const REPORT_DATA = null;` with real data.
    """
    import json

    safe_results = _make_serializable(results)
    payload = json.dumps({"date": game_date, "games": safe_results}, separators=(",", ":"))

    with open(template_path, "r") as f:
        html = f.read()

    injected = html.replace(
        "const REPORT_DATA = null; // INJECTED_BY_REPORTER",
        f"const REPORT_DATA = {payload}; // generated {game_date}",
    )

    with open(template_path, "w") as f:
        f.write(injected)

    logger.info("Injected report data into %s", template_path)
    return template_path


def save_report_json(results: list[dict], game_date: str, output_dir: str = "/home/user/NRFI/data") -> str:
    """Save model results to a JSON file for archival."""
    import json

    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"nrfi_{game_date}.json")

    # Make results JSON-serializable (remove non-serializable objects)
    safe_results = _make_serializable(results)
    with open(filepath, "w") as f:
        json.dump({"date": game_date, "games": safe_results}, f, indent=2)

    logger.info("Saved report to %s", filepath)
    return filepath


def _make_serializable(obj):
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_serializable(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 6)
    return obj
