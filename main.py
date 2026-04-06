#!/usr/bin/env python3
"""
NRFI Predictor — daily runner.

Usage:
  python main.py                   # run for today, print to terminal
  python main.py --date 2026-04-15 # specific date
  python main.py --html            # inject results into index.html
  python main.py --save            # also save JSON to data/
  python main.py --confirmed       # only score games with confirmed lineups
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timezone

# Repo root on path so config / fetcher / model resolve without sub-packages
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import model


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("urllib3", "requests", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NRFI daily predictor")
    p.add_argument("--date",      default=None, metavar="YYYY-MM-DD")
    p.add_argument("--confirmed", action="store_true",
                   help="Only show games with confirmed lineups")
    p.add_argument("--html",      action="store_true",
                   help="Inject results into index.html for GitHub Pages")
    p.add_argument("--save",      action="store_true",
                   help="Save JSON report to data/")
    p.add_argument("--verbose",   action="store_true")
    return p.parse_args()


# ── Terminal report ───────────────────────────────────────────────────────────

def _print_report(results: list[dict], game_date: str) -> None:
    W = 100
    print()
    print("=" * W)
    print(f"  NRFI PREDICTOR  |  {game_date}  |  {len(results)} games scored")
    print("=" * W)

    if not results:
        print("  No games found for this date.")
        return

    recs = [r for r in results if r["bet_recommendation"]["recommended"]]
    if recs:
        print(f"\n  ★  RECOMMENDED PLAYS ({len(recs)})  ★\n")
        for r in recs:
            _print_card(r)
    else:
        print("\n  No games cleared the full bet filter today.\n")

    print("  ALL GAMES (ranked by NRFI probability)")
    print("-" * W)
    print(f"  {'#':>2}  {'Matchup':<34}  {'Pitchers':<30}  "
          f"{'NRFI%':>6}  {'Top':>5}  {'Bot':>5}  {'LU':>3}  {'Rec':>3}")
    print("-" * W)
    for i, r in enumerate(results, 1):
        matchup  = f"{r['away_team'][:15]} @ {r['home_team'][:15]}"
        ap = r["away_pitcher"].get("name", "TBD")[:14]
        hp = r["home_pitcher"].get("name", "TBD")[:14]
        pitchers = f"{ap}/{hp}"
        nrfi     = f"{r['nrfi_prob']:.1%}"
        top1     = f"{r['top_half']['half_prob']:.1%}"
        bot1     = f"{r['bot_half']['half_prob']:.1%}"
        lu       = "Y" if r["lineups_confirmed"] else "n"
        rec      = "YES" if r["bet_recommendation"]["recommended"] else "no"
        print(f"  {i:>2}  {matchup:<34}  {pitchers:<30}  "
              f"{nrfi:>6}  {top1:>5}  {bot1:>5}  {lu:>3}  {rec:>3}")
    print()


def _print_card(r: dict) -> None:
    away = r["away_team"]; home = r["home_team"]
    ap   = r["away_pitcher"].get("name", "TBD")
    hp   = r["home_pitcher"].get("name", "TBD")
    print(f"  {away} @ {home}  |  NRFI: {r['nrfi_prob']:.1%}")
    print(f"  {ap} ({r['away_pitcher'].get('hand','?')}) vs "
          f"{hp} ({r['home_pitcher'].get('hand','?')})")
    top = r["top_half"]; bot = r["bot_half"]
    print(f"  Top 1st P={top['half_prob']:.1%}  |  Bot 1st P={bot['half_prob']:.1%}")
    bet = r["bet_recommendation"]
    for msg in bet["reasons_pass"]: print(f"    ✓ {msg}")
    for msg in bet["reasons_fail"]: print(f"    ✗ {msg}")
    print()


# ── HTML injection ────────────────────────────────────────────────────────────

_ROOT = os.path.dirname(os.path.abspath(__file__))
_HTML_PATH = os.path.join(_ROOT, "index.html")


def _write_html(results: list[dict], game_date: str) -> None:
    """Inject report JSON into index.html. Always succeeds or logs the error."""
    try:
        payload = json.dumps(
            {"date": game_date, "games": _serializable(results)},
            separators=(",", ":"),
        )
        new_line = f"const REPORT_DATA = {payload}; // generated {game_date}"

        with open(_HTML_PATH) as f:
            html = f.read()

        injected, n = re.subn(
            r"const REPORT_DATA = .*?; // .*",
            new_line,
            html,
        )
        if n == 0:
            logging.getLogger(__name__).error(
                "REPORT_DATA placeholder not found in index.html"
            )
            return

        with open(_HTML_PATH, "w") as f:
            f.write(injected)

        logging.getLogger(__name__).info("index.html updated (%d games)", len(results))
    except Exception as exc:
        logging.getLogger(__name__).error("Failed to write index.html: %s", exc)


def _serializable(obj):
    if isinstance(obj, dict):
        return {k: _serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serializable(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 6)
    return obj


# ── JSON save ─────────────────────────────────────────────────────────────────

def _save_json(results: list[dict], game_date: str) -> str:
    out_dir = os.path.join(_ROOT, "data")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"nrfi_{game_date}.json")
    with open(path, "w") as f:
        json.dump({"date": game_date, "games": _serializable(results)}, f, indent=2)
    return path


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    args = _parse_args()
    _setup_logging(args.verbose)
    log = logging.getLogger("nrfi.main")

    game_date = args.date or date.today().isoformat()
    log.info("NRFI Predictor — %s", game_date)

    results = None
    try:
        results = model.run(game_date)

        if args.confirmed:
            results = [r for r in results if r["lineups_confirmed"]]

        if not results:
            print(f"\nNo games found for {game_date}.\n")
        else:
            _print_report(results, game_date)

        if args.save and results:
            path = _save_json(results, game_date)
            print(f"  Saved → {path}\n")

    except Exception as exc:
        log.error("Model error: %s", exc, exc_info=True)

    finally:
        # Always write index.html when requested — even on model failure.
        if args.html:
            _write_html(results or [], game_date)

    return 0


if __name__ == "__main__":
    sys.exit(main())
