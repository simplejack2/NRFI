#!/usr/bin/env python3
"""
NRFI Predictor - Daily Runner
==============================
Usage:
    python main.py                          # Run for today
    python main.py --date 2026-04-15        # Run for a specific date
    python main.py --confirmed              # Only score games with confirmed lineups
    python main.py --save                   # Also save JSON report to data/
    python main.py --top 3                  # Show detailed breakdown for top N games
    python main.py --game <game_pk>         # Show breakdown for a single game
    python main.py --clear-cache            # Clear cached data and re-fetch

Environment variables:
    OPENWEATHER_API_KEY   OpenWeatherMap API key (optional; falls back to wttr.in)

Data sources (all public, no API key required by default):
    MLB Stats API         https://statsapi.mlb.com
    Baseball Savant       https://baseballsavant.mlb.com
    FanGraphs             https://www.fangraphs.com
    wttr.in               https://wttr.in (weather fallback)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date

# Ensure package root is on path
sys.path.insert(0, os.path.dirname(__file__))

from model.nrfi_model import run_daily_model
from output.reporter  import print_daily_report, save_report_json, save_report_html


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet down noisy third-party loggers
    for noisy in ("urllib3", "requests", "diskcache"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def clear_cache() -> None:
    from fetchers._cache import _get_cache
    c = _get_cache()
    if hasattr(c, "clear"):
        c.clear()
        print("Cache cleared.")
    else:
        print("Cache is in-memory; nothing to clear.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NRFI Predictor - daily no-run-first-inning probability model",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date", "-d",
        default=None,
        metavar="YYYY-MM-DD",
        help="Game date (default: today)",
    )
    parser.add_argument(
        "--confirmed", "-c",
        action="store_true",
        default=False,
        help="Only score games with confirmed lineups",
    )
    parser.add_argument(
        "--save", "-s",
        action="store_true",
        default=False,
        help="Save JSON report to data/nrfi_<date>.json",
    )
    parser.add_argument(
        "--html",
        action="store_true",
        default=False,
        help="Inject report data into index.html for GitHub Pages",
    )
    parser.add_argument(
        "--top", "-t",
        type=int,
        default=5,
        metavar="N",
        help="Number of games to show in detailed breakdown (default: 5)",
    )
    parser.add_argument(
        "--game", "-g",
        type=int,
        default=None,
        metavar="GAME_PK",
        help="Show detailed breakdown for a single game",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        default=False,
        help="Clear cached data before running",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=False,
        help="Enable verbose debug logging",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger("nrfi.main")

    if args.clear_cache:
        clear_cache()

    game_date = args.date or date.today().isoformat()
    logger.info("NRFI Predictor starting for %s", game_date)

    results = None
    exit_code = 0

    try:
        results = run_daily_model(
            game_date=game_date,
            require_confirmed=args.confirmed,
        )

        if not results:
            print(f"\nNo games found for {game_date} (off-day or probables not yet posted).\n")
        else:
            # Filter to specific game if requested
            if args.game:
                results = [r for r in results if r["game_pk"] == args.game]
                if not results:
                    print(f"\nGame {args.game} not found in results for {game_date}.\n")

            if results:
                print_daily_report(results, game_date)

                if args.save:
                    path = save_report_json(results, game_date)
                    print(f"  Report saved to: {path}\n")

    except Exception as exc:
        logger.error("Model run failed: %s", exc, exc_info=True)
        exit_code = 1

    finally:
        # Always write index.html when --html is requested, even on failure.
        # This ensures the verify step in CI never sees the raw placeholder.
        if args.html:
            try:
                path = save_report_html(results or [], game_date)
                print(f"  index.html updated: {path}\n")
            except Exception as exc2:
                logger.error("Failed to write index.html: %s", exc2, exc_info=True)
                exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
