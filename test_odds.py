#!/usr/bin/env python3
"""
Diagnose The Odds API integration for NRFI odds.

Usage:
    ODDS_API_KEY=your_key_here python test_odds.py [YYYY-MM-DD]
    python test_odds.py                    # uses today's date
    python test_odds.py 2026-04-12         # specific date
"""
import json
import os
import sys
from datetime import date

import requests

API_KEY = os.environ.get("ODDS_API_KEY", "")
if not API_KEY:
    print("ERROR: ODDS_API_KEY is not set.")
    print()
    print("Steps to fix:")
    print("  1. Sign up for a free key at https://the-odds-api.com")
    print("     (Free tier: 500 requests/month — plenty for ~1 call/game-day)")
    print("  2. In your GitHub repo:")
    print("     Settings → Secrets and variables → Actions → New repository secret")
    print("     Name: ODDS_API_KEY   Value: <your key>")
    print()
    print("Then run the workflow manually (Actions → Daily NRFI Picks → Run workflow)")
    print("to pick up today's odds immediately.")
    sys.exit(1)

game_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
print(f"Testing The Odds API for {game_date}")
print("=" * 60)

S = requests.Session()
S.headers["User-Agent"] = "NRFI-Predictor/test"

# ── Check which market keys are available ────────────────────────────────────
MARKET_KEYS = [
    "totals_1st_1_innings",   # 1st inning combined total O/U 0.5 — ideal
    "h2h_1st_1_innings",      # 1st inning YES/NO run moneyline
    "alternate_totals",       # alternate totals (may include 1st inning)
    "totals",                 # full-game total — free tier baseline check
]

for mkt in MARKET_KEYS:
    try:
        r = S.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds",
            params={
                "apiKey": API_KEY,
                "regions": "us",
                "markets": mkt,
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=15,
        )
        remaining = r.headers.get("x-requests-remaining", "?")
        used      = r.headers.get("x-requests-used", "?")
        data = r.json()
    except Exception as exc:
        print(f"  {mkt:<30} NETWORK ERROR: {exc}")
        continue

    if isinstance(data, dict):
        print(f"  {mkt:<30} API ERROR: {data.get('message', data)}")
        continue

    if not data:
        print(f"  {mkt:<30} No events  (used={used}, remaining={remaining})")
        continue

    # Filter to today's date (UTC + 1 day for west-coast games)
    today_events = [
        ev for ev in data
        if (ev.get("commence_time") or "")[:10] in {game_date}
    ]

    # Check if any event has non-empty bookmakers for this market
    matched = 0
    sample_odds = None
    for ev in today_events:
        for book in ev.get("bookmakers", []):
            for mkt_data in book.get("markets", []):
                if mkt_data.get("key") == mkt and mkt_data.get("outcomes"):
                    matched += 1
                    if sample_odds is None:
                        sample_odds = {
                            "game":     f"{ev['away_team']} @ {ev['home_team']}",
                            "book":     book["key"],
                            "outcomes": mkt_data["outcomes"],
                        }

    status = f"✓ {matched}/{len(today_events)} games have odds" if matched else f"✗ 0/{len(today_events)} games (market key not offered on this plan)"
    print(f"  {mkt:<30} {status}  (used={used}, remaining={remaining})")
    if sample_odds:
        print(f"      Sample: {sample_odds['game']}  [{sample_odds['book']}]")
        for oc in sample_odds["outcomes"]:
            marker = " ← NRFI odds" if oc.get("name") in ("Under", "No") else ""
            print(f"        {oc.get('name'):8} {oc.get('price'):+5}{marker}")

print()
print("=" * 60)
print("If 'totals_1st_1_innings' shows ✓ above, the integration is ready.")
print("If it shows ✗, that market requires a paid plan (~$9-25/mo).")
print("The code will automatically use whatever working market key is found.")
