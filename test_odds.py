#!/usr/bin/env python3
"""
Diagnose NRFI odds sources.

Usage:
    python test_odds.py                    # uses today's date, tests all sources
    python test_odds.py 2026-04-12         # specific date
    ODDS_API_KEY=your_key python test_odds.py  # also tests The Odds API
"""
import json
import os
import sys
from datetime import date

import requests

game_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
print(f"Testing NRFI odds sources for {game_date}")
print("=" * 65)

S = requests.Session()
S.headers["User-Agent"] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

# ── 1. DraftKings unofficial API (no key needed) ──────────────────────────────
print("\n[1] DraftKings unofficial API (no key required)")
print("-" * 65)

DK_BASE   = "https://sportsbook.draftkings.com/sites/US-SB/api/v5"
DK_MLB_EG = 88808
DK_HEADERS = {
    "Accept":  "application/json",
    "Referer": "https://sportsbook.draftkings.com/",
    "User-Agent": S.headers["User-Agent"],
}

try:
    r = S.get(
        f"{DK_BASE}/eventgroups/{DK_MLB_EG}",
        params={"format": "json"},
        headers=DK_HEADERS,
        timeout=15,
    )
    r.raise_for_status()
    eg = r.json().get("eventGroup", {})

    # List all offer categories
    cats = eg.get("offerCategories", [])
    print(f"  MLB offer categories ({len(cats)} total):")
    for cat in cats:
        subcats = [sc.get("name") for sc in cat.get("offerSubcategoryDescriptors", [])]
        print(f"    [{cat.get('id')}] {cat.get('name')}  →  {subcats[:5]}")

    # Find 1st inning category
    cat_id = subcat_id = None
    cat_name = subcat_name = ""
    for cat in cats:
        cname = (cat.get("name") or "").lower()
        if "inning" not in cname:
            continue
        for sc in cat.get("offerSubcategoryDescriptors", []):
            scname = (sc.get("name") or "").lower()
            if "total" in scname or "run" in scname:
                cat_id     = cat.get("id")
                subcat_id  = sc.get("subcategoryId")
                cat_name   = cat.get("name", "")
                subcat_name = sc.get("name", "")
                break
        if cat_id:
            break

    if not cat_id:
        print("\n  ✗ No 1st-inning total category found")
    else:
        print(f"\n  Using: '{cat_name} / {subcat_name}'  (cat={cat_id}, subcat={subcat_id})")
        r2 = S.get(
            f"{DK_BASE}/eventgroups/{DK_MLB_EG}/categories/{cat_id}/subcategories/{subcat_id}",
            params={"format": "json"},
            headers=DK_HEADERS,
            timeout=15,
        )
        r2.raise_for_status()
        data = r2.json()
        eg2 = data.get("eventGroup", {})

        events_lut = {}
        for ev in eg2.get("events", []):
            eid = ev.get("id") or ev.get("eventId")
            if eid:
                events_lut[int(eid)] = ev

        try:
            cats2 = eg2.get("offerCategories", [])
            scs2  = cats2[0].get("offerSubcategoryDescriptors", []) if cats2 else []
            offers_matrix = scs2[0].get("offerSubcategory", {}).get("offers", []) if scs2 else []
        except Exception:
            offers_matrix = []

        found = 0
        for offer_row in offers_matrix:
            items = offer_row if isinstance(offer_row, list) else [offer_row]
            for offer in items:
                under_price = None
                for oc in offer.get("outcomes", []):
                    lbl  = (oc.get("label") or "").lower()
                    try:
                        line = float(oc.get("line") or 0)
                    except Exception:
                        line = 0.0
                    if lbl == "under" and abs(line - 0.5) < 0.1:
                        try:
                            under_price = int(oc.get("oddsAmerican") or 0)
                        except Exception:
                            pass
                        break
                if not under_price:
                    continue

                eid = offer.get("eventId")
                ev  = events_lut.get(int(eid), {}) if eid else {}

                def gname(ev, *keys):
                    for k in keys:
                        v = ev.get(k)
                        if v and isinstance(v, str):
                            return v
                        if v and isinstance(v, dict):
                            n = v.get("name", "")
                            if n:
                                return n
                    return ""

                home = gname(ev, "homeTeamName", "homeName") or gname(ev.get("home", {}), "name")
                away = gname(ev, "awayTeamName", "awayName") or gname(ev.get("away", {}), "name")
                if not home and not away:
                    evname = ev.get("name", "")
                    for sep in (" vs. ", " vs ", " @ "):
                        if sep in evname:
                            parts = evname.split(sep, 1)
                            away, home = parts[0].strip(), parts[1].strip()
                            break

                price_str = f"+{under_price}" if under_price > 0 else str(under_price)
                print(f"  ✓ {away or '?'} @ {home or '?'}  NRFI Under={price_str}")
                found += 1

        if found == 0:
            print("  ✗ No Under-0.5 1st-inning offers found in this subcategory")
        else:
            print(f"\n  Total: {found} game(s) with NRFI odds from DraftKings")

except Exception as exc:
    print(f"  ERROR: {exc}")

# ── 2. The Odds API (key required) ───────────────────────────────────────────
print("\n[2] The Odds API (ODDS_API_KEY required)")
print("-" * 65)

API_KEY = os.environ.get("ODDS_API_KEY", "")
if not API_KEY:
    print("  ODDS_API_KEY not set — skipping")
    print("  Set it with:  ODDS_API_KEY=your_key python test_odds.py")
    print("  Get a free key at https://the-odds-api.com")
else:
    MARKET_KEYS = [
        ("totals_1st_1_innings", "Under"),
        ("h2h_1st_1_innings",    "No"),
        ("alternate_totals",     "Under"),
    ]
    for mkt, outcome_name in MARKET_KEYS:
        try:
            r = S.get(
                "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds",
                params={
                    "apiKey":     API_KEY,
                    "regions":    "us",
                    "markets":    mkt,
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

        today_events = [
            ev for ev in data
            if (ev.get("commence_time") or "")[:10] == game_date
        ]

        matched = 0
        sample = None
        for ev in today_events:
            for book in ev.get("bookmakers", []):
                for mkt_data in book.get("markets", []):
                    if mkt_data.get("key") == mkt and mkt_data.get("outcomes"):
                        matched += 1
                        if sample is None:
                            sample = {
                                "game": f"{ev['away_team']} @ {ev['home_team']}",
                                "book": book["key"],
                                "outcomes": mkt_data["outcomes"],
                            }

        if matched:
            print(f"  ✓ {mkt:<28} {matched}/{len(today_events)} games  (used={used}, remaining={remaining})")
            if sample:
                print(f"      Sample: {sample['game']}  [{sample['book']}]")
                for oc in sample["outcomes"]:
                    tag = " ← NRFI" if oc.get("name") == outcome_name else ""
                    print(f"        {oc.get('name'):8} {oc.get('price'):+5}{tag}")
        else:
            print(f"  ✗ {mkt:<28} 0/{len(today_events)} games (not on this plan)  (used={used}, remaining={remaining})")

print()
print("=" * 65)
print("Summary:")
print("  DraftKings: no API key needed; works if DK posts 1st-inning totals")
print("  The Odds API: first-inning markets require Standard plan (~$9/mo)")
print("  The code tries The Odds API first, then DraftKings automatically.")
