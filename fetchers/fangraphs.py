"""
FanGraphs fetcher.
Pulls park factors (single-year and multi-year) with handedness splits.
FanGraphs uses a 100-based scale where 100 = league average.
Values > 100 favor offense; values < 100 favor pitching.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import FANGRAPHS_BASE, CACHE_TTL, PARK_FACTOR_NEUTRAL
from fetchers._cache import cache_get, cache_set

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.fangraphs.com/",
})

# Known handedness-specific park factors
# Structure: { venue_name_lower: { "overall": int, "lhb": int, "rhb": int } }
# Maintained as a hardcoded fallback; will be updated via scraping
_PARK_FACTOR_FALLBACK: dict[str, dict] = {
    "coors field":              {"overall": 115, "lhb": 113, "rhb": 117, "runs": 115},
    "great american ball park": {"overall": 108, "lhb": 106, "rhb": 110, "runs": 108},
    "yankee stadium":           {"overall": 107, "lhb": 112, "rhb": 103, "runs": 107},
    "fenway park":              {"overall": 106, "lhb": 110, "rhb": 102, "runs": 106},
    "wrigley field":            {"overall": 104, "lhb": 103, "rhb": 105, "runs": 104},
    "globe life field":         {"overall": 103, "lhb": 104, "rhb": 102, "runs": 103},
    "citizens bank park":       {"overall": 103, "lhb": 103, "rhb": 103, "runs": 103},
    "oracle park":              {"overall":  94, "lhb":  92, "rhb":  96, "runs":  94},
    "petco park":               {"overall":  93, "lhb":  94, "rhb":  92, "runs":  93},
    "tropicana field":          {"overall":  93, "lhb":  92, "rhb":  94, "runs":  93},
    "dodger stadium":           {"overall":  96, "lhb":  96, "rhb":  96, "runs":  96},
    "t-mobile park":            {"overall":  95, "lhb":  95, "rhb":  95, "runs":  95},
    "angel stadium":            {"overall":  97, "lhb":  98, "rhb":  96, "runs":  97},
    "minute maid park":         {"overall":  99, "lhb": 101, "rhb":  97, "runs":  99},
    "chase field":              {"overall": 100, "lhb": 100, "rhb": 100, "runs": 100},
    "american family field":    {"overall": 101, "lhb": 101, "rhb": 101, "runs": 101},
    "busch stadium":            {"overall":  97, "lhb":  97, "rhb":  97, "runs":  97},
    "pnc park":                 {"overall":  95, "lhb":  93, "rhb":  97, "runs":  95},
    "progressive field":        {"overall":  98, "lhb":  98, "rhb":  98, "runs":  98},
    "kauffman stadium":         {"overall":  96, "lhb":  96, "rhb":  96, "runs":  96},
    "target field":             {"overall":  99, "lhb":  99, "rhb":  99, "runs":  99},
    "comerica park":            {"overall":  96, "lhb":  95, "rhb":  97, "runs":  96},
    "guaranteed rate field":    {"overall": 104, "lhb": 105, "rhb": 103, "runs": 104},
    "truist park":              {"overall": 103, "lhb": 103, "rhb": 103, "runs": 103},
    "camden yards":             {"overall": 104, "lhb": 105, "rhb": 103, "runs": 104},
    "nationals park":           {"overall": 101, "lhb": 100, "rhb": 102, "runs": 101},
    "citi field":               {"overall":  97, "lhb":  97, "rhb":  97, "runs":  97},
    "loanDepot park":           {"overall":  96, "lhb":  96, "rhb":  96, "runs":  96},
    "loandepot park":           {"overall":  96, "lhb":  96, "rhb":  96, "runs":  96},
    "sutter health park":       {"overall": 100, "lhb": 100, "rhb": 100, "runs": 100},
    "oakland coliseum":         {"overall":  93, "lhb":  93, "rhb":  93, "runs":  93},
    "sac softball":             {"overall": 100, "lhb": 100, "rhb": 100, "runs": 100},
}


def get_park_factors(season: int | None = None) -> dict[str, dict]:
    """
    Return park factors keyed by venue name (lower-case).
    Tries FanGraphs scrape first; falls back to hardcoded table.
    """
    from datetime import date
    season = season or date.today().year
    cache_key = f"fg_park_factors_{season}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    scraped = _scrape_fg_park_factors(season)
    if scraped:
        result = scraped
    else:
        logger.warning("FanGraphs park factor scrape failed, using fallback table")
        result = dict(_PARK_FACTOR_FALLBACK)

    cache_set(cache_key, result, ttl=CACHE_TTL["park_factors"])
    return result


def _scrape_fg_park_factors(season: int) -> dict[str, dict] | None:
    """
    Scrape FanGraphs Guts page for multi-year park factors.
    Returns None on failure.
    """
    url = f"{FANGRAPHS_BASE}/guts.aspx"
    params = {"type": "pf", "teamid": "0", "season": season}

    try:
        resp = SESSION.get(url, params=params, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("FanGraphs park factor fetch failed: %s", exc)
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # FanGraphs table has id="GutsBoard1_dg1_ctl00"
    table = soup.find("table", {"id": re.compile(r"GutsBoard.*dg1_ctl00")})
    if not table:
        # Try any table with park factor columns
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if any("basic" in h or "1yr" in h or "pf" in h for h in headers):
                table = t
                break

    if not table:
        logger.warning("FanGraphs park factor table not found in HTML")
        return None

    result = {}
    rows = table.find_all("tr")[1:]  # skip header
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 3:
            continue
        team_name = cells[0].lower().strip()
        # Columns vary; try to extract basic pf (1yr) and 3yr
        try:
            pf_basic = int(cells[1]) if cells[1].isdigit() else PARK_FACTOR_NEUTRAL
        except (ValueError, IndexError):
            pf_basic = PARK_FACTOR_NEUTRAL

        result[team_name] = {
            "overall": pf_basic,
            "lhb":     pf_basic,   # FanGraphs guts doesn't expose hand splits directly
            "rhb":     pf_basic,
            "runs":    pf_basic,
        }

    if not result:
        return None

    logger.info("Scraped FanGraphs park factors for %d teams", len(result))
    return result


def get_park_factor_for_venue(venue_name: str, batter_hand: str | None = None,
                               season: int | None = None) -> dict:
    """
    Look up park factor for a venue, with optional handedness split.
    Returns dict with 'overall', 'lhb', 'rhb', 'runs', 'adjustment' keys.
    'adjustment' is centered on 0: positive = hitter-friendly, negative = pitcher-friendly.
    """
    factors = get_park_factors(season)
    key = venue_name.lower().strip()

    pf = factors.get(key)

    # Fuzzy match if exact miss
    if pf is None:
        for stored_key, stored_pf in factors.items():
            if stored_key in key or key in stored_key:
                pf = stored_pf
                break

    if pf is None:
        pf = {"overall": PARK_FACTOR_NEUTRAL, "lhb": PARK_FACTOR_NEUTRAL,
              "rhb": PARK_FACTOR_NEUTRAL, "runs": PARK_FACTOR_NEUTRAL}

    # Determine relevant factor based on batter hand
    if batter_hand == "L":
        relevant = pf.get("lhb", pf["overall"])
    elif batter_hand == "R":
        relevant = pf.get("rhb", pf["overall"])
    else:
        relevant = pf["overall"]

    # Normalize: adjustment centered on 0, scaled by ~0.01 per point
    adjustment = (relevant - PARK_FACTOR_NEUTRAL) / 100.0

    return {**pf, "relevant": relevant, "adjustment": adjustment}


def get_all_venue_park_factors() -> dict[str, dict]:
    """Return full park factor table (all known venues)."""
    return get_park_factors()
