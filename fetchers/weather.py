"""
Weather fetcher.
Pulls current conditions for a ballpark using either:
  1. OpenWeatherMap API (requires OPENWEATHER_API_KEY env var), or
  2. wttr.in free JSON API (no key required, used as fallback).

Returns structured weather context: temperature, wind speed/direction, conditions,
roof status, and a derived "weather_adjustment" for the scoring model.
"""

from __future__ import annotations

import logging
import math
import os
import time
from typing import Any

import requests

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import OPENWEATHER_API, CACHE_TTL, WEATHER
from fetchers._cache import cache_get, cache_set

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "NRFI-Predictor/1.0 (research)",
    "Accept": "application/json",
})

# Retractable / domed stadiums and their roof status categories
# "dome"       = always climate-controlled (weather irrelevant)
# "retractable"= may be open or closed (check game notes)
# "open"       = always open (full weather exposure)
VENUE_ROOF_TYPE: dict[str, str] = {
    "tropicana field":          "dome",
    "minute maid park":         "retractable",
    "american family field":    "retractable",
    "chase field":              "retractable",
    "loanDepot park":           "retractable",
    "loandepot park":           "retractable",
    "t-mobile park":            "retractable",
    "globe life field":         "retractable",
    "rogers centre":            "retractable",
    # All others default to "open"
}

# Wind "blowing out" compass bearings (degrees) by venue.
# If the game-time wind direction (degrees from North) is close to this value,
# wind is blowing out toward the outfield seats.
WIND_OUT_BEARING: dict[str, float] = {
    "wrigley field":       225.0,  # Out to left-center (SW wind)
    "fenway park":         270.0,  # Out to right (W wind)
    "yankee stadium":      270.0,
    "coors field":         270.0,
    "great american ball park": 270.0,
    "oracle park":         315.0,  # NW wind blows in from bay
    # Default: no strong directional bias stored
}


def get_weather_for_venue(
    venue_name: str,
    lat: float | None,
    lon: float | None,
    game_time_utc: str | None = None,
) -> dict:
    """
    Fetch weather conditions for a venue.
    Returns dict with temperature_f, wind_mph, wind_direction, conditions,
    roof_type, weather_adjustment.
    weather_adjustment: float in [-1, 1] where positive = hitter-friendly.
    """
    roof_type = _get_roof_type(venue_name)

    # Dome: weather is irrelevant
    if roof_type == "dome":
        return _dome_conditions(venue_name)

    # Retractable: conditions matter but we note uncertainty
    conditions = _fetch_conditions(venue_name, lat, lon)
    conditions["roof_type"] = roof_type

    # Compute adjustment
    conditions["weather_adjustment"] = _compute_weather_adjustment(conditions, venue_name)
    return conditions


def _get_roof_type(venue_name: str) -> str:
    key = venue_name.lower().strip()
    for stored, roof in VENUE_ROOF_TYPE.items():
        if stored in key or key in stored:
            return roof
    return "open"


def _dome_conditions(venue_name: str) -> dict:
    return {
        "venue_name":          venue_name,
        "roof_type":           "dome",
        "temperature_f":       72.0,
        "wind_mph":            0.0,
        "wind_direction_deg":  0.0,
        "wind_direction_str":  "None",
        "conditions":          "Dome",
        "humidity_pct":        50.0,
        "weather_adjustment":  0.0,   # Neutral – dome is controlled
        "source":              "dome_default",
    }


def _fetch_conditions(venue_name: str, lat: float | None, lon: float | None) -> dict:
    """Try OpenWeatherMap first, then wttr.in."""
    api_key = os.environ.get("OPENWEATHER_API_KEY")

    if api_key and lat is not None and lon is not None:
        result = _fetch_openweather(lat, lon, api_key)
        if result:
            result["venue_name"] = venue_name
            return result

    # Fallback: wttr.in
    city = venue_name.lower().replace(" ", "+")
    result = _fetch_wttr(city, lat, lon)
    if result:
        result["venue_name"] = venue_name
        return result

    # Final fallback: neutral
    logger.warning("Weather fetch failed for %s, using neutral defaults", venue_name)
    return {
        "venue_name":          venue_name,
        "temperature_f":       65.0,
        "wind_mph":            5.0,
        "wind_direction_deg":  270.0,
        "wind_direction_str":  "W",
        "conditions":          "Unknown",
        "humidity_pct":        50.0,
        "source":              "fallback",
    }


def _fetch_openweather(lat: float, lon: float, api_key: str) -> dict | None:
    cache_key = f"weather_ow_{lat:.2f}_{lon:.2f}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        resp = SESSION.get(OPENWEATHER_API, params={
            "lat":   lat,
            "lon":   lon,
            "appid": api_key,
            "units": "imperial",
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        logger.warning("OpenWeatherMap request failed: %s", exc)
        return None

    wind_deg  = data.get("wind", {}).get("deg", 270)
    wind_mph  = data.get("wind", {}).get("speed", 0)
    temp_f    = data.get("main", {}).get("temp", 65)
    humidity  = data.get("main", {}).get("humidity", 50)
    cond_list = data.get("weather", [{}])
    conditions= cond_list[0].get("main", "Clear") if cond_list else "Clear"

    result = {
        "temperature_f":       float(temp_f),
        "wind_mph":            float(wind_mph),
        "wind_direction_deg":  float(wind_deg),
        "wind_direction_str":  _degrees_to_cardinal(wind_deg),
        "conditions":          conditions,
        "humidity_pct":        float(humidity),
        "source":              "openweathermap",
    }
    cache_set(cache_key, result, ttl=CACHE_TTL["weather"])
    return result


def _fetch_wttr(city: str, lat: float | None, lon: float | None) -> dict | None:
    if lat is not None and lon is not None:
        query = f"{lat},{lon}"
    else:
        query = city

    cache_key = f"weather_wttr_{query}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        resp = SESSION.get(
            f"https://wttr.in/{query}",
            params={"format": "j1"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("wttr.in request failed: %s", exc)
        return None

    try:
        current = data["current_condition"][0]
        temp_f   = float(current.get("temp_F", 65))
        wind_mph = float(current.get("windspeedMiles", 5))
        wind_dir = float(current.get("winddirDegree", 270))
        humidity = float(current.get("humidity", 50))
        desc     = current.get("weatherDesc", [{}])[0].get("value", "Clear")

        result = {
            "temperature_f":       temp_f,
            "wind_mph":            wind_mph,
            "wind_direction_deg":  wind_dir,
            "wind_direction_str":  _degrees_to_cardinal(wind_dir),
            "conditions":          desc,
            "humidity_pct":        humidity,
            "source":              "wttr.in",
        }
        cache_set(cache_key, result, ttl=CACHE_TTL["weather"])
        return result
    except (KeyError, IndexError, ValueError) as exc:
        logger.warning("wttr.in parse error: %s", exc)
        return None


def _compute_weather_adjustment(conditions: dict, venue_name: str) -> float:
    """
    Derive a single weather_adjustment float in [-0.20, +0.20].
    Positive = hitter-friendly (raises run expectancy).
    Negative = pitcher-friendly.

    Components:
      - Temperature effect
      - Wind effect (out vs in vs crosswind)
      - Precipitation / dome effect
    """
    adj = 0.0

    temp_f   = conditions.get("temperature_f", 65)
    wind_mph = conditions.get("wind_mph", 0)
    wind_deg = conditions.get("wind_direction_deg", 270)
    cond_str = (conditions.get("conditions") or "").lower()
    roof     = conditions.get("roof_type", "open")

    # ── Temperature ───────────────────────────────────────────────────────────
    if temp_f >= WEATHER["warm_temp_f"]:
        # Warm/hot: ball carries more
        adj += 0.04 * min((temp_f - WEATHER["warm_temp_f"]) / 20.0, 1.0)
    elif temp_f <= WEATHER["cold_temp_f"]:
        # Cold: ball doesn't carry, hitters handcuffed
        adj -= 0.06 * min((WEATHER["cold_temp_f"] - temp_f) / 20.0, 1.0)

    # ── Wind ──────────────────────────────────────────────────────────────────
    if wind_mph >= WEATHER["wind_out_threshold"] and roof == "open":
        out_bearing  = _get_out_bearing(venue_name)
        wind_out_component = _wind_out_component(wind_deg, out_bearing, wind_mph)
        # Positive component = blowing out (hitter-friendly)
        adj += 0.08 * wind_out_component

    # ── Precipitation ─────────────────────────────────────────────────────────
    if any(w in cond_str for w in ["rain", "drizzle", "shower", "thunderstorm"]):
        adj -= 0.05   # Wet ball, lower scoring

    # ── Dome / retractable ────────────────────────────────────────────────────
    if roof == "dome":
        adj = 0.0   # No weather effect

    return max(-0.20, min(0.20, adj))


def _get_out_bearing(venue_name: str) -> float:
    """Return compass bearing for 'blowing out' at a venue (default: 270 = west)."""
    key = venue_name.lower().strip()
    for stored_venue, bearing in WIND_OUT_BEARING.items():
        if stored_venue in key or key in stored_venue:
            return bearing
    return 270.0   # Generic assumption


def _wind_out_component(wind_deg: float, out_bearing: float, wind_mph: float) -> float:
    """
    Return a value in [-1, 1] where:
      +1 = full tailwind blowing straight out
      -1 = full headwind blowing straight in
    Scaled by wind strength (mph / 20 mph reference).
    """
    angle_diff = abs((wind_deg - out_bearing + 180) % 360 - 180)
    # Cosine of angle difference: 0° = full tailwind (+1), 180° = full headwind (-1)
    cos_component = math.cos(math.radians(angle_diff))
    speed_factor  = min(wind_mph / 20.0, 1.5)
    return cos_component * speed_factor


def _degrees_to_cardinal(deg: float) -> str:
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = round(deg / 22.5) % 16
    return dirs[idx]
