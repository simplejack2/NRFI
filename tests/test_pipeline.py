"""
Dry-run integration test for the NRFI pipeline.
Patches all external fetchers with synthetic data and verifies the full
scoring → probability → report chain runs without errors.
"""

from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import patch, MagicMock


# ── Synthetic test data ────────────────────────────────────────────────────────

FAKE_SCHEDULE = [{
    "game_pk":        745333,
    "game_date":      "2026-04-03",
    "game_time":      "2026-04-03T17:10:00Z",
    "status":         "S",
    "venue_id":       10,
    "venue_name":     "Dodger Stadium",
    "home_team_id":   119,
    "home_team_name": "Los Angeles Dodgers",
    "away_team_id":   137,
    "away_team_name": "San Francisco Giants",
    "home_probable":  {"id": 543037, "name": "Clayton Kershaw", "hand": "L"},
    "away_probable":  {"id": 605400, "name": "Logan Webb", "hand": "R"},
},
{
    "game_pk":        745334,
    "game_date":      "2026-04-03",
    "game_time":      "2026-04-03T23:10:00Z",
    "status":         "S",
    "venue_id":       3281,
    "venue_name":     "Coors Field",
    "home_team_id":   115,
    "home_team_name": "Colorado Rockies",
    "away_team_id":   109,
    "away_team_name": "Arizona Diamondbacks",
    "home_probable":  {"id": 608566, "name": "Kyle Freeland", "hand": "L"},
    "away_probable":  {"id": 671096, "name": "Merrill Kelly", "hand": "R"},
}]

FAKE_LINEUPS = {
    "home": [
        {"order": 1, "player_id": 660670, "name": "Mookie Betts",    "bat_side": "R"},
        {"order": 2, "player_id": 664023, "name": "Freddie Freeman", "bat_side": "L"},
        {"order": 3, "player_id": 660271, "name": "Shohei Ohtani",   "bat_side": "L"},
        {"order": 4, "player_id": 641355, "name": "Teoscar Hernandez","bat_side": "R"},
        {"order": 5, "player_id": 642708, "name": "Will Smith",      "bat_side": "R"},
        {"order": 6, "player_id": 646240, "name": "Max Muncy",       "bat_side": "L"},
        {"order": 7, "player_id": 596019, "name": "Chris Taylor",    "bat_side": "R"},
        {"order": 8, "player_id": 621563, "name": "Miguel Rojas",    "bat_side": "R"},
        {"order": 9, "player_id": 543037, "name": "P9",             "bat_side": "R"},
    ],
    "away": [
        {"order": 1, "player_id": 686668, "name": "Leadoff G",       "bat_side": "L"},
        {"order": 2, "player_id": 641733, "name": "2-spot G",        "bat_side": "R"},
        {"order": 3, "player_id": 670770, "name": "3-spot G",        "bat_side": "L"},
        {"order": 4, "player_id": 661388, "name": "4-spot G",        "bat_side": "R"},
        {"order": 5, "player_id": 602074, "name": "5-spot G",        "bat_side": "R"},
        {"order": 6, "player_id": 670032, "name": "6-spot G",        "bat_side": "L"},
        {"order": 7, "player_id": 664040, "name": "7-spot G",        "bat_side": "R"},
        {"order": 8, "player_id": 680757, "name": "8-spot G",        "bat_side": "R"},
        {"order": 9, "player_id": 605400, "name": "P9 G",            "bat_side": "R"},
    ],
}

FAKE_VENUES = {
    10: {
        "name":      "Dodger Stadium",
        "city":      "Los Angeles",
        "lat":       34.0736,
        "lon":       -118.2400,
        "roof_type": "Open",
    },
    3281: {
        "name":      "Coors Field",
        "city":      "Denver",
        "lat":       39.7559,
        "lon":       -104.9942,
        "roof_type": "Open",
    },
}

FAKE_PITCHER_STATCAST = {
    543037: {  # Kershaw
        "xwoba_allowed": 0.265, "k_pct": 0.285, "bb_pct": 0.055,
        "hard_hit_pct": 0.320, "barrel_pct": 0.055, "gb_pct": 0.480,
        "first_pitch_strike_pct": 0.670, "pa": 350,
    },
    605400: {  # Webb
        "xwoba_allowed": 0.275, "k_pct": 0.240, "bb_pct": 0.065,
        "hard_hit_pct": 0.340, "barrel_pct": 0.065, "gb_pct": 0.520,
        "first_pitch_strike_pct": 0.660, "pa": 400,
    },
    608566: {  # Freeland
        "xwoba_allowed": 0.330, "k_pct": 0.180, "bb_pct": 0.090,
        "hard_hit_pct": 0.400, "barrel_pct": 0.095, "gb_pct": 0.420,
        "first_pitch_strike_pct": 0.610, "pa": 300,
    },
    671096: {  # Kelly
        "xwoba_allowed": 0.310, "k_pct": 0.220, "bb_pct": 0.070,
        "hard_hit_pct": 0.360, "barrel_pct": 0.075, "gb_pct": 0.440,
        "first_pitch_strike_pct": 0.630, "pa": 380,
    },
}

FAKE_BATTER_STATCAST = {
    pid: {
        "xwoba": 0.320, "bb_pct": 0.090, "hard_hit_pct": 0.380,
        "barrel_pct": 0.085, "pa": 80,
    }
    for pid in [660670, 664023, 660271, 641355, 645708, 686668, 641733, 670770,
                661388, 602074, 670032, 664040, 680757, 642708, 646240, 596019,
                621563]
}

FAKE_SPRINT_SPEED = {
    pid: {"sprint_speed": 27.0} for pid in list(FAKE_BATTER_STATCAST.keys())
}

FAKE_POP_TIME = {
    642708: {"pop_2b_sba": 2.00, "name": "Will Smith", "cs_pct": 0.25}
}

FAKE_WEATHER = {
    "temperature_f": 72.0,
    "wind_mph": 8.0,
    "wind_direction_deg": 270.0,
    "wind_direction_str": "W",
    "conditions": "Clear",
    "humidity_pct": 45.0,
    "roof_type": "open",
    "weather_adjustment": 0.02,
    "source": "mock",
    "venue_name": "Dodger Stadium",
}


# ── Test runner ────────────────────────────────────────────────────────────────

def test_full_pipeline():
    """Patch all external data sources and run the full model."""
    patches = [
        patch("model.nrfi_model.get_schedule",             return_value=FAKE_SCHEDULE),
        patch("model.nrfi_model.get_lineups",              return_value=FAKE_LINEUPS),
        patch("model.nrfi_model.get_venues",               return_value=FAKE_VENUES),
        patch("scoring.pitcher_score.get_pitcher_season_stats",      return_value={}),
        patch("scoring.pitcher_score.get_pitcher_career_stats",      return_value={}),
        patch("scoring.pitcher_score.get_pitcher_splits",            return_value={}),
        patch("scoring.pitcher_score.get_pitcher_statcast_season",   return_value=FAKE_PITCHER_STATCAST),
        patch("scoring.pitcher_score.get_pitcher_statcast_prior_season", return_value={}),
        patch("scoring.pitcher_score.get_pitcher_splits_statcast",   return_value={}),
        patch("scoring.batter_score.get_batter_season_stats",        return_value={"obp": 0.340, "bb_pct": 0.090}),
        patch("scoring.batter_score.get_batter_vs_hand_splits",      return_value={}),
        patch("scoring.batter_score.get_batter_statcast_season",     return_value=FAKE_BATTER_STATCAST),
        patch("scoring.batter_score.get_batter_statcast_prior_season",return_value={}),
        patch("scoring.batter_score.get_batter_splits_statcast",     return_value={}),
        patch("scoring.damage_speed.get_sprint_speed",               return_value=FAKE_SPRINT_SPEED),
        patch("scoring.damage_speed.get_pop_time",                   return_value=FAKE_POP_TIME),
        patch("scoring.damage_speed.get_team_catchers",              return_value=[{"player_id": 642708, "name": "Will Smith"}]),
        patch("scoring.damage_speed.get_pitcher_statcast_season",    return_value=FAKE_PITCHER_STATCAST),
        patch("scoring.damage_speed.get_batter_statcast_season",     return_value=FAKE_BATTER_STATCAST),
        patch("scoring.park_weather.get_park_factor_for_venue",      return_value={"overall": 96, "lhb": 96, "rhb": 96, "runs": 96, "relevant": 96, "adjustment": -0.04}),
        patch("scoring.park_weather.get_weather_for_venue",          return_value=FAKE_WEATHER),
    ]
    for p in patches:
        p.start()
    try:
        from model.nrfi_model import run_daily_model
        results = run_daily_model("2026-04-03")

        assert len(results) == 2, f"Expected 2 games, got {len(results)}"

        # Results are sorted by NRFI prob descending
        assert results[0]["nrfi_prob"] >= results[1]["nrfi_prob"]

        for r in results:
            # Basic structure checks
            assert "nrfi_prob" in r
            assert "top_half" in r
            assert "bot_half" in r
            assert "bet_recommendation" in r
            assert 0 <= r["nrfi_prob"] <= 1
            assert 0 <= r["top_half"]["half_inning_prob"] <= 1
            assert 0 <= r["bot_half"]["half_inning_prob"] <= 1

            # NRFI = product of two halves
            expected_nrfi = round(
                r["top_half"]["half_inning_prob"] * r["bot_half"]["half_inning_prob"], 4
            )
            assert abs(r["nrfi_prob"] - expected_nrfi) < 0.0001

            print(f"  {r['away_team']} @ {r['home_team']}: NRFI={r['nrfi_prob']:.1%}  "
                  f"top={r['top_half']['half_inning_prob']:.1%}  "
                  f"bot={r['bot_half']['half_inning_prob']:.1%}  "
                  f"rec={r['bet_recommendation']['recommended']}")

        # Dodger Stadium should score higher than Coors Field
        dodger_game = next(r for r in results if "Dodger" in r["venue_name"])
        coors_game  = next(r for r in results if "Coors"  in r["venue_name"])
        assert dodger_game["nrfi_prob"] > coors_game["nrfi_prob"], (
            f"Dodger Stadium NRFI {dodger_game['nrfi_prob']:.3f} should > "
            f"Coors Field NRFI {coors_game['nrfi_prob']:.3f}"
        )
        print(f"\n  Dodger Stadium ({dodger_game['nrfi_prob']:.1%}) > "
              f"Coors Field ({coors_game['nrfi_prob']:.1%})  [correct]")

        print("\n  Pipeline test PASSED.")
        return results
    finally:
        for p in patches:
            p.stop()


def test_reporter(results):
    """Test that the report prints without errors."""
    from output.reporter import print_daily_report
    print_daily_report(results, "2026-04-03")
    print("\n  Reporter test PASSED.")


def test_probability_calibration():
    """Verify probability mapping is calibrated reasonably."""
    from model.nrfi_model import _composite_to_probability

    p_avg  = _composite_to_probability(0.5)
    p_high = _composite_to_probability(0.8)
    p_low  = _composite_to_probability(0.2)

    print(f"  P(no run) at avg composite (0.5):  {p_avg:.3f}")
    print(f"  P(no run) at high composite (0.8): {p_high:.3f}")
    print(f"  P(no run) at low composite (0.2):  {p_low:.3f}")
    print(f"  Implied NRFI at avg: {p_avg**2:.3f} (target ~0.72)")

    assert 0.82 <= p_avg  <= 0.88, f"Average half-inning prob out of range: {p_avg}"
    assert p_high > p_avg > p_low
    print("  Calibration test PASSED.")


if __name__ == "__main__":
    print("\n=== NRFI Pipeline Dry-Run Tests ===\n")

    print("1. Probability calibration...")
    test_probability_calibration()
    print()

    print("2. Full pipeline (mocked data)...")
    results = test_full_pipeline()
    print()

    print("3. Reporter output...")
    test_reporter(results)
