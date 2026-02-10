"""Tests for update_picks.py"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest
from datetime import datetime

from update_picks import load_and_transform
from utils.shared_utils import (
    parse_game_teams,
    get_week_dates,
    get_output_filename,
    filter_by_weeks,
    get_seasons,
    SPORTS_CONFIG,
)


class TestParseGameTeams:
    """Test parse_game_teams helper function."""

    def test_normal_vs(self):
        team_a, team_b = parse_game_teams("Kansas City Chiefs vs Buffalo Bills")
        assert team_a == "Kansas City Chiefs"
        assert team_b == "Buffalo Bills"

    def test_vs_with_period(self):
        team_a, team_b = parse_game_teams("Chiefs vs. Raiders")
        assert team_a == "Chiefs"
        assert team_b == "Raiders"

    def test_empty_string(self):
        team_a, team_b = parse_game_teams("")
        assert team_a == ""
        assert team_b == ""

    def test_none_value(self):
        team_a, team_b = parse_game_teams(None)
        assert team_a == ""
        assert team_b == ""

    def test_no_vs(self):
        team_a, team_b = parse_game_teams("No versus here")
        assert team_a == ""
        assert team_b == ""

    def test_multiple_vs(self):
        # Should split on first "vs" only
        team_a, team_b = parse_game_teams("A vs B vs C")
        # This should return empty since split results in 3 parts
        assert team_a == ""
        assert team_b == ""


class TestGetWeekDates:
    """Test get_week_dates function."""

    def test_nfl_week_1(self):
        start, end = get_week_dates(1, "NFL", "2025")
        # NFL 2025 season starts Thu Sep 4, Week 1 is Sep 4-11
        assert start == "2025-09-04"
        assert end == "2025-09-11"

    def test_nfl_week_2(self):
        start, end = get_week_dates(2, "NFL", "2025")
        # Week 2 is Sep 11-18
        assert start == "2025-09-11"
        assert end == "2025-09-18"

    def test_invalid_week_zero(self):
        with pytest.raises(ValueError):
            get_week_dates(0, "NFL", "2025")

    def test_invalid_week_negative(self):
        with pytest.raises(ValueError):
            get_week_dates(-1, "NFL", "2025")


class TestGetOutputFilename:
    """Test get_output_filename function."""

    def test_single_week(self):
        filename = get_output_filename([5], False, "NFL")
        assert filename.endswith("leaderboard_nfl_week_5.xlsx")

    def test_multiple_weeks(self):
        filename = get_output_filename([3, 4, 5], False, "NFL")
        assert filename.endswith("leaderboard_nfl_weeks_3-5.xlsx")

    def test_full_season(self):
        filename = get_output_filename(None, True, "NFL")
        assert filename.endswith("leaderboard_nfl_season_2025.xlsx")

    def test_nba_season(self):
        filename = get_output_filename(None, True, "NBA")
        assert filename.endswith("leaderboard_nba_season_2026.xlsx")


class TestLoadAndTransform:
    """Test load_and_transform function."""

    def test_string_is_correct_pick_values(self, tmp_path):
        # User needs >= 3 games and >= 3 wins to pass the filters
        df = pd.DataFrame({
            "game_start_time": [
                "2025-09-10 12:00:00",
                "2025-09-10 13:00:00",
                "2025-09-10 14:00:00",
                "2025-09-10 15:00:00",
                "2025-09-10 16:00:00",
            ],
            "match_title": [
                "Chiefs vs Raiders",
                "Bills vs Dolphins",
                "Cowboys vs Eagles",
                "Packers vs Bears",
                "49ers vs Seahawks",
            ],
            "is_correct_pick": ["TRUE", "TRUE", "TRUE", "FALSE", ""],
            "user_pick": ["Chiefs", "Bills", "Cowboys", "Bears", "49ers"],
            "yes_avg_price": [0.5, 0.5, 0.5, 0.4, 0.5],
            "no_avg_price": [0.4, 0.4, 0.4, 0.5, 0.4],
            "user_address": ["0x1", "0x1", "0x1", "0x1", "0x1"],
        })

        csv_path = tmp_path / "trades.csv"
        df.to_csv(csv_path, index=False)

        result = load_and_transform(str(csv_path))

        # User has 3 wins, 1 loss, 1 pending (5 games total, 3 wins = passes filters)
        assert result["result"].tolist() == ["won", "won", "won", "lost", "pending"]

    def test_boolean_is_correct_pick_values(self, tmp_path):
        # User needs >= 3 games and >= 3 wins to pass the filters
        df = pd.DataFrame({
            "game_start_time": [
                "2025-09-10 12:00:00",
                "2025-09-10 13:00:00",
                "2025-09-10 14:00:00",
                "2025-09-10 15:00:00",
            ],
            "match_title": [
                "Chiefs vs Raiders",
                "Bills vs Dolphins",
                "Cowboys vs Eagles",
                "Packers vs Bears",
            ],
            "is_correct_pick": [True, True, True, False],
            "user_pick": ["Chiefs", "Bills", "Cowboys", "Bears"],
            "yes_avg_price": [0.5, 0.5, 0.5, 0.4],
            "no_avg_price": [0.4, 0.4, 0.4, 0.5],
            "user_address": ["0x1", "0x1", "0x1", "0x1"],
        })

        csv_path = tmp_path / "trades.csv"
        df.to_csv(csv_path, index=False)

        result = load_and_transform(str(csv_path))

        # User has 3 wins, 1 loss (4 games total, 3 wins = passes filters)
        assert result["result"].tolist() == ["won", "won", "won", "lost"]

    def test_missing_columns_raises_error(self, tmp_path):
        df = pd.DataFrame({
            "game_start_time": ["2025-09-10 12:00:00"],
            "match_title": ["Chiefs vs Raiders"],
            # Missing other required columns
        })

        csv_path = tmp_path / "trades.csv"
        df.to_csv(csv_path, index=False)

        with pytest.raises(ValueError, match="Missing required columns"):
            load_and_transform(str(csv_path))

    def test_late_picks_filtered(self, tmp_path):
        # User 0x1 has 4 games with 3 wins (passes filters)
        # User 0x2 has 1 late pick that gets filtered
        df = pd.DataFrame({
            "game_start_time": [
                "2025-09-10 12:00:00",
                "2025-09-10 13:00:00",
                "2025-09-10 14:00:00",
                "2025-09-10 15:00:00",
                "2025-09-10 16:00:00",  # Late pick for user 0x2
            ],
            "match_title": [
                "Chiefs vs Raiders",
                "Bills vs Dolphins",
                "Cowboys vs Eagles",
                "Packers vs Bears",
                "49ers vs Seahawks",
            ],
            "is_correct_pick": ["TRUE", "TRUE", "TRUE", "FALSE", "TRUE"],
            "user_pick": ["Chiefs", "Bills", "Cowboys", "Bears", "49ers"],
            "yes_avg_price": [0.5, 0.5, 0.5, 0.4, 0.96],  # Last pick is late (>= 0.95)
            "no_avg_price": [0.4, 0.4, 0.4, 0.5, 0.04],
            "user_address": ["0x1", "0x1", "0x1", "0x1", "0x2"],
        })

        csv_path = tmp_path / "trades.csv"
        df.to_csv(csv_path, index=False)

        result = load_and_transform(str(csv_path))

        # User 0x1 has 4 picks remaining (3 wins, 1 loss = passes filters)
        # User 0x2's late pick was filtered, then user 0x2 was filtered (< 3 games)
        assert len(result) == 4
        assert all(result["user_address"] == "0x1")


class TestFilterByWeeks:
    """Test filter_by_weeks function."""

    def test_filter_single_week(self):
        df = pd.DataFrame({
            "game_date": ["2025-09-05", "2025-09-12", "2025-09-19"],
            "user_address": ["0x1", "0x2", "0x3"],
        })

        # Filter to week 1 only (Sep 3-10 for NFL)
        result = filter_by_weeks(df, [1], "NFL", "2025")

        assert len(result) == 1
        assert result["user_address"].iloc[0] == "0x1"

    def test_filter_multiple_weeks(self):
        df = pd.DataFrame({
            "game_date": ["2025-09-05", "2025-09-12", "2025-09-19"],
            "user_address": ["0x1", "0x2", "0x3"],
        })

        # Filter to weeks 1-2 (Sep 3-17 for NFL)
        result = filter_by_weeks(df, [1, 2], "NFL", "2025")

        assert len(result) == 2

    def test_empty_weeks_returns_original(self):
        df = pd.DataFrame({
            "game_date": ["2025-09-05"],
            "user_address": ["0x1"],
        })

        result = filter_by_weeks(df, [], "NFL", "2025")

        assert len(result) == 1


class TestSportsConfig:
    """Test SPORTS_CONFIG values."""

    def test_all_sports_defined(self):
        assert "NFL" in SPORTS_CONFIG
        assert "NBA" in SPORTS_CONFIG
        assert "CFB" in SPORTS_CONFIG
        assert "CBB" in SPORTS_CONFIG

    def test_required_fields(self):
        for sport, config in SPORTS_CONFIG.items():
            assert "input_csv" in config, f"{sport} missing input_csv"
            assert "seasons" in config, f"{sport} missing seasons"
            assert isinstance(config["seasons"], list), f"{sport} seasons must be a list"
            assert len(config["seasons"]) > 0, f"{sport} has no season entries"

            for season in config["seasons"]:
                assert "season_id" in season, f"{sport} season missing season_id"
                assert "label" in season, f"{sport} season missing label"
                assert "start_date" in season, f"{sport} season missing start_date"
                assert "regular_weeks" in season, f"{sport} season missing regular_weeks"
                assert "end_date" in season, f"{sport} season missing end_date"

    def test_get_seasons_returns_sorted(self):
        seasons = get_seasons("NFL")
        assert len(seasons) >= 1
        assert seasons[0]["start_date"] >= seasons[-1]["start_date"]
