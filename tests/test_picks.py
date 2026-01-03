"""Tests for update_picks.py"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np

from update_picks import (
    safe_divide,
    fmt,
    load_and_transform,
    calculate_streaks,
    calculate_time_window_stats,
    calculate_bet_price_stats,
    CFG,
    HISTORICAL_COLUMNS,
    ACTIVE_COLUMNS
)


class TestConfig:
    """Test configuration values are set correctly."""
    
    def test_output_files_historical(self):
        assert CFG.OUTPUT_HISTORICAL_NFL == "output_picks_historical_nfl.csv"
        assert CFG.OUTPUT_HISTORICAL_NBA == "output_picks_historical_nba.csv"
        assert CFG.OUTPUT_HISTORICAL_CFB == "output_picks_historical_cfb.csv"
        assert CFG.OUTPUT_HISTORICAL_CBB == "output_picks_historical_cbb.csv"

    def test_output_files_active(self):
        assert CFG.OUTPUT_ACTIVE_NFL == "output_picks_active_nfl.csv"
        assert CFG.OUTPUT_ACTIVE_NBA == "output_picks_active_nba.csv"
        assert CFG.OUTPUT_ACTIVE_CFB == "output_picks_active_cfb.csv"
        assert CFG.OUTPUT_ACTIVE_CBB == "output_picks_active_cbb.csv"
    
    def test_filtering_thresholds(self):
        assert CFG.MIN_GAMES == 3
        assert CFG.MIN_PNL == 1000.0
        assert CFG.MIN_WIN_RATIO_FOR_ACTIVE == 66.67  # 0-100 scale
    
    def test_api_settings(self):
        assert CFG.API_TIMEOUT == 10
        assert CFG.API_MAX_WORKERS == 5
        assert CFG.API_BATCH_SIZE == 100


class TestColumnDefinitions:
    """Test column lists are properly defined."""
    
    def test_historical_columns_exist(self):
        required = ['user_address', 'games', 'wins', 'loss', 'win_ratio', 'profit']
        for col in required:
            assert col in HISTORICAL_COLUMNS, f"Missing column: {col}"
    
    def test_active_columns_exist(self):
        required = ['user_address', 'condition_id', 'match_title', 'position', 'size']
        for col in required:
            assert col in ACTIVE_COLUMNS, f"Missing column: {col}"


class TestLoadAndTransform:
    def test_string_is_correct_pick_values(self, tmp_path):
        df = pd.DataFrame({
            "game_start_time": [
                "2025-09-10 12:00:00",
                "2025-09-10 12:00:00",
                "2025-09-10 12:00:00",
            ],
            "match_title": [
                "Chiefs vs Raiders",
                "Chiefs vs Raiders",
                "Chiefs vs Raiders",
            ],
            "is_correct_pick": ["TRUE", "FALSE", ""],
            "user_pick": ["Chiefs", "Raiders", "Chiefs"],
            "yes_avg_price": [0.5, 0.5, 0.5],
            "no_avg_price": [0.4, 0.4, 0.4],
            "user_address": ["0x1", "0x1", "0x1"],
        })

        csv_path = tmp_path / "trades.csv"
        df.to_csv(csv_path, index=False)

        result = load_and_transform(str(csv_path))

        assert result["result"].tolist() == ["won", "lost", "pending"]


class TestSafeDivide:
    def test_normal_division(self):
        assert safe_divide(10, 2) == 5.0
    
    def test_divide_by_zero(self):
        assert safe_divide(10, 0) == 0.0
    
    def test_divide_by_zero_custom_default(self):
        assert safe_divide(10, 0, default=-1) == -1
    
    def test_divide_by_nan(self):
        assert safe_divide(10, float('nan')) == 0.0


class TestFmt:
    def test_format_float(self):
        assert fmt(3.14159, 2) == 3.14
    
    def test_format_nan(self):
        assert fmt(float('nan')) == 0.0
    
    def test_format_inf(self):
        assert fmt(float('inf')) == 0.0


class TestCalculateStreaks:
    def test_all_wins(self):
        trades = pd.DataFrame({'is_correct_pick': [1, 1, 1, 1, 1]})
        result = calculate_streaks(trades)
        assert result['current_streak'] == 5
        assert result['max_win_streak'] == 5
        assert result['max_loss_streak'] == 0
    
    def test_all_losses(self):
        trades = pd.DataFrame({'is_correct_pick': [0, 0, 0]})
        result = calculate_streaks(trades)
        assert result['current_streak'] == -3
        assert result['max_win_streak'] == 0
        assert result['max_loss_streak'] == 3
    
    def test_mixed_results(self):
        # W, W, L, W, W, W
        trades = pd.DataFrame({'is_correct_pick': [1, 1, 0, 1, 1, 1]})
        result = calculate_streaks(trades)
        assert result['current_streak'] == 3
        assert result['max_win_streak'] == 3
        assert result['max_loss_streak'] == 1
    
    def test_empty_trades(self):
        trades = pd.DataFrame({'is_correct_pick': []})
        result = calculate_streaks(trades)
        assert result['current_streak'] == 0
        assert result['max_win_streak'] == 0
        assert result['max_loss_streak'] == 0


class TestCalculateTimeWindowStats:
    def test_basic_stats(self):
        trades = pd.DataFrame({
            'is_correct_pick': [1, 1, 0, 1],
            'total_pnl': [100, 200, -50, 150]
        })
        result = calculate_time_window_stats(trades)
        
        assert result['games'] == 4
        assert result['wins'] == 3
        assert result['loss'] == 1
        assert result['win_ratio'] == 75.0
        assert result['profit'] == 400.0
    
    def test_empty_trades(self):
        trades = pd.DataFrame({
            'is_correct_pick': [],
            'total_pnl': []
        })
        result = calculate_time_window_stats(trades)
        
        assert result['games'] == 0
        assert result['wins'] == 0
        assert result['loss'] == 0
        assert result['win_ratio'] == 0
        assert result['profit'] == 0


class TestCalculateBetPriceStats:
    def test_normal_prices(self):
        trades = pd.DataFrame({'avg_price': [0.3, 0.4, 0.5, 0.6]})
        result = calculate_bet_price_stats(trades)
        assert result['avg_bet_price'] == 0.45
        assert result['median_bet_price'] == 0.45
    
    def test_underdog_bettor(self):
        # Low prices = betting on underdogs
        trades = pd.DataFrame({'avg_price': [0.2, 0.25, 0.3, 0.35]})
        result = calculate_bet_price_stats(trades)
        assert result['avg_bet_price'] < 0.4
    
    def test_favorite_bettor(self):
        # High prices = betting on favorites
        trades = pd.DataFrame({'avg_price': [0.65, 0.7, 0.75, 0.8]})
        result = calculate_bet_price_stats(trades)
        assert result['avg_bet_price'] > 0.6
    
    def test_empty_trades(self):
        trades = pd.DataFrame({'avg_price': []})
        result = calculate_bet_price_stats(trades)
        assert result['avg_bet_price'] == 0.5
        assert result['median_bet_price'] == 0.5
    
    def test_missing_column(self):
        trades = pd.DataFrame({'other_column': [1, 2, 3]})
        result = calculate_bet_price_stats(trades)
        assert result['avg_bet_price'] == 0.5
        assert result['median_bet_price'] == 0.5
