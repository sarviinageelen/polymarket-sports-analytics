"""Tests for update_trades.py"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_trades import (
    calculate_unrealized_pnl,
    determine_user_pick,
    calculate_is_correct_pick,
    format_number,
    COLLATERAL_SCALE
)


class TestCalculateUnrealizedPnl:
    def test_profit_scenario(self):
        # Bought at 0.50, now worth 0.75
        amount = 100 * COLLATERAL_SCALE
        avg_price = 0.50 * COLLATERAL_SCALE
        current_price = 0.75 * COLLATERAL_SCALE
        
        pnl = calculate_unrealized_pnl(amount, avg_price, current_price)
        expected = 100 * (0.75 - 0.50) * COLLATERAL_SCALE
        assert abs(pnl - expected) < 0.01
    
    def test_loss_scenario(self):
        # Bought at 0.60, now worth 0.40
        amount = 100 * COLLATERAL_SCALE
        avg_price = 0.60 * COLLATERAL_SCALE
        current_price = 0.40 * COLLATERAL_SCALE
        
        pnl = calculate_unrealized_pnl(amount, avg_price, current_price)
        assert pnl < 0
    
    def test_zero_amount(self):
        pnl = calculate_unrealized_pnl(0, 0.50 * COLLATERAL_SCALE, 0.75 * COLLATERAL_SCALE)
        assert pnl == 0.0


class TestDetermineUserPick:
    def test_yes_position_larger(self):
        yes_pos = {"totalBought": "1000"}
        no_pos = {"totalBought": "100"}
        match_title = "Chiefs vs Raiders"
        
        result = determine_user_pick(yes_pos, no_pos, match_title)
        assert result == "Chiefs"
    
    def test_no_position_larger(self):
        yes_pos = {"totalBought": "100"}
        no_pos = {"totalBought": "1000"}
        match_title = "Chiefs vs Raiders"
        
        result = determine_user_pick(yes_pos, no_pos, match_title)
        assert result == "Raiders"
    
    def test_no_positions(self):
        yes_pos = None
        no_pos = None
        match_title = "Chiefs vs Raiders"
        
        result = determine_user_pick(yes_pos, no_pos, match_title)
        assert result == "NONE"
    
    def test_zero_bought(self):
        yes_pos = {"totalBought": "0"}
        no_pos = {"totalBought": "0"}
        match_title = "Chiefs vs Raiders"

        result = determine_user_pick(yes_pos, no_pos, match_title)
        assert result == "NONE"

    def test_period_separator_in_vs(self):
        """Code handles 'vs.' by replacing with 'vs' - verify this works"""
        yes_pos = {"totalBought": "1000"}
        no_pos = {"totalBought": "100"}
        match_title = "Chiefs vs. Raiders"  # Note the period

        result = determine_user_pick(yes_pos, no_pos, match_title)
        assert result == "Chiefs"

    def test_empty_match_title(self):
        """Empty title should fall back to YES/NO"""
        yes_pos = {"totalBought": "1000"}
        no_pos = {"totalBought": "100"}

        result = determine_user_pick(yes_pos, no_pos, "")
        assert result == "YES"

    def test_malformed_match_title(self):
        """Title without 'vs' should fall back to YES/NO"""
        yes_pos = {"totalBought": "1000"}
        no_pos = {"totalBought": "100"}

        result = determine_user_pick(yes_pos, no_pos, "Some Random Title")
        assert result == "YES"


class TestCalculateIsCorrectPick:
    def test_correct_pick(self):
        assert calculate_is_correct_pick("Chiefs", "Chiefs") is True

    def test_incorrect_pick(self):
        assert calculate_is_correct_pick("Chiefs", "Raiders") is False

    def test_no_pick(self):
        assert calculate_is_correct_pick("NONE", "Chiefs") is None

    def test_pending_outcome(self):
        assert calculate_is_correct_pick("Chiefs", "Pending") is None

    def test_empty_pick(self):
        assert calculate_is_correct_pick("", "Chiefs") is None


class TestFormatNumber:
    def test_format_integer(self):
        assert format_number(1234) == "1,234.00"
    
    def test_format_decimal(self):
        assert format_number(1234.567) == "1,234.57"
    
    def test_format_none(self):
        assert format_number(None) == ""
    
    def test_format_negative(self):
        assert format_number(-1234.56) == "-1,234.56"
