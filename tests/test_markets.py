"""Tests for update_markets.py"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_markets import (
    is_moneyline_market,
    determine_winner,
    extract_teams,
    parse_json_field
)


class TestParseJsonField:
    def test_parse_json_string(self):
        result = parse_json_field('["Chiefs", "Raiders"]')
        assert result == ["Chiefs", "Raiders"]
    
    def test_parse_already_list(self):
        result = parse_json_field(["Chiefs", "Raiders"])
        assert result == ["Chiefs", "Raiders"]
    
    def test_parse_invalid_json(self):
        result = parse_json_field("not json")
        assert result == "not json"
    
    def test_parse_none(self):
        result = parse_json_field(None)
        assert result is None


class TestIsMoneylineMarket:
    def test_explicit_moneyline_type(self):
        market = {'sportsMarketType': 'moneyline'}
        assert is_moneyline_market(market) is True
    
    def test_spread_market(self):
        market = {'sportsMarketType': 'spread'}
        assert is_moneyline_market(market) is False
    
    def test_moneyline_in_question(self):
        market = {'question': 'Moneyline: Chiefs vs Raiders'}
        assert is_moneyline_market(market) is True
    
    def test_vs_without_spread_or_total(self):
        market = {'question': 'Chiefs vs Raiders'}
        assert is_moneyline_market(market) is True
    
    def test_spread_in_question(self):
        market = {'question': 'Chiefs vs Raiders Spread -3.5'}
        assert is_moneyline_market(market) is False
    
    def test_total_in_question(self):
        market = {'question': 'Chiefs vs Raiders Total Over 45.5'}
        assert is_moneyline_market(market) is False

    def test_over_under_vs(self):
        market = {'question': 'Over vs Under'}
        assert is_moneyline_market(market) is False

    def test_over_under_outcomes(self):
        market = {'outcomes': ['Over', 'Under']}
        assert is_moneyline_market(market) is False


class TestDetermineWinner:
    def test_clear_winner(self):
        outcomes = ["Chiefs", "Raiders"]
        prices = ["0.98", "0.02"]
        assert determine_winner(outcomes, prices) == "Chiefs"
    
    def test_second_team_wins(self):
        outcomes = ["Chiefs", "Raiders"]
        prices = ["0.03", "0.97"]
        assert determine_winner(outcomes, prices) == "Raiders"
    
    def test_no_clear_winner(self):
        outcomes = ["Chiefs", "Raiders"]
        prices = ["0.55", "0.45"]
        assert determine_winner(outcomes, prices) == "Pending"
    
    def test_empty_prices(self):
        outcomes = ["Chiefs", "Raiders"]
        prices = []
        assert determine_winner(outcomes, prices) == "Pending"
    
    def test_empty_outcomes(self):
        outcomes = []
        prices = ["0.98", "0.02"]
        assert determine_winner(outcomes, prices) == "Pending"


class TestExtractTeams:
    def test_from_outcomes(self):
        outcomes = ["Kansas City Chiefs", "Las Vegas Raiders"]
        title = ""
        team_a, team_b = extract_teams(outcomes, title)
        assert team_a == "Kansas City Chiefs"
        assert team_b == "Las Vegas Raiders"
    
    def test_from_title(self):
        outcomes = None
        title = "Chiefs vs Raiders"
        team_a, team_b = extract_teams(outcomes, title)
        assert team_a == "Chiefs"
        assert team_b == "Raiders"
    
    def test_unknown_fallback(self):
        outcomes = None
        title = "Some random title"
        team_a, team_b = extract_teams(outcomes, title)
        assert team_a == "Unknown"
        assert team_b == "Unknown"
