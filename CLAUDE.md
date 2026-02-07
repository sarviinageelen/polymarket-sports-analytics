# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Polymarket Sports Analytics - tracks sports prediction market performance on Polymarket. Fetches on-chain trading data, calculates P&L metrics, and generates Excel leaderboards for NFL, NBA, CFB, and CBB markets.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/
pytest tests/test_markets.py -v  # Single test file

# Full pipeline
python update_markets.py && python update_trades.py && python update_picks.py

# update_trades.py options
python update_trades.py --resolved-only      # Only resolved markets
python update_trades.py --unresolved-only    # Only active markets
python update_trades.py --force-reprocess    # Ignore cache
python update_trades.py --verbose            # Detailed output
python update_trades.py --max-users N        # Limit users (testing)
python update_trades.py --sport nfl          # Filter by sport
python update_trades.py --pick-basis amount  # Classify picks by amount held
```

## Architecture

Three-stage CSV pipeline:

```
Gamma API (REST)           GraphQL Subgraphs (Goldsky)
      │                           │
      ▼                           ▼
update_markets.py ──► update_trades.py ──► update_picks.py
      │                    │                    │
      ▼                    ▼                    ▼
db_markets.csv       db_trades_*.csv      leaderboard_*.xlsx
```

**Stage 1 - update_markets.py**: Fetches moneyline markets from Polymarket Gamma API by series ID (NFL=10187, CFB=10210, NBA=10345, CBB=10470). Filters to moneyline only, determines winners (price > 0.95).

**Stage 2 - update_trades.py**: Queries user positions from Goldsky GraphQL subgraphs. Calculates realized/unrealized P&L, determines user picks and correctness. Uses cursor-based pagination with rate limiting (50 req/10 sec).

**Stage 3 - update_picks.py**: Interactive menu for sport/week selection. Generates Excel with conditional formatting (green=win, red=loss, yellow=pending), frozen panes, clickable Polymarket profile links, and consensus formulas.

## Key Filtering Logic

- **Late picks excluded**: Individual picks with price >= 0.95 are filtered out
- **Accuracy threshold**: Users need 70% accuracy with minimum 5 games to appear on leaderboard
- **Moneyline only**: Spreads, totals, and props are excluded

## Data Files

| File | Description |
|------|-------------|
| `db_markets.csv` | All moneyline markets (generated) |
| `db_trades_{sport}.csv` | Sport-specific trade data with P&L |
| `leaderboard_{sport}_weeks_{N}-{M}.xlsx` | Excel leaderboard output |

## Sport Configuration (in update_picks.py)

Each sport has season_start date and total_weeks configured. Week calculations are based on these values for time window filtering.

## API Rate Limits

- Gamma API: 500 events per paginated request
- GraphQL: 50 requests per 10 seconds with exponential backoff
- Batch processing: 500 users per GraphQL query
