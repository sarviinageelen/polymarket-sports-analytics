# Polymarket Sports Analytics

Analytics for tracking and analyzing sports prediction market performance on [Polymarket](https://polymarket.com). This tool aggregates on-chain trading data, calculates performance metrics, and generates interactive leaderboards and analysis reports to identify top forecasters across NFL, NBA, CFB, and CBB markets.

## Overview

This project interfaces with Polymarket's Gamma API and GraphQL subgraphs to:

- **Fetch Market Data**: Retrieve moneyline prediction markets across multiple sports leagues
- **Calculate Performance**: Process on-chain position data to compute realized and unrealized P&L
- **Rank Forecasters**: Apply sophisticated filtering and generate performance leaderboards
- **Analyze Picks**: Generate flat-table analysis with per-pick details and consensus metrics
- **Export Analytics**: Produce Excel workbooks with conditional formatting and interactive features

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Polymarket Sports Analytics                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │   Gamma API  │     │ PNL Subgraph │     │  Orderbook   │                 │
│  │   (Events)   │     │  (Goldsky)   │     │  Subgraph    │                 │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                 │
│         │                    │                    │                         │
│         ▼                    ▼                    ▼                         │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │                     Data Pipeline                             │          │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐              │          │
│  │  │  Markets   │─▶│   Trades   │─▶│   Picks/   │              │          │
│  │  │  Fetcher   │  │ Calculator │  │  Analyze   │              │          │
│  │  └────────────┘  └────────────┘  └────────────┘              │          │
│  └──────────────────────────────────────────────────────────────┘          │
│                              │                                              │
│                              ▼                                              │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │                     Output Layer                              │          │
│  │  • CSV databases (markets, trades by sport)                  │          │
│  │  • Excel leaderboards with conditional formatting            │          │
│  │  • Excel flat-table analysis with per-pick details           │          │
│  │  • Clickable Polymarket profile links                        │          │
│  └──────────────────────────────────────────────────────────────┘          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features

### Data Collection
- **Multi-Sport Support**: NFL, NBA, CFB (College Football), CBB (College Basketball)
- **Real-Time Market Data**: Fetches active and resolved markets from Polymarket Gamma API
- **On-Chain Position Tracking**: Queries GraphQL subgraphs for user positions and trade history
- **Incremental Processing**: Smart caching to avoid re-processing unchanged data

### Analytics Engine
- **P&L Calculations**: Realized, unrealized, and total profit/loss per position
- **Accuracy Tracking**: Historical and rolling (last 10 games) performance metrics
- **Streak Analysis**: Current, max winning, and max losing streak calculations
- **Timing Analysis**: Filters late entries (>95% probability) to ensure quality signals

### Leaderboard Generation
- **Week-by-Week Breakdown**: Track performance across customizable date ranges
- **Pick-by-Pick Grid**: Visual matrix showing each forecaster's picks for every game
- **Conditional Formatting**: Green (correct), red (incorrect), yellow (pending)
- **Interactive Links**: Clickable wallet addresses linking to Polymarket profiles
- **Consensus Calculations**: Automatic formulas showing crowd sentiment percentages

## Installation

### Prerequisites
- Python 3.8+
- pip package manager

### Setup

```bash
# Clone the repository
git clone https://github.com/sarviinageelen/polymarket-sports-analytics.git
cd polymarket-sports-analytics

# Install dependencies
pip install -r requirements.txt
```

### Dependencies
```
requests>=2.28.0
pandas>=2.0.0
numpy>=1.24.0
scipy>=1.10.0
python-dateutil>=2.8.0
openpyxl>=3.1.0
pytest>=7.0.0
```

## Usage

### Quick Start

Run the scripts in sequence to generate leaderboards and analysis:

```bash
# Step 1: Fetch market data from Polymarket
python update_markets.py

# Step 2: Calculate P&L for all traders
python update_trades.py

# Step 3: Generate leaderboards (interactive prompts)
python update_picks.py

# Step 4 (Optional): Generate flat-table analysis
python update_analyze.py
```
Follow the prompts to select sport and time period.

### Command Line Options

#### update_markets.py
```bash
python update_markets.py
```
Fetches all moneyline markets for configured sports. No arguments required.

#### update_trades.py
```bash
python update_trades.py [OPTIONS]

Options:
  --resolved-only      Only process resolved markets
  --unresolved-only    Only process unresolved (active) markets
  --force-reprocess    Reprocess all markets (ignore cache)
  --verbose            Show detailed per-user output
  --max-users N        Limit users per market (for testing)
  --sport {all,nfl,nba,cfb,cbb}  Filter by sport
  --pick-basis {total_bought,amount}  Classify pick by total bought or current amount
```

#### update_picks.py
```bash
python update_picks.py
```
Runs an interactive menu to choose sport, then season, then time window.
Time window options are postseason-inclusive and include explicit date ranges
(latest week, previous week, last 5 weeks, full season).
Generates a leaderboard-style Excel with a pick-by-pick grid.
Terminal output is concise and milestone-based (no terminal preview table).

CLI mode:
```bash
python update_picks.py --sport NFL --season-id 2025 --weeks 5
python update_picks.py --sport NBA --season-id 2025-26 --last5
python update_picks.py --sport all --season
```

#### update_analyze.py

```bash
python update_analyze.py
```
Runs an interactive menu to choose sport, then season, then time window.
Time window options are postseason-inclusive and include explicit date ranges.
Generates a flat-table Excel analysis with one row per user per game,
including entry prices, consensus percentages, and pick results.
Terminal output is concise and milestone-based (no terminal preview table).

CLI mode:
```bash
python update_analyze.py --sport CFB --season-id 2025 --weeks 3-5
python update_analyze.py --sport all --season
```

### Example Workflows

**Generate NBA leaderboard for the last 5 weeks:**
Run `python update_picks.py`, select NBA, select season, then choose "Last 5 weeks".

**Refresh only active markets:**
```bash
python update_trades.py --unresolved-only
```

**Full pipeline refresh:**
```bash
python update_markets.py && python update_trades.py --force-reprocess && python update_picks.py
```

## API Integration

### Polymarket Gamma API

The Gamma API provides event and market metadata:

```
Endpoint: https://gamma-api.polymarket.com/events
```

**Series IDs:**
| ID | Sport |
|----|-------|
| 10187 | NFL (National Football League) |
| 10210 | CFB (College Football - NCAA FBS) |
| 10345 | NBA (National Basketball Association) |
| 10470 | CBB (College Basketball - Men's) |

### GraphQL Subgraphs

Position and P&L data is queried via Goldsky-hosted subgraphs:

**PNL Subgraph:**
```
https://api.goldsky.com/api/public/.../subgraphs/polymarket-pnl/prod/gn
```

**Orderbook Subgraph:**
```
https://api.goldsky.com/api/public/.../subgraphs/polymarket-orderbook-resync/prod/gn
```

### Rate Limiting

The system implements intelligent rate limiting:
- **Gamma API**: 500 events per request (paginated)
- **GraphQL**: 50 requests per 10 seconds with exponential backoff
- **Batch Processing**: 500 users per query batch

## Output Files

### Database Files (CSV)
| File | Description |
|------|-------------|
| `db_markets.csv` | All fetched moneyline markets |
| `db_trades_nfl.csv` | NFL trade records with P&L |
| `db_trades_nba.csv` | NBA trade records with P&L |
| `db_trades_cfb.csv` | CFB trade records with P&L |
| `db_trades_cbb.csv` | CBB trade records with P&L |

### Leaderboard Files (Excel)

| File | Description |
|------|-------------|
| `leaderboard_{sport}_weeks_{N}-{M}.xlsx` | Performance leaderboard for specified weeks |
| `leaderboard_{sport}_season_{year}.xlsx` | Full season leaderboard |

### Analysis Files (Excel)

| File | Description |
|------|-------------|
| `analysis_{sport}_weeks_{N}-{M}.xlsx` | Flat-table analysis for specified weeks |
| `analysis_{sport}_season_{year}.xlsx` | Full season flat-table analysis |

### Excel Leaderboard Structure

The generated Excel file contains:

**Header Rows (1-6):**
- Row 1: Team A consensus % (formula-based)
- Row 2: Team A name
- Row 3: Team B consensus % (formula-based)
- Row 4: Team B name
- Row 5: Game date
- Row 6: Column headers

**Columns:**
| Column | Description |
|--------|-------------|
| rank | Overall ranking |
| user_address | Wallet address (clickable link to Polymarket profile) |
| games | Total games predicted |
| wins | Correct predictions |
| losses | Incorrect predictions |
| win_pct | Accuracy percentage |
| win_streak | Current winning streak |
| loss_streak | Current losing streak |
| last_10 | Correct in last 10 games |
| roi_pct | ROI percentage (total_pnl / total_bought) |
| total_pnl | Total profit/loss |
| contrarian_win_pct | Win % when picking against majority |
| contrarian_games | Number of contrarian picks |
| [Game columns] | Pick for each game (team name) |

### Excel Analysis Structure (Flat Table)

The analysis Excel file (`update_analyze.py`) contains a flat table with one row per user per game:

| Column | Description |
|--------|-------------|
| User | Wallet address (clickable link to Polymarket profile) |
| Games | Total games predicted by user |
| Wins | Correct predictions |
| Losses | Incorrect predictions |
| Win % | Accuracy percentage |
| Total PnL | Total profit/loss |
| ROI % | ROI percentage (total_pnl / total_bought) |
| Streak | Current winning streak |
| Last 10 | Correct in last 10 games |
| Game | Match title (Team A vs Team B) |
| Game Date | Date of the game |
| Pick | Team the user picked |
| Result | won/lost/pending |
| Price | Entry price when pick was made |
| Pick % | Percentage of users who made the same pick |
| Majority Pick | Team picked by the majority |
| Majority? | Whether the pick matches the majority |
| Contrarian? | Whether the pick fades the majority |
| Winner | Winner (if resolved) |

The analysis workbook also includes a **markets** sheet with per-game summaries
(consensus percentages, majority pick, winner, and winner pick rate).

## Filtering Logic

### Quality Filters
1. **Late Entry Exclusion**: Individual picks with price >= 0.95 are excluded
2. **Accuracy Threshold**: Users with <70% accuracy (minimum 5 games) are filtered
3. **Minimum Activity**: Configurable minimum games threshold

### Why These Filters?

- **Late entries** at >95% probability offer minimal signal and often represent near-certainty outcomes
- **Accuracy filtering** surfaces consistently successful forecasters, not lucky streaks
- **Minimum games** ensures statistical significance

Filters can be overridden in CLI mode via:
`--late-pick-threshold`, `--min-win-pct`, `--min-games-win-pct`, `--min-games`, `--min-wins`.

## Testing

```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v

# Run specific test file
pytest tests/test_picks.py -v
```

## Project Structure

```
polymarket-sports-analytics/
├── update_markets.py      # Gamma API market fetcher
├── update_trades.py       # GraphQL P&L calculator
├── update_picks.py        # Leaderboard generator
├── update_analyze.py      # Flat-table analysis generator
├── requirements.txt       # Python dependencies
├── README.md              # This file
├── CLAUDE.md              # AI assistant guidance
├── tests/                 # Test suite
│   ├── __init__.py
│   ├── test_markets.py
│   ├── test_trades.py
│   └── test_picks.py
└── logs/                  # Runtime logs
    ├── markets.log
    ├── trades.log
    └── picks.log
```

## Technical Highlights

- **Cursor-Based Pagination**: Efficiently handles large datasets from GraphQL endpoints
- **Concurrent Processing**: ThreadPoolExecutor for parallel API requests
- **Incremental Updates**: Smart diffing to only process new/changed markets
- **Memory Efficient**: Streaming CSV writes for large trade datasets
- **Excel Formula Integration**: Dynamic consensus calculations update automatically

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.

## Disclaimer

This tool is for informational and analytical purposes only. It is not financial advice. Prediction markets involve risk, and past performance does not guarantee future results.

---

Built with data from [Polymarket](https://polymarket.com) | Powered by [Goldsky](https://goldsky.com) subgraphs
