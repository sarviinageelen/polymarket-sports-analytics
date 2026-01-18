"""
Polymarket PNL Calculator
=========================

Calculates Profit and Loss (PNL) for all users who traded in
sports markets by querying Polymarket's GraphQL subgraphs.

Data Sources:
    - PNL Subgraph: User positions, realized PNL, average prices
    - Orderbook Subgraph: Current market prices from recent trades

Input:
    - db_markets.csv: List of markets (from update_markets.py)

Output:
    - db_trades.csv: Trade records with PNL calculations:
        - User position data (holdings, avg price, total bought)
        - Realized PNL (from closed positions)
        - Unrealized PNL (current value - cost basis)
        - Total PNL (realized + unrealized)
        - user_pick: Which team the user bet on
        - is_correct_pick: Whether the pick matched the winner

Usage:
    python update_trades.py                  # Process ALL markets (default)
    python update_trades.py --resolved-only  # Only resolved markets
    python update_trades.py --unresolved-only # Only unresolved markets
    python update_trades.py --verbose        # Detailed output per user
    python update_trades.py --force-reprocess # Reprocess all markets
    python update_trades.py --max-users 10   # Limit users per market (testing)

Architecture:
    1. Load markets from db_markets.csv (filtered by --resolved-only/--unresolved-only)
    2. For each market:
       a. Fetch all users with positions via cursor-based pagination
       b. Batch query user positions (500 users per batch)
       c. Fetch current prices once per market
       d. Calculate PNL for each user
    3. Write results to CSV in batches (for performance)

API Rate Limiting:
    - Goldsky endpoints: 50 requests per 10 seconds
    - Implements exponential backoff on 429 errors
    - Uses rate delay between requests (0.04s default)

Caching:
    - Condition data cached to avoid duplicate API calls
    - Supports incremental processing (skips already-processed markets)
"""

import argparse
import requests
from typing import Dict, Optional, List
import csv
import os
import time
import logging
import pandas as pd
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------

def setup_logging():
    """Configure logging to both console and file."""
    os.makedirs("logs", exist_ok=True)
    
    log_logger = logging.getLogger(__name__)
    log_logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers on reimport
    if not log_logger.handlers:
        file_handler = logging.FileHandler("logs/trades.log", encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        log_logger.addHandler(file_handler)
    
    return log_logger

logger = setup_logging()

# ------------------------------------------------------------------------------
# Configuration & Constants
# ------------------------------------------------------------------------------

# Polymarket Goldsky GraphQL Subgraph Endpoints
# - PNL Subgraph: User positions, realized PNL, condition data
# - Orderbook Subgraph: Recent trades for current price discovery
PNL_SUBGRAPH_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/pnl-subgraph/0.0.14/gn"
ORDERBOOK_SUBGRAPH_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

# USDC token has 6 decimal places
# All amounts from subgraph are in base units (divide by 10^6 to get USDC)
COLLATERAL_SCALE = 1_000_000

# Rate Limiting Configuration
# Goldsky public endpoints allow ~50 requests per 10 seconds
# Using 25 req/s provides headroom while maintaining good throughput
RATE_LIMIT_REQUESTS_PER_SECOND = 25.0
RATE_LIMIT_DELAY = 1.0 / RATE_LIMIT_REQUESTS_PER_SECOND  # ~0.04 seconds between requests

# Retry configuration for transient failures
MAX_RETRIES = 3        # Maximum attempts before giving up
RETRY_DELAY = 2        # Base delay in seconds (uses exponential backoff)

# In-memory cache for condition data to avoid redundant API calls
# Key: condition_id (lowercase), Value: condition entity from subgraph
_condition_cache = {}


# ============================================================================
# PROGRESS DISPLAY HELPERS
# ============================================================================

def progress_bar(current: int, total: int, width: int = 40) -> str:
    """Generate a progress bar string."""
    if total == 0:
        return f"[{'=' * width}] 0/0"

    pct = current / total
    filled = int(width * pct)
    bar = '=' * filled + '-' * (width - filled)
    return f"[{bar}] {current}/{total} ({pct*100:.1f}%)"


def print_progress(current: int, total: int, prefix: str = "", suffix: str = "") -> None:
    """Print progress bar that updates in place."""
    bar = progress_bar(current, total)
    end_char = '\n' if current >= total else '\r'
    line = f"{prefix}{bar} {suffix}".ljust(80)
    print(line, end=end_char, flush=True)


def format_time(seconds: float) -> str:
    """
    Format seconds into human-readable time.

    Args:
        seconds: Time in seconds

    Returns:
        Formatted string like "1h 23m" or "45 sec"
    """
    if seconds < 60:
        return f"{int(seconds)} sec"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"


def calculate_eta(elapsed: float, current: int, total: int) -> str:
    """
    Calculate estimated time remaining.

    Args:
        elapsed: Elapsed time in seconds
        current: Current progress (items completed)
        total: Total items to process

    Returns:
        Formatted ETA string
    """
    if current == 0:
        return "calculating..."

    avg_time_per_item = elapsed / current
    remaining_items = total - current
    eta_seconds = avg_time_per_item * remaining_items

    return format_time(eta_seconds)


def query_subgraph(url: str, query: str, variables: Optional[Dict] = None) -> Dict:
    """
    Execute a GraphQL query against a Goldsky subgraph endpoint.
    
    Implements rate limiting (RATE_LIMIT_DELAY between requests) and
    exponential backoff retry logic for transient failures and 429 errors.
    
    Args:
        url: GraphQL endpoint URL (PNL_SUBGRAPH_URL or ORDERBOOK_SUBGRAPH_URL)
        query: GraphQL query string with optional $variables
        variables: Optional dict of variable values for the query
    
    Returns:
        The 'data' field from the JSON response (parsed GraphQL result)
    
    Raises:
        Exception: If query fails after MAX_RETRIES attempts, or if
                   response contains 'errors' key or missing 'data' key
    
    Note:
        Rate limiting delay is applied BEFORE each request to avoid
        overwhelming the public Goldsky endpoints.
    """
    payload = {
        "query": query,
        "variables": variables or {}
    }

    for attempt in range(MAX_RETRIES):
        try:
            # Rate limiting: wait before making request
            time.sleep(RATE_LIMIT_DELAY)

            response = requests.post(url, json=payload)

            # Handle rate limiting (429 Too Many Requests)
            if response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)  # Exponential backoff
                    print(f"  Warning: Rate limit hit (429), waiting {wait_time}s before retry {attempt + 1}/{MAX_RETRIES}...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"Rate limit exceeded after {MAX_RETRIES} retries")

            response.raise_for_status()

            result = response.json()
            if "errors" in result:
                raise Exception(f"GraphQL query failed: {result['errors']}")
            
            if "data" not in result:
                raise Exception(f"Unexpected response format: no 'data' key in response")

            return result["data"]

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"  Warning: Request failed: {e}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                raise Exception(f"Request failed after {MAX_RETRIES} retries: {e}")

    raise Exception("Query failed after all retries")


def get_condition(condition_id: str, use_cache: bool = True) -> Optional[Dict]:
    """
    Fetch condition metadata including position IDs for YES/NO outcomes.
    
    A condition represents a market and contains the token IDs needed
    to query user positions. Results are cached in _condition_cache to
    avoid redundant API calls when processing multiple users.
    
    Args:
        condition_id: Market's condition ID (hex string from Polymarket)
        use_cache: If True, return cached result if available
    
    Returns:
        Condition entity dict with fields:
        - id: Condition ID (lowercase)
        - positionIds: List of 2 token IDs [YES_token, NO_token]
        - payoutNumerators: Payout ratios for resolution
        - payoutDenominator: Denominator for payout calculation
        
        Returns None if condition not found.
    
    Note:
        Cache key is lowercased condition_id for case-insensitive matching.
    """
    cache_key = condition_id.lower()
    
    # Return cached result if available
    if use_cache and cache_key in _condition_cache:
        return _condition_cache[cache_key]
    
    query = """
    query GetCondition($conditionId: ID!) {
        condition(id: $conditionId) {
            id
            positionIds
            payoutNumerators
            payoutDenominator
        }
    }
    """

    variables = {"conditionId": cache_key}
    data = query_subgraph(PNL_SUBGRAPH_URL, query, variables)
    
    result = data.get("condition")
    
    # Cache the result
    if result:
        _condition_cache[cache_key] = result
    
    return result


def _fetch_single_price(token_id: str) -> tuple:
    """
    Fetch current market price for a single token from orderbook.
    
    Internal helper for parallel price fetching. Queries the orderbook
    subgraph for the most recent trade (buy or sell) and calculates
    price from the filled amounts.
    
    Args:
        token_id: Token ID to fetch price for (YES or NO outcome token)
    
    Returns:
        Tuple of (token_id, price) where price is in scaled units
        (divide by COLLATERAL_SCALE for decimal). Returns (token_id, None)
        if no recent orders found or on error.
    
    Note:
        - Compares timestamps of most recent buy vs sell order
        - Price calculation handles both maker/taker directions
        - Includes division-by-zero protection
    """
    query = """
    query GetRecentOrders($makerAssetId: String!, $takerAssetId: String!) {
        buyOrders: orderFilledEvents(
            where: { makerAssetId: $makerAssetId }
            orderBy: timestamp
            orderDirection: desc
            first: 1
        ) {
            makerAssetId
            takerAssetId
            makerAmountFilled
            takerAmountFilled
            timestamp
        }
        sellOrders: orderFilledEvents(
            where: { takerAssetId: $makerAssetId }
            orderBy: timestamp
            orderDirection: desc
            first: 1
        ) {
            makerAssetId
            takerAssetId
            makerAmountFilled
            takerAmountFilled
            timestamp
        }
    }
    """

    variables = {"makerAssetId": token_id, "takerAssetId": token_id}

    try:
        data = query_subgraph(ORDERBOOK_SUBGRAPH_URL, query, variables)

        buy_orders = data.get("buyOrders", [])
        sell_orders = data.get("sellOrders", [])

        recent_order = None
        if buy_orders and sell_orders:
            buy_timestamp = int(buy_orders[0]["timestamp"])
            sell_timestamp = int(sell_orders[0]["timestamp"])
            recent_order = buy_orders[0] if buy_timestamp >= sell_timestamp else sell_orders[0]
        elif buy_orders:
            recent_order = buy_orders[0]
        elif sell_orders:
            recent_order = sell_orders[0]

        if recent_order:
            maker_amount = float(recent_order["makerAmountFilled"])
            taker_amount = float(recent_order["takerAmountFilled"])

            # Prevent division by zero
            if maker_amount > 0 and taker_amount > 0:
                if recent_order["makerAssetId"] == token_id:
                    price = (taker_amount / maker_amount) * COLLATERAL_SCALE
                else:
                    price = (maker_amount / taker_amount) * COLLATERAL_SCALE

                return (token_id, price)

    except Exception as e:
        print(f"Warning: Could not fetch price for token {token_id}: {e}")

    return (token_id, None)


def get_current_prices(token_ids: List[str]) -> Dict[str, float]:
    """
    Fetch current market prices for multiple tokens in parallel.
    
    Uses ThreadPoolExecutor to fetch prices concurrently, significantly
    speeding up price discovery when querying both YES and NO tokens.
    
    Args:
        token_ids: List of token IDs to get prices for
    
    Returns:
        Dict mapping token_id -> current price (in scaled units).
        Tokens with no recent trades are excluded from the result.
    
    Note:
        Worker count is capped at 10 to avoid overwhelming the API.
        Prices are derived from the most recent orderbook fills.
    """
    prices = {}

    # Fetch prices in parallel using ThreadPoolExecutor (cap workers to avoid overwhelming API)
    max_workers = min(len(token_ids), 10)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_single_price, tid): tid for tid in token_ids}
        
        for future in as_completed(futures):
            token_id, price = future.result()
            if price is not None:
                prices[token_id] = price

    return prices


def calculate_unrealized_pnl(amount: float, avg_price: float, current_price: float) -> float:
    """
    Calculate unrealized profit/loss for an open position.
    
    Unrealized PNL represents the gain/loss that would be realized
    if the position were closed at the current market price.
    
    Args:
        amount: Current token holdings (in base units)
        avg_price: Average entry price (scaled by COLLATERAL_SCALE)
        current_price: Current market price (scaled by COLLATERAL_SCALE)
    
    Returns:
        Unrealized PNL in USDC (divide by COLLATERAL_SCALE for dollars)
    
    Formula:
        PNL = amount * (current_price - avg_price) / COLLATERAL_SCALE
    """
    if amount == 0:
        return 0.0

    # PNL = amount * (current_price - avg_price) / COLLATERAL_SCALE
    return amount * (current_price - avg_price) / COLLATERAL_SCALE


def format_price(price: float) -> str:
    """Format price from base units to dollar amount."""
    return f"${price / COLLATERAL_SCALE:.6f}"


def format_usdc(amount: float) -> str:
    """Format USDC amount."""
    return f"${amount / COLLATERAL_SCALE:.2f}"


# CSV column definitions
CSV_FIELDNAMES = [
    "sport", "condition_id", "match_title",
    "game_start_time", "is_resolved", "winning_outcome", "user_address",
    "user_pick", "is_correct_pick",
    "yes_current_holdings", "yes_avg_price", "yes_total_bought", "yes_current_price",
    "yes_realized_pnl", "yes_unrealized_pnl", "yes_total_pnl",
    "no_current_holdings", "no_avg_price", "no_total_bought", "no_current_price",
    "no_realized_pnl", "no_unrealized_pnl", "no_total_pnl",
    "total_realized_pnl", "total_unrealized_pnl", "total_pnl"
]


def format_number(value, decimals=2):
    """Format numbers with comma separators (e.g., '1,234.56')"""
    if value is None:
        return ""
    rounded = round(value, decimals)
    return f"{rounded:,.{decimals}f}"


def determine_user_pick(yes_position, no_position, match_title):
    """
    Determine which team/outcome the user bet on based on position sizes.
    
    Compares totalBought amounts for YES vs NO positions to identify
    the user's primary betting direction, then maps it to a team name.
    
    Args:
        yes_position: User's YES position dict (or None)
        no_position: User's NO position dict (or None)
        match_title: Match title in format "Team A vs Team B"
    
    Returns:
        Team name if match_title can be parsed, otherwise:
        - "YES" or "NO" if positions exist but title parsing fails
        - "NONE" if user has no positions
    
    Logic:
        - YES outcome = Team A (first team in title)
        - NO outcome = Team B (second team in title)
        - Larger totalBought determines the pick
    """
    yes_bought = float(yes_position["totalBought"]) if yes_position else 0.0
    no_bought = float(no_position["totalBought"]) if no_position else 0.0

    # No positions at all
    if yes_bought == 0 and no_bought == 0:
        return "NONE"

    # Parse team names from match_title (format: "Team A vs Team B" or "Team A vs. Team B")
    team_a = ""
    team_b = ""
    if match_title and " vs" in match_title.lower():
        parts = match_title.replace(" vs. ", " vs ").split(" vs ")
        if len(parts) == 2:
            team_a = parts[0].strip()
            team_b = parts[1].strip()

    # Determine which side they bet
    # Equal bets on both sides: default to YES (Team A) as tiebreaker
    if yes_bought >= no_bought:
        return team_a if team_a else "YES"
    else:
        return team_b if team_b else "NO"


def calculate_is_correct_pick(user_pick, winning_outcome):
    """
    Check if the user's pick matches the market's winning outcome.
    
    Args:
        user_pick: User's predicted team/outcome (from determine_user_pick)
        winning_outcome: Actual winning team/outcome from market resolution
    
    Returns:
        "TRUE" - User picked correctly
        "FALSE" - User picked incorrectly
        "" (empty) - No pick, or market not yet resolved
    
    Note:
        Comparison is exact string match, so team names must be
        consistently formatted between user_pick and winning_outcome.
    """
    if not user_pick or user_pick == "NONE":
        return ""
    if not winning_outcome or winning_outcome == "Pending":
        return ""  # Not yet resolved
    return "TRUE" if user_pick == winning_outcome else "FALSE"


def build_csv_row(
    condition_id: str,
    user_address: str,
    results: Dict,
    market_metadata: Optional[Dict] = None
) -> Dict:
    """
    Build a complete CSV row dict from PNL calculation results.
    
    Transforms raw position data and PNL calculations into a formatted
    row ready for CSV output. All monetary values are formatted with
    comma separators and appropriate decimal places.
    
    Args:
        condition_id: Market's condition ID
        user_address: User's wallet address
        results: Dict with YES/NO position data and PNL calculations
        market_metadata: Optional dict with sport, match_title, game_start_time,
                        is_resolved, and winning_outcome
    
    Returns:
        Dict with keys matching CSV_FIELDNAMES, ready for DictWriter.
        All values are strings formatted for human readability.
    """
    yes_pos = results["YES"]["position"]
    no_pos = results["NO"]["position"]
    metadata = market_metadata or {}
    
    # Determine user's pick and if it was correct
    match_title = metadata.get("match_title", "")
    winning_outcome = metadata.get("winning_outcome", "")
    user_pick = determine_user_pick(yes_pos, no_pos, match_title)
    is_correct = calculate_is_correct_pick(user_pick, winning_outcome)

    return {
        "sport": metadata.get("sport", ""),
        "condition_id": condition_id,
        "match_title": match_title,
        "game_start_time": metadata.get("game_start_time", ""),
        "is_resolved": metadata.get("is_resolved", ""),
        "winning_outcome": winning_outcome,
        "user_address": user_address,
        "user_pick": user_pick,
        "is_correct_pick": is_correct,
        "yes_current_holdings": format_number(float(yes_pos["amount"]) / COLLATERAL_SCALE if yes_pos is not None else 0.0),
        "yes_avg_price": format_number(float(yes_pos["avgPrice"]) / COLLATERAL_SCALE if yes_pos is not None else 0.0),
        "yes_total_bought": format_number(float(yes_pos["totalBought"]) / COLLATERAL_SCALE if yes_pos is not None else 0.0),
        "yes_current_price": format_number(results["YES"]["current_price"] / COLLATERAL_SCALE if results["YES"]["current_price"] is not None else None),
        "yes_realized_pnl": format_number(results["YES"]["realized_pnl"] / COLLATERAL_SCALE),
        "yes_unrealized_pnl": format_number(results["YES"]["unrealized_pnl"] / COLLATERAL_SCALE),
        "yes_total_pnl": format_number(results["YES"]["total_pnl"] / COLLATERAL_SCALE),
        "no_current_holdings": format_number(float(no_pos["amount"]) / COLLATERAL_SCALE if no_pos is not None else 0.0),
        "no_avg_price": format_number(float(no_pos["avgPrice"]) / COLLATERAL_SCALE if no_pos is not None else 0.0),
        "no_total_bought": format_number(float(no_pos["totalBought"]) / COLLATERAL_SCALE if no_pos is not None else 0.0),
        "no_current_price": format_number(results["NO"]["current_price"] / COLLATERAL_SCALE if results["NO"]["current_price"] is not None else None),
        "no_realized_pnl": format_number(results["NO"]["realized_pnl"] / COLLATERAL_SCALE),
        "no_unrealized_pnl": format_number(results["NO"]["unrealized_pnl"] / COLLATERAL_SCALE),
        "no_total_pnl": format_number(results["NO"]["total_pnl"] / COLLATERAL_SCALE),
        "total_realized_pnl": format_number((results["YES"]["realized_pnl"] + results["NO"]["realized_pnl"]) / COLLATERAL_SCALE),
        "total_unrealized_pnl": format_number((results["YES"]["unrealized_pnl"] + results["NO"]["unrealized_pnl"]) / COLLATERAL_SCALE),
        "total_pnl": format_number((results["YES"]["total_pnl"] + results["NO"]["total_pnl"]) / COLLATERAL_SCALE)
    }


def write_rows_to_csv(rows: List[Dict], csv_file: Optional[str] = None):
    """
    Write multiple rows to CSV in a single file operation.
    
    Batched writes are significantly faster than per-row writes,
    especially for large datasets. Creates file with header if
    it doesn't exist, appends otherwise.
    
    Args:
        rows: List of dicts with keys matching CSV_FIELDNAMES
        csv_file: Output file path (defaults to db_trades.csv)
    
    Note:
        Uses DictWriter in append mode with automatic header handling.
    """
    if csv_file is None:
        csv_file = "db_trades.csv"
    
    if not rows:
        return
    
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, mode='a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerows(rows)


def write_to_csv(
    condition_id: str,
    user_address: str,
    results: Dict,
    csv_file: Optional[str] = None,
    verbose: bool = True,
    market_metadata: Optional[Dict] = None
):
    """
    Write a single user's PNL result to CSV file.
    
    Legacy function for individual writes. For batch operations,
    use build_csv_row() + write_rows_to_csv() instead for better performance.
    
    Args:
        condition_id: Market's condition ID
        user_address: User's wallet address
        results: PNL calculation results dict
        csv_file: Output file path (defaults to db_trades.csv)
        verbose: If True, prints confirmation message
        market_metadata: Optional market info dict
    """
    if csv_file is None:
        csv_file = "db_trades.csv"
    
    row = build_csv_row(condition_id, user_address, results, market_metadata)
    write_rows_to_csv([row], csv_file)
    
    if verbose:
        print(f"\nResults appended to {csv_file}")


def get_all_users_for_market(condition_id: str) -> List[str]:
    """
    Discover all unique wallet addresses with positions in a market.
    
    Uses cursor-based pagination (id_gt) instead of skip/offset to
    bypass The Graph's 5000 skip limit. Essential for markets with
    many traders.
    
    Args:
        condition_id: Market's condition ID
    
    Returns:
        Sorted list of unique user addresses (lowercase)
    
    Raises:
        ValueError: If condition not found in subgraph
    
    Note:
        - Queries UserPositions for both YES and NO token IDs
        - Uses id_gt cursor pattern for unlimited pagination
        - Batch size is 1000 per query
    """
    # First get the condition to find position IDs
    condition = get_condition(condition_id)
    if not condition:
        raise ValueError(f"Condition {condition_id} not found")

    position_ids = condition["positionIds"]

    # Query all UserPositions using cursor-based pagination (id_gt instead of skip)
    # This bypasses The Graph's skip limit of 5000
    query = """
    query GetAllUserPositions($tokenIds: [String!]!, $lastId: String!) {
        userPositions(
            where: { tokenId_in: $tokenIds, id_gt: $lastId }
            first: 1000
            orderBy: id
            orderDirection: asc
        ) {
            id
            user
            tokenId
        }
    }
    """

    users = set()
    last_id = ""  # Start with empty string to get all records
    total_positions = 0
    batch_size = 1000

    while True:
        variables = {"tokenIds": position_ids, "lastId": last_id}
        data = query_subgraph(PNL_SUBGRAPH_URL, query, variables)

        positions = data.get("userPositions", [])

        if not positions:
            break

        total_positions += len(positions)

        # Extract user addresses
        for position in positions:
            users.add(position["user"])

        # Update cursor to last id for next batch
        last_id = positions[-1]["id"]

        # If we got fewer results than batch_size, we've reached the end
        if len(positions) < batch_size:
            break

    return sorted(list(users))


def get_batch_user_positions(user_addresses: List[str], position_ids: List[str]) -> Dict[str, Dict]:
    """
    Fetch positions for multiple users in a single GraphQL query.
    
    More efficient than individual queries when processing many users.
    Queries both YES and NO positions for all users at once.
    
    Args:
        user_addresses: List of wallet addresses to fetch
        position_ids: Token IDs for YES and NO outcomes [yes_id, no_id]
    
    Returns:
        Dict mapping lowercase user address -> {"YES": position, "NO": position}.
        Position is None if user has no holdings for that outcome.
        Users with no positions still appear with {"YES": None, "NO": None}.
    
    Note:
        - Limited to 1000 positions per query (500 users × 2 outcomes)
        - Caller should batch if processing >500 users
    """
    # Create UserPosition IDs for all users
    user_position_ids = []
    for user_addr in user_addresses:
        for token_id in position_ids:
            user_position_ids.append(f"{user_addr.lower()}-{token_id}")

    query = """
    query GetBatchUserPositions($ids: [ID!]!) {
        userPositions(where: { id_in: $ids }, first: 1000) {
            id
            user
            tokenId
            amount
            avgPrice
            realizedPnl
            totalBought
        }
    }
    """

    variables = {"ids": user_position_ids}
    data = query_subgraph(PNL_SUBGRAPH_URL, query, variables)

    # Organize by user address (normalize to lowercase for consistent matching)
    user_data = {}
    for position in data.get("userPositions", []):
        user = position["user"].lower()  # Normalize to lowercase
        token_id = position["tokenId"]

        if user not in user_data:
            user_data[user] = {"YES": None, "NO": None}

        # Determine if YES or NO based on token_id
        if token_id == position_ids[0]:
            user_data[user]["YES"] = position
        else:
            user_data[user]["NO"] = position

    # Fill in missing users with empty positions
    for user in user_addresses:
        if user.lower() not in user_data:
            user_data[user.lower()] = {"YES": None, "NO": None}

    return user_data


def calculate_pnl_for_all_users(
    condition_id: str,
    output_csv: Optional[str] = None,
    verbose: bool = False,
    max_users: Optional[int] = None,
    batch_size: int = 500,  # Increased from 100 for faster processing
    market_metadata: Optional[Dict] = None
):
    """
    Calculate PNL for every trader in a resolved market.
    
    High-throughput batch processing that minimizes API calls:
    1. Discover all users who traded in the market
    2. Fetch current prices ONCE (same for all users)
    3. Batch-fetch user positions (500 users per query)
    4. Calculate PNL in-memory, batch-write to CSV
    
    Args:
        condition_id: Market's condition ID
        output_csv: CSV file path (defaults to db_trades.csv)
        verbose: If True, prints per-user details; if False, shows progress bar
        max_users: Optional limit for testing (None = all users)
        batch_size: Users per batch query (default 500)
        market_metadata: Dict with sport, match_title, game_start_time, etc.
    
    Returns:
        Summary dict with total_users, successful, failed, elapsed_time
    
    Performance:
        - Processes ~100+ users/second with batching
        - Single price fetch shared across all users
        - Single CSV write at end (not per-user)
    """
    if output_csv is None:
        output_csv = "db_trades.csv"
    
    # Get all users
    all_users = get_all_users_for_market(condition_id)

    # Limit users if requested
    if max_users:
        users = all_users[:max_users]
        if verbose:
            print(f"Found {len(all_users)} users total, processing first {len(users)}\n")
    else:
        users = all_users

    total_users = len(users)

    # Get condition to find position IDs
    condition = get_condition(condition_id)
    if not condition:
        raise ValueError(f"Condition {condition_id} not found in subgraph")
    position_ids = condition["positionIds"]

    # Fetch current prices ONCE (they're the same for all users)
    if verbose:
        print("Fetching current market prices...")
    current_prices = get_current_prices(position_ids)
    yes_price = current_prices.get(position_ids[0])
    no_price = current_prices.get(position_ids[1])

    # Calculate number of batches
    num_batches = (total_users + batch_size - 1) // batch_size


    start_time = time.time()
    successful = 0
    failed = 0
    total_pnl_sum = 0.0
    batch_start_time = time.time()
    rows_to_write = []  # Collect all rows for batch CSV write

    # Process users in batches
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_users)
        batch_users = users[start_idx:end_idx]

        if verbose:
            print(f"\n[Batch {batch_num + 1}/{num_batches}] Fetching {len(batch_users)} users...")

        try:
            # Fetch all positions for this batch
            batch_positions = get_batch_user_positions(batch_users, position_ids)

            batch_pnl_sum = 0.0

            # Process each user in the batch
            for i, user_address in enumerate(batch_users):
                try:
                    user_idx = start_idx + i + 1

                    # Get positions for this user
                    positions = batch_positions.get(user_address.lower(), {"YES": None, "NO": None})

                    # Calculate PNL manually (we already have prices)
                    # Use .get() for safe field access in case API schema changes
                    results = {
                        "YES": {
                            "position": positions["YES"],
                            "current_price": yes_price,
                            "realized_pnl": float(positions["YES"].get("realizedPnl", 0)) if positions["YES"] else 0.0,
                            "unrealized_pnl": 0.0,
                            "total_pnl": 0.0
                        },
                        "NO": {
                            "position": positions["NO"],
                            "current_price": no_price,
                            "realized_pnl": float(positions["NO"].get("realizedPnl", 0)) if positions["NO"] else 0.0,
                            "unrealized_pnl": 0.0,
                            "total_pnl": 0.0
                        }
                    }

                    # Calculate unrealized PNL for YES
                    if positions["YES"] and yes_price:
                        amount = float(positions["YES"]["amount"])
                        avg_price = float(positions["YES"]["avgPrice"])
                        results["YES"]["unrealized_pnl"] = calculate_unrealized_pnl(amount, avg_price, yes_price)

                    # Calculate unrealized PNL for NO
                    if positions["NO"] and no_price:
                        amount = float(positions["NO"]["amount"])
                        avg_price = float(positions["NO"]["avgPrice"])
                        results["NO"]["unrealized_pnl"] = calculate_unrealized_pnl(amount, avg_price, no_price)

                    # Calculate total PNL
                    results["YES"]["total_pnl"] = results["YES"]["realized_pnl"] + results["YES"]["unrealized_pnl"]
                    results["NO"]["total_pnl"] = results["NO"]["realized_pnl"] + results["NO"]["unrealized_pnl"]

                    total_pnl = results["YES"]["total_pnl"] + results["NO"]["total_pnl"]
                    batch_pnl_sum += total_pnl
                    total_pnl_sum += total_pnl

                    # Build row for batch CSV write (much faster than per-user writes)
                    row = build_csv_row(condition_id, user_address, results, market_metadata)
                    rows_to_write.append(row)

                    successful += 1

                    # Progress display
                    if verbose:
                        pnl_str = format_usdc(total_pnl)
                        print(f"  [{user_idx}/{total_users}] {user_address[:10]}... PNL: {pnl_str}")
                    else:
                        # Live-updating progress line
                        elapsed = time.time() - start_time
                        eta = calculate_eta(elapsed, user_idx, total_users)
                        avg_pnl = total_pnl_sum / successful if successful > 0 else 0
                        suffix = f"| Avg: {format_usdc(avg_pnl)} | ETA: {eta}"
                        print_progress(user_idx, total_users, prefix="  ", suffix=suffix)

                except Exception as e:
                    failed += 1
                    if verbose:
                        print(f"  [{user_idx}/{total_users}] {user_address[:10]}... Error: {e}")

            # Batch summary (only in verbose mode)
            if verbose:
                batch_elapsed = time.time() - batch_start_time
                avg_batch_pnl = batch_pnl_sum / len(batch_users) if len(batch_users) > 0 else 0
                print(f"  Batch {batch_num + 1}/{num_batches} complete: {len(batch_users)} users | Avg: {format_usdc(avg_batch_pnl)} | {format_time(batch_elapsed)}")
                batch_start_time = time.time()

        except Exception as e:
            print(f"\n  Batch {batch_num + 1} error: {e}")
            failed += len(batch_users)

    # Clear any remaining progress line (already handled by print_progress)
    if not verbose:
        print()  # Add newline after final progress update

    # Batch write all rows to CSV (single file operation instead of per-user)
    if rows_to_write:
        write_rows_to_csv(rows_to_write, output_csv)

    elapsed_time = time.time() - start_time
    avg_pnl = total_pnl_sum / successful if successful > 0 else 0

    if not verbose:
        print()  # New line after progress
    
    # Compact completion message
    status = f"[OK] {successful:,} users | {format_time(elapsed_time)}"
    if failed > 0:
        status += f" | {failed} failed"
    print(f"  {status}")

    # Return summary statistics
    return {
        "total_users": total_users,
        "successful": successful,
        "failed": failed,
        "elapsed_time": elapsed_time
    }


def get_processed_markets(output_csv: str) -> dict:
    """
    Get dict of condition_ids already processed with their resolution status and outcome.

    Used for incremental processing - allows resuming from where
    we left off without reprocessing existing markets. Also tracks
    resolution status and winning_outcome so markets can be re-processed
    when they become resolved or when the winner changes.

    Args:
        output_csv: Path to the output CSV file (db_trades.csv)

    Returns:
        Dict mapping condition_id -> {'is_resolved': bool, 'winning_outcome': str}.
        Returns empty dict if file doesn't exist or is unreadable.
    """
    if not os.path.isfile(output_csv):
        return {}

    try:
        df = pd.read_csv(output_csv, usecols=['condition_id', 'is_resolved', 'winning_outcome'])
        # Get unique condition_id with their resolution status (take first occurrence)
        unique_markets = df.drop_duplicates(subset=['condition_id'])
        result = {}
        for _, row in unique_markets.iterrows():
            result[row['condition_id']] = {
                'is_resolved': row['is_resolved'],
                'winning_outcome': str(row['winning_outcome']) if pd.notna(row['winning_outcome']) else ''
            }
        return result
    except Exception as e:
        print(f"Warning: Could not read existing output file: {e}")
        return {}


def process_all_markets(
    markets_csv: Optional[str] = None,
    output_csv: Optional[str] = None,
    verbose: bool = False,
    max_users_per_market: Optional[int] = None,
    force_reprocess: bool = False,
    market_filter: str = "all",
    sport_filter: str = "all"
):
    """
    Process markets and calculate PNL for every user.

    Main entry point for batch PNL calculation. Reads markets
    from db_markets.csv and generates comprehensive trade data.

    Features:
    - Incremental processing: skips markets already in output
    - Progress tracking with ETA for each market
    - Excel-friendly date formatting (YYYY-MM-DD HH:MM:SS)
    - Resume capability after interruption
    - Flexible filtering: all, resolved-only, or unresolved-only

    Args:
        markets_csv: Input markets file (defaults to db_markets.csv)
        output_csv: Output trades file (defaults to db_trades.csv)
        verbose: If True, prints per-user details
        max_users_per_market: Limit users per market (for testing)
        force_reprocess: If True, deletes output and starts fresh
        market_filter: Filter markets - "all", "resolved", or "unresolved"

    Output (db_trades.csv):
        One row per user per market with position sizes, entry prices,
        realized/unrealized PNL, user pick, and correctness.

    Performance:
        - Processes markets sequentially (one at a time)
        - Within each market, uses batch processing for users
        - Typical runtime: 1-5 minutes per market depending on user count
    """
    if markets_csv is None:
        markets_csv = "db_markets.csv"
    if output_csv is None:
        output_csv = "db_trades.csv"
    
    # Validate input file exists
    if not os.path.isfile(markets_csv):
        print(f"Error: Markets file not found: {markets_csv}")
        print("Run update_markets.py first to fetch market data.")
        logger.error(f"Markets file not found: {markets_csv}")
        return
    
    logger.info(f"Starting trade processing from {markets_csv}")
    
    # Read markets CSV
    markets_df = pd.read_csv(markets_csv)

    # Filter by sport if specified
    if sport_filter != "all":
        markets_df = markets_df[markets_df['sport'].str.lower() == sport_filter].copy()
        if len(markets_df) == 0:
            print(f"No {sport_filter.upper()} markets found.")
            return

    # Filter markets based on market_filter parameter
    if market_filter == "resolved":
        filtered_markets = markets_df[markets_df['is_resolved'] == True].copy()
        filter_desc = "resolved"
    elif market_filter == "unresolved":
        filtered_markets = markets_df[markets_df['is_resolved'] == False].copy()
        filter_desc = "unresolved"
    else:  # "all"
        filtered_markets = markets_df.copy()
        filter_desc = "all"

    if len(filtered_markets) == 0:
        print(f"No {filter_desc} markets found.")
        return

    # Check for existing output and determine what needs processing
    if force_reprocess and os.path.isfile(output_csv):
        os.remove(output_csv)
        processed_markets = {}
    else:
        processed_markets = get_processed_markets(output_csv)

    # Find markets that need re-processing:
    # 1. Markets that were unresolved before but are now resolved
    # 2. ALL unresolved markets (to capture new users who placed bets since last run)
    # 3. Markets where winning_outcome has changed (rare but possible)
    markets_to_reprocess = []
    for _, market in filtered_markets.iterrows():
        cid = market['condition_id']
        if cid in processed_markets:
            prev_data = processed_markets[cid]
            was_resolved = prev_data['is_resolved']
            prev_winner = prev_data['winning_outcome']
            is_now_resolved = market['is_resolved']
            current_winner = str(market['winning_outcome']) if pd.notna(market['winning_outcome']) else ''

            # Re-process if it was unresolved before but is resolved now
            if not was_resolved and is_now_resolved:
                markets_to_reprocess.append(cid)
            # Also re-process if it's STILL unresolved (to capture new bets)
            elif not is_now_resolved:
                markets_to_reprocess.append(cid)
            # Re-process if winning_outcome changed (rare edge case)
            elif is_now_resolved and prev_winner != current_winner:
                markets_to_reprocess.append(cid)

    # Remove old rows for markets that need re-processing
    if markets_to_reprocess and os.path.isfile(output_csv):
        print(f"Re-processing {len(markets_to_reprocess)} markets (unresolved + newly resolved)...")
        try:
            df_existing = pd.read_csv(output_csv)
            df_existing = df_existing[~df_existing['condition_id'].isin(markets_to_reprocess)]
            df_existing.to_csv(output_csv, index=False)
            # Update processed_markets to exclude re-processing markets
            for cid in markets_to_reprocess:
                del processed_markets[cid]
        except Exception as e:
            logger.error(f"Failed to update CSV for re-processing: {e}")
            print(f"Error: Could not update {output_csv}: {e}")
            return

    # Filter out already processed markets (excluding those marked for re-processing)
    markets_to_process = filtered_markets[
        ~filtered_markets['condition_id'].isin(processed_markets.keys())
    ].copy()

    total_to_process = len(markets_to_process)

    if total_to_process == 0:
        print(f"All {filter_desc} markets already processed.")
        return

    # Count resolved vs unresolved in markets to process
    resolved_count = len(markets_to_process[markets_to_process['is_resolved'] == True])
    unresolved_count = len(markets_to_process[markets_to_process['is_resolved'] == False])
    reprocess_count = len(markets_to_reprocess)

    # Compact header
    print(f"\n{'='*50}")
    print(f"Processing {total_to_process} {filter_desc} markets ({len(processed_markets)} already done)")
    if reprocess_count > 0:
        print(f"  (includes {reprocess_count} re-processed markets for new bets)")
    print(f"  Resolved: {resolved_count} | Unresolved: {unresolved_count}")
    print(f"{'='*50}")

    # Processing loop
    processed = 0
    failed = 0
    start_time = time.time()
    total_users_processed = 0

    for idx, (_, market) in enumerate(markets_to_process.iterrows(), 1):
        try:
            elapsed = time.time() - start_time

            # OVERALL PROGRESS
            overall_bar = progress_bar(idx, total_to_process, width=30)
            eta = calculate_eta(elapsed, idx, total_to_process)

            print(f"\n{overall_bar} | ETA: {eta}")
            print(f"  {market['outcome_team_a']} vs {market['outcome_team_b']}")

            # Prepare market metadata
            # Format game_start_time for Excel date recognition
            game_time_str = str(market['game_start_time'])
            try:
                # Parse and format as YYYY-MM-DD HH:MM:SS for Excel
                if dateutil_parser is not None:
                    dt = dateutil_parser.parse(game_time_str)
                    game_time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # Fallback: try pandas datetime parsing
                    dt = pd.to_datetime(game_time_str)
                    game_time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logger.debug(f"Could not parse date '{game_time_str}': {e}")
                pass  # Keep original if parsing fails

            market_metadata = {
                "sport": str(market['sport']),
                "match_title": str(market['match_title']),
                "game_start_time": game_time_str,
                "is_resolved": str(market['is_resolved']),
                "winning_outcome": str(market['winning_outcome'])
            }

            # Calculate PNL for all users in this market
            market_summary = calculate_pnl_for_all_users(
                condition_id=market['condition_id'],
                output_csv=output_csv,
                verbose=verbose,
                max_users=max_users_per_market,
                market_metadata=market_metadata
            )

            # Track cumulative statistics
            if market_summary:
                total_users_processed += market_summary['successful']

            processed += 1

        except Exception as e:
            failed += 1
            print(f"\nError processing market {market['match_title']}: {e}")
            import traceback
            traceback.print_exc()
            print("-"*80)

    elapsed_time = time.time() - start_time
    avg_time_per_market = elapsed_time / processed if processed > 0 else 0

    # Final summary
    print(f"\n{'='*50}")
    print(f"[OK] Done: {processed} markets | {total_users_processed:,} users | {format_time(elapsed_time)}")
    if failed > 0:
        print(f"  Failed: {failed} markets")
    print(f"{'='*50}")
    
    # Segment trades by sport
    print("\nSegmenting trades by sport...")
    segment_trades_by_sport(output_csv)
    print()


# =============================================================================
# Interactive Menu Functions
# =============================================================================

def select_sport() -> Optional[str]:
    """Display sport selection menu and return selected sport."""
    print("=" * 50)
    print("Trade PNL Calculator")
    print("=" * 50)
    print()
    print("Select sport:")
    print("  1. All sports")
    print("  2. NFL")
    print("  3. NBA")
    print("  4. CFB")
    print("  5. CBB")
    print("  0. Exit")
    print()

    choice = input("Enter choice (0-5): ").strip()

    if choice == "0":
        return "exit"

    sport_map = {"1": "all", "2": "nfl", "3": "nba", "4": "cfb", "5": "cbb"}
    return sport_map.get(choice)


def select_resolution_status() -> Optional[str]:
    """Display resolution status menu and return filter type."""
    print()
    print("Select market status:")
    print("  1. All markets")
    print("  2. Resolved only")
    print("  3. Unresolved only")
    print("  0. Exit")
    print()

    choice = input("Enter choice (0-3): ").strip()

    if choice == "0":
        return "exit"

    status_map = {"1": "all", "2": "resolved", "3": "unresolved"}
    return status_map.get(choice)


def segment_trades_by_sport(trades_csv: str = "db_trades.csv"):
    """
    Segment trades CSV into separate files by sport.
    
    Creates:
        - db_trades_nfl.csv
        - db_trades_nba.csv
        - db_trades_cfb.csv
    
    Args:
        trades_csv: Path to the combined trades CSV file
    """
    if not os.path.isfile(trades_csv):
        print(f"  Trades file not found: {trades_csv}")
        return
    
    df = pd.read_csv(trades_csv)
    
    sport_files = {
        "nfl": "db_trades_nfl.csv",
        "nba": "db_trades_nba.csv",
        "cfb": "db_trades_cfb.csv",
        "cbb": "db_trades_cbb.csv",
    }
    
    # Normalize sport column
    df['sport'] = df['sport'].str.lower().str.strip()
    
    for sport, filename in sport_files.items():
        sport_df = df[df['sport'] == sport]
        count = len(sport_df)
        
        if count > 0:
            sport_df.to_csv(filename, index=False)
            print(f"  {sport.upper()}: {count:,} trades -> {filename}")
            logger.info(f"Saved {count} {sport.upper()} trades to {filename}")
        else:
            print(f"  {sport.upper()}: 0 trades (skipped)")


if __name__ == "__main__":
    # Check if any command-line args provided
    if len(sys.argv) > 1:
        # CLI mode: Parse command-line arguments
        parser = argparse.ArgumentParser(
            description="Calculate PNL for users in Polymarket sports markets"
        )
        parser.add_argument(
            "--resolved-only",
            action="store_true",
            help="Only process resolved markets"
        )
        parser.add_argument(
            "--unresolved-only",
            action="store_true",
            help="Only process unresolved markets"
        )
        parser.add_argument(
            "--verbose", "-v",
            action="store_true",
            help="Print detailed output per user"
        )
        parser.add_argument(
            "--force-reprocess",
            action="store_true",
            help="Delete existing output and reprocess everything"
        )
        parser.add_argument(
            "--max-users",
            type=int,
            default=None,
            help="Limit users per market (for testing)"
        )

        args = parser.parse_args()

        # Determine market filter based on flags
        if args.resolved_only and args.unresolved_only:
            print("Error: Cannot use both --resolved-only and --unresolved-only")
            exit(1)
        elif args.resolved_only:
            market_filter = "resolved"
        elif args.unresolved_only:
            market_filter = "unresolved"
        else:
            market_filter = "all"  # Default: process all markets

        try:
            process_all_markets(
                verbose=args.verbose,
                max_users_per_market=args.max_users,
                force_reprocess=args.force_reprocess,
                market_filter=market_filter
            )
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
    else:
        # Interactive menu mode
        sport = select_sport()
        if sport == "exit":
            print("Exiting.")
            exit(0)
        if not sport:
            print("Invalid choice")
            exit(1)

        status = select_resolution_status()
        if status == "exit":
            print("Exiting.")
            exit(0)
        if not status:
            print("Invalid choice")
            exit(1)

        sport_display = "All sports" if sport == "all" else sport.upper()
        print(f"\nProcessing {sport_display} markets ({status})...")

        try:
            process_all_markets(
                verbose=False,
                market_filter=status,
                sport_filter=sport
            )
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
