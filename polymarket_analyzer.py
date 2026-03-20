#!/usr/bin/env python3
"""
Polymarket wallet collector and rolling win-rate analyzer.

Examples:
    python polymarket_analyzer.py
    python polymarket_analyzer.py --max_wallets 200 --period_days 180 --output_dir ./polymarket_analysis
    python polymarket_analyzer.py --leaderboard_only
    python polymarket_analyzer.py --use_subgraph --max_wallets 50

Warning:
    Public Polymarket endpoints are rate-limited and their parameter caps change over time.
    This script sleeps between requests, retries transient failures, and uses the documented 2026-safe
    pagination caps for leaderboard, trades, and closed-positions endpoints.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from random import uniform
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from tqdm import tqdm

# =========================
# Constants and API config
# =========================
DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
POSITIONS_SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/positions-subgraph/0.0.7/gn"
)
PNL_SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/pnl-subgraph/0.0.14/gn"
)
ORDERBOOK_SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/orderbook-subgraph/0.0.1/gn"
)

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "polymarket-analyzer/1.1",
}
GRAPHQL_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "polymarket-analyzer/1.1",
}

RETRY_BACKOFF_SECONDS = [1, 5, 10]
REQUEST_SLEEP_RANGE = (0.6, 1.0)
NON_RETRYABLE_HTTP_STATUS = {400, 401, 403, 404, 422}

LEADERBOARD_CATEGORY = "OVERALL"
LEADERBOARD_TIME_PERIODS = ["ALL", "MONTH", "WEEK", "DAY"]
LEADERBOARD_ORDER_BY = ["PNL", "VOL"]
LEADERBOARD_PAGE_LIMIT = 50
LEADERBOARD_MAX_OFFSET = 1000

# Latest documented caps for /trades and /activity changed on August 26, 2025.
TRADES_PAGE_LIMIT = 500
TRADES_MAX_OFFSET = 1000
CLOSED_POSITIONS_PAGE_LIMIT = 50
CLOSED_POSITIONS_MAX_OFFSET = 100000
MARKETS_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60
MARKET_LOOKUP_CHUNK_SIZE = 50

DEFAULT_MAX_WALLETS = 100
DEFAULT_PERIOD_DAYS = 180


def setup_logging(output_dir: str) -> logging.Logger:
    """Configure console and file logging."""
    os.makedirs(output_dir, exist_ok=True)
    log_path = os.path.join(output_dir, "app.log")

    logger = logging.getLogger("polymarket_analyzer")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


LOGGER = logging.getLogger("polymarket_analyzer")


# =========================
# Utility helpers
# =========================
def rate_limit_sleep() -> None:
    """Sleep between requests to respect public API rate limits."""
    time.sleep(uniform(*REQUEST_SLEEP_RANGE))


def parse_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if value is None:
        return default
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        stripped = str(value).replace(",", "").replace("$", "").strip()
        return float(stripped) if stripped else default
    except (TypeError, ValueError):
        return default


def parse_int(value: Any, default: int = 0) -> int:
    """Safely convert a value to int."""
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        stripped = str(value).strip()
        return int(float(stripped)) if stripped else default
    except (TypeError, ValueError):
        return default


def normalize_wallet(value: Any) -> Optional[str]:
    """Normalize and validate EVM wallet addresses."""
    if not value:
        return None
    wallet = str(value).strip()
    if wallet.startswith("\\x") and len(wallet) == 42:
        wallet = "0x" + wallet[2:]
    if wallet.lower().startswith("0x") and len(wallet) == 42:
        return wallet.lower()
    return None


def epoch_now() -> int:
    """Return current UTC epoch seconds."""
    return int(datetime.now(timezone.utc).timestamp())


def ensure_dir(path: str) -> str:
    """Create directory if missing and return it."""
    os.makedirs(path, exist_ok=True)
    return path


def save_json(path: str, payload: Any) -> None:
    """Write JSON with UTF-8 encoding."""
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_json(path: str, default: Any = None) -> Any:
    """Load JSON or return default when unavailable."""
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return default


def extract_timestamp(item: Dict[str, Any]) -> Optional[int]:
    """Extract a timestamp from multiple possible fields."""
    candidate_fields = [
        "timestamp",
        "timeStamp",
        "createdAt",
        "created_at",
        "updatedAt",
        "lastActiveTimestamp",
    ]
    for field in candidate_fields:
        raw = item.get(field)
        if raw is None:
            continue
        if isinstance(raw, (int, float)):
            ts = int(raw)
            if ts > 10**12:
                ts //= 1000
            return ts
        if isinstance(raw, str):
            raw = raw.strip()
            if raw.isdigit():
                ts = int(raw)
                if ts > 10**12:
                    ts //= 1000
                return ts
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                    return int(dt.timestamp())
                except ValueError:
                    continue
    return None


def request_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Any:
    """Perform HTTP request with retries, backoff, logging, and rate limiting."""
    merged_headers = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(headers)

    total_attempts = len(RETRY_BACKOFF_SECONDS)
    for attempt_index, backoff in enumerate(RETRY_BACKOFF_SECONDS, start=1):
        try:
            rate_limit_sleep()
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_payload,
                headers=merged_headers,
                timeout=timeout,
            )

            if response.status_code >= 400:
                error = requests.HTTPError(
                    f"HTTP {response.status_code} for {url}: {response.text[:300]}",
                    response=response,
                )
                if response.status_code in NON_RETRYABLE_HTTP_STATUS:
                    raise error
                raise error

            if not response.text.strip():
                return None
            return response.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            LOGGER.error(
                "Request failed (attempt %s/%s): %s %s params=%s error=%s",
                attempt_index,
                total_attempts,
                method,
                url,
                params,
                exc,
            )
            if attempt_index >= total_attempts or status_code in NON_RETRYABLE_HTTP_STATUS:
                raise
            time.sleep(backoff)
    return None


def graphql_query(url: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute a GraphQL request against a Goldsky subgraph."""
    payload = {"query": query, "variables": variables or {}}
    data = request_json("POST", url, json_payload=payload, headers=GRAPHQL_HEADERS)
    if not isinstance(data, dict):
        return {}
    if data.get("errors"):
        LOGGER.error("GraphQL returned errors for %s: %s", url, data["errors"])
    return data


# =========================
# API helpers
# =========================
def unwrap_list_payload(payload: Any, keys: Iterable[str]) -> List[Dict[str, Any]]:
    """Normalize list payloads returned by Polymarket endpoints."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def request_json_or_empty(*args: Any, **kwargs: Any) -> Any:
    """Wrap request_json and return None on hard API errors after logging."""
    try:
        return request_json(*args, **kwargs)
    except requests.RequestException as exc:
        LOGGER.error("API call failed permanently: %s", exc)
        return None


def paginate_offset_requests(
    url: str,
    *,
    params: Optional[Dict[str, Any]],
    limit: int,
    max_offset: int,
    list_keys: Iterable[str],
) -> List[Dict[str, Any]]:
    """Collect paginated records using limit/offset semantics."""
    records: List[Dict[str, Any]] = []
    offset = 0
    while offset <= max_offset:
        current_params = dict(params or {})
        current_params.update({"limit": limit, "offset": offset})
        payload = request_json_or_empty("GET", url, params=current_params)
        page = unwrap_list_payload(payload, list_keys)
        if not page:
            break
        records.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return records


# =========================
# Leaderboard and wallet collection
# =========================
def get_leaderboard() -> Dict[str, Dict[str, float]]:
    """Collect leaderboard entries across supported time periods and sort options."""
    leaderboard_wallets: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"wallet": "", "volume_usd": 0.0, "pnl_usd": 0.0}
    )

    total_queries = len(LEADERBOARD_TIME_PERIODS) * len(LEADERBOARD_ORDER_BY)
    progress = tqdm(total=total_queries, desc="Leaderboard queries")
    for time_period in LEADERBOARD_TIME_PERIODS:
        for order_by in LEADERBOARD_ORDER_BY:
            for offset in range(0, LEADERBOARD_MAX_OFFSET + LEADERBOARD_PAGE_LIMIT, LEADERBOARD_PAGE_LIMIT):
                params = {
                    "category": LEADERBOARD_CATEGORY,
                    "timePeriod": time_period,
                    "orderBy": order_by,
                    "limit": LEADERBOARD_PAGE_LIMIT,
                    "offset": offset,
                }
                payload = request_json_or_empty("GET", f"{DATA_API_BASE}/v1/leaderboard", params=params)
                rows = unwrap_list_payload(payload, ("leaderboard", "data", "results"))
                if not rows:
                    break
                for item in rows:
                    wallet = normalize_wallet(
                        item.get("proxyWallet")
                        or item.get("wallet")
                        or item.get("user")
                        or item.get("address")
                    )
                    if not wallet:
                        continue
                    row = leaderboard_wallets[wallet]
                    row["wallet"] = wallet
                    row["volume_usd"] = max(
                        row["volume_usd"],
                        parse_float(item.get("vol") or item.get("volume") or item.get("volumeUsd")),
                    )
                    row["pnl_usd"] = max(
                        row["pnl_usd"],
                        parse_float(item.get("pnl") or item.get("pnlUsd") or item.get("profit")),
                    )
                if len(rows) < LEADERBOARD_PAGE_LIMIT:
                    break
            progress.update(1)
    progress.close()
    return dict(leaderboard_wallets)


def fetch_trades_page(user: Optional[str] = None, *, limit: int = TRADES_PAGE_LIMIT, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetch one page of trades from the Data API using current documented caps."""
    safe_limit = min(limit, TRADES_PAGE_LIMIT)
    safe_offset = min(offset, TRADES_MAX_OFFSET)
    params: Dict[str, Any] = {"limit": safe_limit, "offset": safe_offset}
    if user:
        params["user"] = user
    payload = request_json_or_empty("GET", f"{DATA_API_BASE}/trades", params=params)
    return unwrap_list_payload(payload, ("trades", "data", "results"))


def fetch_closed_positions(user: str) -> List[Dict[str, Any]]:
    """Fetch all available closed positions for a wallet."""
    return paginate_offset_requests(
        f"{DATA_API_BASE}/closed-positions",
        params={"user": user, "sortBy": "TIMESTAMP"},
        limit=CLOSED_POSITIONS_PAGE_LIMIT,
        max_offset=CLOSED_POSITIONS_MAX_OFFSET,
        list_keys=("positions", "data", "results"),
    )


def collect_wallets(output_dir: str, leaderboard_only: bool = False) -> List[Dict[str, Any]]:
    """Collect wallet universe from leaderboard and public trades."""
    wallets_map = get_leaderboard()
    LOGGER.info("Collected %s unique wallets from leaderboard", len(wallets_map))

    if not leaderboard_only:
        for offset in tqdm(
            range(0, TRADES_MAX_OFFSET + TRADES_PAGE_LIMIT, TRADES_PAGE_LIMIT),
            desc="Public trades",
        ):
            trades = fetch_trades_page(None, limit=TRADES_PAGE_LIMIT, offset=offset)
            if not trades:
                break
            for trade in trades:
                wallet = normalize_wallet(
                    trade.get("proxyWallet")
                    or trade.get("user")
                    or trade.get("wallet")
                    or trade.get("maker")
                    or trade.get("owner")
                )
                if not wallet:
                    continue
                if wallet not in wallets_map:
                    wallets_map[wallet] = {
                        "wallet": wallet,
                        "volume_usd": 0.0,
                        "pnl_usd": 0.0,
                    }
            if len(trades) < TRADES_PAGE_LIMIT:
                break

    wallets = [row for row in wallets_map.values() if normalize_wallet(row.get("wallet"))]
    wallets.sort(
        key=lambda item: (parse_float(item.get("volume_usd")), parse_float(item.get("pnl_usd"))),
        reverse=True,
    )

    wallets_csv_path = os.path.join(output_dir, "wallets.csv")
    with open(wallets_csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["wallet", "volume_usd", "pnl_usd"])
        writer.writeheader()
        writer.writerows(wallets)

    LOGGER.info("Saved %s wallets to %s", len(wallets), wallets_csv_path)
    return wallets


# =========================
# Market cache and metadata
# =========================
def get_market_cache(cache_dir: str) -> Dict[str, Dict[str, Any]]:
    """Load market metadata from cache when still fresh."""
    ensure_dir(cache_dir)
    cache_path = os.path.join(cache_dir, "markets.json")
    cached = load_json(cache_path, default={})
    if isinstance(cached, dict):
        fetched_at = parse_int(cached.get("fetched_at"))
        if fetched_at and epoch_now() - fetched_at < MARKETS_CACHE_MAX_AGE_SECONDS:
            markets = cached.get("markets", {})
            if isinstance(markets, dict):
                return markets
    return {}


def save_market_cache(cache_dir: str, markets: Dict[str, Dict[str, Any]]) -> None:
    """Persist market cache with fetch timestamp."""
    ensure_dir(cache_dir)
    cache_path = os.path.join(cache_dir, "markets.json")
    save_json(cache_path, {"fetched_at": epoch_now(), "markets": markets})


def build_market_record(market: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize market metadata coming from Gamma API."""
    winning_outcome = (
        market.get("outcome")
        or market.get("resolvedOutcome")
        or market.get("winningOutcome")
        or market.get("resolution")
    )
    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            outcomes = [chunk.strip() for chunk in outcomes.split(",") if chunk.strip()]

    return {
        "condition_id": str(market.get("conditionId") or market.get("questionID") or market.get("id") or ""),
        "market_id": str(market.get("id") or market.get("conditionId") or ""),
        "question": market.get("question") or market.get("title") or "",
        "slug": market.get("slug") or "",
        "resolved": bool(
            market.get("resolved")
            or market.get("isResolved")
            or market.get("closed")
            or market.get("archived")
        ),
        "winning_outcome": str(winning_outcome).strip() if winning_outcome is not None else None,
        "outcomes": outcomes if isinstance(outcomes, list) else [],
        "end_date": market.get("endDate") or market.get("closeTime") or market.get("resolutionTime"),
    }


def fetch_all_markets(cache_dir: str) -> Dict[str, Dict[str, Any]]:
    """Load the persisted on-demand market cache."""
    cached = get_market_cache(cache_dir)
    if cached:
        LOGGER.info("Loaded %s cached market metadata entries", len(cached))
    return cached


def chunked(values: List[str], chunk_size: int) -> Iterable[List[str]]:
    """Yield fixed-size chunks from a list."""
    for start in range(0, len(values), chunk_size):
        yield values[start : start + chunk_size]


def fetch_markets_by_condition_ids(condition_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch market metadata in batches using the documented `condition_ids` query parameter."""
    unique_ids = sorted({str(item) for item in condition_ids if item})
    if not unique_ids:
        return {}

    resolved: Dict[str, Dict[str, Any]] = {}
    for batch in chunked(unique_ids, MARKET_LOOKUP_CHUNK_SIZE):
        payload = request_json_or_empty(
            "GET",
            f"{GAMMA_API_BASE}/markets",
            params={"condition_ids": batch},
        )
        rows = unwrap_list_payload(payload, ("markets", "data", "results"))
        for market in rows:
            normalized = build_market_record(market)
            condition_id = normalized.get("condition_id")
            if condition_id:
                resolved[str(condition_id)] = normalized
    return resolved


# =========================
# Subgraph fallback
# =========================
def fetch_wallet_trades_subgraph(wallet: str, cutoff_ts: int) -> List[Dict[str, Any]]:
    """Fetch trades from the orderbook subgraph."""
    results: List[Dict[str, Any]] = []
    skip = 0
    first = 1000
    query = """
    query WalletOrders($wallet: String!, $first: Int!, $skip: Int!) {
      orders(
        first: $first
        skip: $skip
        orderBy: timestamp
        orderDirection: desc
        where: { user: $wallet }
      ) {
        id
        user
        timestamp
        outcome
        side
        price
        size
        conditionId
        market
      }
    }
    """

    while True:
        payload = graphql_query(
            ORDERBOOK_SUBGRAPH_URL,
            query,
            {"wallet": wallet, "first": first, "skip": skip},
        )
        page = payload.get("data", {}).get("orders", []) if isinstance(payload, dict) else []
        if not page:
            break
        reached_cutoff = False
        for order in page:
            ts = extract_timestamp(order)
            if ts is not None and ts < cutoff_ts:
                reached_cutoff = True
                continue
            results.append(order)
        skip += len(page)
        if len(page) < first or reached_cutoff:
            break
    return results


def fetch_wallet_pnl_subgraph(wallet: str) -> List[Dict[str, Any]]:
    """Fetch realized PnL rows from the pnl subgraph."""
    query = """
    query WalletPnl($wallet: String!) {
      pnls(where: { user: $wallet }) {
        id
        user
        conditionId
        realizedPnl
        redeemed
      }
    }
    """
    payload = graphql_query(PNL_SUBGRAPH_URL, query, {"wallet": wallet})
    return payload.get("data", {}).get("pnls", []) if isinstance(payload, dict) else []


def fetch_wallet_positions_subgraph(wallet: str) -> List[Dict[str, Any]]:
    """Fetch historical positions from the positions subgraph as an auxiliary fallback."""
    query = """
    query WalletPositions($wallet: String!) {
      positions(where: { user: $wallet }) {
        id
        user
        conditionId
        outcome
        totalBought
        totalSold
        avgPrice
        realizedPnl
      }
    }
    """
    payload = graphql_query(POSITIONS_SUBGRAPH_URL, query, {"wallet": wallet})
    return payload.get("data", {}).get("positions", []) if isinstance(payload, dict) else []


# =========================
# Analysis helpers
# =========================
def infer_trade_side(trade: Dict[str, Any]) -> str:
    """Infer whether the trade is a buy or sell."""
    raw = str(trade.get("side") or trade.get("type") or trade.get("action") or "").strip().lower()
    if raw in {"buy", "bid", "bought"}:
        return "buy"
    if raw in {"sell", "ask", "sold"}:
        return "sell"
    return raw or "unknown"


def infer_trade_outcome(trade: Dict[str, Any]) -> str:
    """Infer YES/NO or token outcome text from a trade payload."""
    for field in ("outcome", "outcomeIndex", "tokenOutcome", "tokenId", "asset"):
        value = trade.get(field)
        if value is None:
            continue
        text = str(value).strip()
        lower = text.lower()
        if lower in {"0", "no"}:
            return "No"
        if lower in {"1", "yes"}:
            return "Yes"
        if text:
            return text
    title = str(trade.get("title") or trade.get("marketQuestion") or "").lower()
    if " yes " in f" {title} ":
        return "Yes"
    if " no " in f" {title} ":
        return "No"
    return "Unknown"


def infer_trade_volume_usd(trade: Dict[str, Any]) -> float:
    """Estimate USD notional for a trade."""
    for field in ("volume", "volumeUsd", "usdcSize", "amountUsd", "notionalUsd"):
        amount = parse_float(trade.get(field))
        if amount > 0:
            return amount
    price = parse_float(trade.get("price") or trade.get("avgPrice"))
    size = parse_float(trade.get("size") or trade.get("amount") or trade.get("shares") or trade.get("totalBought"))
    return price * size if price > 0 and size > 0 else 0.0


def infer_condition_id(item: Dict[str, Any]) -> Optional[str]:
    """Infer the grouping identifier for a market position or trade."""
    for field in ("conditionId", "market", "marketId", "questionID", "slug"):
        value = item.get(field)
        if value:
            return str(value)
    return None


def fetch_wallet_trades(wallet: str, cutoff_ts: int) -> List[Dict[str, Any]]:
    """Fetch user trades using the capped Data API pagination."""
    trades: List[Dict[str, Any]] = []
    for offset in range(0, TRADES_MAX_OFFSET + TRADES_PAGE_LIMIT, TRADES_PAGE_LIMIT):
        page = fetch_trades_page(wallet, limit=TRADES_PAGE_LIMIT, offset=offset)
        if not page:
            break
        reached_cutoff = False
        for trade in page:
            ts = extract_timestamp(trade)
            if ts is None or ts >= cutoff_ts:
                trades.append(trade)
            else:
                reached_cutoff = True
        if len(page) < TRADES_PAGE_LIMIT or reached_cutoff:
            break
    return trades


def sum_realized_pnl_from_closed_positions(records: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    """Aggregate realized PnL by condition ID from closed positions."""
    realized_by_market: Dict[str, float] = defaultdict(float)
    for row in records:
        condition_id = infer_condition_id(row)
        if not condition_id:
            continue
        realized_by_market[condition_id] += parse_float(
            row.get("realizedPnl") or row.get("pnl") or row.get("profit") or row.get("amount")
        )
    return dict(realized_by_market)


def sum_realized_pnl_from_subgraph(records: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    """Aggregate realized PnL by condition ID from subgraph rows."""
    realized_by_market: Dict[str, float] = defaultdict(float)
    for row in records:
        condition_id = infer_condition_id(row)
        if not condition_id:
            continue
        realized_by_market[condition_id] += parse_float(row.get("realizedPnl"))
    return dict(realized_by_market)


def classify_market_win(
    trades: List[Dict[str, Any]],
    market: Dict[str, Any],
    realized_pnl: float,
) -> Tuple[bool, Dict[str, Any]]:
    """Determine whether a resolved market counts as a win."""
    winning_outcome = str(market.get("winning_outcome") or "").strip().lower()
    buys_yes = 0.0
    buys_no = 0.0
    sells_yes = 0.0
    sells_no = 0.0

    for trade in trades:
        side = infer_trade_side(trade)
        outcome = infer_trade_outcome(trade).strip().lower()
        volume = infer_trade_volume_usd(trade)
        if outcome == "yes":
            if side == "buy":
                buys_yes += volume
            elif side == "sell":
                sells_yes += volume
        elif outcome == "no":
            if side == "buy":
                buys_no += volume
            elif side == "sell":
                sells_no += volume

    net_yes = buys_yes - sells_yes
    net_no = buys_no - sells_no
    expected_win = False
    if winning_outcome == "yes":
        expected_win = net_yes > net_no
    elif winning_outcome == "no":
        expected_win = net_no > net_yes

    is_win = realized_pnl > 0 or expected_win
    return is_win, {
        "winning_outcome": market.get("winning_outcome"),
        "realized_pnl": round(realized_pnl, 6),
        "net_yes_volume": round(net_yes, 6),
        "net_no_volume": round(net_no, 6),
        "buy_yes_volume": round(buys_yes, 6),
        "buy_no_volume": round(buys_no, 6),
        "sell_yes_volume": round(sells_yes, 6),
        "sell_no_volume": round(sells_no, 6),
    }


def write_results_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    """Write aggregate wallet metrics to CSV without requiring pandas."""
    fieldnames = [
        "wallet",
        "period_days",
        "winrate_%",
        "winning_markets",
        "total_resolved_markets",
        "total_markets",
        "total_trades",
        "total_volume_usd",
        "total_pnl_usd",
        "roi",
        "avg_bet_size",
        "active_days",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def analyze_wallet(
    wallet: str,
    markets_cache: Dict[str, Dict[str, Any]],
    period_days: int,
    cache_dir: str,
    use_subgraph: bool = False,
) -> Dict[str, Any]:
    """Analyze one wallet's recent trading performance and win rate."""
    cutoff_ts = epoch_now() - period_days * 24 * 60 * 60
    wallet_trades = (
        fetch_wallet_trades_subgraph(wallet, cutoff_ts) if use_subgraph else fetch_wallet_trades(wallet, cutoff_ts)
    )
    closed_positions = [] if use_subgraph else fetch_closed_positions(wallet)
    pnl_subgraph = fetch_wallet_pnl_subgraph(wallet) if use_subgraph else []
    positions_subgraph = fetch_wallet_positions_subgraph(wallet) if use_subgraph else []

    realized_pnl_map = sum_realized_pnl_from_closed_positions(closed_positions)
    for pnl_map in (sum_realized_pnl_from_subgraph(pnl_subgraph), sum_realized_pnl_from_subgraph(positions_subgraph)):
        for key, value in pnl_map.items():
            realized_pnl_map[key] = realized_pnl_map.get(key, 0.0) + value

    trades_by_market: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    total_volume_usd = 0.0
    trade_timestamps: List[int] = []

    for trade in wallet_trades:
        condition_id = infer_condition_id(trade)
        if not condition_id:
            continue
        trades_by_market[condition_id].append(trade)
        total_volume_usd += infer_trade_volume_usd(trade)
        ts = extract_timestamp(trade)
        if ts is not None:
            trade_timestamps.append(ts)

    missing_condition_ids = [
        condition_id for condition_id in trades_by_market if condition_id not in markets_cache
    ]
    if missing_condition_ids:
        fetched_markets = fetch_markets_by_condition_ids(missing_condition_ids)
        if fetched_markets:
            markets_cache.update(fetched_markets)
            save_market_cache(cache_dir, markets_cache)

    winning_markets = 0
    total_resolved_markets = 0
    market_rows: List[Dict[str, Any]] = []

    for condition_id, market_trades in trades_by_market.items():
        market_info = markets_cache.get(condition_id)
        if not market_info:
            market_info = {
                "condition_id": condition_id,
                "market_id": condition_id,
                "question": "Unknown market",
                "resolved": False,
                "winning_outcome": None,
            }

        realized_pnl = realized_pnl_map.get(condition_id, 0.0)
        market_row = {
            "condition_id": condition_id,
            "market_id": market_info.get("market_id"),
            "question": market_info.get("question"),
            "resolved": bool(market_info.get("resolved")),
            "winning_outcome": market_info.get("winning_outcome"),
            "trade_count": len(market_trades),
            "volume_usd": round(sum(infer_trade_volume_usd(item) for item in market_trades), 6),
            "realized_pnl_usd": round(realized_pnl, 6),
        }
        if market_row["resolved"]:
            total_resolved_markets += 1
            is_win, diagnostics = classify_market_win(market_trades, market_info, realized_pnl)
            if is_win:
                winning_markets += 1
            market_row["is_win"] = is_win
            market_row.update(diagnostics)
        else:
            market_row["is_win"] = None
        market_rows.append(market_row)

    total_pnl_usd = sum(realized_pnl_map.get(condition_id, 0.0) for condition_id in trades_by_market)
    active_days = len(
        {
            datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            for ts in trade_timestamps
        }
    )
    total_trades = sum(len(items) for items in trades_by_market.values())
    avg_bet_size = total_volume_usd / total_trades if total_trades else 0.0
    winrate = (winning_markets / total_resolved_markets * 100.0) if total_resolved_markets else 0.0
    roi = (total_pnl_usd / total_volume_usd * 100.0) if total_volume_usd else 0.0

    return {
        "wallet": wallet,
        "period_days": period_days,
        "total_trades": total_trades,
        "total_markets": len(trades_by_market),
        "total_resolved_markets": total_resolved_markets,
        "winning_markets": winning_markets,
        "winrate_%": round(winrate, 4),
        "total_volume_usd": round(total_volume_usd, 6),
        "total_pnl_usd": round(total_pnl_usd, 6),
        "roi": round(roi, 4),
        "avg_bet_size": round(avg_bet_size, 6),
        "active_days": active_days,
        "markets": sorted(market_rows, key=lambda item: item["volume_usd"], reverse=True),
    }


def analyze_wallets(
    wallets: List[Dict[str, Any]],
    output_dir: str,
    period_days: int,
    max_wallets: int,
    use_subgraph: bool,
) -> List[Dict[str, Any]]:
    """Analyze a wallet cohort and save CSV/JSON outputs."""
    reports_dir = ensure_dir(os.path.join(output_dir, "reports"))
    cache_dir = ensure_dir(os.path.join(output_dir, "cache"))
    markets_cache = fetch_all_markets(cache_dir)

    results: List[Dict[str, Any]] = []
    for row in tqdm(wallets[:max_wallets], desc="Wallet analysis"):
        wallet = row["wallet"]
        try:
            report = analyze_wallet(
                wallet=wallet,
                markets_cache=markets_cache,
                period_days=period_days,
                cache_dir=cache_dir,
                use_subgraph=use_subgraph,
            )
        except requests.RequestException as exc:
            LOGGER.error("Wallet analysis failed for %s: %s", wallet, exc)
            report = {
                "wallet": wallet,
                "period_days": period_days,
                "total_trades": 0,
                "total_markets": 0,
                "total_resolved_markets": 0,
                "winning_markets": 0,
                "winrate_%": 0.0,
                "total_volume_usd": 0.0,
                "total_pnl_usd": 0.0,
                "roi": 0.0,
                "avg_bet_size": 0.0,
                "active_days": 0,
                "markets": [],
                "error": str(exc),
            }
        save_json(os.path.join(reports_dir, f"{wallet}.json"), report)
        results.append(report)

    results.sort(
        key=lambda item: (parse_float(item.get("winrate_%")), parse_float(item.get("total_pnl_usd"))),
        reverse=True,
    )
    write_results_csv(os.path.join(output_dir, "results.csv"), results)
    save_json(os.path.join(output_dir, "detailed_report.json"), results)
    LOGGER.info("Saved %s wallet reports", len(results))
    return results


# =========================
# CLI
# =========================
def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Collect Polymarket proxy wallets and analyze wallet win rate over a configurable period."
    )
    parser.add_argument("--max_wallets", type=int, default=DEFAULT_MAX_WALLETS)
    parser.add_argument("--period_days", type=int, default=DEFAULT_PERIOD_DAYS)
    parser.add_argument("--output_dir", type=str, default="./polymarket_analysis")
    parser.add_argument("--use_subgraph", action="store_true")
    parser.add_argument("--leaderboard_only", action="store_true")
    return parser


def main() -> None:
    """Script entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args()

    output_dir = ensure_dir(args.output_dir)
    global LOGGER
    LOGGER = setup_logging(output_dir)

    LOGGER.info("Starting Polymarket analyzer")
    LOGGER.info(
        "Arguments: max_wallets=%s period_days=%s output_dir=%s use_subgraph=%s leaderboard_only=%s",
        args.max_wallets,
        args.period_days,
        args.output_dir,
        args.use_subgraph,
        args.leaderboard_only,
    )

    wallets = collect_wallets(output_dir=output_dir, leaderboard_only=args.leaderboard_only)
    LOGGER.info("Wallet collection completed with %s wallets", len(wallets))

    results = analyze_wallets(
        wallets=wallets,
        output_dir=output_dir,
        period_days=args.period_days,
        max_wallets=args.max_wallets,
        use_subgraph=args.use_subgraph,
    )
    LOGGER.info("Analysis completed for %s wallets", len(results))


if __name__ == "__main__":
    main()
