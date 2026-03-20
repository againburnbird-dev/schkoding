#!/usr/bin/env python3
"""
Polymarket wallet collector and profitability analyzer.

Examples:
    python polymarket_analyzer.py
    python polymarket_analyzer.py --max_wallets 500 --period_days 180 --output_dir ./polymarket_analysis
    python polymarket_analyzer.py --max_wallets 2000 --subgraph_wallet_pages 500 --workers 0
    python polymarket_analyzer.py --leaderboard_only --max_cpu_percent 30 --max_mem_percent 30

Warning:
    Public Polymarket and Goldsky endpoints can throttle aggressive crawlers. This script uses retries,
    adaptive worker selection, request pacing, local response caching, market batching, and buffered writes.
    Tune `--workers`, `--max_cpu_percent`, `--max_mem_percent`, and `--request_interval_seconds` conservatively
    on a VPS. Default adaptive limits target no more than ~40% CPU/memory pressure for the analysis workers.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import hashlib
import json
import logging
import math
import os
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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
DUNE_API_BASE = "https://api.dune.com/api/v1"

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "polymarket-analyzer/2.0",
}
GRAPHQL_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "polymarket-analyzer/2.0",
}

RETRY_BACKOFF_SECONDS = [1, 5, 10]
NON_RETRYABLE_HTTP_STATUS = {400, 401, 403, 404, 422}
DEFAULT_REQUEST_INTERVAL_SECONDS = 0.6
REQUEST_JITTER_SECONDS = 0.25

LEADERBOARD_CATEGORY = "OVERALL"
LEADERBOARD_TIME_PERIODS = ["ALL", "MONTH", "WEEK", "DAY"]
LEADERBOARD_ORDER_BY = ["PNL", "VOL"]
LEADERBOARD_PAGE_LIMIT = 50
LEADERBOARD_MAX_OFFSET = 1000

# Latest documented caps for /trades changed on August 26, 2025.
TRADES_PAGE_LIMIT = 500
TRADES_MAX_OFFSET = 1000
CLOSED_POSITIONS_PAGE_LIMIT = 50
CLOSED_POSITIONS_MAX_OFFSET = 100000
MARKET_LOOKUP_CHUNK_SIZE = 50
SUBGRAPH_PAGE_SIZE = 1000
DEFAULT_SUBGRAPH_WALLET_PAGES = 200

DEFAULT_MAX_WALLETS = 100
DEFAULT_PERIOD_DAYS = 180
DEFAULT_MAX_CPU_PERCENT = 40.0
DEFAULT_MAX_MEM_PERCENT = 40.0
DEFAULT_PER_WORKER_MEMORY_MB = 256
DEFAULT_HTTP_CACHE_TTL_SECONDS = 6 * 60 * 60
MARKET_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60
REPORT_FLUSH_EVERY = 25
DEFAULT_RPC_BLOCK_CHUNK = 50000
DEFAULT_RPC_START_BLOCK = 40000000
ADDRESS_REGEX = re.compile(r"0x[a-fA-F0-9]{40}")

POLYMARKET_CONTRACTS = {
    "ctf_exchange": "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "neg_risk_ctf_exchange": "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "ctf": "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
    "polymarket_proxy_factory": "0xab45c5a4b0c941a2f231c04c3f49182e1a254052",
    "gnosis_safe_factory": "0xaacfeea03eb1561c4e67d661e40682bd20e3541b",
}

LOGGER = logging.getLogger("polymarket_analyzer")
HTTP_RATE_LOCK = threading.Lock()
HTTP_NEXT_ALLOWED_TS = 0.0
CACHE_WRITE_LOCK = threading.Lock()


# =========================
# Logging and filesystem
# =========================
def setup_logging(output_dir: str) -> logging.Logger:
    """Configure console and file logging."""
    os.makedirs(output_dir, exist_ok=True)
    log_path = os.path.join(output_dir, "app.log")

    logger = logging.getLogger("polymarket_analyzer")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
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


def ensure_dir(path: str) -> str:
    """Create a directory if needed and return its path."""
    os.makedirs(path, exist_ok=True)
    return path


def save_json(path: str, payload: Any) -> None:
    """Persist JSON with UTF-8 encoding."""
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_json(path: str, default: Any = None) -> Any:
    """Load JSON or return a default value when the file is absent/broken."""
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError, MemoryError):
        return default


# =========================
# System profiling and worker sizing
# =========================
def get_total_memory_bytes() -> int:
    """Best-effort cross-platform total physical memory detection using only stdlib."""
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/meminfo", "r", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("MemTotal:"):
                        parts = line.split()
                        return int(parts[1]) * 1024
        except OSError:
            return 0

    if sys.platform == "darwin":
        try:
            return int(os.popen("sysctl -n hw.memsize").read().strip())
        except (OSError, ValueError):
            return 0

    if os.name == "nt":
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        memory_status = MEMORYSTATUSEX()
        memory_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory_status)):  # type: ignore[attr-defined]
            return int(memory_status.ullTotalPhys)
    return 0


def detect_system_profile() -> Dict[str, Any]:
    """Collect a lightweight machine profile for adaptive worker sizing."""
    cpu_count = os.cpu_count() or 1
    memory_bytes = get_total_memory_bytes()
    memory_gb = round(memory_bytes / (1024 ** 3), 2) if memory_bytes else 0.0
    return {
        "cpu_count": cpu_count,
        "total_memory_bytes": memory_bytes,
        "total_memory_gb": memory_gb,
    }


def choose_worker_count(
    system_profile: Dict[str, Any],
    requested_workers: int,
    max_cpu_percent: float,
    max_mem_percent: float,
    per_worker_memory_mb: int,
) -> int:
    """Pick a conservative thread count for network-bound work."""
    if requested_workers > 0:
        return max(1, requested_workers)

    cpu_count = max(1, int(system_profile.get("cpu_count") or 1))
    cpu_based = max(1, int(math.floor(cpu_count * max_cpu_percent / 100.0)))

    memory_bytes = int(system_profile.get("total_memory_bytes") or 0)
    if memory_bytes > 0 and per_worker_memory_mb > 0:
        allowed_memory_bytes = memory_bytes * max_mem_percent / 100.0
        memory_based = max(1, int(allowed_memory_bytes // (per_worker_memory_mb * 1024 * 1024)))
    else:
        memory_based = cpu_based

    network_bonus = 2
    return max(1, min(32, cpu_based + network_bonus, memory_based))


# =========================
# Primitive helpers
# =========================
def epoch_now() -> int:
    """Return current UTC epoch seconds."""
    return int(datetime.now(timezone.utc).timestamp())


def parse_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if value is None:
        return default
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        normalized = str(value).replace(",", "").replace("$", "").strip()
        return float(normalized) if normalized else default
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
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def normalize_wallet(value: Any) -> Optional[str]:
    """Normalize an EVM wallet address."""
    if not value:
        return None
    wallet = str(value).strip().lower()
    if wallet.startswith("\\x") and len(wallet) == 42:
        wallet = "0x" + wallet[2:]
    if wallet.startswith("0x") and len(wallet) == 42:
        return wallet
    return None


def extract_timestamp(item: Dict[str, Any]) -> Optional[int]:
    """Extract a timestamp from common REST and subgraph field names."""
    for field in (
        "timestamp",
        "timeStamp",
        "createdAt",
        "created_at",
        "updatedAt",
        "lastActiveTimestamp",
    ):
        value = item.get(field)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            timestamp = int(value)
            return timestamp // 1000 if timestamp > 10 ** 12 else timestamp
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.isdigit():
                timestamp = int(stripped)
                return timestamp // 1000 if timestamp > 10 ** 12 else timestamp
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    dt = datetime.strptime(stripped, fmt).replace(tzinfo=timezone.utc)
                    return int(dt.timestamp())
                except ValueError:
                    continue
    return None


def unwrap_list_payload(payload: Any, keys: Iterable[str]) -> List[Dict[str, Any]]:
    """Normalize APIs that can return a raw list or a dict with list keys."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def chunked(values: List[str], chunk_size: int) -> Iterable[List[str]]:
    """Yield fixed-size chunks from a list."""
    for start in range(0, len(values), chunk_size):
        yield values[start : start + chunk_size]


def extract_wallets_from_text(value: Any) -> List[str]:
    """Extract all wallet-looking addresses from arbitrary text."""
    if value is None:
        return []
    return [match.group(0).lower() for match in ADDRESS_REGEX.finditer(str(value))]


def extract_wallets_from_object(value: Any) -> List[str]:
    """Recursively extract wallet-looking addresses from a nested object."""
    found = set()
    if isinstance(value, dict):
        for nested in value.values():
            found.update(extract_wallets_from_object(nested))
    elif isinstance(value, list):
        for nested in value:
            found.update(extract_wallets_from_object(nested))
    else:
        found.update(extract_wallets_from_text(value))
    return sorted(found)


# =========================
# Cache and rate limiting
# =========================
def get_cache_key(method: str, url: str, params: Optional[Dict[str, Any]], json_payload: Optional[Dict[str, Any]]) -> str:
    """Build a deterministic cache key for HTTP requests."""
    raw = json.dumps(
        {
            "method": method,
            "url": url,
            "params": params or {},
            "json": json_payload or {},
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_cached_http_response(cache_dir: str, cache_key: str, ttl_seconds: int) -> Any:
    """Load a cached HTTP response if the TTL is still valid."""
    cache_path = os.path.join(cache_dir, f"{cache_key}.json")
    cached = load_json(cache_path, default={})
    if not isinstance(cached, dict):
        return None
    created_at = parse_int(cached.get("created_at"))
    if created_at and epoch_now() - created_at <= ttl_seconds:
        return cached.get("payload")
    return None


def set_cached_http_response(cache_dir: str, cache_key: str, payload: Any) -> None:
    """Persist a GET response in the local HTTP cache."""
    cache_path = os.path.join(cache_dir, f"{cache_key}.json")
    with CACHE_WRITE_LOCK:
        save_json(cache_path, {"created_at": epoch_now(), "payload": payload})


def rate_limit_wait(min_interval_seconds: float) -> None:
    """Global pacing guard shared by all worker threads."""
    global HTTP_NEXT_ALLOWED_TS
    with HTTP_RATE_LOCK:
        now = time.time()
        if now < HTTP_NEXT_ALLOWED_TS:
            time.sleep(HTTP_NEXT_ALLOWED_TS - now)
        HTTP_NEXT_ALLOWED_TS = time.time() + min_interval_seconds + uniform(0.0, REQUEST_JITTER_SECONDS)


# =========================
# HTTP clients
# =========================
def request_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    cache_dir: Optional[str] = None,
    cache_ttl_seconds: int = 0,
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
) -> Any:
    """Perform an HTTP request with retries, pacing, and optional local caching."""
    merged_headers = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(headers)

    if method.upper() == "GET" and cache_dir and cache_ttl_seconds > 0:
        cache_key = get_cache_key(method, url, params, json_payload)
        cached_payload = get_cached_http_response(cache_dir, cache_key, cache_ttl_seconds)
        if cached_payload is not None:
            return cached_payload
    else:
        cache_key = ""

    total_attempts = len(RETRY_BACKOFF_SECONDS)
    for attempt_index, backoff in enumerate(RETRY_BACKOFF_SECONDS, start=1):
        try:
            rate_limit_wait(request_interval_seconds)
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_payload,
                headers=merged_headers,
                timeout=timeout,
            )
            if response.status_code >= 400:
                raise requests.HTTPError(
                    f"HTTP {response.status_code} for {url}: {response.text[:400]}",
                    response=response,
                )
            payload = response.json() if response.text.strip() else None
            if method.upper() == "GET" and cache_dir and cache_ttl_seconds > 0:
                set_cached_http_response(cache_dir, cache_key, payload)
            return payload
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


def request_json_or_empty(*args: Any, **kwargs: Any) -> Any:
    """Wrap request_json and turn permanent HTTP failures into empty results."""
    try:
        return request_json(*args, **kwargs)
    except requests.RequestException as exc:
        LOGGER.error("API call failed permanently: %s", exc)
        return None


def graphql_query(
    url: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    *,
    cache_dir: Optional[str] = None,
    cache_ttl_seconds: int = 0,
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
) -> Dict[str, Any]:
    """Execute a GraphQL request against a Goldsky subgraph."""
    payload = request_json_or_empty(
        "POST",
        url,
        json_payload={"query": query, "variables": variables or {}},
        headers=GRAPHQL_HEADERS,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
        request_interval_seconds=request_interval_seconds,
    )
    if not isinstance(payload, dict):
        return {}
    if payload.get("errors"):
        LOGGER.error("GraphQL returned errors for %s: %s", url, payload["errors"])
    return payload


def graphql_introspect_query_fields(
    url: str,
    http_cache_dir: str,
    request_interval_seconds: float,
) -> Dict[str, str]:
    """Return a mapping of query field name -> item type name for a subgraph endpoint."""
    query = """
    query IntrospectQueryFields {
      __schema {
        queryType {
          fields {
            name
            type {
              name
              kind
              ofType {
                name
                kind
                ofType {
                  name
                  kind
                }
              }
            }
          }
        }
      }
    }
    """
    payload = graphql_query(
        url,
        query,
        cache_dir=http_cache_dir,
        cache_ttl_seconds=24 * 60 * 60,
        request_interval_seconds=request_interval_seconds,
    )
    result: Dict[str, str] = {}
    fields = payload.get("data", {}).get("__schema", {}).get("queryType", {}).get("fields", [])
    for field in fields:
        type_info = field.get("type") or {}
        item_type = (
            type_info.get("name")
            or (type_info.get("ofType") or {}).get("name")
            or ((type_info.get("ofType") or {}).get("ofType") or {}).get("name")
        )
        if field.get("name") and item_type:
            result[str(field["name"])] = str(item_type)
    return result


def graphql_introspect_type_fields(
    url: str,
    type_name: str,
    http_cache_dir: str,
    request_interval_seconds: float,
) -> List[str]:
    """Return field names for a GraphQL object type."""
    query = """
    query IntrospectType($typeName: String!) {
      __type(name: $typeName) {
        fields {
          name
        }
      }
    }
    """
    payload = graphql_query(
        url,
        query,
        {"typeName": type_name},
        cache_dir=http_cache_dir,
        cache_ttl_seconds=24 * 60 * 60,
        request_interval_seconds=request_interval_seconds,
    )
    return [
        item.get("name")
        for item in payload.get("data", {}).get("__type", {}).get("fields", [])
        if item.get("name")
    ]


# =========================
# Market cache and lookup
# =========================
def market_cache_dir(cache_dir: str) -> str:
    """Return the sharded market cache directory."""
    return ensure_dir(os.path.join(cache_dir, "market_objects"))


def get_market_cache(cache_dir: str) -> Dict[str, Dict[str, Any]]:
    """Keep only an in-memory per-run market cache to avoid loading oversized monolithic files."""
    legacy_cache_path = os.path.join(cache_dir, "markets.json")
    if os.path.exists(legacy_cache_path):
        LOGGER.warning(
            "Legacy market cache detected at %s; it will be ignored in favor of sharded on-demand cache files.",
            legacy_cache_path,
        )
    return {}


def load_market_cache_entries(cache_dir: str, condition_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """Load sharded cached market records for a set of condition IDs."""
    base_dir = market_cache_dir(cache_dir)
    loaded: Dict[str, Dict[str, Any]] = {}
    for condition_id in condition_ids:
        normalized = str(condition_id)
        cache_path = os.path.join(base_dir, f"{normalized}.json")
        cached = load_json(cache_path, default={})
        if not isinstance(cached, dict):
            continue
        fetched_at = parse_int(cached.get("fetched_at"))
        if fetched_at and epoch_now() - fetched_at <= MARKET_CACHE_MAX_AGE_SECONDS:
            market = cached.get("market")
            if isinstance(market, dict):
                loaded[normalized] = market
    return loaded


def save_market_cache(cache_dir: str, markets: Dict[str, Dict[str, Any]]) -> None:
    """Persist market records as individual sharded JSON files."""
    base_dir = market_cache_dir(cache_dir)
    with CACHE_WRITE_LOCK:
        for condition_id, market in markets.items():
            cache_path = os.path.join(base_dir, f"{condition_id}.json")
            save_json(cache_path, {"fetched_at": epoch_now(), "market": market})


def build_market_record(market: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Gamma market metadata."""
    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            outcomes = [item.strip() for item in outcomes.split(",") if item.strip()]

    winning_outcome = (
        market.get("outcome")
        or market.get("resolvedOutcome")
        or market.get("winningOutcome")
        or market.get("resolution")
    )
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


def fetch_markets_by_condition_ids(
    condition_ids: Iterable[str],
    http_cache_dir: str,
    request_interval_seconds: float,
) -> Dict[str, Dict[str, Any]]:
    """Fetch market metadata in batches using `condition_ids` to avoid invalid direct-id lookups."""
    unique_ids = sorted({str(item) for item in condition_ids if item})
    if not unique_ids:
        return {}

    resolved: Dict[str, Dict[str, Any]] = {}
    for batch in chunked(unique_ids, MARKET_LOOKUP_CHUNK_SIZE):
        payload = request_json_or_empty(
            "GET",
            f"{GAMMA_API_BASE}/markets",
            params={"condition_ids": batch},
            cache_dir=http_cache_dir,
            cache_ttl_seconds=MARKET_CACHE_MAX_AGE_SECONDS,
            request_interval_seconds=request_interval_seconds,
        )
        rows = unwrap_list_payload(payload, ("markets", "data", "results"))
        for market in rows:
            normalized = build_market_record(market)
            condition_id = normalized.get("condition_id")
            if condition_id:
                resolved[str(condition_id)] = normalized
    return resolved


# =========================
# Data API: leaderboard, trades, positions
# =========================
def get_leaderboard(http_cache_dir: str, request_interval_seconds: float) -> Dict[str, Dict[str, float]]:
    """Collect leaderboard entries across supported periods and sort modes."""
    wallets: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"wallet": "", "volume_usd": 0.0, "pnl_usd": 0.0}
    )
    total_queries = len(LEADERBOARD_TIME_PERIODS) * len(LEADERBOARD_ORDER_BY)
    progress = tqdm(total=total_queries, desc="Leaderboard queries")

    for time_period in LEADERBOARD_TIME_PERIODS:
        for order_by in LEADERBOARD_ORDER_BY:
            for offset in range(0, LEADERBOARD_MAX_OFFSET + LEADERBOARD_PAGE_LIMIT, LEADERBOARD_PAGE_LIMIT):
                payload = request_json_or_empty(
                    "GET",
                    f"{DATA_API_BASE}/v1/leaderboard",
                    params={
                        "category": LEADERBOARD_CATEGORY,
                        "timePeriod": time_period,
                        "orderBy": order_by,
                        "limit": LEADERBOARD_PAGE_LIMIT,
                        "offset": offset,
                    },
                    cache_dir=http_cache_dir,
                    cache_ttl_seconds=12 * 60 * 60,
                    request_interval_seconds=request_interval_seconds,
                )
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
                    row = wallets[wallet]
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
    return dict(wallets)


def fetch_trades_page(
    user: Optional[str],
    offset: int,
    http_cache_dir: str,
    request_interval_seconds: float,
) -> List[Dict[str, Any]]:
    """Fetch one capped Data API trades page."""
    payload = request_json_or_empty(
        "GET",
        f"{DATA_API_BASE}/trades",
        params={
            "limit": TRADES_PAGE_LIMIT,
            "offset": min(offset, TRADES_MAX_OFFSET),
            **({"user": user} if user else {}),
        },
        cache_dir=http_cache_dir,
        cache_ttl_seconds=DEFAULT_HTTP_CACHE_TTL_SECONDS,
        request_interval_seconds=request_interval_seconds,
    )
    return unwrap_list_payload(payload, ("trades", "data", "results"))


def fetch_wallet_trades(wallet: str, cutoff_ts: int, http_cache_dir: str, request_interval_seconds: float) -> List[Dict[str, Any]]:
    """Fetch a wallet's trades via Data API with time-based early stop."""
    trades: List[Dict[str, Any]] = []
    for offset in range(0, TRADES_MAX_OFFSET + TRADES_PAGE_LIMIT, TRADES_PAGE_LIMIT):
        page = fetch_trades_page(wallet, offset, http_cache_dir, request_interval_seconds)
        if not page:
            break
        reached_cutoff = False
        for trade in page:
            timestamp = extract_timestamp(trade)
            if timestamp is None or timestamp >= cutoff_ts:
                trades.append(trade)
            else:
                reached_cutoff = True
        if len(page) < TRADES_PAGE_LIMIT or reached_cutoff:
            break
    return trades


def fetch_closed_positions(wallet: str, http_cache_dir: str, request_interval_seconds: float) -> List[Dict[str, Any]]:
    """Fetch closed positions for a wallet."""
    records: List[Dict[str, Any]] = []
    for offset in range(0, CLOSED_POSITIONS_MAX_OFFSET + CLOSED_POSITIONS_PAGE_LIMIT, CLOSED_POSITIONS_PAGE_LIMIT):
        payload = request_json_or_empty(
            "GET",
            f"{DATA_API_BASE}/closed-positions",
            params={
                "user": wallet,
                "limit": CLOSED_POSITIONS_PAGE_LIMIT,
                "offset": offset,
                "sortBy": "TIMESTAMP",
            },
            cache_dir=http_cache_dir,
            cache_ttl_seconds=DEFAULT_HTTP_CACHE_TTL_SECONDS,
            request_interval_seconds=request_interval_seconds,
        )
        page = unwrap_list_payload(payload, ("positions", "data", "results"))
        if not page:
            break
        records.extend(page)
        if len(page) < CLOSED_POSITIONS_PAGE_LIMIT:
            break
    return records


# =========================
# Subgraph discovery and fallback
# =========================
def fetch_subgraph_wallets(
    url: str,
    entity_candidates: List[str],
    max_pages: int,
    http_cache_dir: str,
    request_interval_seconds: float,
) -> List[str]:
    """Scan a Goldsky entity and extract unique wallet fields."""
    if max_pages <= 0:
        return []

    query_fields = graphql_introspect_query_fields(url, http_cache_dir, request_interval_seconds)
    wallet_field_candidates = ["user", "owner", "account", "proxyWallet", "maker", "taker"]
    entity_name = None
    query = ""
    selected_wallet_fields: List[str] = []

    def build_query(root_name: str, wallet_fields: List[str]) -> str:
        requested_fields = "\n        ".join(wallet_fields)
        return f"""
        query ScanEntity($first: Int!, $skip: Int!) {{
          {root_name}(
            first: $first
            skip: $skip
            orderBy: id
            orderDirection: asc
          ) {{
            {requested_fields}
          }}
        }}
        """

    for candidate in entity_candidates:
        type_name = query_fields.get(candidate, "")
        type_fields = (
            graphql_introspect_type_fields(url, type_name, http_cache_dir, request_interval_seconds)
            if type_name
            else []
        )
        wallet_fields = [field for field in wallet_field_candidates if field in type_fields]
        if not wallet_fields and type_fields:
            if "order" in candidate.lower():
                wallet_fields = [field for field in ("maker", "taker") if field in type_fields]
            elif "position" in candidate.lower() or "balance" in candidate.lower():
                wallet_fields = [field for field in ("user", "proxyWallet") if field in type_fields]
        if not wallet_fields:
            if "order" in candidate.lower():
                wallet_fields = ["maker", "taker"]
            elif "position" in candidate.lower() or "balance" in candidate.lower():
                wallet_fields = ["user", "proxyWallet"]
            else:
                wallet_fields = ["user"]
        if not wallet_fields:
            continue

        candidate_query = build_query(candidate, wallet_fields)
        payload = graphql_query(
            url,
            candidate_query,
            {"first": 1, "skip": 0},
            cache_dir=http_cache_dir,
            cache_ttl_seconds=DEFAULT_HTTP_CACHE_TTL_SECONDS,
            request_interval_seconds=request_interval_seconds,
        )
        rows = payload.get("data", {}).get(candidate, []) if isinstance(payload, dict) else []
        if isinstance(rows, list):
            entity_name = candidate
            query = candidate_query
            selected_wallet_fields = wallet_fields
            break

    if not entity_name:
        LOGGER.warning("No supported query root found for %s. Available fields: %s", url, sorted(query_fields))
        return []

    wallets = set()
    for page_index in tqdm(range(max_pages), desc=f"Subgraph {entity_name}"):
        payload = graphql_query(
            url,
            query,
            {"first": SUBGRAPH_PAGE_SIZE, "skip": page_index * SUBGRAPH_PAGE_SIZE},
            cache_dir=http_cache_dir,
            cache_ttl_seconds=DEFAULT_HTTP_CACHE_TTL_SECONDS,
            request_interval_seconds=request_interval_seconds,
        )
        rows = payload.get("data", {}).get(entity_name, []) if isinstance(payload, dict) else []
        if not rows:
            break
        for row in rows:
            for field_name in selected_wallet_fields:
                wallet = normalize_wallet(row.get(field_name))
                if wallet:
                    wallets.add(wallet)
        if len(rows) < SUBGRAPH_PAGE_SIZE:
            break
    return sorted(wallets)


def fetch_wallet_trades_subgraph(
    wallet: str,
    cutoff_ts: int,
    http_cache_dir: str,
    request_interval_seconds: float,
) -> List[Dict[str, Any]]:
    """Fetch wallet order rows from the orderbook subgraph."""
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
    results: List[Dict[str, Any]] = []
    for page_index in range(DEFAULT_SUBGRAPH_WALLET_PAGES):
        payload = graphql_query(
            ORDERBOOK_SUBGRAPH_URL,
            query,
            {"wallet": wallet, "first": SUBGRAPH_PAGE_SIZE, "skip": page_index * SUBGRAPH_PAGE_SIZE},
            cache_dir=http_cache_dir,
            cache_ttl_seconds=DEFAULT_HTTP_CACHE_TTL_SECONDS,
            request_interval_seconds=request_interval_seconds,
        )
        rows = payload.get("data", {}).get("orders", []) if isinstance(payload, dict) else []
        if not rows:
            break
        reached_cutoff = False
        for row in rows:
            timestamp = extract_timestamp(row)
            if timestamp is not None and timestamp < cutoff_ts:
                reached_cutoff = True
                continue
            results.append(row)
        if len(rows) < SUBGRAPH_PAGE_SIZE or reached_cutoff:
            break
    return results


def fetch_wallet_pnl_subgraph(wallet: str, http_cache_dir: str, request_interval_seconds: float) -> List[Dict[str, Any]]:
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
    payload = graphql_query(
        PNL_SUBGRAPH_URL,
        query,
        {"wallet": wallet},
        cache_dir=http_cache_dir,
        cache_ttl_seconds=DEFAULT_HTTP_CACHE_TTL_SECONDS,
        request_interval_seconds=request_interval_seconds,
    )
    return payload.get("data", {}).get("pnls", []) if isinstance(payload, dict) else []


def fetch_wallet_positions_subgraph(wallet: str, http_cache_dir: str, request_interval_seconds: float) -> List[Dict[str, Any]]:
    """Fetch position rows from the positions subgraph."""
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
    payload = graphql_query(
        POSITIONS_SUBGRAPH_URL,
        query,
        {"wallet": wallet},
        cache_dir=http_cache_dir,
        cache_ttl_seconds=DEFAULT_HTTP_CACHE_TTL_SECONDS,
        request_interval_seconds=request_interval_seconds,
    )
    return payload.get("data", {}).get("positions", []) if isinstance(payload, dict) else []


def fetch_dune_wallets(
    http_cache_dir: str,
    request_interval_seconds: float,
    query_id: int,
    api_key: str,
) -> List[str]:
    """Fetch wallet addresses from a Dune query result set."""
    headers = {"X-Dune-API-Key": api_key}
    payload = request_json_or_empty(
        "GET",
        f"{DUNE_API_BASE}/query/{query_id}/results",
        headers=headers,
        cache_dir=http_cache_dir,
        cache_ttl_seconds=12 * 60 * 60,
        request_interval_seconds=request_interval_seconds,
    )
    rows = payload.get("result", {}).get("rows", []) if isinstance(payload, dict) else []
    wallets = set()
    for row in rows:
        wallets.update(extract_wallets_from_object(row))
    return sorted(wallets)


def rpc_call(
    rpc_url: str,
    method: str,
    params: List[Any],
    request_interval_seconds: float,
) -> Any:
    """Execute a JSON-RPC request against a Polygon node."""
    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000) % 10**9,
        "method": method,
        "params": params,
    }
    response = request_json_or_empty(
        "POST",
        rpc_url,
        json_payload=payload,
        headers={"Content-Type": "application/json"},
        request_interval_seconds=request_interval_seconds,
    )
    if isinstance(response, dict) and response.get("error"):
        LOGGER.error("RPC error for %s: %s", method, response["error"])
    return response.get("result") if isinstance(response, dict) else None


def scan_contract_logs_for_wallets(
    rpc_url: str,
    contract_addresses: Iterable[str],
    from_block: int,
    to_block: int,
    block_chunk: int,
    request_interval_seconds: float,
) -> List[str]:
    """Collect wallet addresses from raw logs and tx senders for Polymarket-related contracts."""
    wallets = set()
    normalized_addresses = [str(address).lower() for address in contract_addresses if address]
    for start_block in tqdm(range(from_block, to_block + 1, block_chunk), desc="RPC log scan"):
        end_block = min(start_block + block_chunk - 1, to_block)
        logs = rpc_call(
            rpc_url,
            "eth_getLogs",
            [
                {
                    "fromBlock": hex(start_block),
                    "toBlock": hex(end_block),
                    "address": normalized_addresses,
                }
            ],
            request_interval_seconds,
        )
        if not isinstance(logs, list):
            continue
        tx_hashes = set()
        for log in logs:
            wallets.update(extract_wallets_from_object(log))
            tx_hash = log.get("transactionHash")
            if tx_hash:
                tx_hashes.add(tx_hash)
        for tx_hash in tx_hashes:
            tx = rpc_call(rpc_url, "eth_getTransactionByHash", [tx_hash], request_interval_seconds)
            if isinstance(tx, dict):
                wallets.update(extract_wallets_from_object(tx))
    return sorted(wallets)


# =========================
# Wallet collection
# =========================
def collect_wallets(
    output_dir: str,
    leaderboard_only: bool,
    subgraph_wallet_pages: int,
    http_cache_dir: str,
    request_interval_seconds: float,
    dune_query_id: int,
    dune_api_key: str,
    polygon_rpc_url: str,
    rpc_start_block: int,
    rpc_end_block: int,
    rpc_block_chunk: int,
) -> List[Dict[str, Any]]:
    """Collect the broadest possible wallet universe for downstream filtering."""
    source_counts: Dict[str, int] = defaultdict(int)
    wallets_map = get_leaderboard(http_cache_dir, request_interval_seconds)
    source_counts["leaderboard"] = len(wallets_map)
    LOGGER.info("Collected %s unique wallets from leaderboard", len(wallets_map))

    if not leaderboard_only:
        for offset in tqdm(range(0, TRADES_MAX_OFFSET + TRADES_PAGE_LIMIT, TRADES_PAGE_LIMIT), desc="Public trades"):
            rows = fetch_trades_page(None, offset, http_cache_dir, request_interval_seconds)
            if not rows:
                break
            for trade in rows:
                wallet = normalize_wallet(
                    trade.get("proxyWallet")
                    or trade.get("user")
                    or trade.get("wallet")
                    or trade.get("maker")
                    or trade.get("owner")
                )
                if wallet and wallet not in wallets_map:
                    wallets_map[wallet] = {"wallet": wallet, "volume_usd": 0.0, "pnl_usd": 0.0}
                    source_counts["public_trades"] += 1
            if len(rows) < TRADES_PAGE_LIMIT:
                break

        subgraph_sources = [
            (
                ORDERBOOK_SUBGRAPH_URL,
                "orderbook",
                ["orderFilledEvents", "ordersMatchedEvents", "orderFilledEvent", "ordersMatchedEvent", "marketData"],
            ),
            (PNL_SUBGRAPH_URL, "pnl", ["userPositions", "userPosition"]),
            (POSITIONS_SUBGRAPH_URL, "positions", ["userBalances", "netUserBalances", "userBalance", "netUserBalance"]),
        ]
        for url, label, entity_candidates in subgraph_sources:
            discovered = fetch_subgraph_wallets(
                url,
                entity_candidates,
                subgraph_wallet_pages,
                http_cache_dir,
                request_interval_seconds,
            )
            added = 0
            for wallet in discovered:
                if wallet not in wallets_map:
                    wallets_map[wallet] = {"wallet": wallet, "volume_usd": 0.0, "pnl_usd": 0.0}
                    added += 1
            source_counts[f"subgraph_{label}"] = added
            LOGGER.info("Added %s wallets from %s subgraph scan", added, label)

        if dune_query_id > 0 and dune_api_key:
            dune_wallets = fetch_dune_wallets(http_cache_dir, request_interval_seconds, dune_query_id, dune_api_key)
            added = 0
            for wallet in dune_wallets:
                if wallet not in wallets_map:
                    wallets_map[wallet] = {"wallet": wallet, "volume_usd": 0.0, "pnl_usd": 0.0}
                    added += 1
            source_counts["dune"] = added
            LOGGER.info("Added %s wallets from Dune query %s", added, dune_query_id)

        if polygon_rpc_url:
            rpc_wallets = scan_contract_logs_for_wallets(
                polygon_rpc_url,
                [
                    POLYMARKET_CONTRACTS["ctf_exchange"],
                    POLYMARKET_CONTRACTS["neg_risk_ctf_exchange"],
                    POLYMARKET_CONTRACTS["ctf"],
                    POLYMARKET_CONTRACTS["polymarket_proxy_factory"],
                    POLYMARKET_CONTRACTS["gnosis_safe_factory"],
                ],
                rpc_start_block,
                rpc_end_block,
                rpc_block_chunk,
                request_interval_seconds,
            )
            added = 0
            for wallet in rpc_wallets:
                if wallet not in wallets_map:
                    wallets_map[wallet] = {"wallet": wallet, "volume_usd": 0.0, "pnl_usd": 0.0}
                    added += 1
            source_counts["raw_rpc_logs"] = added
            LOGGER.info("Added %s wallets from raw Polygon RPC log scan", added)

    wallets = [item for item in wallets_map.values() if normalize_wallet(item.get("wallet"))]
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
    LOGGER.info("Wallet source breakdown: %s", dict(source_counts))
    return wallets


# =========================
# Analysis helpers
# =========================
def infer_condition_id(item: Dict[str, Any]) -> Optional[str]:
    """Infer a market grouping key."""
    for field in ("conditionId", "market", "marketId", "questionID", "slug"):
        value = item.get(field)
        if value:
            return str(value)
    return None


def infer_trade_side(trade: Dict[str, Any]) -> str:
    """Infer buy/sell side from a trade row."""
    raw = str(trade.get("side") or trade.get("type") or trade.get("action") or "").strip().lower()
    if raw in {"buy", "bid", "bought"}:
        return "buy"
    if raw in {"sell", "ask", "sold"}:
        return "sell"
    return raw or "unknown"


def infer_trade_outcome(trade: Dict[str, Any]) -> str:
    """Infer outcome label from a trade row."""
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
    """Estimate trade notional in USD."""
    for field in ("volume", "volumeUsd", "usdcSize", "amountUsd", "notionalUsd"):
        value = parse_float(trade.get(field))
        if value > 0:
            return value
    price = parse_float(trade.get("price") or trade.get("avgPrice"))
    size = parse_float(
        trade.get("size") or trade.get("amount") or trade.get("shares") or trade.get("totalBought")
    )
    return price * size if price > 0 and size > 0 else 0.0


def sum_realized_pnl(records: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    """Aggregate realized PnL by condition id from heterogeneous sources."""
    totals: Dict[str, float] = defaultdict(float)
    for row in records:
        condition_id = infer_condition_id(row)
        if not condition_id:
            continue
        totals[condition_id] += parse_float(
            row.get("realizedPnl") or row.get("pnl") or row.get("profit") or row.get("amount")
        )
    return dict(totals)


def classify_market_win(
    trades: List[Dict[str, Any]],
    market: Dict[str, Any],
    realized_pnl: float,
) -> Tuple[bool, Dict[str, Any]]:
    """Classify a resolved market as win or loss."""
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
    predicted_win = False
    if winning_outcome == "yes":
        predicted_win = net_yes > net_no
    elif winning_outcome == "no":
        predicted_win = net_no > net_yes

    is_win = realized_pnl > 0 or predicted_win
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


# =========================
# Wallet analysis
# =========================
def analyze_wallet(
    wallet: str,
    period_days: int,
    use_subgraph: bool,
    cache_dir: str,
    http_cache_dir: str,
    markets_cache: Dict[str, Dict[str, Any]],
    markets_cache_lock: threading.Lock,
    request_interval_seconds: float,
) -> Dict[str, Any]:
    """Analyze one wallet and compute profitability metrics for the last N days."""
    cutoff_ts = epoch_now() - period_days * 24 * 60 * 60
    wallet_trades = (
        fetch_wallet_trades_subgraph(wallet, cutoff_ts, http_cache_dir, request_interval_seconds)
        if use_subgraph
        else fetch_wallet_trades(wallet, cutoff_ts, http_cache_dir, request_interval_seconds)
    )
    closed_positions = [] if use_subgraph else fetch_closed_positions(wallet, http_cache_dir, request_interval_seconds)
    pnl_subgraph = fetch_wallet_pnl_subgraph(wallet, http_cache_dir, request_interval_seconds) if use_subgraph else []
    positions_subgraph = (
        fetch_wallet_positions_subgraph(wallet, http_cache_dir, request_interval_seconds) if use_subgraph else []
    )

    realized_pnl_map = sum_realized_pnl(closed_positions)
    for pnl_map in (sum_realized_pnl(pnl_subgraph), sum_realized_pnl(positions_subgraph)):
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
        timestamp = extract_timestamp(trade)
        if timestamp is not None:
            trade_timestamps.append(timestamp)

    missing_condition_ids = []
    with markets_cache_lock:
        for condition_id in trades_by_market:
            if condition_id not in markets_cache:
                missing_condition_ids.append(condition_id)
        disk_cached_markets = load_market_cache_entries(cache_dir, missing_condition_ids)
        if disk_cached_markets:
            markets_cache.update(disk_cached_markets)
            missing_condition_ids = [
                condition_id for condition_id in missing_condition_ids if condition_id not in disk_cached_markets
            ]

    if missing_condition_ids:
        fetched_markets = fetch_markets_by_condition_ids(
            missing_condition_ids,
            http_cache_dir,
            request_interval_seconds,
        )
        if fetched_markets:
            with markets_cache_lock:
                markets_cache.update(fetched_markets)
                save_market_cache(cache_dir, markets_cache)

    winning_markets = 0
    total_resolved_markets = 0
    market_rows: List[Dict[str, Any]] = []

    with markets_cache_lock:
        local_market_snapshot = dict(markets_cache)

    for condition_id, market_trades in trades_by_market.items():
        market_info = local_market_snapshot.get(condition_id) or {
            "condition_id": condition_id,
            "market_id": condition_id,
            "question": "Unknown market",
            "resolved": False,
            "winning_outcome": None,
        }
        realized_pnl = realized_pnl_map.get(condition_id, 0.0)
        row = {
            "condition_id": condition_id,
            "market_id": market_info.get("market_id"),
            "question": market_info.get("question"),
            "resolved": bool(market_info.get("resolved")),
            "winning_outcome": market_info.get("winning_outcome"),
            "trade_count": len(market_trades),
            "volume_usd": round(sum(infer_trade_volume_usd(item) for item in market_trades), 6),
            "realized_pnl_usd": round(realized_pnl, 6),
        }
        if row["resolved"]:
            total_resolved_markets += 1
            is_win, diagnostics = classify_market_win(market_trades, market_info, realized_pnl)
            if is_win:
                winning_markets += 1
            row["is_win"] = is_win
            row.update(diagnostics)
        else:
            row["is_win"] = None
        market_rows.append(row)

    total_pnl_usd = sum(realized_pnl_map.get(condition_id, 0.0) for condition_id in trades_by_market)
    total_trades = sum(len(items) for items in trades_by_market.values())
    active_days = len(
        {
            datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
            for timestamp in trade_timestamps
        }
    )
    avg_bet_size = total_volume_usd / total_trades if total_trades else 0.0
    winrate = (winning_markets / total_resolved_markets * 100.0) if total_resolved_markets else 0.0
    roi = (total_pnl_usd / total_volume_usd * 100.0) if total_volume_usd else 0.0
    profitable = total_pnl_usd > 0

    return {
        "wallet": wallet,
        "period_days": period_days,
        "profitable": profitable,
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


def write_results_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    """Write aggregate wallet metrics to CSV."""
    fieldnames = [
        "wallet",
        "period_days",
        "profitable",
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
            writer.writerow({name: row.get(name) for name in fieldnames})


def analyze_wallets(
    wallets: List[Dict[str, Any]],
    output_dir: str,
    period_days: int,
    max_wallets: int,
    use_subgraph: bool,
    workers: int,
    request_interval_seconds: float,
) -> List[Dict[str, Any]]:
    """Analyze wallets concurrently and persist aggregate + per-wallet reports."""
    reports_dir = ensure_dir(os.path.join(output_dir, "reports"))
    cache_dir = ensure_dir(os.path.join(output_dir, "cache"))
    http_cache_dir = ensure_dir(os.path.join(cache_dir, "http"))
    markets_cache = get_market_cache(cache_dir)
    markets_cache_lock = threading.Lock()

    selected_wallets = wallets[:max_wallets]
    results: List[Dict[str, Any]] = []
    profitable_results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(
                analyze_wallet,
                wallet=row["wallet"],
                period_days=period_days,
                use_subgraph=use_subgraph,
                cache_dir=cache_dir,
                http_cache_dir=http_cache_dir,
                markets_cache=markets_cache,
                markets_cache_lock=markets_cache_lock,
                request_interval_seconds=request_interval_seconds,
            ): row["wallet"]
            for row in selected_wallets
        }

        progress = tqdm(total=len(future_map), desc="Wallet analysis")
        for index, future in enumerate(as_completed(future_map), start=1):
            wallet = future_map[future]
            try:
                report = future.result()
            except requests.RequestException as exc:
                LOGGER.error("Wallet analysis failed for %s: %s", wallet, exc)
                report = {
                    "wallet": wallet,
                    "period_days": period_days,
                    "profitable": False,
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
            results.append(report)
            if report.get("profitable"):
                profitable_results.append(report)
            save_json(os.path.join(reports_dir, f"{wallet}.json"), report)
            if index % REPORT_FLUSH_EVERY == 0:
                profitable_results.sort(
                    key=lambda item: (
                        parse_float(item.get("winrate_%")),
                        parse_float(item.get("total_pnl_usd")),
                        parse_float(item.get("roi")),
                    ),
                    reverse=True,
                )
                write_results_csv(os.path.join(output_dir, "results.csv"), profitable_results)
                save_json(os.path.join(output_dir, "detailed_report.json"), profitable_results)
            progress.update(1)
        progress.close()

    profitable_results.sort(
        key=lambda item: (
            parse_float(item.get("winrate_%")),
            parse_float(item.get("total_pnl_usd")),
            parse_float(item.get("roi")),
        ),
        reverse=True,
    )
    write_results_csv(os.path.join(output_dir, "results.csv"), profitable_results)
    save_json(os.path.join(output_dir, "detailed_report.json"), profitable_results)
    save_json(os.path.join(output_dir, "all_results.json"), results)
    LOGGER.info("Finished analysis: %s profitable wallets out of %s analyzed", len(profitable_results), len(results))
    return profitable_results


# =========================
# CLI
# =========================
def build_arg_parser() -> argparse.ArgumentParser:
    """Build the command-line interface."""
    parser = argparse.ArgumentParser(
        description="Collect Polymarket wallets, rank profitable traders over the last N days, and export candidate copy-trading reports."
    )
    parser.add_argument("--max_wallets", type=int, default=DEFAULT_MAX_WALLETS)
    parser.add_argument("--period_days", type=int, default=DEFAULT_PERIOD_DAYS)
    parser.add_argument("--output_dir", type=str, default="./polymarket_analysis")
    parser.add_argument("--use_subgraph", action="store_true")
    parser.add_argument("--leaderboard_only", action="store_true")
    parser.add_argument("--workers", type=int, default=0, help="0 = auto-detect safe worker count")
    parser.add_argument("--max_cpu_percent", type=float, default=DEFAULT_MAX_CPU_PERCENT)
    parser.add_argument("--max_mem_percent", type=float, default=DEFAULT_MAX_MEM_PERCENT)
    parser.add_argument("--per_worker_memory_mb", type=int, default=DEFAULT_PER_WORKER_MEMORY_MB)
    parser.add_argument("--subgraph_wallet_pages", type=int, default=DEFAULT_SUBGRAPH_WALLET_PAGES)
    parser.add_argument("--request_interval_seconds", type=float, default=DEFAULT_REQUEST_INTERVAL_SECONDS)
    parser.add_argument("--dune_query_id", type=int, default=0)
    parser.add_argument("--dune_api_key", type=str, default=os.getenv("DUNE_API_KEY", ""))
    parser.add_argument("--polygon_rpc_url", type=str, default=os.getenv("POLYGON_RPC_URL", ""))
    parser.add_argument("--rpc_start_block", type=int, default=DEFAULT_RPC_START_BLOCK)
    parser.add_argument("--rpc_end_block", type=int, default=0, help="0 = latest block at runtime is not resolved automatically here; set explicitly for full scans")
    parser.add_argument("--rpc_block_chunk", type=int, default=DEFAULT_RPC_BLOCK_CHUNK)
    return parser


def main() -> None:
    """Script entrypoint."""
    args = build_arg_parser().parse_args()
    output_dir = ensure_dir(args.output_dir)
    cache_dir = ensure_dir(os.path.join(output_dir, "cache"))
    http_cache_dir = ensure_dir(os.path.join(cache_dir, "http"))

    global LOGGER
    LOGGER = setup_logging(output_dir)

    system_profile = detect_system_profile()
    workers = choose_worker_count(
        system_profile,
        args.workers,
        args.max_cpu_percent,
        args.max_mem_percent,
        args.per_worker_memory_mb,
    )

    LOGGER.info("Starting Polymarket analyzer")
    LOGGER.info(
        "System profile: cpu_count=%s total_memory_gb=%s chosen_workers=%s",
        system_profile.get("cpu_count"),
        system_profile.get("total_memory_gb"),
        workers,
    )
    LOGGER.info(
        "Arguments: max_wallets=%s period_days=%s output_dir=%s use_subgraph=%s leaderboard_only=%s subgraph_wallet_pages=%s max_cpu_percent=%s max_mem_percent=%s request_interval_seconds=%s",
        args.max_wallets,
        args.period_days,
        args.output_dir,
        args.use_subgraph,
        args.leaderboard_only,
        args.subgraph_wallet_pages,
        args.max_cpu_percent,
        args.max_mem_percent,
        args.request_interval_seconds,
    )

    effective_rpc_end_block = args.rpc_end_block
    if args.polygon_rpc_url and effective_rpc_end_block <= 0:
        latest_block_hex = rpc_call(
            args.polygon_rpc_url,
            "eth_blockNumber",
            [],
            args.request_interval_seconds,
        )
        if isinstance(latest_block_hex, str) and latest_block_hex.startswith("0x"):
            effective_rpc_end_block = int(latest_block_hex, 16)
        else:
            effective_rpc_end_block = args.rpc_start_block

    wallets = collect_wallets(
        output_dir=output_dir,
        leaderboard_only=args.leaderboard_only,
        subgraph_wallet_pages=args.subgraph_wallet_pages,
        http_cache_dir=http_cache_dir,
        request_interval_seconds=args.request_interval_seconds,
        dune_query_id=args.dune_query_id,
        dune_api_key=args.dune_api_key,
        polygon_rpc_url=args.polygon_rpc_url,
        rpc_start_block=args.rpc_start_block,
        rpc_end_block=effective_rpc_end_block,
        rpc_block_chunk=args.rpc_block_chunk,
    )
    LOGGER.info("Wallet collection completed with %s wallets", len(wallets))

    results = analyze_wallets(
        wallets=wallets,
        output_dir=output_dir,
        period_days=args.period_days,
        max_wallets=args.max_wallets,
        use_subgraph=args.use_subgraph,
        workers=workers,
        request_interval_seconds=args.request_interval_seconds,
    )
    LOGGER.info("Analysis completed with %s profitable candidate wallets", len(results))


if __name__ == "__main__":
    main()
