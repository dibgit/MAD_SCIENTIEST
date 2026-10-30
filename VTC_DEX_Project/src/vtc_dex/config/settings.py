"""Environment-backed settings and project paths."""

import os
from pathlib import Path

import numpy as np
from dotenv import load_dotenv

from vtc_dex.security.secrets import get_secret


PROJECT_DIR = str(Path(__file__).resolve().parents[3])
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

GRID_EXECUTION_MODE = "OPTION_B"
if GRID_EXECUTION_MODE == "OPTION_A":
    MIN_ROUND_TRIP_GAP_PCT = np.float64(0.008)
    DEX_EXECUTION_FEE_RATE = np.float64(0.006)
    TARGET_NET_RETURN_PCT = np.float64(0.0015)
else:
    TARGET_NET_RETURN_PCT = np.float64(0.0015)
    BUY_COST_FRACTION = np.float64(0.006)
    SELL_COST_FRACTION = np.float64(0.006)
    MINIMUM_GROSS_PRICE_RATIO = (
        (np.float64(1.0) + TARGET_NET_RETURN_PCT)
        / (
            (np.float64(1.0) - BUY_COST_FRACTION)
            * (np.float64(1.0) - SELL_COST_FRACTION)
        )
    )
    MIN_ROUND_TRIP_GAP_PCT = MINIMUM_GROSS_PRICE_RATIO - 1.0
    DEX_EXECUTION_FEE_RATE = np.float64(0.006)

NUMBER_OF_LADDER = int(os.getenv("NUMBER_OF_LADDER", default=257))
if NUMBER_OF_LADDER < 2:
    raise ValueError("NUMBER_OF_LADDER must be at least 2.")

ORACLE_STAGNANT_TRACKER = {
    "frozen_baseline": np.float64(0.0),
    "last_price": np.float64(0.0),
    "flatline_count": 0,
    "is_stale_mode": False,
}
OVERFLOW_VAULT = {
    "SOL_DUST": np.float64(0.0),
    "USDC_DUST": np.float64(0.0),
}

rpc_url = os.getenv("SOLANA_RPC_URL")
wallet_address_str = os.getenv("MY_PUBLIC_WALLET")
jupiter_api_key = get_secret("JUPITER_API_KEY")
RUN_SIMULATION_PRICE_SHIFTER = os.getenv(
    "RUN_SIMULATION_PRICE_SHIFTER", "false"
).strip().lower() in {"1", "true", "yes", "on"}
INITIAL_ALLOCATION_USDC = np.float64(
    os.getenv("INITIAL_ALLOCATION_USDC", default="1000.0")
)
if not np.isfinite(INITIAL_ALLOCATION_USDC) or INITIAL_ALLOCATION_USDC <= 0:
    raise ValueError("INITIAL_ALLOCATION_USDC must be a finite positive amount.")
INVENTORY_MODE = "SIMULATION" if RUN_SIMULATION_PRICE_SHIFTER else "WALLET"
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "MOCK").strip().upper()

LEDGER_DIR = os.path.join(PROJECT_DIR, "ledger")
os.makedirs(LEDGER_DIR, exist_ok=True)

CSV_LEDGER_PATH = os.path.join(LEDGER_DIR, "grid_ledger.csv")
STATE_CACHE_PATH = os.path.join(LEDGER_DIR, "grid_state_cache.json")
for filename in ("grid_ledger.csv", "grid_state_cache.json"):
    legacy_path = os.path.join(PROJECT_DIR, filename)
    ledger_path = os.path.join(LEDGER_DIR, filename)
    if os.path.isfile(legacy_path):
        if os.path.exists(ledger_path):
            raise FileExistsError(
                f"Both legacy and ledger copies of {filename} exist; "
                "reconcile them before starting the engine."
            )
        os.replace(legacy_path, ledger_path)

DATA_DIR = os.path.join(PROJECT_DIR, "data")

MAX_LOG_SIZE_BYTES = int(os.getenv("MAX_LOG_SIZE_KB", default=10)) * 1024
MAX_LOG_BACKUPS = int(os.getenv("MAX_LOG_BACKUPS", default=10))
LOG_FILE_BASE_PATH = os.path.join(DATA_DIR, "messages.log")
MAX_BUY_SELL_RUNG_LOG_SIZE_BYTES = (
    int(os.getenv("MAX_BUY_SELL_RUNG_LOG_SIZE_KB", default=1000)) * 1024
)
MAX_BUY_SELL_RUNG_LOG_BACKUPS = int(
    os.getenv("MAX_BUY_SELL_RUNG_LOG_BACKUPS", default=100)
)
BUY_SELL_RUNG_LOG_BASE_PATH = os.path.join(DATA_DIR, "buy_sell_rung.log")

TOKEN_MINTS = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
}
ORACLE_FEEDS = {
    "SOL/USD": "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
}
CONVEYOR_THRESHOLD_CAP_SOL = np.float64(
    os.getenv("CONVEYOR_THRESHOLD_CAP_SOL", default="3.0")
)
BUCKET_OVERFLOW_THRESHOLD = np.float64(1.20)
SAFETY_BUFFER_RUNGS = 4
GIT_VAULT_COOLDOWN_SECONDS = 300

__all__ = [
    "GRID_EXECUTION_MODE",
    "MIN_ROUND_TRIP_GAP_PCT",
    "DEX_EXECUTION_FEE_RATE",
    "TARGET_NET_RETURN_PCT",
    "BUY_COST_FRACTION",
    "SELL_COST_FRACTION",
    "MINIMUM_GROSS_PRICE_RATIO",
    "NUMBER_OF_LADDER",
    "ORACLE_STAGNANT_TRACKER",
    "OVERFLOW_VAULT",
    "rpc_url",
    "wallet_address_str",
    "jupiter_api_key",
    "RUN_SIMULATION_PRICE_SHIFTER",
    "INITIAL_ALLOCATION_USDC",
    "INVENTORY_MODE",
    "EXECUTION_MODE",
    "PROJECT_DIR",
<<<<<<< HEAD
    "LEDGER_DIR",
=======
>>>>>>> bdad163e186c84d75f8883e2ca21e0546310775e
    "CSV_LEDGER_PATH",
    "STATE_CACHE_PATH",
    "DATA_DIR",
    "MAX_LOG_SIZE_BYTES",
    "MAX_LOG_BACKUPS",
    "LOG_FILE_BASE_PATH",
    "MAX_BUY_SELL_RUNG_LOG_SIZE_BYTES",
    "MAX_BUY_SELL_RUNG_LOG_BACKUPS",
    "BUY_SELL_RUNG_LOG_BASE_PATH",
    "TOKEN_MINTS",
    "ORACLE_FEEDS",
    "CONVEYOR_THRESHOLD_CAP_SOL",
    "BUCKET_OVERFLOW_THRESHOLD",
    "SAFETY_BUFFER_RUNGS",
    "GIT_VAULT_COOLDOWN_SECONDS",
]
