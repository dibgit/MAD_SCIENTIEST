import os
import sys
import requests
import asyncio
import struct
import csv
import time
import random
import logging
from logging.handlers import RotatingFileHandler
import subprocess
import json
import numpy as np
from dotenv import load_dotenv
from solana.rpc.async_api import AsyncClient
from solana.rpc.core import TokenAccountOpts
from datetime import datetime
from solders.pubkey import Pubkey

load_dotenv()

# ==============================================================================
# AUTHORITATIVE GLOBAL STORAGE MANAGEMENT MATRIX
# ==============================================================================
# REVERTED: Standardized gap to absorb 0.6% fees + ensure a clean +0.15% compounding return
# ==============================================================================
# STRATEGIC DYNAMIC MODE CONFIGURATION MATRIX (WINMERGE TARGET)
# ==============================================================================
# Global Operational Mode Selector Switch: "OPTION_A" or "OPTION_B"
GRID_EXECUTION_MODE = "OPTION_B"

# --- LEGACY OPTION A CONFIGURATION BLOCK ---
if GRID_EXECUTION_MODE == "OPTION_A":
    MIN_ROUND_TRIP_GAP_PCT = np.float64(0.008)
    DEX_EXECUTION_FEE_RATE = np.float64(0.006)
    TARGET_NET_RETURN_PCT = np.float64(0.0015)

# --- AGGRESSIVE OPTION B ARCHITECTURE BLOCK (HIGH VOLATILITY OPTIMIZED) ---
else:
    # Explicit User Parameters
    TARGET_NET_RETURN_PCT = np.float64(0.0015)  # Target +0.15% pure compounding alpha return
    BUY_COST_FRACTION = np.float64(0.006)       # 0.6% buy execution friction
    SELL_COST_FRACTION = np.float64(0.006)      # 0.6% sell execution friction

    # Authoritative Mathematical Gross Price Ratio Formula:
    # minimum gross ratio = (1 + net_return) / ((1 - buy_cost) * (1 - sell_cost))
    MINIMUM_GROSS_PRICE_RATIO = (
        (np.float64(1.0) + TARGET_NET_RETURN_PCT)
        / (
            (np.float64(1.0) - BUY_COST_FRACTION)
            * (np.float64(1.0) - SELL_COST_FRACTION)
        )
    )

    # Derived Round-Trip Spread Delta Gap
    MIN_ROUND_TRIP_GAP_PCT = MINIMUM_GROSS_PRICE_RATIO - 1.0  # Evaluates to exactly ~0.01362703 (1.363%)
    DEX_EXECUTION_FEE_RATE = np.float64(0.006)

# Global Interleaved Offset Factor matching the derived round-trip gap baseline
# DYNAMIC ENGINE CONFIGURATION EXTRACTION
# Dynamically extract and enforce ladder step sizes. Default to 257 if missing.
NUMBER_OF_LADDER = int(os.getenv("NUMBER_OF_LADDER", default=257))
if NUMBER_OF_LADDER < 2:
    raise ValueError("NUMBER_OF_LADDER must be at least 2.")

# Keep track of previous prices globally to detect a flatlining feed
ORACLE_STAGNANT_TRACKER = {
    "frozen_baseline": np.float64(0.0),  # Stores the stale price line to watch for wake-ups
    "last_price": np.float64(0.0),       # Stores the active working price matrix anchor
    "flatline_count": 0,       # Sequential tracking tick counter
    "is_stale_mode": False     # Lockout flag: True if we are running in Jupiter Fallback mode
}
# THE N+1th or 258th ELEMENT TRANSIT VAULT:
# Completely isolates fractional leftovers, dust balances, and transaction fees
# to keep your core N=257-step matrix from expanding or breaking.
OVERFLOW_VAULT = {
    "SOL_DUST": np.float64(0.0),
    "USDC_DUST": np.float64(0.0)
}

# Initialize environment configuration pathing
rpc_url = os.getenv("SOLANA_RPC_URL")
wallet_address_str = os.getenv("MY_PUBLIC_WALLET")
jupiter_api_key = os.getenv("JUPITER_API_KEY")
RUN_SIMULATION_PRICE_SHIFTER = os.getenv(
    "RUN_SIMULATION_PRICE_SHIFTER", "false"
).strip().lower() in {"1", "true", "yes", "on"}
INITIAL_ALLOCATION_USDC = np.float64(os.getenv("INITIAL_ALLOCATION_USDC", default="1000.0"))
if not np.isfinite(INITIAL_ALLOCATION_USDC) or INITIAL_ALLOCATION_USDC <= 0:
    raise ValueError("INITIAL_ALLOCATION_USDC must be a finite positive amount.")
INVENTORY_MODE = "SIMULATION" if RUN_SIMULATION_PRICE_SHIFTER else "WALLET"
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "MOCK").strip().upper()
if EXECUTION_MODE != "MOCK":
    raise ValueError(
        "Only EXECUTION_MODE=MOCK is available; no live swap adapter is implemented."
    )



# DYNAMIC RELATIVE PATH EXTRACTION
# os.path.dirname(__file__) extracts the exact folder this file resides in (D:\MAD_SCIENTIEST\VTC_DEX_Project)
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_LEDGER_PATH = os.path.join(PROJECT_DIR, "grid_ledger.csv")
# DYNAMIC STATE TRACKING CACHE PATH DEFINITION
STATE_CACHE_PATH = os.path.join(PROJECT_DIR, "grid_state_cache.json")

# PRODUCTION DIRECTORY ENFORCEMENT & ENGINE CONFIGURATION
DATA_DIR = os.path.join(PROJECT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Extract and scale configuration metrics for rotating file buffers
MAX_LOG_SIZE_BYTES = int(os.getenv("MAX_LOG_SIZE_KB", default=10)) * 1024
MAX_LOG_BACKUPS = int(os.getenv("MAX_LOG_BACKUPS", default=10))
LOG_FILE_BASE_PATH = os.path.join(DATA_DIR, "messages.log")

# DEDICATED CONFIGURATION FOR ISOLATED LADDER POSITION LOGS
MAX_BUY_SELL_RUNG_LOG_SIZE_BYTES = int(os.getenv("MAX_BUY_SELL_RUNG_LOG_SIZE_KB", default=1000)) * 1024
MAX_BUY_SELL_RUNG_LOG_BACKUPS = int(os.getenv("MAX_BUY_SELL_RUNG_LOG_BACKUPS", default=100))
BUY_SELL_RUNG_LOG_BASE_PATH = os.path.join(DATA_DIR, "buy_sell_rung.log")

# Custom stream interceptor routing log logic to console & file buffers simultaneously
class MasterStreamLogger:
    def __init__(self, original_stream, file_logger):
        self.original_stream = original_stream
        self.file_logger = file_logger

    def write(self, message):
        # Stream raw text directly to native PowerShell window console lanes
        self.original_stream.write(message)
        self.original_stream.flush()
        
        # Filter terminal spacer newlines from cluttering clean text log strings
        cleaned_msg = message.strip()
        if cleaned_msg:
            self.file_logger.info(cleaned_msg)

    def flush(self):
        self.original_stream.flush()

# Define structural layout definitions for rotating handlers
class SequentialLogNamer(RotatingFileHandler):
    """Custom file naming strategy handler to enforce sequential index suffixes: messages0.log, messages1.log..."""
    def do_rollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
            
        # Dynamically isolate folder and string structures to shift suffixes cleanly
        dir_name, base_name = os.path.split(self.baseFilename)
        name_part, ext_part = os.path.splitext(base_name)
            
        # Execute downset shifting logic across historic logs sequentially up to limit boundaries
        for i in range(self.backupCount - 1, -1, -1):
            sfn = os.path.join(dir_name, f"{name_part}{i}{ext_part}" if i > 0 else base_name)
            dfn = os.path.join(dir_name, f"{name_part}{i+1}{ext_part}")
            if os.path.exists(sfn):
                if os.path.exists(dfn):
                    os.remove(dfn)
                os.rename(sfn, dfn)
                
        # Re-initialize clean working target log parameters
        dfn = os.path.join(dir_name, f"{name_part}1{ext_part}")
        if os.path.exists(self.baseFilename):
            if os.path.exists(dfn):
                os.remove(dfn)
            os.rename(self.baseFilename, dfn)
            
        if not self.delay:
            self.stream = self._open()

# Instantiate the custom logging sub-routines
file_handler = SequentialLogNamer(LOG_FILE_BASE_PATH, maxBytes=MAX_LOG_SIZE_BYTES, backupCount=MAX_LOG_BACKUPS)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

internal_logger = logging.getLogger("MatrixEngineLogger")
internal_logger.setLevel(logging.INFO)
internal_logger.addHandler(file_handler)

# Overwrite native standard output vectors to trap standard prints
sys.stdout = MasterStreamLogger(sys.stdout, internal_logger)

# ==============================================================================
# ISOLATED RUNTIME POSITION LOG HANDLER SETUP
# ==============================================================================
def setup_ladder_state_logger():
    """Instantiates a dedicated, isolated rotating file buffer exclusively for transaction events and ladder matrix states."""
    ladder_logger = logging.getLogger("LadderStateLogger")
    ladder_logger.setLevel(logging.INFO)
    ladder_logger.propagate = False  # Strict insulation: Stops this data from leaking into console stdout
    
    # Wrap with our sequential rolling logic using custom configuration parameters
    handler = SequentialLogNamer(
        BUY_SELL_RUNG_LOG_BASE_PATH, 
        maxBytes=MAX_BUY_SELL_RUNG_LOG_SIZE_BYTES, 
        backupCount=MAX_BUY_SELL_RUNG_LOG_BACKUPS
    )
    handler.setFormatter(logging.Formatter("%(asctime)s [GRID_MATRIX] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    
    ladder_logger.addHandler(handler)
    return ladder_logger

# Dedicated tracking logger for structural output mapping targets
ladder_state_logger = setup_ladder_state_logger()


# Universal Token Mint Repository Map
TOKEN_MINTS = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
}
# Permanent Pyth Network Mainnet Price Oracle Mapping Index
ORACLE_FEEDS = {
    "SOL/USD": "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
}
# ==============================================================================
# PHASE 2: DYNAMIC CAPITAL & CONVEYOR THRESHOLD ENVIROMENT INPUTS
# ==============================================================================
# Enforce your strict Conveyor Belt accumulation ceiling cap parameter per step line
CONVEYOR_THRESHOLD_CAP_SOL = np.float64(
    os.getenv("CONVEYOR_THRESHOLD_CAP_SOL", default="3.0")
)

# The profit multiplier threshold before a single bucket overflows (e.g., 1.20 = 20% growth)
BUCKET_OVERFLOW_THRESHOLD = np.float64(1.20)
# INFINITE SHIFT BUFFER THRESHOLD
SAFETY_BUFFER_RUNGS = 4

#The minimum cooldown window in seconds between remote repository backup push execution cycles (300 seconds = 5 minutes)

GIT_VAULT_COOLDOWN_SECONDS = 300

#==============================================================================
#STATE CACHE MANAGEMENT UTILITIES FOR INF OPERATIONAL RUNS
#==============================================================================
def dump_engine_state_cache(
    base_anchor_price: float,
    loop_id: int,
    overflow_vault: dict,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    cash_bucket_baselines: tuple[float, float],
):
    """Serializes both discrete interleaved ladder states to prevent data overwrite collisions."""
    try:
        l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
        state_payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "base_anchor_price": float(base_anchor_price),
            "loop_counter": int(loop_id),
            "number_of_ladder": len(l1_sol),
            "inventory_mode": INVENTORY_MODE,
            "l1_sol_stock": l1_sol.tolist(),
            "l1_usdc_stock": l1_usdc.tolist(),
            "l2_sol_stock": l2_sol.tolist(),
            "l2_usdc_stock": l2_usdc.tolist(),
            "overflow_vault_state": overflow_vault,
            "cash_bucket_baselines": list(cash_bucket_baselines),
        }
        with open(STATE_CACHE_PATH, "w") as json_file:
            json.dump(state_payload, json_file, indent=4)
    except Exception as e:
        print(f"[CACHE WRITE ERROR] Dual-ladder serialization failure: {e}")

def load_engine_state_cache(current_price: float) -> dict | None:
    """Loads and price-maps cached inventory onto the configured interleaved ladders."""
    if not os.path.exists(STATE_CACHE_PATH):
        return None
    try:
        current_price = np.float64(current_price)
        with open(STATE_CACHE_PATH, "r") as json_file:
            state_data = json.load(json_file)

        if state_data.get("inventory_mode") != INVENTORY_MODE:
            cached_mode = state_data.get("inventory_mode", "unspecified/legacy")
            print(
                f"[STATE RESET] Cache inventory mode is {cached_mode}; active mode is "
                f"{INVENTORY_MODE}. Reinitializing from the active mode's allocation."
            )
            return None

        anchor_price = np.float64(state_data["base_anchor_price"])
        if not np.isfinite(anchor_price) or anchor_price <= 0:
            raise ValueError("Cached anchor price must be finite and positive.")

        new_l1_grid, new_l2_grid = generate_infinite_interleaved_matrix(
            anchor_price,
            mode="geometric",
        )

        def validate_stock(key_name):
            raw_arr = np.asarray(state_data[key_name], dtype=np.float64)
            if raw_arr.ndim != 1 or len(raw_arr) < 2:
                raise ValueError(f"Cached {key_name} must contain at least two rungs.")
            if not np.all(np.isfinite(raw_arr)) or np.any(raw_arr < 0):
                raise ValueError(f"Cached {key_name} contains invalid inventory values.")
            return raw_arr

        def map_stock_to_grid(stock, source_grid, target_grid):
            if len(stock) != len(source_grid):
                raise ValueError("Cached inventory length does not match its source grid.")
            mapped = np.zeros(len(target_grid), dtype=np.float64)
            right_indices = np.searchsorted(target_grid, source_grid)
            right_indices = np.clip(right_indices, 0, len(target_grid) - 1)
            left_indices = np.clip(right_indices - 1, 0, len(target_grid) - 1)
            use_right = (
                np.abs(target_grid[right_indices] - source_grid)
                <= np.abs(target_grid[left_indices] - source_grid)
            )
            target_indices = np.where(use_right, right_indices, left_indices)
            np.add.at(mapped, target_indices, stock)
            return mapped

        dual_layer_keys = (
            "l1_sol_stock",
            "l1_usdc_stock",
            "l2_sol_stock",
            "l2_usdc_stock",
        )
        if all(key in state_data for key in dual_layer_keys):
            old_l1_sol, old_l1_usdc, old_l2_sol, old_l2_usdc = (
                validate_stock(key) for key in dual_layer_keys
            )
            old_sizes = {
                len(old_l1_sol),
                len(old_l1_usdc),
                len(old_l2_sol),
                len(old_l2_usdc),
            }
            if len(old_sizes) != 1:
                raise ValueError("Cached ladder inventory arrays have inconsistent lengths.")
            old_size = old_sizes.pop()
            source_size = old_size
            old_l1_grid, old_l2_grid = generate_pure_geometric_mesh(
                anchor_price,
                old_size,
                MIN_ROUND_TRIP_GAP_PCT
            )
            l1_sol = map_stock_to_grid(old_l1_sol, old_l1_grid, new_l1_grid)
            l1_usdc = map_stock_to_grid(old_l1_usdc, old_l1_grid, new_l1_grid)
            l2_sol = map_stock_to_grid(old_l2_sol, old_l2_grid, new_l2_grid)
            l2_usdc = map_stock_to_grid(old_l2_usdc, old_l2_grid, new_l2_grid)
        elif "sol_stock_array" in state_data and "usdc_stock_array" in state_data:
            old_sol = validate_stock("sol_stock_array")
            old_usdc = validate_stock("usdc_stock_array")
            if len(old_sol) != len(old_usdc):
                raise ValueError("Cached single-ladder inventory arrays have inconsistent lengths.")
            source_size = len(old_sol)
            old_l1_grid, _ = generate_pure_geometric_mesh(
                anchor_price,
                len(old_sol),
                MIN_ROUND_TRIP_GAP_PCT
            )
            half_sol = old_sol * 0.5
            half_usdc = old_usdc * 0.5
            l1_sol = map_stock_to_grid(half_sol, old_l1_grid, new_l1_grid)
            l1_usdc = map_stock_to_grid(half_usdc, old_l1_grid, new_l1_grid)
            l2_sol = map_stock_to_grid(half_sol, old_l1_grid, new_l2_grid)
            l2_usdc = map_stock_to_grid(half_usdc, old_l1_grid, new_l2_grid)
            print(
                "[STATE MIGRATION] Mapped legacy single-ladder inventory across "
                "both interleaved ladders while preserving total balances."
            )
        else:
            raise ValueError("Cache does not contain a recognized ladder inventory schema.")

        # Identify if an environmental parameter change has been triggered
        size_changed = (source_size != NUMBER_OF_LADDER)

        if size_changed:
            print(
                f"[STATE MIGRATION] Remapped {source_size} cached rung(s) per source ladder "
                f"to {NUMBER_OF_LADDER} configured rung(s) per ladder."
            )
            # DYNAMIC SPREAD STEP NORMALIZER: Recalculate baselines to scale smoothly with your new ladder size
            reference_price = current_price if current_price > 0 else anchor_price
            cash_baselines = (
                np.sum(l1_usdc, dtype=np.float64) / max(1, int(np.count_nonzero(new_l1_grid < reference_price))),
                np.sum(l2_usdc, dtype=np.float64) / max(1, int(np.count_nonzero(new_l2_grid < reference_price))),
            )
        else:
            cash_baselines = state_data.get(
                "cash_bucket_baselines",
                (np.float64(0.0), np.float64(0.0)),
            )

        if len(cash_baselines) != 2 or any(
            not np.isfinite(np.float64(value)) or np.float64(value) < 0
            for value in cash_baselines
        ):
            raise ValueError("Cached cash bucket baselines are invalid.")
        if "cash_bucket_baselines" not in state_data:
            reference_price = current_price if current_price > 0 else anchor_price
            cash_baselines = (
                np.sum(l1_usdc, dtype=np.float64)
                / max(1, int(np.count_nonzero(new_l1_grid < reference_price))),
                np.sum(l2_usdc, dtype=np.float64)
                / max(1, int(np.count_nonzero(new_l2_grid < reference_price))),
            )

        cached_vault = state_data.get(
            "overflow_vault_state",
            {"SOL_DUST": np.float64(0.0), "USDC_DUST": np.float64(0.0)},
        )
        return {
            "base_anchor_price": anchor_price,
            "loop_counter": int(state_data["loop_counter"]),
            "l1_sol": l1_sol,
            "l1_usdc": l1_usdc,
            "l2_sol": l2_sol,
            "l2_usdc": l2_usdc,
            "cash_bucket_baselines": tuple(
                np.float64(value) for value in cash_baselines
            ),
            "overflow_vault": {
                "SOL_DUST": np.float64(cached_vault.get("SOL_DUST", 0.0)),
                "USDC_DUST": np.float64(cached_vault.get("USDC_DUST", 0.0)),
            },
        }
    except Exception as e:
        print(f"[HOT BOOT ERROR] Dual-ladder recovery failure: {e}")
        return None

def log_transaction_to_csv(
    loop_id: int,
    live_price: float,
    current_step: int,
    direction: str,
    steps_moved: int,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
):
    """Appends high-precision split-ladder metric snapshots directly to your Excel database file."""
    live_price = np.float64(live_price)
    file_exists = os.path.exists(CSV_LEDGER_PATH)
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    l1_sol_total = np.sum(l1_sol, dtype=np.float64)
    l1_usdc_total = np.sum(l1_usdc, dtype=np.float64)
    l2_sol_total = np.sum(l2_sol, dtype=np.float64)
    l2_usdc_total = np.sum(l2_usdc, dtype=np.float64)
    total_sol = l1_sol_total + l2_sol_total + OVERFLOW_VAULT["SOL_DUST"]
    total_usdc = l1_usdc_total + l2_usdc_total + OVERFLOW_VAULT["USDC_DUST"]
    portfolio_value = (total_sol * live_price) + total_usdc
    
    # Expanded database tracking headers matching your multi-ladder asset layers
    headers = [
        "timestamp", "tick_id", "market_price", "active_step", "trade_direction", "steps_crossed", 
        "l1_sol_total", "l1_usdc_total", "l2_sol_total", "l2_usdc_total", "vault_sol", "vault_usdc", "net_worth_usd"
    ]
    
    try:
        with open(CSV_LEDGER_PATH, mode="a", newline="") as csv_file:
            writer = csv.writer(csv_file)
            if not file_exists:
                writer.writerow(headers)
                
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                loop_id,
                f"{live_price:.4f}",
                current_step,
                direction,
                steps_moved,
                f"{l1_sol_total:.6f}",
                f"{l1_usdc_total:.4f}",
                f"{l2_sol_total:.6f}",
                f"{l2_usdc_total:.4f}",
                f"{OVERFLOW_VAULT['SOL_DUST']:.6f}",
                f"{OVERFLOW_VAULT['USDC_DUST']:.4f}",
                f"{portfolio_value:.4f}"
            ])
    except Exception as e:
        print(f"[ERROR] CSV Database Multi-Grid Write Failure: {e}")

async def fetch_live_token_balance(client: AsyncClient, token_ticker: str) -> float:
    """
    Programmatically queries the live Solana blockchain ledger via RPC.
    Natively supports System Account SOL and derives Associated Token Accounts for SPL tokens.
    """
    if not wallet_address_str:
        print(f"[ERROR] Cannot fetch {token_ticker}: MY_PUBLIC_WALLET missing from .env.")
        return 0.0
        
    try:
        clean_wallet = str(wallet_address_str).replace('"', '').replace("'", "").strip()
        owner_pubkey = Pubkey.from_string(clean_wallet)

        # --- NATIVE SOL DIRECT SHORTCUT LANE (Unified Future-Proofing Core) --------
        if token_ticker == "SOL":
            balance_response = await client.get_balance(owner_pubkey)
            live_sol_balance = np.float64(balance_response.value) / np.float64(1_000_000_000)
            print(f"[BALANCE CHECK] Live SOL Balance Confirmed: {live_sol_balance:,.4f} SOL")
            return np.float64(live_sol_balance)
        # --------------------------------------------------------------------------

        token_mint_str = TOKEN_MINTS.get(token_ticker)
        if not token_mint_str:
            print(f"[ERROR] Ticker mapping configuration missing for target: {token_ticker}")
            return 0.0

        clean_mint = str(token_mint_str).replace('"', '').replace("'", "").strip()
        mint_pubkey = Pubkey.from_string(clean_mint)
        
        # Modern v0.36+ SDK layout passing verified TokenAccountOpts to serialize arguments
        response = await client.get_token_accounts_by_owner(
            owner_pubkey,
            TokenAccountOpts(mint=mint_pubkey)
        )
        
        if not response.value:
            # A wallet without this token account has no balance for this mint.
            print(f"[BALANCE CHECK] Live {token_ticker} Wallet State: 0.0000 (No Associated Account Found)")
            return 0.0
            
        account_info = response.value[0]
        account_pubkey = account_info.pubkey
        
        balance_response = await client.get_token_account_balance(account_pubkey)
        if balance_response.value:
            live_balance = np.float64(balance_response.value.ui_amount)
            print(f"[BALANCE CHECK] Live {token_ticker} Balance Confirmed: {live_balance:,.4f} tokens")
            return live_balance
            
        return 0.0
    except Exception as e:
        print(f"[ERROR] Failed to execute Web3 token balance ledger interrogation: {e}")
        return 0.0

async def fetch_onchain_price(client: AsyncClient, pair_ticker: str = "SOL/USD"):
    """
    Reads fixed-point raw integer byte data from smart contracts with an intelligent, 
    low-frequency Jupiter fallback state-machine.
    """
    feed_address = ORACLE_FEEDS.get(pair_ticker)
    if not feed_address:
        print(f"[ABORT] Oracle mapping configuration target missing for: {pair_ticker}")
        return None
        
    try:
        pubkey = Pubkey.from_string(str(feed_address).strip())
        account_info = await client.get_account_info(pubkey)
        
        if not account_info.value:
            print("[ERROR] Node connection failed to locate targeted price account memory.")
            return None
            
        data = account_info.value.data
        
        # Unpack binary data sequence starting at exact Pyth memory offset 208
        price_offset = 208
        price_data = data[price_offset:price_offset + 20]
        raw_price, confidence, exponent = struct.unpack("<qQi", price_data)
        # Enforce fixed-point decimal scaling rule of 10^8
        # Parse the raw on-chain state value
        oracle_price = np.float64(raw_price) / np.float64(100_000_000)
        
        # Initialize the baseline price tracking variable on your very first boot tick
        if ORACLE_STAGNANT_TRACKER["frozen_baseline"] == 0.0:
            ORACLE_STAGNANT_TRACKER["frozen_baseline"] = oracle_price
            ORACLE_STAGNANT_TRACKER["last_price"] = oracle_price
            ORACLE_STAGNANT_TRACKER["flatline_count"] = 0
            
        # ---- ORACLE INTERROGATION ENGINE ----
        if ORACLE_STAGNANT_TRACKER["is_stale_mode"]:
            # CRITICAL CHECK: Has the oracle finally woken up and broken past its frozen baseline?
            if oracle_price != ORACLE_STAGNANT_TRACKER["frozen_baseline"]:
                print(f"[ORACLE AWOKEN] On-chain data block changed! Re-engaging native oracle pipeline.")
                ORACLE_STAGNANT_TRACKER["is_stale_mode"] = False
                ORACLE_STAGNANT_TRACKER["frozen_baseline"] = oracle_price
                ORACLE_STAGNANT_TRACKER["last_price"] = oracle_price
                ORACLE_STAGNANT_TRACKER["flatline_count"] = 0
            else:
                # The oracle is still frozen at its old value. Maintain lockout mode.
                ORACLE_STAGNANT_TRACKER["flatline_count"] += 1
        else:
            # Standard monitoring mode: check if oracle is flatlining against itself
            if oracle_price == ORACLE_STAGNANT_TRACKER["last_price"]:
                ORACLE_STAGNANT_TRACKER["flatline_count"] += 1
            else:
                ORACLE_STAGNANT_TRACKER["last_price"] = oracle_price
                ORACLE_STAGNANT_TRACKER["flatline_count"] = 0

        # ---- LOW-FREQUENCY JUPITER ROTATION ENGINE ----
        # Trigger Jupiter if standard flatline hits 5, OR every 5 ticks while inside stale mode
        if ORACLE_STAGNANT_TRACKER["flatline_count"] >= 5:
            print(f"[WARNING] HYBRID FALLBACK triggered. Evaluation count: {ORACLE_STAGNANT_TRACKER['flatline_count']}")
            
            if not jupiter_api_key:
                print("[WARNING] Flatline threshold breached, but JUPITER_API_KEY is missing from .env.")
            else:
                # Activate Lockout Flag immediately to protect downstream loops
                ORACLE_STAGNANT_TRACKER["is_stale_mode"] = True
                
                sol_mint = TOKEN_MINTS["SOL"]
                url = "https://api.jup.ag/price/v3"
                params = {'ids': sol_mint}
                headers = {'x-api-key': jupiter_api_key.strip()}
                
                try:
                    response = requests.get(url, params=params, headers=headers, timeout=5)
                    if response.status_code == 200:
                        prices = response.json()
                        spot_price = np.float64(prices[sol_mint]['usdPrice'])
                        
                        print(f"[JUPITER TICK] {pair_ticker} Active Working Value: ${spot_price:.4f}")
                        
                        # Cache the fresh Jupiter price and reset the threshold interval
                        ORACLE_STAGNANT_TRACKER["last_price"] = spot_price
                        # Reset flatline back to 0 so we don't spam the API on the next tick.
                        ORACLE_STAGNANT_TRACKER["flatline_count"] = 0
                        
                        # TERMINATE OPERATION HERE: Directly return the correct spot price to your grid engine.
                        print(f"[JUPITER TICK] {pair_ticker} Active Working Value: ${spot_price:,.4f}")
                        return spot_price
                except Exception as ex_jup:
                    print(f"[WARNING] Jupiter V3 API fallback interface error: {ex_jup}")
                    
        # Return the last known verified price out of our secure cache
        active_working_price = ORACLE_STAGNANT_TRACKER["last_price"]
        print(f"[ORACLE TICK] {pair_ticker} Active Working Value: ${active_working_price:,.4f}")
        return active_working_price
        
    except Exception as e:
        print(f"[ERROR] On-chain cryptograph execution failure: {e}")
        return None

def generate_pure_geometric_mesh(
    anchor_price: float,
    rung_count: int,
    gap_pct: float,
) -> tuple[np.ndarray, np.ndarray]:
    """
    REPLACES np.geomspace(): Generates a symmetrical interleaved dual-ladder grid 
    built directly out from a central anchor price using a fixed geometric trading spread.
    """
    anchor_price = np.float64(anchor_price)
    gap_pct = np.float64(gap_pct)
    
    # Establish your constant geometric multiplier ratio (e.g., 1.01362703 for Option B)
    # CORRECTED SYSTEM DEPENDENCY: Scales the buy-rung distance to exactly 
    # double your execution spread, preventing gridlock order collisions.
    step_multiplier = np.float64(1.0) + (gap_pct * np.float64(2.0))
    # Determine the number of tiers to deploy above and below your active working center anchor
    # Using an equal split gives your matrix a perfectly balanced bidirectional runway
    half_rungs = rung_count // 2
    
    # --- GENERATE LADDER 1 BASELINE CROSSINGS ---
    l1_grid = np.zeros(rung_count, dtype=np.float64)
    l1_grid[half_rungs] = anchor_price
    
    # Project prices downward for the lower buy rungs
    for i in range(half_rungs - 1, -1, -1):
        l1_grid[i] = l1_grid[i + 1] / step_multiplier
        
    # Project prices upward for the higher sell rungs
    for i in range(half_rungs + 1, rung_count):
        l1_grid[i] = l1_grid[i - 1] * step_multiplier

    # --- GENERATE LADDER 2 STAGGERED MIDZONE INTERLEAVE ---
    # To place Ladder 2 perfectly between Ladder 1's rungs geometrically, 
    # we multiply Ladder 1 by the square root of your step multiplier
    # half_step_ratio = np.sqrt(step_multiplier)
    half_step_ratio = np.sqrt(step_multiplier) * (np.float64(1.0) + (gap_pct * np.float64(0.5)))
    l2_grid = l1_grid * half_step_ratio

    return l1_grid, l2_grid


def generate_pure_arithmetic_mesh(
    anchor_price: float,
    rung_count: int,
    gap_pct: float,
) -> tuple[np.ndarray, np.ndarray]:
    """
    REPLACES np.linspace(): Generates a symmetrical interleaved dual-ladder grid 
    built directly out from a central anchor price using a localized linear volatility step size.
    """
    anchor_price = np.float64(anchor_price)
    gap_pct = np.float64(gap_pct)
    
    # Derive a static dollar increment bound right at your active center spot price
    # Example: If SOL is $100 and gap is 1.363%, your flat stepping increment is exactly $1.363
    linear_dollar_step = anchor_price * (gap_pct * np.float64(2.0))
    
    half_rungs = rung_count // 2
    
    # --- GENERATE LADDER 1 LINEAR PROGRESSIONS ---
    l1_grid = np.zeros(rung_count, dtype=np.float64)
    l1_grid[half_rungs] = anchor_price
    
    for i in range(half_rungs - 1, -1, -1):
        l1_grid[i] = l1_grid[i + 1] - linear_dollar_step
        
    for i in range(half_rungs + 1, rung_count):
        l1_grid[i] = l1_grid[i - 1] + linear_dollar_step

    # --- GENERATE LADDER 2 STAGGERED MIDZONE INTERLEAVE ---
    # To place Ladder 2 perfectly between Ladder 1's rungs arithmetically, 
    # we shift it upward by exactly half of your flat linear dollar step
    l2_grid = l1_grid + (linear_dollar_step / 2.0)

    return l1_grid, l2_grid


def generate_infinite_interleaved_matrix(
    low_price: float,  # This now acts as our central anchor spot price parameter input
    mode: str = "geometric",
) -> tuple[np.ndarray, np.ndarray]:
    """Generates the configured pair of interleaved price ladders using pure scaling mesh rules."""
    
    # Force direct execution routing straight to your pure scaling mesh structures
    if mode == "arithmetic":
        l1_grid, l2_grid = generate_pure_arithmetic_mesh(low_price, NUMBER_OF_LADDER, MIN_ROUND_TRIP_GAP_PCT)
    else:
        l1_grid, l2_grid = generate_pure_geometric_mesh(low_price, NUMBER_OF_LADDER, MIN_ROUND_TRIP_GAP_PCT)
        
    # Flatten arrays explicitly via .ravel() to guarantee string interpolation handles them as scalar layers
    l1_flat = l1_grid.ravel()
    l2_flat = l2_grid.ravel()

    print(
        f"[INTERLEAVE INITIALIZED] Ladder 1: {len(l1_flat)} rungs "
        f"(${float(l1_flat[0]):,.4f}-${float(l1_flat[-1]):,.4f})"
    )
    print(
        f"[INTERLEAVE INITIALIZED] Ladder 2: {len(l2_flat)} rungs "
        f"(${float(l2_flat[0]):,.4f}-${float(l2_flat[-1]):,.4f})"
    )
    return l1_grid, l2_grid


def build_interleaved_event_mesh(
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
) -> tuple[np.ndarray, list[tuple[int, int]]]:
    """Returns sorted prices with each event's source ladder and rung index."""
    events = [
        (np.float64(price), 0, rung_index)
        for rung_index, price in enumerate(l1_grid)
    ] + [
        (np.float64(price), 1, rung_index)
        for rung_index, price in enumerate(l2_grid)
    ]
    events.sort(key=lambda event: event[0])
    prices = np.array([event[0] for event in events], dtype=np.float64)
    sources = [(event[1], event[2]) for event in events]
    return prices, sources


def get_simulated_price(real_oracle_price: float, current_tick: int) -> float:
    """Apply the optional smooth price wave for local ladder-accounting tests."""
    real_oracle_price = np.float64(real_oracle_price)
    if not RUN_SIMULATION_PRICE_SHIFTER or current_tick <= 5:
        return real_oracle_price

    wave_frequency = np.float64(current_tick - 5) * (
        np.float64(2.0) * np.pi / np.float64(60.0)
    )
    simulated_price = real_oracle_price * (
        np.float64(1.0) + np.sin(wave_frequency) * np.float64(0.30)
    )
    print(f"[SIMULATION SHIFTER] Local accounting price: ${simulated_price:,.4f}")
    return simulated_price


def initialize_live_inventory_matrix(grid_matrix, current_price, live_sol_total: float, live_usdc_total: float):
    """
    PRODUCTION INITIALIZER: Dynamically distributes your true, live on-chain wallet 
    balances across the active 257 geometric conveyor tiers based on real spot values.
    """
    current_price = np.float64(current_price)
    live_sol_total = np.float64(live_sol_total)
    live_usdc_total = np.float64(live_usdc_total)
    total_steps = len(grid_matrix)
    sol_inventory = np.zeros(total_steps, dtype=np.float64)
    usdc_inventory = np.zeros(total_steps, dtype=np.float64)
    
    # Identify which steps are below market (buy lanes) and which are above market (sell lanes)
    below_market_indices = np.where(grid_matrix < current_price)[0]
    above_market_indices = np.where(grid_matrix >= current_price)[0]
    
    # 1. Distribute available stablecoin cash across the buy rungs
    buy_steps_count = len(below_market_indices)
    if buy_steps_count > 0 and live_usdc_total > 0:
        usdc_per_step = live_usdc_total / buy_steps_count
        for idx in below_market_indices:
            usdc_inventory[idx] = usdc_per_step
            
    # 2. Distribute available base asset inventory across the sell rungs
    sell_steps_count = len(above_market_indices)
    if sell_steps_count > 0 and live_sol_total > 0:
        sol_per_step = live_sol_total / sell_steps_count
        for idx in above_market_indices:
            sol_inventory[idx] = sol_per_step
            
    print(f"[LIVE INVENTORY MATED] True ledger metrics successfully injected across {total_steps} tiers.")
    print(f"  Live Asset Allocation Base : {np.sum(sol_inventory, dtype=np.float64):.4f} SOL distributed over {sell_steps_count} sell steps.")
    print(f"  Live Cash Allocation Base  : ${np.sum(usdc_inventory, dtype=np.float64):.2f} USDC distributed over {buy_steps_count} buy steps.")
    return sol_inventory, usdc_inventory


def initialize_simulation_inventory(
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
    current_price: float,
    total_budget_usdc: float,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    tuple[float, float],
]:
    """Allocates one shared USD budget across both simulation ladders."""
    if not np.isfinite(current_price) or current_price <= 0:
        raise ValueError("Simulation price must be finite and positive.")
    if not np.isfinite(total_budget_usdc) or total_budget_usdc <= 0:
        raise ValueError("Simulation budget must be finite and positive.")
    if len(l1_grid) != len(l2_grid) or len(l1_grid) == 0:
        raise ValueError("Simulation ladders must have matching, non-empty price grids.")

    current_price = np.float64(current_price)
    total_budget_usdc = np.float64(total_budget_usdc)
    bucket_budget = total_budget_usdc / np.float64(len(l1_grid) + len(l2_grid))
    inventories = []
    cash_baselines = []
    for grid in (l1_grid, l2_grid):
        sol_inventory = np.zeros(len(grid), dtype=np.float64)
        usdc_inventory = np.zeros(len(grid), dtype=np.float64)
        buy_indices = grid < current_price
        usdc_inventory[buy_indices] = bucket_budget
        sol_inventory[~buy_indices] = bucket_budget / current_price
        inventories.extend((sol_inventory, usdc_inventory))
        cash_baselines.append(np.float64(bucket_budget))

    print(
        f"[SIMULATION INVENTORY] Allocated ${total_budget_usdc:,.2f} total across "
        f"L1+L2 at ${current_price:,.4f}; "
        f"${bucket_budget:,.4f} budget per rung."
    )
    return (*inventories, tuple(cash_baselines))


def execute_conveyor_bucket_spillover(
    step_index: int,
    sol_inventory: np.ndarray,
    usdc_inventory: np.ndarray,
    grid_matrix: np.ndarray,
    cash_bucket_baseline: float,
):
    """
    Moves SOL surplus toward higher-price rungs and USDC surplus toward lower-price
    rungs, vaulting only the amount that cannot fit within the ladder boundaries.
    """
    global OVERFLOW_VAULT

    if len(sol_inventory) != len(usdc_inventory) or len(sol_inventory) != len(grid_matrix):
        raise ValueError("SOL, USDC, and price-grid arrays must have matching lengths.")
    if not 0 <= step_index < len(grid_matrix):
        raise IndexError("Spillover step_index is outside the ladder.")

    # A zero baseline means this ladder has no funded cash-bucket target yet.
    # In that case, retain sale proceeds at their rung instead of draining them
    # through the ladder and into the vault.
    cash_bucket_baseline = np.float64(cash_bucket_baseline)

    rung_price = np.float64(grid_matrix[step_index])
    cash_limit = (
        CONVEYOR_THRESHOLD_CAP_SOL * rung_price * BUCKET_OVERFLOW_THRESHOLD
    )
    cash_index = step_index
    while usdc_inventory[cash_index] > cash_limit:
        excess_cash = np.float64(usdc_inventory[cash_index] - cash_limit)
        usdc_inventory[cash_index] = cash_limit
        if cash_index > 0:
            lower_step = cash_index - 1
            usdc_inventory[lower_step] += excess_cash
            print(
                f"[CASH CONVEYOR] Step {cash_index + 1} sent "
                f"${excess_cash:.4f} USDC down to Step {lower_step + 1}."
            )
            cash_index = lower_step
            # Recalculate cash limit for the lower step to keep the loop mathematically bounded
            cash_limit = CONVEYOR_THRESHOLD_CAP_SOL * np.float64(grid_matrix[cash_index]) * BUCKET_OVERFLOW_THRESHOLD
        else:
            OVERFLOW_VAULT["USDC_DUST"] += excess_cash
            print(
                f"[CASH FLOOR VAULTED] Step 1 reached the lower boundary; "
                f"vaulted ${excess_cash:.4f} USDC."
            )
            break

    # SOL surplus moves only toward higher-price rungs in this same ladder.
    asset_index = step_index
    asset_upper_limit = CONVEYOR_THRESHOLD_CAP_SOL * BUCKET_OVERFLOW_THRESHOLD
    while sol_inventory[asset_index] > asset_upper_limit:
        excess_sol = np.float64(sol_inventory[asset_index] - asset_upper_limit)
        sol_inventory[asset_index] = asset_upper_limit
        if asset_index < len(grid_matrix) - 1:
            upper_step = asset_index + 1
            sol_inventory[upper_step] += excess_sol
            print(
                f"[SOL CONVEYOR] Step {asset_index + 1} sent "
                f"{excess_sol:.8f} SOL up to Step {upper_step + 1}."
            )
            asset_index = upper_step
        else:
            OVERFLOW_VAULT["SOL_DUST"] += excess_sol
            print(
                f"[SOL CEILING VAULTED] Step {asset_index + 1} reached the upper "
                f"boundary; vaulted {excess_sol:.8f} SOL."
            )
            break


def rebalance_ladder_spillovers(
    sol_inventory: np.ndarray,
    usdc_inventory: np.ndarray,
    grid_matrix: np.ndarray,
    cash_bucket_baseline: float,
) -> None:
    """Sweep every rung of one ladder without accessing the other ladder's stocks."""
    for step_index in range(len(grid_matrix)):
        execute_conveyor_bucket_spillover(
            step_index,
            sol_inventory,
            usdc_inventory,
            grid_matrix,
            cash_bucket_baseline,
        )

def execute_live_onchain_sell(
    layer_index: int,
    rung_index: int,
    amount_in_sol: float,
    target_price: float,
) -> float:
    """Mock sell interface; replace its body with a confirmed DEX adapter later."""
    print(
        f"[MOCK EXECUTION] L{layer_index + 1} rung {rung_index + 1}: "
        f"SELL {amount_in_sol:.8f} SOL near ${target_price:.4f}; no transaction sent."
    )
    return 1.0


def execute_live_onchain_buy(
    layer_index: int,
    rung_index: int,
    amount_in_usdc: float,
    target_price: float,
) -> float:
    """Mock buy interface; replace its body with a confirmed DEX adapter later."""
    print(
        f"[MOCK EXECUTION] L{layer_index + 1} rung {rung_index + 1}: "
        f"BUY with ${amount_in_usdc:.4f} USDC near ${target_price:.4f}; "
        "no transaction sent."
    )
    return 1.0


def apply_interleaved_crossing(
    direction: str,
    layer_index: int,
    rung_index: int,
    rung_price: float,
    layer_grids: tuple[np.ndarray, np.ndarray],
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    cash_bucket_baselines: tuple[float, float],
    is_terminal_step: bool,  # Passed from the main crossing event scanner
) -> None:
    """Updates one ladder's local inventory accounting for a crossed price level."""
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    if layer_index == 0:
        sol_stock, usdc_stock = l1_sol, l1_usdc
    elif layer_index == 1:
        sol_stock, usdc_stock = l2_sol, l2_usdc
    else:
        raise ValueError(f"Invalid ladder index: {layer_index}")
    if not 0 <= rung_index < len(sol_stock):
        raise IndexError(f"Invalid rung index {rung_index} for ladder {layer_index + 1}.")

    if direction == "UPWARD" and sol_stock[rung_index] > 0:
        available_sol = np.float64(sol_stock[rung_index])
        if RUN_SIMULATION_PRICE_SHIFTER:
            # check if this is the final event index in our current wave
            fill_fraction = 1.00 if not is_terminal_step else random.uniform(0.10, 1.00)
        else:
            fill_fraction = execute_live_onchain_sell(
                layer_index,
                rung_index,
                available_sol,
                rung_price,
            )
        if not np.isfinite(fill_fraction) or not 0.0 <= fill_fraction <= 1.0:
            raise ValueError(f"Sell adapter returned invalid fill fraction: {fill_fraction}")
        sol_sold = available_sol * fill_fraction
        if sol_sold == 0:
            return
        usdc_received = sol_sold * rung_price * (1.0 - DEX_EXECUTION_FEE_RATE)
        sol_stock[rung_index] -= sol_sold
        usdc_stock[rung_index] += usdc_received
        print(
            f"  [FILL ACCOUNTING] L{layer_index + 1} rung {rung_index + 1} "
            f"sold {sol_sold:.8f} SOL at ${rung_price:.4f}; "
            f"credited ${usdc_received:.4f} USDC "
            f"({fill_fraction:.1%} fill; fee included)."
        )
    elif direction == "DOWNWARD" and usdc_stock[rung_index] > 0:
        available_usdc = np.float64(usdc_stock[rung_index])
        if RUN_SIMULATION_PRICE_SHIFTER:
            fill_fraction = 1.00 if not is_terminal_step else random.uniform(0.10, 1.00)
        else:
            fill_fraction = execute_live_onchain_buy(
                layer_index,
                rung_index,
                available_usdc,
                rung_price,
            )
        if not np.isfinite(fill_fraction) or not 0.0 <= fill_fraction <= 1.0:
            raise ValueError(f"Buy adapter returned invalid fill fraction: {fill_fraction}")
        usdc_spent = available_usdc * fill_fraction
        if usdc_spent == 0:
            return
        sol_received = (usdc_spent / rung_price) * (1.0 - DEX_EXECUTION_FEE_RATE)
        usdc_stock[rung_index] -= usdc_spent
        sol_stock[rung_index] += sol_received
        print(
            f"  [FILL ACCOUNTING] L{layer_index + 1} rung {rung_index + 1} "
            f"spent ${usdc_spent:.4f} at ${rung_price:.4f}; "
            f"credited {sol_received:.8f} SOL "
            f"({fill_fraction:.1%} fill; fee included)."
        )
    else:
        return

    cash_baseline = cash_bucket_baselines[layer_index]
    rebalance_ladder_spillovers(
        sol_stock,
        usdc_stock,
        layer_grids[layer_index],
        cash_baseline,
    )


def execute_automated_git_vault(tick_id: int, current_step: int, market_price: float, last_vault_time: float) -> float:
    """
    Automates local repository updates using a strict time-locked pacing threshold configuration.
    Prevents thread blocking during high-frequency volatility cycles.
    """
    current_time = time.time()
    elapsed_time = current_time - last_vault_time
    
    # Check if the system time constraints have been cleared
    if elapsed_time < GIT_VAULT_COOLDOWN_SECONDS:
        # Time threshold has not been cleared yet; pass execution parameters back to main track safely
        return last_vault_time
        
    repo_directory = os.path.dirname(PROJECT_DIR)
    project_relative_path = os.path.relpath(PROJECT_DIR, repo_directory)
    commit_message = f"Vault Update: Tick #{tick_id} | Active Step: {current_step} | Market Benchmark: ${market_price:.2f}"
    
    print(f"[GIT VAULT] System cooldown cleared ({int(elapsed_time)}s elapsed). Initializing background sync...")
    
    try:
        # Execute background processes using thin execution masks
        subprocess.run(
            [
                "git",
                "add",
                os.path.join(project_relative_path, "dex_grid.py"),
                os.path.join(project_relative_path, "grid_ledger.csv"),
            ],
            cwd=repo_directory,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        
        subprocess.run(["git", "commit", "-m", commit_message], 
                       cwd=repo_directory, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        subprocess.run(["git", "push", "origin", "main"], 
                       cwd=repo_directory, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        print(f"[GIT VAULT] Repository state securely synchronized to remote main branch.")
        # Return the new current time to reset the 5-minute cooldown clock anchor point
        return current_time
    except subprocess.CalledProcessError:
        print(f"[GIT VAULT] Check skipped: No new structural asset adjustments to log.")
        return last_vault_time
    except Exception as e:
        print(f"[ERROR] Automated vault interface routine failure: {e}")
        return last_vault_time

def print_interleaved_ladder_state(
    event_id: int,
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
) -> None:
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    ladder_size = len(l1_grid)
    if any(
        len(values) != ladder_size
        for values in (l2_grid, l1_sol, l1_usdc, l2_sol, l2_usdc)
    ):
        raise ValueError("Both ladders and their inventory arrays must have equal lengths.")

    header = (
        "Rung | Buy price (USDC/SOL) | Buy fund (USDC) | "
        "Sell price (USDC/SOL) | Sell fund (SOL)"
    )
    print(f"[USDC/SOL LADDER AFTER EVENT {event_id:03d}]")
    print(f"{'L1':<2} {header} || {'L2':<2} {header}")
    for rung_index in range(ladder_size):
        l1_buy_price = np.float64(l1_grid[rung_index])
        l2_buy_price = np.float64(l2_grid[rung_index])
        l1_sell_price = l1_buy_price * (
            np.float64(1.0) + MIN_ROUND_TRIP_GAP_PCT
        )
        l2_sell_price = l2_buy_price * (
            np.float64(1.0) + MIN_ROUND_TRIP_GAP_PCT
        )
        l1_row = (
            f"{rung_index + 1:4d} | {l1_buy_price:19.6f} | "
            f"{l1_usdc[rung_index]:15.2f} | {l1_sell_price:21.6f} | "
            f"{l1_sol[rung_index]:14.4f}"
        )
        l2_row = (
            f"{rung_index + 1:4d} | {l2_buy_price:19.6f} | "
            f"{l2_usdc[rung_index]:15.2f} | {l2_sell_price:21.6f} | "
            f"{l2_sol[rung_index]:14.4f}"
        )
        print(f"{l1_row} || {l2_row}")


def log_ladder_state_isolated(
    context_trigger: str,
    current_tier: int,
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    market_price: float,
):
    """
    Dumps both parallel interleaved grid ladder layers (L1 & L2) side by side 
    directly into the dedicated rotating log file without leaking to console.
    """
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    total_steps = len(l1_grid)
    l1_sol_total = np.sum(l1_sol, dtype=np.float64)
    l1_usdc_total = np.sum(l1_usdc, dtype=np.float64)
    l2_sol_total = np.sum(l2_sol, dtype=np.float64)
    l2_usdc_total = np.sum(l2_usdc, dtype=np.float64)
    total_sol = l1_sol_total + l2_sol_total + OVERFLOW_VAULT["SOL_DUST"]
    total_usdc = l1_usdc_total + l2_usdc_total + OVERFLOW_VAULT["USDC_DUST"]
    market_price = np.float64(market_price)
    estimated_fund_usd = total_sol * market_price + total_usdc

    ladder_state_logger.info(f"=== [GRID_SNAPSHOT_START] Trigger: {context_trigger} ===")
    ladder_state_logger.info(f"Current Matrix Focus Index Layer: Step #{current_tier + 1}")
    ladder_state_logger.info(
        f"[PORTFOLIO AUDIT] Price=${market_price:.4f} | "
        f"L1={l1_sol_total:.8f} SOL / ${l1_usdc_total:.4f} USDC | "
        f"L2={l2_sol_total:.8f} SOL / ${l2_usdc_total:.4f} USDC | "
        f"Total incl. vault={total_sol:.8f} SOL / ${total_usdc:.4f} USDC | "
        f"Estimated fund=${estimated_fund_usd:.4f}"
    )
    ladder_state_logger.info(
        f"{'Step':<5} | "
        f"{'L1 Price':<11} | {'L1 SOL':<10} | {'L1 USDC':<10} || "
        f"{'L2 Price':<11} | {'L2 SOL':<10} | {'L2 USDC':<10}"
    )
    ladder_state_logger.info("-" * 95)
    
    for i in range(total_steps):
        # Tracking both structures cleanly using relative row indexing markers
        ladder_state_logger.info(
            f"#{i+1:03d} | "
            f"{l1_grid[i]:<11.4f} | {l1_sol[i]:<10.4f} | ${l1_usdc[i]:<9.2f} || "
            f"{l2_grid[i]:<11.4f} | {l2_sol[i]:<10.4f} | ${l2_usdc[i]:<9.2f}"
        )
    ladder_state_logger.info(
        f"Overflow Vault Account Balances: "
        f"{OVERFLOW_VAULT['SOL_DUST']:.6f} SOL | ${OVERFLOW_VAULT['USDC_DUST']:.4f} USDC"
    )
    ladder_state_logger.info("=== [GRID_SNAPSHOT_END] ===")

def analyze_and_suggest_ladder_expansion():
    """
    Analyzes historical volatility crossings from the CSV database ledger.
    Generates a structured configuration advice snapshot to recommend safety window expansion parameters.
    """
    if not os.path.exists(CSV_LEDGER_PATH):
        return
        
    try:
        with open(CSV_LEDGER_PATH, mode="r") as csv_file:
            lines = list(csv.reader(csv_file))
            if len(lines) <= 5:
                return # Insufficient statistical track data collected
                
            # Extract historical step crossing tracks
            steps_logged = [int(row[3]) for row in lines[1:] if len(row) > 3 and row[3].strip().isdigit()]
            if not steps_logged:
                return
                
            min_step_reached = min(steps_logged)
            max_step_reached = max(steps_logged)
            
            # --- EVALUATE MATRIX EXHAUSTION THREAT LEVELS ---
            # If historical tracks show trades hitting within 1 step of your boundaries, recommend growth
            combined_rung_count = NUMBER_OF_LADDER * 2

            ladder_state_logger.info("=== [QUANTITATIVE GRID ANALYSIS REPORT] ===")
            ladder_state_logger.info(
                f"Historical Active Step Range Bounds: Min Step #{min_step_reached} | "
                f"Max Step #{max_step_reached} "
                f"(Per Ladder: {NUMBER_OF_LADDER}; Combined: {combined_rung_count})"
            )

            if min_step_reached <= 2 or max_step_reached >= (combined_rung_count - 1):
                recommended_per_ladder = int(np.ceil(NUMBER_OF_LADDER * 1.25))
                recommended_combined = recommended_per_ladder * 2

                ladder_state_logger.info("[ALERT: EXHAUSTION RISK] Price volatility has aggressively pushed near your ladder boundaries.")
                print("[ALERT: EXHAUSTION RISK] Price volatility has aggressively pushed near your ladder boundaries.")
                recommendation = (
                    f"Recommended Action Strategy: Set NUMBER_OF_LADDER in .env "
                    f"from {NUMBER_OF_LADDER} to {recommended_per_ladder} "
                    f"rungs per ladder ({combined_rung_count} to "
                    f"{recommended_combined} combined). Restart the engine to apply."
                )
                ladder_state_logger.info(recommendation)
                print(recommendation)
                ladder_state_logger.info("This adjustment will compress inter-rung delta gaps, boost localized trading volume capture, and shield assets from permanent vault lockout events.")
            else:
                ladder_state_logger.info("[STATUS: OPTIMAL] Current ladder configuration limits provide a safe structural cushioning barrier against immediate out-of-bounds market exposure.")
            ladder_state_logger.info("==========================================")
            
    except Exception as e:
        print(f"[RECONFIG ADVISOR ERROR] Failed to compute quantitative ledger metrics: {e}")

async def main():
    global OVERFLOW_VAULT

    print("[SYSTEM START] Starting the two-ladder SOL/USDC price monitor")
    print(f"[INVENTORY MODE] {INVENTORY_MODE}.")
    print("[ACCOUNTING MODE] Mock fills update local inventory only; no swaps are broadcast.")
    print(
        "[PRICE MODE] Synthetic price shifter is "
        f"{'enabled' if RUN_SIMULATION_PRICE_SHIFTER else 'disabled'}."
    )
    print("[EXECUTION MODE] MOCK fills only; no swap transactions are signed or sent.")

    async with AsyncClient(rpc_url) as client:
        if not await client.is_connected():
            print("[CRITICAL] RPC node is unavailable. The monitor will not start.")
            return

        print("[CONNECTION] RPC node status: online")
        live_sol = 0.0
        live_usdc = 0.0
        if INVENTORY_MODE == "WALLET":
            live_sol = await fetch_live_token_balance(client, "SOL")
            live_usdc = await fetch_live_token_balance(client, "USDC")
            if live_sol <= 0 and live_usdc <= 0:
                print("[CRITICAL] No SOL or USDC balance was available to initialize the ladders.")
                return
        else:
            print(
                f"[SIMULATION MODE] Using configured ${INITIAL_ALLOCATION_USDC:,.2f} "
                "virtual budget; live wallet balances are not used."
            )

        calibrated_boot_price = await fetch_onchain_price(client, "SOL/USD")
        if calibrated_boot_price is None or not np.isfinite(calibrated_boot_price) or calibrated_boot_price <= 0:
            print("[CRITICAL] Price calibration failed; the ladders were not initialized.")
            return

        recovered_state = load_engine_state_cache(calibrated_boot_price)
        anchor_price = (
            recovered_state["base_anchor_price"]
            if recovered_state is not None
            else calibrated_boot_price
        )
        grid_floor = anchor_price * 0.50
        grid_ceiling = anchor_price * 1.50
        l1_grid, l2_grid = generate_infinite_interleaved_matrix(
            anchor_price,
            mode="geometric",
        )
        merged_prices, merged_sources = build_interleaved_event_mesh(l1_grid, l2_grid)
        layer_grids = (l1_grid, l2_grid)

        if recovered_state is not None:
            print("[STATE RECOVERY] Restoring both ladder inventories from the local state cache.")
            loop_counter = recovered_state["loop_counter"]
            l1_sol_stock = recovered_state["l1_sol"]
            l1_usdc_stock = recovered_state["l1_usdc"]
            l2_sol_stock = recovered_state["l2_sol"]
            l2_usdc_stock = recovered_state["l2_usdc"]
            OVERFLOW_VAULT = recovered_state["overflow_vault"]
            cash_bucket_baselines = recovered_state["cash_bucket_baselines"]
        else:
            loop_counter = 0
            if INVENTORY_MODE == "SIMULATION":
                (
                    l1_sol_stock,
                    l1_usdc_stock,
                    l2_sol_stock,
                    l2_usdc_stock,
                    cash_bucket_baselines,
                ) = initialize_simulation_inventory(
                    l1_grid,
                    l2_grid,
                    calibrated_boot_price,
                    INITIAL_ALLOCATION_USDC,
                )
            else:
                l1_sol_stock, l1_usdc_stock = initialize_live_inventory_matrix(
                    l1_grid,
                    calibrated_boot_price,
                    live_sol_total=live_sol / 2.0,
                    live_usdc_total=live_usdc / 2.0,
                )
                l2_sol_stock, l2_usdc_stock = initialize_live_inventory_matrix(
                    l2_grid,
                    calibrated_boot_price,
                    live_sol_total=live_sol / 2.0,
                    live_usdc_total=live_usdc / 2.0,
                )
                cash_bucket_baselines = (
                    (live_usdc / 2.0) / max(1, int(np.count_nonzero(l1_grid < calibrated_boot_price))),
                    (live_usdc / 2.0) / max(1, int(np.count_nonzero(l2_grid < calibrated_boot_price))),
                )

        rebalance_ladder_spillovers(
            l1_sol_stock,
            l1_usdc_stock,
            l1_grid,
            cash_bucket_baselines[0],
        )
        rebalance_ladder_spillovers(
            l2_sol_stock,
            l2_usdc_stock,
            l2_grid,
            cash_bucket_baselines[1],
        )
        layer_stocks = (l1_sol_stock, l1_usdc_stock, l2_sol_stock, l2_usdc_stock)
        previous_event_index = int(np.abs(merged_prices - calibrated_boot_price).argmin())
        print(
            f"[LADDERS READY] L1/L2 rungs: {len(l1_grid)} each; "
            f"combined crossing levels: {len(merged_prices)}; anchor: ${anchor_price:,.4f}."
        )
        print(
            f"[INVENTORY] L1+L2 totals: "
            f"{np.sum(l1_sol_stock, dtype=np.float64) + np.sum(l2_sol_stock, dtype=np.float64):.8f} SOL, "
            f"${np.sum(l1_usdc_stock, dtype=np.float64) + np.sum(l2_usdc_stock, dtype=np.float64):.4f} USDC."
        )

        dump_engine_state_cache(
            anchor_price,
            loop_counter,
            OVERFLOW_VAULT,
            layer_stocks,
            cash_bucket_baselines,
        )
        log_ladder_state_isolated(
            "ENGINE_BOOT_INITIALIZATION",
            previous_event_index,
            l1_grid,
            l2_grid,
            layer_stocks,
            calibrated_boot_price,
        )

        last_git_vault_timestamp = time.time()
        while True:
            loop_counter += 1
            live_price = await fetch_onchain_price(client, "SOL/USD")
            if live_price is None or not np.isfinite(live_price) or live_price <= 0:
                await asyncio.sleep(3)
                continue

            live_price = get_simulated_price(live_price, loop_counter)
            current_event_index = int(np.abs(merged_prices - live_price).argmin())
            if current_event_index == previous_event_index:
                print(
                    f"[TICK {loop_counter}] Price ${live_price:,.4f} remains near combined level "
                    f"{current_event_index + 1}/{len(merged_prices)}."
                )
                await asyncio.sleep(3)
                continue

            direction = "UPWARD" if current_event_index > previous_event_index else "DOWNWARD"
            crossed_indices = (
                range(previous_event_index + 1, current_event_index + 1)
                if direction == "UPWARD"
                else range(previous_event_index - 1, current_event_index - 1, -1)
            )
            steps_moved = abs(current_event_index - previous_event_index)
            print(
                f"[CROSSING] {direction}: {steps_moved} interleaved level(s), "
                f"combined index {previous_event_index + 1} -> {current_event_index + 1}."
            )

            # Convert crossed_indices to a list to safely capture the terminal tracking index
            crossed_list = list(crossed_indices)
            for event_index in crossed_indices:
                layer_index, rung_index = merged_sources[event_index]
                # Evaluates to True ONLY for the absolute final step in the current wave profile
                terminal_flag = (event_index == crossed_list[-1])
                apply_interleaved_crossing(
                    direction=direction,
                    layer_index=layer_index,
                    rung_index=rung_index,
                    rung_price=np.float64(merged_prices[event_index]),
                    layer_grids=layer_grids,
                    layer_stocks=layer_stocks,
                    cash_bucket_baselines=cash_bucket_baselines,
                    is_terminal_step=terminal_flag
                )
            # GLOBAL INTERLEAVED PERSISTENCE SWEEP
            # Ensures any asset or cash surplus pushed past boundary caps cascades 
            # completely into permanent long-term vault archives unconditionally.
            rebalance_ladder_spillovers(l1_sol_stock, l1_usdc_stock, l1_grid, cash_bucket_baselines[0])
            rebalance_ladder_spillovers(l2_sol_stock, l2_usdc_stock, l2_grid, cash_bucket_baselines[1])

            total_sol = np.sum(l1_sol_stock, dtype=np.float64) + np.sum(
                l2_sol_stock, dtype=np.float64
            )
            total_usdc = np.sum(l1_usdc_stock, dtype=np.float64) + np.sum(
                l2_usdc_stock, dtype=np.float64
            )
            print(f"[PORTFOLIO MODEL] {total_sol:.8f} SOL + ${total_usdc:,.4f} USDC")
            print_interleaved_ladder_state(
                loop_counter,
                l1_grid,
                l2_grid,
                layer_stocks,
            )
            log_transaction_to_csv(
                loop_id=loop_counter,
                live_price=live_price,
                current_step=current_event_index + 1,
                direction=direction,
                steps_moved=steps_moved,
                layer_stocks=layer_stocks,
            )
            last_git_vault_timestamp = execute_automated_git_vault(
                tick_id=loop_counter,
                current_step=current_event_index + 1,
                market_price=live_price,
                last_vault_time=last_git_vault_timestamp,
            )
            # Resolve clean initialization focus row index parameter
            active_focal_rung = previous_event_index if previous_event_index is not None else 0

            log_ladder_state_isolated(
                "ENGINE_BOOT_INITIALIZATION",
                active_focal_rung,
                l1_grid,
                l2_grid,
                layer_stocks,
                calibrated_boot_price,
            )
            dump_engine_state_cache(
                anchor_price,
                loop_counter,
                OVERFLOW_VAULT,
                layer_stocks,
                cash_bucket_baselines,
            )
            analyze_and_suggest_ladder_expansion()
            previous_event_index = current_event_index
            if RUN_SIMULATION_PRICE_SHIFTER:
                # Fast-forward lab tests at 20 ticks per second to audit multi-day curves instantly
                await asyncio.sleep(0.05) 
            else:
                # Production Track: Paced threshold buffer to protect Alchemy RPC node limits
                await asyncio.sleep(3.0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM SHUTDOWN] Execution loop terminated by user.")
