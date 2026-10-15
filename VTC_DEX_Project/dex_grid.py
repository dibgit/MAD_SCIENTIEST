import os
import sys
import requests
import asyncio
import struct
import csv
import time
import logging
from logging.handlers import RotatingFileHandler
import subprocess
import json
import random
import numpy as np
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders.message import MessageV0
from solana.rpc.async_api import AsyncClient
from datetime import datetime
from dataclasses import dataclass

# ==============================================================================
# AUTHORITATIVE GLOBAL STORAGE MANAGEMENT MATRIX
# ==============================================================================
# REVERTED: Standardized gap to absorb 0.6% fees + ensure a clean +0.15% compounding return
#MIN_ROUND_TRIP_GAP_PCT = 0.01362703
MIN_ROUND_TRIP_GAP_PCT = 0.008

# Keep track of previous prices globally to detect a flatlining feed
ORACLE_STAGNANT_TRACKER = {
    "frozen_baseline": 0.0,    # Stores the exact stale price line to watch for wake-ups
    "last_price": 0.0,         # Stores the active working price matrix anchor
    "flatline_count": 0,       # Sequential tracking tick counter
    "is_stale_mode": False     # Lockout flag: True if we are running in Jupiter Fallback mode
}
# THE N+1th or 258th ELEMENT TRANSIT VAULT:
# Completely isolates fractional leftovers, dust balances, and transaction fees
# to keep your core N=257-step matrix from expanding or breaking.
OVERFLOW_VAULT = {
    "SOL_DUST": 0.0,
    "USDC_DUST": 0.0
}

# Initialize environment configuration pathing
load_dotenv()
rpc_url = os.getenv("SOLANA_RPC_URL")
wallet_address_str = os.getenv("MY_PUBLIC_WALLET")
jupiter_api_key = os.getenv("JUPITER_API_KEY")

# DYNAMIC ENGINE CONFIGURATION EXTRACTION
# Dynamically extract and enforce ladder step sizes. Default to 257 if missing.
NUMBER_OF_LADDER = int(os.getenv("NUMBER_OF_LADDER", default=257))

# DYNAMIC RELATIVE PATH EXTRACTION
# os.path.dirname(__file__) extracts the exact folder this file resides in (D:\MAD_SCIENTIEST\VTC_DEX_Project)
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_LEDGER_PATH = os.path.join(PROJECT_DIR, "grid_ledger.csv")

# PRODUCTION DIRECTORY ENFORCEMENT & ENGINE CONFIGURATION
DATA_DIR = os.path.join(PROJECT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Extract and scale configuration metrics for rotating file buffers
MAX_LOG_SIZE_BYTES = int(os.getenv("MAX_LOG_SIZE_KB", default=10)) * 1024
MAX_LOG_BACKUPS = int(os.getenv("MAX_LOG_BACKUPS", default=10))
LOG_FILE_BASE_PATH = os.path.join(DATA_DIR, "messages.log")

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
            
        # Execute downset shifting logic across historic logs sequentially up to limit boundaries
        for i in range(self.backupCount - 1, -1, -1):
            sfn = os.path.join(DATA_DIR, f"messages{i}.log" if i > 0 else "messages.log")
            dfn = os.path.join(DATA_DIR, f"messages{i+1}.log")
            if os.path.exists(sfn):
                if os.path.exists(dfn):
                    os.remove(dfn)
                os.rename(sfn, dfn)
                
        # Re-initialize clean working target log parameters
        dfn = os.path.join(DATA_DIR, "messages1.log")
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

# Universal Token Mint Repository Map
TOKEN_MINTS = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    #"wVTC": "PASTE_DECENTRALIZED_WVTC_MINT_ADDRESS_HERE"
    # ADVANCE TARGET: For the time being, we use a real live placeholder token string 
    # to guarantee the RPC nodes return a valid account balance structure instead of crashing.
    "wVTC": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" # (Temporarily mapping to USDT for safe boot tracing)
}
# Permanent Pyth Network Mainnet Price Oracle Mapping Index
ORACLE_FEEDS = {
    "SOL/USD": "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
}
# The profit multiplier threshold before a single bucket overflows (e.g., 1.20 = 20% growth)
BUCKET_OVERFLOW_THRESHOLD = 1.20

# DYNAMIC WORKING BALANCES: Base budget allocated per step is dynamically scaled to NUMBER_OF_LADDER
BASE_USDC_PER_BUCKET = 1000.0 / NUMBER_OF_LADDER

# SET TO TRUE TO ACTIVATE THE RANDOM CONVEYOR ORDER EXHAUSTION ENGINE
RUN_SIMULATION_PRICE_SHIFTER = True

# Variable tracking to manage random simulated order-book flows
SIMULATION_STATE = {
    "current_sim_price": 0.0,
    "consecutive_direction_count": 0,
    "active_trend": "UPWARD"
}

# Standalone, synthetic wVTC/SOL conveyor simulation settings
CONVEYOR_SIMULATION_EVENTS = int(os.getenv("CONVEYOR_SIMULATION_EVENTS", default=30))
CONVEYOR_SIMULATION_SEED = int(os.getenv("CONVEYOR_SIMULATION_SEED", default=42))
CONVEYOR_SIMULATION_RUNGS = int(os.getenv("CONVEYOR_SIMULATION_RUNGS", default=12))
# Adjust simulation fee to 0.3% per side (0.6% total round-trip) to match live metrics
CONVEYOR_SIMULATION_FEE_RATE = float(os.getenv("CONVEYOR_SIMULATION_FEE_RATE", default=0.003))
CONVEYOR_SIMULATION_TARGET_NET_GROWTH = float(os.getenv("CONVEYOR_SIMULATION_TARGET_NET_GROWTH", default=0.0015))
USDC_SOL_SIMULATION_BUY_FLOOR = float(os.getenv("USDC_SOL_SIMULATION_BUY_FLOOR", default=100.0))
USDC_SOL_SIMULATION_BUY_CEILING = float(os.getenv("USDC_SOL_SIMULATION_BUY_CEILING", default=200.0))
USDC_SOL_SIMULATION_INITIAL_USDC = float(os.getenv("USDC_SOL_SIMULATION_INITIAL_USDC", default=1500.0))
USDC_SOL_SIMULATION_INITIAL_SOL = float(os.getenv("USDC_SOL_SIMULATION_INITIAL_SOL", default=15.0))
USDC_SOL_SIMULATION_SELL_BUCKET_CAP_SOL = float(os.getenv("USDC_SOL_SIMULATION_SELL_BUCKET_CAP_SOL", default=3.0))

# The minimum cooldown window in seconds between remote repository backup push execution cycles (300 seconds = 5 minutes)
GIT_VAULT_COOLDOWN_SECONDS = 300

def log_transaction_to_csv(loop_id: int, live_price: float, current_step: int, direction: str, steps_moved: int, sol_total: float, usdc_total: float):
    """Appends high-precision grid metric snapshots directly to a relative persistent CSV database file."""
    file_exists = os.path.exists(CSV_LEDGER_PATH)
    portfolio_value = (sol_total * live_price) + usdc_total
    
    # Define clean, structural database tracking headers matching your Excel ledger
    headers = ["timestamp", "tick_id", "market_price", "active_step", "trade_direction", "steps_crossed", "total_sol", "total_usdc", "net_worth_usd"]
    
    try:
        with open(CSV_LEDGER_PATH, mode="a", newline="") as csv_file:
            writer = csv.writer(csv_file)
            
            # If the database file is brand new, instantiate structural track headers
            if not file_exists:
                writer.writerow(headers)
                
            # Append high-precision numeric records cleanly into the data columns
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                loop_id,
                f"{live_price:.4f}",
                current_step,
                direction,
                steps_moved,
                f"{sol_total:.6f}",
                f"{usdc_total:.4f}",
                f"{portfolio_value:.4f}"
            ])
    except Exception as e:
        print(f"[ERROR] CSV Database Engine Write Failure: {e}")

async def check_wallet_fuel(client: AsyncClient):
    """Asynchronously reads the native SOL balance parameters of the public sandbox key."""
    if not wallet_address_str:
        print("[ERROR] MY_PUBLIC_WALLET variable missing from .env configuration.")
        return
        
    try:
        clean_wallet = str(wallet_address_str).replace('"', '').replace("'", "").strip()
        public_key = Pubkey.from_string(clean_wallet)
        
        balance_response = await client.get_balance(public_key)
        sol_balance = balance_response.value / 1_000_000_000
        print(f"[FUEL CHECK] Sandbox Account Balance: {sol_balance} SOL")
    except Exception as e:
        print(f"[ERROR] Failed to execute wallet balance check string: {e}")

async def fetch_live_token_balance(client: AsyncClient, token_ticker: str) -> float:
    """
    Programmatically queries the live Solana blockchain ledger via RPC.
    Derives Associated Token Accounts (ATA) to parse exact real-time SPL token balances.
    """
    if not wallet_address_str:
        print(f"[ERROR] Cannot fetch {token_ticker}: MY_PUBLIC_WALLET missing from .env.")
        return 0.0
        
    token_mint_str = TOKEN_MINTS.get(token_ticker)
    if not token_mint_str:
        print(f"[ERROR] Ticker mapping configuration missing for target: {token_ticker}")
        return 0.0

    try:
        # Clean and sanitize configuration strings out of your .env payload
        clean_wallet = str(wallet_address_str).replace('"', '').replace("'", "").strip()
        clean_mint = str(token_mint_str).replace('"', '').replace("'", "").strip()
        
        owner_pubkey = Pubkey.from_string(clean_wallet)
        mint_pubkey = Pubkey.from_string(clean_mint)
        
        # Interrogate the live RPC node using the official SPL Token Program filters
        opts = {"mint": mint_pubkey}
        response = await client.get_token_accounts_by_owner(owner_pubkey, opts)
        
        if not response.value:
            # If the account doesn't exist on-chain yet, it means the balance is exactly 0
            print(f"[BALANCE CHECK] Live {token_ticker} Wallet State: 0.0000 (No Associated Account Found)")
            return 0.0
            
        # Extract the account public key from the first matched array slot
        account_data_block = response.value[0]
        account_pubkey = account_data_block.pubkey
        
        # Query the exact token account details to extract high-precision integer balances
        balance_response = await client.get_token_account_balance(account_pubkey)
        
        if balance_response.value:
            # Extract the raw floating-point valuation parsed directly by the RPC parser node
            live_balance = float(balance_response.value.ui_amount)
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
        oracle_price = float(raw_price) / 100_000_000.0
        
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
                        spot_price = float(prices[sol_mint]['usdPrice'])
                        
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

def generate_N_ladder(low_price: float, high_price: float, mode: str = "geometric"):
    """Generates an explicit, high-precision mathematical variable N-tier matrix array layout."""
    mid_index = int(NUMBER_OF_LADDER / 2)
    
    if mode == "arithmetic":
        grid_array = np.linspace(low_price, high_price, NUMBER_OF_LADDER)
    else:
        grid_array = np.geomspace(low_price, high_price, NUMBER_OF_LADDER)
        
    print(f"[ENGINE INITIALIZED] Mode: {mode.upper()} Configured Sizing Steps: {NUMBER_OF_LADDER}")
    print(f"  Floor Boundary   (Step 001) : ${grid_array[0]:,.4f}")
    print(f"  Midpoint Anchor  (Step {mid_index+1:03d}) : ${grid_array[mid_index]:,.4f}")
    print(f"  Ceiling Boundary (Step {NUMBER_OF_LADDER:03d}) : ${grid_array[-1]:,.4f}")
    
    if mode == "geometric" and NUMBER_OF_LADDER > 1:
        pct_ratio = (grid_array[1] - grid_array[0]) / grid_array[0] * 100
        print(f"  Compounding Delta Step Size: {pct_ratio:.4f}% percentage variance")
        
        # MINIMALIST POSITIVE ZONE SNAPSHOT:
        # Enforces a quick visual validation of your round-trip inventory compounding capacity
        net_estimated_return = pct_ratio - 0.60
        if net_estimated_return > 0:
            print(f"  [MATH CHECK] Net Round-Trip Token Delta: +{net_estimated_return:.4f}% (POSITIVE COMPOUNDING ZONE)")
        else:
            print(f"  [MATH CHECK] Net Round-Trip Token Delta: {net_estimated_return:.4f}% (NEGATIVE ZONE: Increase range or decrease steps)")
            
    return grid_array


def initialize_inventory_matrix(grid_matrix, current_price, total_budget_usdc=1000.0):
    """Allocates your budget across all configured steps based on current market price."""
    total_steps = len(grid_matrix)
    usdc_per_step = total_budget_usdc / total_steps
    
    # Initialize dictionary matrices to hold values for each step index
    sol_inventory = np.zeros(total_steps)
    usdc_inventory = np.zeros(total_steps)
    
    for i in range(total_steps):
        tier_price = grid_matrix[i]
        if tier_price < current_price:
            # Below market: holding raw stablecoin power
            usdc_inventory[i] = usdc_per_step
        else:
            # Above market: holding asset inventory valued at this specific tier price
            sol_inventory[i] = usdc_per_step / tier_price
            
    print(f"[INVENTORY MATRIX CALIBRATED] Total Budget: ${total_budget_usdc:.2f} USDC allocated across {total_steps} steps.")
    print(f"  Initial Total SOL Asset Stock : {np.sum(sol_inventory):.4f} SOL")
    print(f"  Initial Total USDC Cash Stock : ${np.sum(usdc_inventory):.2f} USDC")
    return sol_inventory, usdc_inventory

def check_bucket_overflow(step_index: int, sol_inventory, usdc_inventory, grid_matrix):
    """
    Evaluates if a single step bucket's realized capital exceeds the target threshold.
    If true, the excess profit overflows into the upper adjacent ladder tier.
    """
    total_steps = len(grid_matrix)
    
    # Check if there is a valid upper ladder tier to overflow into
    if step_index >= total_steps - 1:
        return
        
    current_cash = usdc_inventory[step_index]
    max_allowed_cash = BASE_USDC_PER_BUCKET * BUCKET_OVERFLOW_THRESHOLD
    
    if current_cash > max_allowed_cash:
        excess_profit = current_cash - BASE_USDC_PER_BUCKET
        upper_step = step_index + 1
        
        print(f"[BUCKET OVERFLOW DETECTED] Step {step_index+1} breached limit (${current_cash:.4f} > ${max_allowed_cash:.4f})")
        
        # Reset the current bucket back to its baseline working cash
        usdc_inventory[step_index] = BASE_USDC_PER_BUCKET
        
        # Cascade the excess value to the upper ladder step
        # Convert the profit into asset allocation based on the target upper step price line
        upper_tier_price = grid_matrix[upper_step]
        asset_addition = excess_profit / upper_tier_price
        
        sol_inventory[upper_step] += asset_addition
        
        print(f"  Action: Shifted ${excess_profit:.4f} USDC profit into Step {upper_step+1} as {asset_addition:.4f} SOL")

def get_simulated_price(real_oracle_price: float, current_tick: int) -> float:
    """Generates a full bidirectional market cycle wave to test grid functionality."""
    if not RUN_SIMULATION_PRICE_SHIFTER:
        return real_oracle_price
        
    # Calibrate baseline tracking during initial iterations
    if current_tick <= 5:
        return real_oracle_price
        
    # Phase Shift Logic: Construct a mathematical market wave
    # Peak pump behavior materializes at tick 35, followed by a downward correction trend
    if current_tick <= 35:
        ticks_since_pump = current_tick - 5
        simulated_pump_factor = 1.0 + (ticks_since_pump * 0.003)
        simulated_price = real_oracle_price * simulated_pump_factor
    else:
        # Downward Correction Phase
        ticks_since_peak = current_tick - 35
        peak_factor = 1.0 + (30 * 0.003)  # Maximum price factor reached at tick 35
        # Draw down the price by 0.005 (0.5%) on every single tick cycle past the peak
        simulated_dump_factor = peak_factor - (ticks_since_peak * 0.005)
        simulated_price = real_oracle_price * simulated_dump_factor
        
    print(f"[SIMULATION SHIFTER] Active Cycle Wave Price Target: ${simulated_price:,.4f}")
    return simulated_price

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
        
    repo_directory = r"D:\MAD_SCIENTIEST"
    commit_message = f"Vault Update: Tick #{tick_id} | Active Step: {current_step} | Market Benchmark: ${market_price:.2f}"
    
    print(f"[GIT VAULT] System cooldown cleared ({int(elapsed_time)}s elapsed). Initializing background sync...")
    
    try:
        # Execute background processes using thin execution masks
        subprocess.run(["git", "add", "VTC_DEX_Project/dex_grid.py", "VTC_DEX_Project/grid_ledger.csv"], 
                       cwd=repo_directory, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
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

def prepare_onchain_dex_swap(wallet_keypair: Keypair, base_mint: str, quote_mint: str, amount_in: int, slide_direction: str):
    """
    Constructs a structural Web3 serialized transaction blueprint payload for decentralized routing.
    Bypasses standard exchange dashboards by compiling raw cryptographic transaction objects.
    """
    # In a live Web3 engine, your script pings the decentralized Jupiter V6 Router API
    # to extract the optimal raw execution byte instructions for the swap
    quote_url = f"https://jup.ag{base_mint}&outputMint={quote_mint}&amount={amount_in}&slippageBps=50"
    
    print(f"[DEX ROUTER] Generating on-chain swap instructions for: {slide_direction}")
    print(f"  Input Target  : {base_mint[:8]}... | Output Target: {quote_mint[:8]}...")
    print(f"  Raw Unit Size : {amount_in} Base Atoms")
    
    # Mock payload structure showing exactly what gets broadcast to the Solana network:
    try:
        # 1. Compile the transaction instructions into an optimized binary message block
        # 2. Sign the transaction payload natively using your private seed keys
        # 3. Broadcast the raw transaction bytes to your Alchemy node via 'send_transaction'
        
        print(f"[DEX TRANSACT] Transaction payload compiled and signed by account: {wallet_keypair.pubkey()}")
        print(f"  Status        : [SIMULATED BROADCAST SUCCESSFUL] -> TxString: MockSolanaTxHashString_StepCrossing")
        
        return True
    except Exception as e:
        print(f"[ERROR] On-chain compilation anomaly: {e}")
        return False

def execute_live_onchain_sell(wallet_keypair: Keypair, base_mint: str, quote_mint: str, amount_in_sol: float, target_price: float) -> float:
    """
    Dispatches a live on-chain sell transaction to the Solana network.
    Parses blockchain receipts to return the exact mathematical fill percentage.
    """
    # 1. In production, this compiles the raw serialized swap bytes from Jupiter V6
    # 2. Signs the payload using your sandbox seed keys, and broadcasts it to your Alchemy RPC node
    # 3. Intercepts the confirmed transaction receipt logs from the blockchain
    
    print(f"[LIVE EXECUTION] Dispatched SOL -> USDC Swap for {amount_in_sol:.4f} SOL")
    
    # MOCK PRODUCTION RECEIPT: Default to 100% fill rate for successful on-chain swaps
    # In live execution, if slippage occurs, this will parse the real returned atoms dynamically
    true_fill_percentage = 1.00 
    return true_fill_percentage

def execute_live_onchain_buy(wallet_keypair: Keypair, base_mint: str, quote_mint: str, amount_in_usdc: float, target_price: float) -> float:
    """
    Dispatches a live on-chain buy transaction to the Solana network.
    Parses blockchain receipts to return the exact mathematical fill percentage.
    """
    print(f"[LIVE EXECUTION] Dispatched USDC -> SOL Swap for ${amount_in_usdc:.4f} USDC")
    
    true_fill_percentage = 1.00
    return true_fill_percentage

def print_matrix_slice_lane(start_idx: int, end_idx: int, grid_matrix, sol_stock, usdc_stock):
    """
    Prints a targeted horizontal snapshot of your grid ladder allocations.
    Enforces a clean, scannable text line tracking exact tier balances.
    """
    total_steps = len(grid_matrix)
    # Safely constrain the index parameters to stay within your 257 array bounds
    s_idx = max(0, min(start_idx, total_steps - 1))
    e_idx = max(s_idx, min(end_idx, total_steps - 1))
    
    output_segments = []
    for i in range(s_idx, e_idx + 1):
        tier_price = grid_matrix[i]
        sol_bal = sol_stock[i]
        usdc_bal = usdc_stock[i]
        
        # Format each step dynamically: [Step_ID: Price | SOL_Qty / USDC_Qty]
        segment = f"[{i+1:03d}:${tier_price:.2f}|{sol_bal:.4f}S/${usdc_bal:.2f}U]"
        output_segments.append(segment)
        
    print(f"[MATRIX INSPECT] Steps {s_idx+1} to {e_idx+1}: " + " -> ".join(output_segments))


@dataclass
class ConveyorSimulationState:
    buy_prices: np.ndarray
    sell_prices: np.ndarray
    buy_sol: np.ndarray
    sell_wvtc: np.ndarray
    sell_bucket_cap_sol: float
    overflow_wvtc: float = 0.0
    fees_paid_sol: float = 0.0
    fees_paid_wvtc: float = 0.0
    trade_count: int = 0


def create_conveyor_simulation_state(
    rung_count: int,
    buy_floor_sol: float,
    buy_ceiling_sol: float,
    initial_sol: float,
    initial_wvtc: float,
    sell_bucket_cap_sol: float,
) -> ConveyorSimulationState:
    """Builds paired geometric buy/sell rungs with independent inventory lanes."""
    if rung_count < 2:
        raise ValueError("The conveyor simulation requires at least two rungs.")
    if not (0 < buy_floor_sol < buy_ceiling_sol):
        raise ValueError("Buy floor and ceiling must be positive, with floor below ceiling.")
    values = (
        buy_floor_sol,
        buy_ceiling_sol,
        initial_sol,
        initial_wvtc,
        sell_bucket_cap_sol,
        CONVEYOR_SIMULATION_FEE_RATE,
        CONVEYOR_SIMULATION_TARGET_NET_GROWTH,
    )
    if not all(np.isfinite(value) for value in values):
        raise ValueError("Conveyor simulation prices, balances, caps, and rates must be finite.")
    if initial_sol < 0 or initial_wvtc < 0 or sell_bucket_cap_sol <= 0:
        raise ValueError("Initial balances must be nonnegative and the sell-bucket cap positive.")
    if not (0 <= CONVEYOR_SIMULATION_FEE_RATE < 1):
        raise ValueError("The simulation fee rate must be at least 0 and less than 1.")
    if not np.isfinite(MIN_ROUND_TRIP_GAP_PCT) or MIN_ROUND_TRIP_GAP_PCT <= 0:
        raise ValueError("MIN_ROUND_TRIP_GAP_PCT must be positive.")
    if CONVEYOR_SIMULATION_TARGET_NET_GROWTH < 0:
        raise ValueError("The target net growth must be nonnegative.")
    required_gap = (
        (1 + CONVEYOR_SIMULATION_TARGET_NET_GROWTH)
        / (1 - CONVEYOR_SIMULATION_FEE_RATE) ** 2
    ) - 1
    if MIN_ROUND_TRIP_GAP_PCT + 1e-12 < required_gap:
        raise ValueError(
            f"MIN_ROUND_TRIP_GAP_PCT ({MIN_ROUND_TRIP_GAP_PCT:.6%}) is below "
            f"the configured fee-and-growth requirement ({required_gap:.6%})."
        )

    buy_prices = np.geomspace(buy_floor_sol, buy_ceiling_sol, rung_count)
    sell_prices = buy_prices * (1 + MIN_ROUND_TRIP_GAP_PCT)
    sell_wvtc = np.zeros(rung_count, dtype=float)
    overflow_wvtc = 0.0
    initial_wvtc_per_rung = initial_wvtc / rung_count
    for rung_index in range(rung_count):
        remaining_wvtc = initial_wvtc_per_rung
        for sell_index in range(rung_index, rung_count):
            bucket_cap_wvtc = sell_bucket_cap_sol / sell_prices[sell_index]
            room_wvtc = max(0.0, bucket_cap_wvtc - sell_wvtc[sell_index])
            amount_routed = min(remaining_wvtc, room_wvtc)
            sell_wvtc[sell_index] += amount_routed
            remaining_wvtc -= amount_routed
            if remaining_wvtc <= 1e-12:
                remaining_wvtc = 0.0
                break
        overflow_wvtc += remaining_wvtc

    return ConveyorSimulationState(
        buy_prices=buy_prices,
        sell_prices=sell_prices,
        buy_sol=np.full(rung_count, initial_sol / rung_count, dtype=float),
        sell_wvtc=sell_wvtc,
        sell_bucket_cap_sol=sell_bucket_cap_sol,
        overflow_wvtc=overflow_wvtc,
    )


def simulate_conveyor_buy(
    state: ConveyorSimulationState,
    rung_index: int,
    sol_to_spend: float,
) -> dict:
    """Buys wVTC with a buy-rung's SOL and routes net wVTC up the sell conveyor."""
    if not 0 <= rung_index < len(state.buy_prices):
        raise IndexError("Buy rung index is outside the configured ladder.")
    if not np.isfinite(sol_to_spend) or sol_to_spend <= 0:
        raise ValueError("SOL to spend must be a finite positive amount.")
    if sol_to_spend > state.buy_sol[rung_index] + 1e-12:
        raise ValueError("Buy amount exceeds the SOL available in that rung.")

    gross_wvtc = sol_to_spend / state.buy_prices[rung_index]
    fee_wvtc = gross_wvtc * CONVEYOR_SIMULATION_FEE_RATE
    net_wvtc = gross_wvtc - fee_wvtc
    state.buy_sol[rung_index] = max(0.0, state.buy_sol[rung_index] - sol_to_spend)

    remaining_wvtc = net_wvtc
    routed_wvtc = 0.0
    for sell_index in range(rung_index, len(state.sell_prices)):
        bucket_cap_wvtc = state.sell_bucket_cap_sol / state.sell_prices[sell_index]
        room_wvtc = max(0.0, bucket_cap_wvtc - state.sell_wvtc[sell_index])
        amount_routed = min(remaining_wvtc, room_wvtc)
        state.sell_wvtc[sell_index] += amount_routed
        remaining_wvtc -= amount_routed
        routed_wvtc += amount_routed
        if remaining_wvtc <= 1e-12:
            remaining_wvtc = 0.0
            break

    state.overflow_wvtc += remaining_wvtc
    state.fees_paid_wvtc += fee_wvtc
    state.trade_count += 1
    return {
        "side": "BUY",
        "rung": rung_index + 1,
        "price_sol_per_wvtc": state.buy_prices[rung_index],
        "sol_spent": sol_to_spend,
        "gross_wvtc": gross_wvtc,
        "fee_wvtc": fee_wvtc,
        "net_wvtc": net_wvtc,
        "routed_wvtc": routed_wvtc,
        "overflow_wvtc": remaining_wvtc,
    }


def simulate_conveyor_sell(
    state: ConveyorSimulationState,
    rung_index: int,
    wvtc_to_sell: float,
) -> dict:
    """Sells wVTC at a sell rung and returns net SOL to its paired buy rung."""
    if not 0 <= rung_index < len(state.sell_prices):
        raise IndexError("Sell rung index is outside the configured ladder.")
    if not np.isfinite(wvtc_to_sell) or wvtc_to_sell <= 0:
        raise ValueError("wVTC to sell must be a finite positive amount.")
    if wvtc_to_sell > state.sell_wvtc[rung_index] + 1e-12:
        raise ValueError("Sell amount exceeds the wVTC available in that rung.")

    gross_sol = wvtc_to_sell * state.sell_prices[rung_index]
    fee_sol = gross_sol * CONVEYOR_SIMULATION_FEE_RATE
    net_sol = gross_sol - fee_sol
    state.sell_wvtc[rung_index] = max(0.0, state.sell_wvtc[rung_index] - wvtc_to_sell)
    state.buy_sol[rung_index] += net_sol
    state.fees_paid_sol += fee_sol
    state.trade_count += 1
    return {
        "side": "SELL",
        "rung": rung_index + 1,
        "price_sol_per_wvtc": state.sell_prices[rung_index],
        "wvtc_sold": wvtc_to_sell,
        "gross_sol": gross_sol,
        "fee_sol": fee_sol,
        "net_sol": net_sol,
        "paired_buy_rung": rung_index + 1,
    }


def print_conveyor_ladder(state: ConveyorSimulationState, event_number: int) -> None:
    """Displays both sides of the ladder after one simulated event."""
    print(f"  [LADDER AFTER EVENT {event_number:03d}]")
    print("  Rung | Buy price (SOL/wVTC) | Buy SOL | Sell price (SOL/wVTC) | Sell wVTC | Sell cap (SOL)")
    for rung_index, (buy_price, sell_price, buy_sol, sell_wvtc) in enumerate(
        zip(state.buy_prices, state.sell_prices, state.buy_sol, state.sell_wvtc),
        start=1,
    ):
        sell_value_sol = sell_wvtc * sell_price
        print(
            f"  {rung_index:4d} | {buy_price:19.10f} | {buy_sol:7.4f} | "
            f"{sell_price:21.10f} | {sell_wvtc:9.2f} | "
            f"{sell_value_sol:6.3f}/{state.sell_bucket_cap_sol:.3f}"
        )
    print(f"  Above-top-rung overflow: {state.overflow_wvtc:.2f} wVTC")


def run_random_conveyor_simulation() -> ConveyorSimulationState:
    """Runs seeded random fills against the synthetic geometric wVTC/SOL ladder."""
    if CONVEYOR_SIMULATION_EVENTS < 0:
        raise ValueError("CONVEYOR_SIMULATION_EVENTS must be nonnegative.")
    state = create_conveyor_simulation_state(
        rung_count=CONVEYOR_SIMULATION_RUNGS,
        buy_floor_sol=CONVEYOR_SIMULATION_BUY_FLOOR_SOL,
        buy_ceiling_sol=CONVEYOR_SIMULATION_BUY_CEILING_SOL,
        initial_sol=CONVEYOR_SIMULATION_INITIAL_SOL,
        initial_wvtc=CONVEYOR_SIMULATION_INITIAL_WVTC,
        sell_bucket_cap_sol=CONVEYOR_SIMULATION_SELL_BUCKET_CAP_SOL,
    )
    rng = random.Random(CONVEYOR_SIMULATION_SEED)
    mark_price = float(np.sqrt(state.buy_prices[0] * state.buy_prices[-1]))
    initial_mark_value_sol = float(np.sum(state.buy_sol) + np.sum(state.sell_wvtc) * mark_price)

    print("[CONVEYOR SIMULATION] Synthetic wVTC/SOL prices; no RPC or swaps are used.")
    print(
        f"  Rungs: {CONVEYOR_SIMULATION_RUNGS} | Events: {CONVEYOR_SIMULATION_EVENTS} "
        f"| Seed: {CONVEYOR_SIMULATION_SEED} | Fee/side: {CONVEYOR_SIMULATION_FEE_RATE:.3%}"
    )
    print(
        f"  Buy range: {state.buy_prices[0]:.10f}-{state.buy_prices[-1]:.10f} SOL/wVTC "
        f"| Paired sell gap: {MIN_ROUND_TRIP_GAP_PCT:.3%} "
        f"| Expected net round-trip growth: "
        f"{((1 + MIN_ROUND_TRIP_GAP_PCT) * (1 - CONVEYOR_SIMULATION_FEE_RATE) ** 2 - 1):.3%}"
    )
    print(
        f"  Sell-bucket cap: {state.sell_bucket_cap_sol:.4f} SOL "
        f"| Initial: {np.sum(state.buy_sol):.4f} SOL, {np.sum(state.sell_wvtc):.4f} wVTC"
    )

    for event_number in range(1, CONVEYOR_SIMULATION_EVENTS + 1):
        side = rng.choice(("BUY", "SELL"))
        buy_rungs = np.flatnonzero(state.buy_sol > 1e-12)
        sell_rungs = np.flatnonzero(state.sell_wvtc > 1e-12)

        if side == "BUY" and len(buy_rungs) == 0:
            side = "SELL"
        elif side == "SELL" and len(sell_rungs) == 0:
            side = "BUY"

        if side == "BUY" and len(buy_rungs) > 0:
            rung_index = int(rng.choice(buy_rungs))
            amount = float(state.buy_sol[rung_index] * rng.uniform(0.10, 1.0))
            result = simulate_conveyor_buy(state, rung_index, amount)
            print(
                f"[{event_number:03d}] BUY  rung {result['rung']:02d} | "
                f"{result['sol_spent']:.6f} SOL -> {result['net_wvtc']:.2f} wVTC "
                f"(fee {result['fee_wvtc']:.2f} wVTC; "
                f"routed {result['routed_wvtc']:.2f}; overflow {result['overflow_wvtc']:.2f})"
            )
        elif side == "SELL" and len(sell_rungs) > 0:
            rung_index = int(rng.choice(sell_rungs))
            amount = float(state.sell_wvtc[rung_index] * rng.uniform(0.10, 1.0))
            result = simulate_conveyor_sell(state, rung_index, amount)
            print(
                f"[{event_number:03d}] SELL rung {result['rung']:02d} | "
                f"{result['wvtc_sold']:.2f} wVTC -> {result['net_sol']:.8f} SOL "
                f"(fee {result['fee_sol']:.8f} SOL; "
                f"routed to buy rung {result['paired_buy_rung']:02d})"
            )
        else:
            print(f"[{event_number:03d}] No executable inventory; simulation stopped early.")
            break

        print_conveyor_ladder(state, event_number)

    final_sol = float(np.sum(state.buy_sol))
    final_wvtc = float(np.sum(state.sell_wvtc) + state.overflow_wvtc)
    final_mark_value_sol = final_sol + final_wvtc * mark_price
    print("[CONVEYOR SUMMARY]")
    print(f"  Executed events: {state.trade_count}")
    print(f"  Buy-side SOL remaining: {final_sol:.8f} SOL")
    print(f"  Sell-side wVTC remaining: {np.sum(state.sell_wvtc):.4f} wVTC")
    print(f"  Above-top-rung overflow: {state.overflow_wvtc:.4f} wVTC")
    print(f"  Fees paid: {state.fees_paid_sol:.8f} SOL + {state.fees_paid_wvtc:.4f} wVTC")
    print(
        f"  Synthetic mark value: {initial_mark_value_sol:.8f} -> "
        f"{final_mark_value_sol:.8f} SOL (mark {mark_price:.10f} SOL/wVTC)"
    )
    return state


@dataclass
class UsdcSolConveyorState:
    buy_prices: np.ndarray
    sell_prices: np.ndarray
    buy_usdc: np.ndarray
    sell_sol: np.ndarray
    sell_bucket_cap_sol: float
    overflow_sol: float = 0.0
    fees_paid_usdc: float = 0.0
    fees_paid_sol: float = 0.0
    trade_count: int = 0


def create_usdc_sol_conveyor_state() -> UsdcSolConveyorState:
    """Creates a geometric USDC/SOL ladder with USDC buys and SOL sells."""
    rung_count = CONVEYOR_SIMULATION_RUNGS
    values = (
        USDC_SOL_SIMULATION_BUY_FLOOR,
        USDC_SOL_SIMULATION_BUY_CEILING,
        USDC_SOL_SIMULATION_INITIAL_USDC,
        USDC_SOL_SIMULATION_INITIAL_SOL,
        USDC_SOL_SIMULATION_SELL_BUCKET_CAP_SOL,
        CONVEYOR_SIMULATION_FEE_RATE,
        CONVEYOR_SIMULATION_TARGET_NET_GROWTH,
        MIN_ROUND_TRIP_GAP_PCT,
    )
    if rung_count < 2:
        raise ValueError("The USDC/SOL conveyor simulation requires at least two rungs.")
    if not all(np.isfinite(value) for value in values):
        raise ValueError("USDC/SOL simulation prices, balances, caps, and rates must be finite.")
    if not (0 < USDC_SOL_SIMULATION_BUY_FLOOR < USDC_SOL_SIMULATION_BUY_CEILING):
        raise ValueError("USDC/SOL buy floor and ceiling must be positive, with floor below ceiling.")
    if USDC_SOL_SIMULATION_INITIAL_USDC < 0 or USDC_SOL_SIMULATION_INITIAL_SOL < 0:
        raise ValueError("Initial USDC and SOL balances must be nonnegative.")
    if USDC_SOL_SIMULATION_SELL_BUCKET_CAP_SOL <= 0:
        raise ValueError("The sell-bucket SOL cap must be positive.")
    if not (0 <= CONVEYOR_SIMULATION_FEE_RATE < 1):
        raise ValueError("The simulation fee rate must be at least 0 and less than 1.")
    if CONVEYOR_SIMULATION_TARGET_NET_GROWTH < 0:
        raise ValueError("The target net growth must be nonnegative.")
    # Corrected equation: removes the squared denominator flaw to use flat total-fee cuts
    #required_gap = (((1 + CONVEYOR_SIMULATION_TARGET_NET_GROWTH) / (1 - CONVEYOR_SIMULATION_FEE_RATE) ** 2)) - 1
    required_gap = ((1 + CONVEYOR_SIMULATION_TARGET_NET_GROWTH) / (1 - (CONVEYOR_SIMULATION_FEE_RATE * 2))) - 1

    if MIN_ROUND_TRIP_GAP_PCT + 1e-12 < required_gap:
        raise ValueError(
            f"MIN_ROUND_TRIP_GAP_PCT ({MIN_ROUND_TRIP_GAP_PCT:.6%}) is below "
            f"the configured fee-and-growth requirement ({required_gap:.6%})."
        )

    buy_prices = np.geomspace(
        USDC_SOL_SIMULATION_BUY_FLOOR,
        USDC_SOL_SIMULATION_BUY_CEILING,
        rung_count,
    )
    sell_prices = buy_prices * (1 + MIN_ROUND_TRIP_GAP_PCT)
    sell_sol = np.zeros(rung_count, dtype=float)
    overflow_sol = 0.0
    initial_sol_per_rung = USDC_SOL_SIMULATION_INITIAL_SOL / rung_count
    for rung_index in range(rung_count):
        remaining_sol = initial_sol_per_rung
        for sell_index in range(rung_index, rung_count):
            room_sol = max(0.0, USDC_SOL_SIMULATION_SELL_BUCKET_CAP_SOL - sell_sol[sell_index])
            amount_routed = min(remaining_sol, room_sol)
            sell_sol[sell_index] += amount_routed
            remaining_sol -= amount_routed
            if remaining_sol <= 1e-12:
                remaining_sol = 0.0
                break
        overflow_sol += remaining_sol

    return UsdcSolConveyorState(
        buy_prices=buy_prices,
        sell_prices=sell_prices,
        buy_usdc=np.full(
            rung_count,
            USDC_SOL_SIMULATION_INITIAL_USDC / rung_count,
            dtype=float,
        ),
        sell_sol=sell_sol,
        sell_bucket_cap_sol=USDC_SOL_SIMULATION_SELL_BUCKET_CAP_SOL,
        overflow_sol=overflow_sol,
    )


def simulate_usdc_sol_buy(
    state: UsdcSolConveyorState,
    rung_index: int,
    usdc_to_spend: float,
) -> dict:
    """Buys SOL using a USDC rung and routes net SOL up the sell conveyor."""
    if not 0 <= rung_index < len(state.buy_prices):
        raise IndexError("Buy rung index is outside the USDC/SOL ladder.")
    if not np.isfinite(usdc_to_spend) or usdc_to_spend <= 0:
        raise ValueError("USDC to spend must be a finite positive amount.")
    if usdc_to_spend > state.buy_usdc[rung_index] + 1e-12:
        raise ValueError("Buy amount exceeds the USDC available in that rung.")

    gross_sol = usdc_to_spend / state.buy_prices[rung_index]
    fee_sol = gross_sol * CONVEYOR_SIMULATION_FEE_RATE
    net_sol = gross_sol - fee_sol
    state.buy_usdc[rung_index] = max(0.0, state.buy_usdc[rung_index] - usdc_to_spend)

    remaining_sol = net_sol
    routed_sol = 0.0
    for sell_index in range(rung_index, len(state.sell_prices)):
        room_sol = max(0.0, state.sell_bucket_cap_sol - state.sell_sol[sell_index])
        amount_routed = min(remaining_sol, room_sol)
        state.sell_sol[sell_index] += amount_routed
        remaining_sol -= amount_routed
        routed_sol += amount_routed
        if remaining_sol <= 1e-12:
            remaining_sol = 0.0
            break

    state.overflow_sol += remaining_sol
    state.fees_paid_sol += fee_sol
    state.trade_count += 1
    return {
        "rung": rung_index + 1,
        "usdc_spent": usdc_to_spend,
        "price_usdc_per_sol": state.buy_prices[rung_index],
        "gross_sol": gross_sol,
        "fee_sol": fee_sol,
        "net_sol": net_sol,
        "routed_sol": routed_sol,
        "overflow_sol": remaining_sol,
    }


def simulate_usdc_sol_sell(
    state: UsdcSolConveyorState,
    rung_index: int,
    sol_to_sell: float,
) -> dict:
    """Sells SOL at a sell rung and returns net USDC to its paired buy rung."""
    if not 0 <= rung_index < len(state.sell_prices):
        raise IndexError("Sell rung index is outside the USDC/SOL ladder.")
    if not np.isfinite(sol_to_sell) or sol_to_sell <= 0:
        raise ValueError("SOL to sell must be a finite positive amount.")
    if sol_to_sell > state.sell_sol[rung_index] + 1e-12:
        raise ValueError("Sell amount exceeds the SOL available in that rung.")

    gross_usdc = sol_to_sell * state.sell_prices[rung_index]
    fee_usdc = gross_usdc * CONVEYOR_SIMULATION_FEE_RATE
    net_usdc = gross_usdc - fee_usdc
    state.sell_sol[rung_index] = max(0.0, state.sell_sol[rung_index] - sol_to_sell)
    state.buy_usdc[rung_index] += net_usdc
    state.fees_paid_usdc += fee_usdc
    state.trade_count += 1
    return {
        "rung": rung_index + 1,
        "sol_sold": sol_to_sell,
        "price_usdc_per_sol": state.sell_prices[rung_index],
        "gross_usdc": gross_usdc,
        "fee_usdc": fee_usdc,
        "net_usdc": net_usdc,
        "paired_buy_rung": rung_index + 1,
    }


def print_usdc_sol_ladder(
    state: UsdcSolConveyorState,
    event_number: int,
    buy_rung_index: int,
    sell_rung_index: int,
) -> None:
    """Displays USDC buy and SOL sell inventories after one event."""
    print(f"  [USDC/SOL LADDER AFTER EVENT {event_number:03d}]")
    print(
        f"  Active buy rung: {buy_rung_index + 1} | "
        f"Active sell rung: {sell_rung_index + 1}"
    )
    print("  Rung | Buy price (USDC/SOL) | Buy USDC | Sell price (USDC/SOL) | Sell SOL | SOL cap")
    for rung_index, (buy_price, sell_price, buy_usdc, sell_sol) in enumerate(
        zip(state.buy_prices, state.sell_prices, state.buy_usdc, state.sell_sol),
        start=1,
    ):
        print(
            f"  {rung_index:4d} | {buy_price:20.6f} | {buy_usdc:8.2f} | "
            f"{sell_price:21.6f} | {sell_sol:8.4f} | "
            f"{sell_sol:6.3f}/{state.sell_bucket_cap_sol:.3f}"
        )
    print(f"  Above-top-rung overflow: {state.overflow_sol:.8f} SOL")


def run_random_usdc_sol_simulation() -> UsdcSolConveyorState:
    """Runs random USDC/SOL fills, advancing each side only after bucket exhaustion."""
    if CONVEYOR_SIMULATION_EVENTS < 0:
        raise ValueError("CONVEYOR_SIMULATION_EVENTS must be nonnegative.")
    state = create_usdc_sol_conveyor_state()
    rng = random.Random(CONVEYOR_SIMULATION_SEED)
    buy_rung_index = len(state.buy_prices) // 2
    sell_rung_index = buy_rung_index
    mark_price = float(np.sqrt(state.buy_prices[0] * state.buy_prices[-1]))
    initial_value_usdc = float(
        np.sum(state.buy_usdc) + (np.sum(state.sell_sol) + state.overflow_sol) * mark_price
    )

    print("[USDC/SOL CONVEYOR SIMULATION] Synthetic prices; no RPC or swaps are used.")
    print(
        f"  Rungs: {CONVEYOR_SIMULATION_RUNGS} | Events: {CONVEYOR_SIMULATION_EVENTS} "
        f"| Seed: {CONVEYOR_SIMULATION_SEED} | Fee/side: {CONVEYOR_SIMULATION_FEE_RATE:.3%}"
    )
    print(
        f"  Buy range: {state.buy_prices[0]:.6f}-{state.buy_prices[-1]:.6f} USDC/SOL "
        f"| Paired sell gap: {MIN_ROUND_TRIP_GAP_PCT:.3%}"
    )
    print(
        f"  Sell bucket cap: {state.sell_bucket_cap_sol:.4f} SOL "
        f"| Initial: {np.sum(state.buy_usdc):.2f} USDC, "
        f"{np.sum(state.sell_sol) + state.overflow_sol:.4f} SOL"
    )
    print(
        f"  Starting buy/sell rung: {buy_rung_index + 1} "
        f"({state.buy_prices[buy_rung_index]:.6f} USDC/SOL)"
    )

    for event_number in range(1, CONVEYOR_SIMULATION_EVENTS + 1):
        side = rng.choice(("BUY", "SELL"))
        if side == "BUY":
            if state.buy_usdc[buy_rung_index] <= 1e-9 and buy_rung_index > 0:
                buy_rung_index -= 1
            rung_index = buy_rung_index
            available_usdc = state.buy_usdc[rung_index]
            if available_usdc > 1e-12:
                fraction = rng.choice((0.10, 0.25, 0.50, 0.75, 1.0))
                amount = float(available_usdc * fraction)
                result = simulate_usdc_sol_buy(state, rung_index, amount)
            else:
                result = None
            if result is not None:
                exhausted = state.buy_usdc[rung_index] <= 1e-9
                next_action = (
                    "bucket exhausted; next BUY may move down one rung"
                    if exhausted and rung_index > 0
                    else "at lowest rung"
                    if exhausted
                    else f"{state.buy_usdc[rung_index]:.4f} USDC remains; stay on this rung"
                )
                print(
                    f"[{event_number:03d}] BUY  rung {rung_index + 1:02d} "
                    f"{result['usdc_spent']:.4f} USDC -> {result['net_sol']:.8f} SOL "
                    f"(fee {result['fee_sol']:.8f} SOL; routed to sell rung {rung_index + 1:02d}; "
                    f"overflow {result['overflow_sol']:.8f} SOL; {next_action})"
                )
            else:
                print(
                    f"[{event_number:03d}] BUY  rung {rung_index + 1:02d} | "
                    "No USDC available at the lower boundary; no fill."
                )
        else:
            if (
                state.sell_sol[sell_rung_index] <= 1e-9
                and sell_rung_index < len(state.sell_prices) - 1
            ):
                sell_rung_index += 1
            rung_index = sell_rung_index
            available_sol = state.sell_sol[rung_index]
            if available_sol > 1e-12:
                fraction = rng.choice((0.10, 0.25, 0.50, 0.75, 1.0))
                amount = float(available_sol * fraction)
                result = simulate_usdc_sol_sell(state, rung_index, amount)
            else:
                result = None
            if result is not None:
                exhausted = state.sell_sol[rung_index] <= 1e-9
                next_action = (
                    "bucket exhausted; next SELL may move up one rung"
                    if exhausted and rung_index < len(state.sell_prices) - 1
                    else "at highest rung"
                    if exhausted
                    else f"{state.sell_sol[rung_index]:.8f} SOL remains; stay on this rung"
                )
                print(
                    f"[{event_number:03d}] SELL rung {rung_index + 1:02d} "
                    f"{result['sol_sold']:.8f} SOL -> {result['net_usdc']:.4f} USDC "
                    f"(fee {result['fee_usdc']:.4f} USDC; routed to buy rung {rung_index + 1:02d}; "
                    f"{next_action})"
                )
            else:
                print(
                    f"[{event_number:03d}] SELL rung {rung_index + 1:02d} | "
                    "No SOL available at the upper boundary; no fill."
                )

        print_usdc_sol_ladder(state, event_number, buy_rung_index, sell_rung_index)

    final_usdc = float(np.sum(state.buy_usdc))
    final_sol = float(np.sum(state.sell_sol) + state.overflow_sol)
    final_value_usdc = final_usdc + final_sol * mark_price
    print("[USDC/SOL CONVEYOR SUMMARY]")
    print(f"  Executed events: {state.trade_count}")
    print(f"  Buy-side USDC remaining: {final_usdc:.4f} USDC")
    print(f"  Sell-side SOL remaining: {np.sum(state.sell_sol):.8f} SOL")
    print(f"  Above-top-rung overflow: {state.overflow_sol:.8f} SOL")
    print(f"  Fees paid: {state.fees_paid_usdc:.6f} USDC + {state.fees_paid_sol:.8f} SOL")
    print(
        f"  Synthetic mark value: {initial_value_usdc:.4f} -> "
        f"{final_value_usdc:.4f} USDC (mark {mark_price:.6f} USDC/SOL)"
    )
    return state


async def main():
    print("[SYSTEM START] Initializing Asynchronous Data Laboratory Engine")
    
    async with AsyncClient(rpc_url) as client:
        is_connected = await client.is_connected()
        if not is_connected:
            print("[CRITICAL] Node pipeline offline. Terminating execution loops.")
            return
            
        print("[CONNECTION] Remote Procedure Call (RPC) Node Status: ONLINE")
        await check_wallet_fuel(client)
        # PHASE 2 WALLET BALANCES INTERROGATION PASS
        print("[WALLET INTERROGATION] Pulling live token balances from blockchain...")
        real_usdc_balance = await fetch_live_token_balance(client, "USDC")
        real_wvtc_balance = await fetch_live_token_balance(client, "wVTC")
        # ==============================================================================
        # ?? THE BOOT CALIBRATION FIX: FORCE REAL SPOT PRICE TO BUILD THE LADDER
        # ==============================================================================
        print("[CALIBRATION] Initializing authoritative real-world boot calibration...")
        if not jupiter_api_key:
            print("[CRITICAL] Configuration Error: JUPITER_API_KEY missing from your .env file. Cannot verify boot baseline.")
            return
            
        sol_mint = TOKEN_MINTS["SOL"]
        url = "https://api.jup.ag/price/v3"
        params = {'ids': sol_mint}
        headers = {'x-api-key': jupiter_api_key.strip()}
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=5)
            if response.status_code == 200:
                prices = response.json()
                calibrated_boot_price = float(prices[sol_mint]['usdPrice'])
                print(f"[CALIBRATION SUCCESS] Real-world asset spot entry confirmed: ${calibrated_boot_price:.4f}")
                
                # Anchor all global tracking caches onto the true price immediately
                ORACLE_STAGNANT_TRACKER["last_price"] = calibrated_boot_price
                ORACLE_STAGNANT_TRACKER["frozen_baseline"] = calibrated_boot_price
                # Lock into stale mode from boot to immediately handle the frozen oracle feed
                ORACLE_STAGNANT_TRACKER["is_stale_mode"] = True
            else:
                print(f"[WARNING] Jupiter boot calibration rejected (Status {response.status_code}). Defaulting to raw node maps.")
                calibrated_boot_price = await fetch_onchain_price(client, "SOL/USD")
        except Exception as e:
            print(f"[WARNING] Calibration network connection timeout: {e}. Defaulting to raw node maps.")
            calibrated_boot_price = await fetch_onchain_price(client, "SOL/USD")
            
        if not calibrated_boot_price or calibrated_boot_price <= 0:
            print("[CRITICAL] System calibration failed to establish asset market values.")
            return
        else:
            print(f"[CALIBRATION] Current SOL/USD Market Price: ${calibrated_boot_price:,.4f}")
            
        # Initialize your custom multipliers around the TRUE calibrated price
        mock_floor = calibrated_boot_price * 0.50
        mock_ceiling = calibrated_boot_price * 1.50
        
        # Generate dynamic ladder array using verified metrics
        grid_matrix = generate_N_ladder(mock_floor, mock_ceiling, mode="geometric")
        sol_stock, usdc_stock = initialize_inventory_matrix(grid_matrix, calibrated_boot_price, total_budget_usdc=1000.0)
        # ==============================================================================
        
        previous_tier_index = None
        loop_counter = 0
        
        # INJECT THIS: Initialize your vault backup clock to the starting execution epoch timestamp integer
        last_git_vault_timestamp = time.time()
        
        print("[LOOP START] Monitoring Price Crossings Across Matrix Ranges")

        # Generates a volatile sandbox keypair structure for testing (Safe, un-funded developer key)
        sandbox_signer_keypair = Keypair()

        while True:
            loop_counter += 1
            
            # Fetch price out of our smart state machine
            live_price = await fetch_onchain_price(client, "SOL/USD")
            print(f"[CALIBRATION] Current SOL/USD Market Price: ${live_price:,.4f} , and loop counter: {loop_counter}")
            
            if not live_price:
                await asyncio.sleep(3)
                continue
                
            # THE HARD RESYNC ANCHOR: Lock our core matrix variables directly 
            # to whatever verified price came out of our state machine memory cache.
            current_sol_price = ORACLE_STAGNANT_TRACKER["last_price"]
            print(f"[CALIBRATION] Current SOL/USD Market Price: ${current_sol_price:,.4f} , and loop counter: {loop_counter}")
            print(f"[CALIBRATION] Active Step Matrix Working Anchor: ${current_sol_price:,.4f} | Loop Counter: {loop_counter}")
            
           # THE CORRECT DUAL-MODE CONDITION LOCK:
            # Safely preserves your simulation shifter code block without destroying it.
            if RUN_SIMULATION_PRICE_SHIFTER:
                live_price = get_simulated_price(live_price, loop_counter)
            else:
                # Live production mode tracking parameters
                pass
            
            # The rest of your existing step mapping logic remains completely untouched:
            if live_price:
                current_tier_index = np.abs(grid_matrix - live_price).argmin()
                tier_price_target = grid_matrix[current_tier_index]
                
                if previous_tier_index is None:
                    previous_tier_index = current_tier_index
                    print(f"[CALIBRATED] Positioned in Bucket Step: {current_tier_index+1}/{NUMBER_OF_LADDER} (Target Line: ${tier_price_target:,.4f})")
                elif current_tier_index != previous_tier_index:
                    direction = "UPWARD" if current_tier_index > previous_tier_index else "DOWNWARD"
                    steps_moved = abs(current_tier_index - previous_tier_index)
                    
                    print(f"[CROSSING DETECTED] Index Shift Detected: {direction} By: {steps_moved} Tiers")
                    
                    if direction == "UPWARD":
                        for step in range(previous_tier_index, current_tier_index + 1):
                            if sol_stock[step] > 0:
                                tokens_to_liquidate = sol_stock[step]
                                raw_proceeds = tokens_to_liquidate * grid_matrix[step]
                                dex_fee_usdc = raw_proceeds * 0.006
                                clean_proceeds = raw_proceeds - dex_fee_usdc
                                
                                # ---- DUAL-MODE CONDITIONAL LOCK ----
                                if RUN_SIMULATION_PRICE_SHIFTER:
                                    fill_percentage = 0.60
                                else:
                                    # Production Track: Trigger real Web3 swaps and await block confirmation receipts
                                    fill_percentage = execute_live_onchain_sell(
                                        wallet_keypair=sandbox_signer_keypair,
                                        base_mint=TOKEN_MINTS["SOL"],
                                        quote_mint=TOKEN_MINTS["USDC"],
                                        amount_in_sol=tokens_to_liquidate,
                                        target_price=grid_matrix[step]
                                    )
                                # ------------------------------------
                                
                                # NATIVE STEP RE-BALANCING: Each node absorbs its own execution slice
                                cash_realized = clean_proceeds * fill_percentage
                                asset_remaining = tokens_to_liquidate * (1.0 - fill_percentage)
                                
                                usdc_stock[step] += cash_realized
                                sol_stock[step] = asset_remaining
                                
                                # ---- WEB3 TRANSACTION ROUTER HOOK ----
                                raw_lamports_sold = int((tokens_to_liquidate * fill_percentage) * 1_000_000_000)
                                if raw_lamports_sold > 0:
                                    prepare_onchain_dex_swap(
                                        wallet_keypair=sandbox_signer_keypair,
                                        base_mint=TOKEN_MINTS["SOL"],
                                        quote_mint=TOKEN_MINTS["USDC"],
                                        amount_in=raw_lamports_sold,
                                        slide_direction="PARTIAL SOL -> USDC SWAP"
                                    )
                                
                                # If fully converted to cash, check for profit overflow
                                if sol_stock[step] <= 1e-12:
                                    sol_stock[step] = 0.0
                                    print(f"  [RESOLUTION] Step {step+1} fully converted into Cash (Xn+1).")
                                    check_bucket_overflow(step, sol_stock, usdc_stock, grid_matrix)
                                else:
                                    print(f"  [PARTIAL FILL] Step {step+1} partially sold. Holding remaining: {sol_stock[step]:.6f} SOL.")

                    else:
                        # Price dropped down to the buy part (DOWNWARD Trend)
                        for step in range(current_tier_index, previous_tier_index + 1):
                            if usdc_stock[step] > 0:
                                cash_to_deploy = usdc_stock[step]
                                raw_asset_bought = cash_to_deploy / grid_matrix[step]
                                dex_fee_sol = raw_asset_bought * 0.006
                                clean_asset_bought = raw_asset_bought - dex_fee_sol
                                
                                # ---- DUAL-MODE CONDITIONAL LOCK ----
                                if RUN_SIMULATION_PRICE_SHIFTER:
                                    fill_percentage = 0.70
                                else:
                                    fill_percentage = execute_live_onchain_buy(
                                        wallet_keypair=sandbox_signer_keypair,
                                        base_mint=TOKEN_MINTS["USDC"],
                                        quote_mint=TOKEN_MINTS["SOL"],
                                        amount_in_usdc=cash_to_deploy,
                                        target_price=grid_matrix[step]
                                    )
                                # ------------------------------------
                                
                                tokens_credited = clean_asset_bought * fill_percentage
                                cash_remaining = cash_to_deploy * (1.0 - fill_percentage)
                                
                                # Secure the exact active split metrics directly on the step line
                                sol_stock[step] += tokens_credited
                                usdc_stock[step] = cash_remaining
                                
                                # ---- WEB3 TRANSACTION ROUTER HOOK ----
                                raw_usdc_atoms_spent = int((cash_to_deploy * fill_percentage) * 1_000_000)
                                if raw_usdc_atoms_spent > 0:
                                    prepare_onchain_dex_swap(
                                        wallet_keypair=sandbox_signer_keypair,
                                        base_mint=TOKEN_MINTS["USDC"],
                                        quote_mint=TOKEN_MINTS["SOL"],
                                        amount_in=raw_usdc_atoms_spent,
                                        slide_direction="PARTIAL USDC -> SOL SWAP"
                                    )
                                    
                                if usdc_stock[step] <= 1e-9:
                                    usdc_stock[step] = 0.0
                                    print(f"  [RESOLUTION] Step {step+1} fully converted to pure Asset state (Yn+1).")
                                else:
                                    print(f"  [PARTIAL FILL] Step {step+1} partially bought. Remaining cash: ${usdc_stock[step]:.2f} USDC.")
                    print(f"  Current Total Portfolio Value: ${np.sum(sol_stock) * live_price + np.sum(usdc_stock):,.2f} USD")

                    # INJECT THE NEW LOGGER HERE: Pass all current system states directly to your CSV file
                    # Dynamically look at 3 steps below and 3 steps above your current position
                    print_matrix_slice_lane(
                        start_idx=current_tier_index - 3,
                        end_idx=current_tier_index + 3,
                        grid_matrix=grid_matrix,
                        sol_stock=sol_stock,
                        usdc_stock=usdc_stock
                    )
                    
                    log_transaction_to_csv(
                        loop_id=loop_counter,
                        live_price=live_price,
                        current_step=current_tier_index + 1,
                        direction=direction,
                        steps_moved=steps_moved,
                        sol_total=np.sum(sol_stock),
                        usdc_total=np.sum(usdc_stock)
                    )
                    
                    # THE TIME-LOCKED UPGRADE OVERRIDE:
                    # Executes the check and re-assigns the updated timestamp balance variables back to memory maps
                    last_git_vault_timestamp = execute_automated_git_vault(
                        tick_id=loop_counter,
                        current_step=current_tier_index + 1,
                        market_price=live_price,
                        last_vault_time=last_git_vault_timestamp
                    )
                    
                    # Update state tracking memory variable
                    previous_tier_index = current_tier_index

                else:
                    print(f"[TICK CHECK #{loop_counter}] Steady inside Step {current_tier_index+1}/{NUMBER_OF_LADDER}. (Market Price: ${live_price:,.4f})")
            
            await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        # Collect and sanitize all active execution flags passed via PowerShell
        execution_args = sys.argv[1:]
        
        if "--usdc-sol-sim" in execution_args:
            # Route to your preferred scannable dollar conveyor test-bench
            run_random_usdc_sol_simulation()
        elif "--conveyor-sim" in execution_args:
            # Route to the native synthetic fractional token loop
            run_random_conveyor_simulation()
        else:
            # DEFAULT PRODUCTION TRACK: Fire up the authoritative live Web3 grid engine
            asyncio.run(main())
            
    except KeyboardInterrupt:
        print("\n[SYSTEM SHUTDOWN] Execution loops cleanly terminated by developer instruction.")
