import os
import requests
import asyncio
import struct
import csv
import time
import subprocess
import json
import numpy as np
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders.message import MessageV0
from solana.rpc.async_api import AsyncClient
from datetime import datetime

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

# Universal Token Mint Repository Map
TOKEN_MINTS = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "wVTC": "PASTE_DECENTRALIZED_WVTC_MINT_ADDRESS_HERE" 
}
# Permanent Pyth Network Mainnet Price Oracle Mapping Index
ORACLE_FEEDS = {
    "SOL/USD": "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
}
# The profit multiplier threshold before a single bucket overflows (e.g., 1.20 = 20% growth)
BUCKET_OVERFLOW_THRESHOLD = 1.20

# DYNAMIC WORKING BALANCES: Base budget allocated per step is dynamically scaled to NUMBER_OF_LADDER
BASE_USDC_PER_BUCKET = 1000.0 / NUMBER_OF_LADDER

# Set to True to overwrite live oracle feeds with an automated simulation price pump
RUN_SIMULATION_PRICE_SHIFTER = True

# The minimum cooldown window in seconds between remote repository backup push execution cycles (300 seconds = 5 minutes)
GIT_VAULT_COOLDOWN_SECONDS = 300

# Keep track of previous prices globally to detect a flatlining feed
ORACLE_STAGNANT_TRACKER = {
    "last_price": 0.0,
    "flatline_count": 0
}

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
        
        # ---- STATE MACHINE EVALUATION CONTROLS ----
        if oracle_price == ORACLE_STAGNANT_TRACKER["last_price"]:
            # Rule 1: Oracle is flatlining against itself. Increase the counter.
            ORACLE_STAGNANT_TRACKER["flatline_count"] += 1
        else:
            # Rule 2: Oracle data is not flat. Consider oracle correct, reset counters.
            ORACLE_STAGNANT_TRACKER["last_price"] = oracle_price
            ORACLE_STAGNANT_TRACKER["flatline_count"] = 0
            
        # Trigger Jupiter check ONLY when the counter hits or passes the threshold boundary
        if ORACLE_STAGNANT_TRACKER["flatline_count"] >= 5:
            print(f"[WARNING] HYBRID FALLBACK triggered. Evaluation count: {ORACLE_STAGNANT_TRACKER['flatline_count']}")
            
            if not jupiter_api_key:
                print("[WARNING] Fallback aborted: JUPITER_API_KEY is missing from .env configuration.")
            else:
                sol_mint = TOKEN_MINTS["SOL"]
                url = "https://api.jup.ag/price/v3"
                params = {'ids': sol_mint}
                headers = {'x-api-key': jupiter_api_key.strip()}
                
                try:
                    response = requests.get(url, params=params, headers=headers, timeout=5)
                    if response.status_code == 200:
                        prices = response.json()
                        spot_price = float(prices[sol_mint]['usdPrice'])
                        
                        print(f"[HYBRID FEED] Success. Extrapolating via Jupiter V3: ${spot_price:.4f}")
                        
                        # CRITICAL MEMORY ALIGNMENT:
                        # Lock Jupiter's fresh price into our global state tracker.
                        ORACLE_STAGNANT_TRACKER["last_price"] = spot_price
                        # Reset flatline back to 0 so we don't spam the API on the next tick.
                        ORACLE_STAGNANT_TRACKER["flatline_count"] = 0
                        
                        # TERMINATE OPERATION HERE: Directly return the correct spot price to your grid engine.
                        print(f"[JUPITER TICK] {pair_ticker} Active Working Value: ${spot_price:,.4f}")
                        return spot_price
                except Exception as ex_jup:
                    print(f"[WARNING] Jupiter V3 API fallback interface error: {ex_jup}")
                    
        # If the oracle data is fresh (count < 5), or if Jupiter calls fail, use the tracking data cache.
        # This enforces your rule: if oracle stays flat, it continues using the last correct price baseline.
        active_working_price = ORACLE_STAGNANT_TRACKER["last_price"]
        print(f"[ORACLE TICK] {pair_ticker} Active Working Value: ${active_working_price:,.4f}")
        return active_working_price
        
    except Exception as e:
        print(f"[ERROR] On-chain cryptograph execution failure: {e}")
        return None

def generate_N_ladder(low_price: float, high_price: float, mode: str = "geometric"):
    """Generates an explicit, high-precision mathematical variable N-tier matrix array layout."""
    # Calculates midpoint index mathematically to handle even/odd N integers cleanly
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

async def main():
    print("[SYSTEM START] Initializing Asynchronous Data Laboratory Engine")
    
    async with AsyncClient(rpc_url) as client:
        is_connected = await client.is_connected()
        if not is_connected:
            print("[CRITICAL] Node pipeline offline. Terminating execution loops.")
            return
            
        print("[CONNECTION] Remote Procedure Call (RPC) Node Status: ONLINE")
        await check_wallet_fuel(client)
        
        # Run baseline calibration parameters
        current_sol_price = await fetch_onchain_price(client, "SOL/USD")
        if not current_sol_price or current_sol_price <= 0:
            print("[CRITICAL] System calibration failed to establish asset market values.")
            return
        else:
            print(f"[CALIBRATION] Current SOL/USD Market Price: ${current_sol_price:,.4f}")
            
        # Structure boundaries (50% crash margin / 50% pump margin)
        mock_floor = current_sol_price * 0.50
        mock_ceiling = current_sol_price * 1.50
        
        # Generate grid structure
        grid_matrix = generate_N_ladder(mock_floor, mock_ceiling, mode="geometric")
        
        # Initialize simulated capital matrix
        sol_stock, usdc_stock = initialize_inventory_matrix(grid_matrix, current_sol_price, total_budget_usdc=1000.0)
        
        print("[LOOP START] Monitoring Price Crossings Across Matrix Ranges")
        
        previous_tier_index = None
        loop_counter = 0
        
        # Initialize tracking memory states
        previous_tier_index = None
        loop_counter = 0
        
        # INJECT THIS: Initialize your vault backup clock to the starting execution epoch timestamp integer
        last_git_vault_timestamp = time.time()
        
        print("[LOOP START] Monitoring Price Crossings Across Matrix Ranges")

        # Generates a volatile sandbox keypair structure for testing (Safe, un-funded developer key)
        sandbox_signer_keypair = Keypair()

        while True:
            loop_counter += 1
            live_price = await fetch_onchain_price(client, "SOL/USD")
            
            # INJECT THE SIMULATION OVERRIDE HERE:
            if live_price:
                live_price = get_simulated_price(live_price, loop_counter)
            
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
                    
                    # Simulation: execute transaction balance shifts between crossed tiers
                    if direction == "UPWARD":
                        for step in range(previous_tier_index, current_tier_index + 1):
                            if sol_stock[step] > 0:
                                proceeds = sol_stock[step] * grid_matrix[step]
                                usdc_stock[step] += proceeds
                                sol_stock[step] = 0.0
                                print(f"  [SIMULATED TRADED] Sold step {step+1} inventory for ${proceeds:.4f} USDC")
                                
                                # INJECT THE TRANSACTION MAPPER:
                                # Converts 0.03 SOL into raw atomic Lamports (1 SOL = 10^9 Lamports)
                                raw_lamports = int(0.03 * 1_000_000_000)
                                prepare_onchain_dex_swap(
                                    wallet_keypair=sandbox_signer_keypair,
                                    base_mint=TOKEN_MINTS["SOL"],
                                    quote_mint=TOKEN_MINTS["USDC"],
                                    amount_in=raw_lamports,
                                    slide_direction="SOL -> USDC SWAP"
                                )
                                check_bucket_overflow(step, sol_stock, usdc_stock, grid_matrix)

                    else:
                        # Price went down: we bought asset at this tier, converting USDC into SOL
                        for step in range(current_tier_index, previous_tier_index + 1):
                            if usdc_stock[step] > 0:
                                asset_bought = usdc_stock[step] / grid_matrix[step]
                                sol_stock[step] += asset_bought
                                usdc_stock[step] = 0.0
                                print(f"  [SIMULATED TRADED] Bought step {step+1} inventory: {asset_bought:.4f} SOL")
                                # INJECT THE DOWNWARD SWAP HOOK HERE:
                                # Converts the USDC amount into raw atomic units (USDC uses 6 decimals: 1 USDC = 1,000,000 atoms)
                                raw_usdc_atoms = int(BASE_USDC_PER_BUCKET * 1_000_000)
                                prepare_onchain_dex_swap(
                                    wallet_keypair=sandbox_signer_keypair,
                                    base_mint=TOKEN_MINTS["USDC"],
                                    quote_mint=TOKEN_MINTS["SOL"],
                                    amount_in=raw_usdc_atoms,
                                    slide_direction="USDC -> SOL SWAP"
                                )

                    print(f"  Current Total Portfolio Value: ${np.sum(sol_stock) * live_price + np.sum(usdc_stock):,.2f} USD")

                    # INJECT THE NEW LOGGER HERE: Pass all current system states directly to your CSV file
              
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
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[SYSTEM SHUTDOWN] Execution loops cleanly terminated by developer instruction.")
