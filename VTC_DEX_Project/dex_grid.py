import os
import asyncio
import struct
import csv
import numpy as np
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from datetime import datetime

# Initialize environment configuration pathing
load_dotenv()
rpc_url = os.getenv("SOLANA_RPC_URL")
wallet_address_str = os.getenv("MY_PUBLIC_WALLET")

# Permanent Pyth Network Mainnet Price Oracle Mapping Index
ORACLE_FEEDS = {
    "SOL/USD": "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
}
# The profit multiplier threshold before a single bucket overflows (e.g., 1.20 = 20% growth)
BUCKET_OVERFLOW_THRESHOLD = 1.20

# The initial base cash baseline allocated per bucket step ($1000 / 257 = ~$3.891)
BASE_USDC_PER_BUCKET = 1000.0 / 257

# Set to True to overwrite live oracle feeds with an automated simulation price pump
RUN_SIMULATION_PRICE_SHIFTER = True

def log_transaction_to_csv(loop_id: int, live_price: float, current_step: int, direction: str, steps_moved: int, sol_total: float, usdc_total: float):
    """
    Appends high-precision grid metric snapshots directly to a persistent CSV database file.
    Target Path: D:\\MAD_SCIENTIEST\\VTC_DEX_Project\\grid_ledger.csv
    """
    csv_path = r"D:\MAD_SCIENTIEST\VTC_DEX_Project\grid_ledger.csv"
    file_exists = os.path.exists(csv_path)
    
    # Calculate the active dollar value of your total combined asset stock
    portfolio_value = (sol_total * live_price) + usdc_total
    
    # Define clean, structural database tracking headers matching your Excel ledger
    headers = ["timestamp", "tick_id", "market_price", "active_step", "trade_direction", "steps_crossed", "total_sol", "total_usdc", "net_worth_usd"]
    
    try:
        with open(csv_path, mode="a", newline="") as csv_file:
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
    """Reads fixed-point raw integer byte data straight from smart contract storage channels."""
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
        final_price = float(raw_price) / 100_000_000.0
        print(f"[ORACLE TICK] {pair_ticker} Feed Value: ${final_price:,.4f}")
        return final_price
    except Exception as e:
        print(f"[ERROR] On-chain cryptograph execution failure: {e}")
        return None

def generate_257_ladder(low_price: float, high_price: float, mode: str = "geometric"):
    """Generates an explicit, high-precision mathematical 257-tier matrix array layout."""
    total_steps = 257
    
    if mode == "arithmetic":
        grid_array = np.linspace(low_price, high_price, total_steps)
    else:
        grid_array = np.geomspace(low_price, high_price, total_steps)
        
    print(f"[ENGINE INITIALIZED] Mode: {mode.upper()} Steps: {total_steps}")
    print(f"  Floor Boundary   (Step 001) : ${grid_array[0]:,.4f}")
    print(f"  Midpoint Anchor  (Step 128) : ${grid_array[127]:,.4f}")
    print(f"  Ceiling Boundary (Step 257) : ${grid_array[-1]:,.4f}")
    
    if mode == "geometric":
        # Pull position [1] to gauge single steps relative to position [0]
        pct_ratio = (grid_array[1] - grid_array[0]) / grid_array[0] * 100
        print(f"  Compounding Delta Step Size: {pct_ratio:.4f}% percentage variance")
        
    return grid_array

def initialize_inventory_matrix(grid_matrix, current_price, total_budget_usdc=1000.0):
    """
    Allocates your budget across all 257 steps based on current market price.
    Steps below current price hold USDC (to buy the dip).
    Steps above current price hold SOL (to sell the pump).
    """
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
    """
    Generates a full bidirectional market cycle wave to test grid functionality.
    Ticks 01-05: Stable baseline calibration.
    Ticks 06-35: Upward expansion trend (Pump phase).
    Ticks 36-70: Downward market correction trend (Dump phase).
    """
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
            
        # Structure boundaries (50% crash margin / 50% pump margin)
        mock_floor = current_sol_price * 0.50
        mock_ceiling = current_sol_price * 1.50
        
        # Generate grid structure
        grid_matrix = generate_257_ladder(mock_floor, mock_ceiling, mode="geometric")
        
        # Initialize simulated capital matrix
        sol_stock, usdc_stock = initialize_inventory_matrix(grid_matrix, current_sol_price, total_budget_usdc=1000.0)
        
        print("[LOOP START] Monitoring Price Crossings Across Matrix Ranges")
        
        previous_tier_index = None
        loop_counter = 0
        
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
                    print(f"[CALIBRATED] Positioned in Bucket Step: {current_tier_index+1}/257 (Target Line: ${tier_price_target:,.4f})")
                elif current_tier_index != previous_tier_index:
                    direction = "UPWARD" if current_tier_index > previous_tier_index else "DOWNWARD"
                    steps_moved = abs(current_tier_index - previous_tier_index)
                    
                    print(f"[CROSSING DETECTED] Index Shift Detected: {direction} By: {steps_moved} Tiers")
                    
                    # Simulation: execute transaction balance shifts between crossed tiers
                    # Your current UPWARD trading loop block inside main():
                    if direction == "UPWARD":
                        for step in range(previous_tier_index, current_tier_index + 1):
                            if sol_stock[step] > 0:
                                proceeds = sol_stock[step] * grid_matrix[step]
                                usdc_stock[step] += proceeds
                                sol_stock[step] = 0.0
                                print(f"  [SIMULATED TRADED] Sold step {step+1} inventory for ${proceeds:.4f} USDC")
                                
                                # INJECT THE NEW HOOK HERE: Check if this specific sale caused an overflow
                                check_bucket_overflow(step, sol_stock, usdc_stock, grid_matrix)

                    else:
                        # Price went down: we bought asset at this tier, converting USDC into SOL
                        for step in range(current_tier_index, previous_tier_index + 1):
                            if usdc_stock[step] > 0:
                                asset_bought = usdc_stock[step] / grid_matrix[step]
                                sol_stock[step] += asset_bought
                                usdc_stock[step] = 0.0
                                print(f"  [SIMULATED TRADED] Bought step {step+1} inventory: {asset_bought:.4f} SOL")

                    print(f"  Current Total Portfolio Value: ${np.sum(sol_stock) * live_price + np.sum(usdc_stock):,.2f} USD")
                                        # Your current transaction logging block inside main():
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

                    # Keep your tracking variables updated
                    previous_tier_index = current_tier_index
                else:
                    print(f"[TICK CHECK #{loop_counter}] Steady inside Step {current_tier_index+1}/257. (Market Price: ${live_price:,.4f})")
            
            await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[SYSTEM SHUTDOWN] Execution loops cleanly terminated by developer instruction.")
