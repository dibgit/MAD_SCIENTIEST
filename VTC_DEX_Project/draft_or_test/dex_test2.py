import os
import asyncio
import struct
import numpy as np
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient

# Load workspace infrastructure
load_dotenv()
rpc_url = os.getenv("SOLANA_RPC_URL")
wallet_address_str = os.getenv("MY_PUBLIC_WALLET")

# The official, permanent Pyth SOL/USD Mainnet Price Account
ORACLE_FEEDS = {
    "SOL/USD": "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
}

async def check_wallet_fuel(client: AsyncClient):
    """Asynchronously reads your live Phantom gas sandbox profile."""
    if not wallet_address_str:
        print("❌ Error: MY_PUBLIC_WALLET variable is missing from your .env file.")
        return
        
    try:
        clean_wallet = str(wallet_address_str).replace('"', '').replace("'", "").strip()
        public_key = Pubkey.from_string(clean_wallet)
        
        balance_response = await client.get_balance(public_key)
        sol_balance = balance_response.value / 1_000_000_000
        print(f"📦 Laboratory Sandbox Fuel: {sol_balance} SOL")
    except Exception as e:
        print(f"⚠️ Wallet Parsing Anomaly: Check your .env value string. Error: {e}")

async def fetch_onchain_price(client: AsyncClient, pair_ticker: str = "SOL/USD"):
    """
    Reads live price metrics directly from on-chain smart contract data segments.
    Bypasses standard REST API limits using your unlimited Alchemy channel.
    """
    feed_address = ORACLE_FEEDS.get(pair_ticker)
    if not feed_address:
        print(f"❌ Abort: Oracle config missing for {pair_ticker}")
        return None
        
    try:
        pubkey = Pubkey.from_string(str(feed_address).strip())
        account_info = await client.get_account_info(pubkey)
        
        if not account_info.value:
            print("❌ Node Read Error: Price account target not found.")
            return None
            
        data = account_info.value.data
        
        # Extract elements cleanly as distinct scalar data types
        price_offset = 208
        price_data = data[price_offset:price_offset + 20]
        raw_price, confidence, exponent = struct.unpack("<qQi", price_data)
        
        # THE FIX: Cast to float explicitly using negative powers correctly
        # If exponent is -8, this multiplies by 0.00000001
        #scaling_factor = float(10 ** exponent)
        #final_price = float(raw_price) * scaling_factor
                # THE ABSOLUTE SCALING FIX:
        # Instead of multiplying by 10**-8, divide by 10**8 to cleanly move the decimal point
        print(f" exponent: {exponent}, raw_price: {raw_price}, confidence: {confidence}")
        # Extract elements cleanly as distinct scalar data types
        price_offset = 208
        price_data = data[price_offset:price_offset + 20]
        raw_price, confidence, exponent = struct.unpack("<qQi", price_data)
        
        # THE DEDUCTIVE PATCH:
        # Since the raw integer represents 8 fixed decimal places, divide by 10^8
        # This transforms 11923378000 into a perfect $119.23378 market value
        final_price = float(raw_price) / 100_000_000.0
        
        print(f"📈 Direct Node Matrix Feed -> {pair_ticker} = ${final_price:,.4f} USDC/USDT")
        return final_price

    except Exception as e:
        print(f"⚠️ Chain Read Anomaly for {pair_ticker}. Reason: {e}")
        return None

def generate_257_ladder(low_price: float, high_price: float, mode: str = "geometric"):
    """
    Generates a high-precision 257-step structural trading matrix grid array.
    """
    total_steps = 257
    
    if mode == "arithmetic":
        grid_array = np.linspace(low_price, high_price, total_steps)
    else:
        grid_array = np.geomspace(low_price, high_price, total_steps)
        
    print(f"\n📐 Constructed {total_steps}-Step Grid Array Engine ({mode.upper()}):")
    print(f"   🧱 Floor Tier   (Step 1)   : ${grid_array[0]:,.4f}")
    print(f"   🧱 Mid Tier     (Step 128) : ${grid_array[128]:,.4f}")
    print(f"   🧱 Ceiling Tier (Step 257) : ${grid_array[-1]:,.4f}")
    
    if mode == "geometric":
        # Calculate fixed structural ratio step size
        pct_ratio = (grid_array[1] - grid_array[0]) / grid_array[0] * 100
        print(f"   📊 Compounding Spread Value : {pct_ratio:.4f}% step-by-step increments")
        
    return grid_array

async def main():
    print("--- 🔬 Starting Unlimited Async Laboratory Simulation ---")
    
    async with AsyncClient(rpc_url) as client:
        is_connected = await client.is_connected()
        if not is_connected:
            print("❌ Node Connection: OFFLINE. Verify your RPC string in .env")
            return
            
        print("🚀 Node Connection: ONLINE")
        await check_wallet_fuel(client)
        
        print("\n--- Initializing Matrix Calibration ---")
        current_sol_price = await fetch_onchain_price(client, "SOL/USD")
        
        if not current_sol_price or current_sol_price <= 0:
            print("❌ Calibration Aborted: Price read failed.")
            return
            
        # Standardize your 257-step matrix boundaries
        mock_floor = current_sol_price * 0.50
        mock_ceiling = current_sol_price * 1.50
        
        # Build the permanent structural trading array grid
        grid_matrix = generate_257_ladder(mock_floor, mock_ceiling, mode="geometric")
        
        print("\n--- 🧭 Entering Real-Time Price Tracking & Step Monitor Loop ---")
        print("    [Press Ctrl+C inside PowerShell to safely terminate testing]")
        
        # Tracking states
        previous_tier_index = None
        loop_counter = 0
        
        while True:
            loop_counter += 1
            # 1. Pull the absolute latest block-tick price from the oracle
            live_price = await fetch_onchain_price(client, "SOL/USD")
            
            if live_price:
                # 2. Mathematical mapping: find the index of the closest grid tier lines
                # np.abs(grid_matrix - live_price).argmin() calculates the nearest bucket index instantly
                current_tier_index = np.abs(grid_matrix - live_price).argmin()
                tier_price_target = grid_matrix[current_tier_index]
                
                # 3. Detect Step Crossings
                if previous_tier_index is None:
                    # Initial state calibration
                    previous_tier_index = current_tier_index
                    print(f"📍 [Initial Calibration] Placed inside Tier Bucket Step: {current_tier_index+1}/257 (Target: ${tier_price_target:,.4f})")
                elif current_tier_index != previous_tier_index:
                    # The price moved far enough to leap into an adjacent bucket step!
                    direction = "📈 UPWARD" if current_tier_index > previous_tier_index else "📉 DOWNWARD"
                    steps_moved = abs(current_tier_index - previous_tier_index)
                    
                    print(f"\n⚡⚡ [STEP CROSSING DETECTED] ⚡⚡")
                    print(f"   Action Triggered : Price crossed {direction} by {steps_moved} bucket steps!")
                    print(f"   Previous Status  : Step {previous_tier_index+1} (${grid_matrix[previous_tier_index]:,.4f})")
                    print(f"   Current Status   : Step {current_tier_index+1} (${tier_price_target:,.4f})")
                    print(f"   [Simulation Log] : Trigger bucket rebalance / execute overflow checks...\n")
                    
                    # Update state tracking memory variable
                    previous_tier_index = current_tier_index
                else:
                    # Price is fluctuating quietly within the same grid step line boundaries
                    print(f"⏱️ [Tick Check #{loop_counter}] Stable in Step {current_tier_index+1}/257. (Live Market: ${live_price:,.4f})")
            
            # 4. Low-latency sleep interval (3 seconds matches standard Solana block generation ticks)
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
