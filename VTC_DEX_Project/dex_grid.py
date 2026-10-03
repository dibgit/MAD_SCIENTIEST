import os
import asyncio
import struct
import numpy as np
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient

# Initialize environment configuration pathing
load_dotenv()
rpc_url = os.getenv("SOLANA_RPC_URL")
wallet_address_str = os.getenv("MY_PUBLIC_WALLET")

# Permanent Pyth Network Mainnet Price Oracle Mapping Index
ORACLE_FEEDS = {
    "SOL/USD": "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
}

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
    # THE CORRECTION: Point to specific array index integers [0] and [127]
    print(f"  Floor Boundary   (Step 001) : ${grid_array[0]:,.4f}")
    print(f"  Midpoint Anchor  (Step 128) : ${grid_array[127]:,.4f}")
    print(f"  Ceiling Boundary (Step 257) : ${grid_array[-1]:,.4f}")
    
    if mode == "geometric":
        # Pull position [1] to gauge single steps relative to position [0]
        pct_ratio = (grid_array[1] - grid_array[0]) / grid_array[0] * 100
        print(f"  Compounding Delta Step Size: {pct_ratio:.4f}% percentage variance")

        
    return grid_array

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
            
        # Structure the mock 257-step sandbox boundaries
        mock_floor = current_sol_price * 0.50
        mock_ceiling = current_sol_price * 1.50
        
        # Initialize immutable trading tiers layout configuration arrays
        grid_matrix = generate_257_ladder(mock_floor, mock_ceiling, mode="geometric")
        
        print("[LOOP START] Monitoring Price Crossings Across Matrix Ranges")
        
        previous_tier_index = None
        loop_counter = 0
        
        while True:
            loop_counter += 1
            live_price = await fetch_onchain_price(client, "SOL/USD")
            
            if live_price:
                # Instantly map incoming numbers to the closest mathematical tier index
                current_tier_index = np.abs(grid_matrix - live_price).argmin()
                tier_price_target = grid_matrix[current_tier_index]
                
                if previous_tier_index is None:
                    previous_tier_index = current_tier_index
                    print(f"[CALIBRATED] Positioned in Bucket Step: {current_tier_index+1}/257 (Target Line: ${tier_price_target:,.4f})")
                elif current_tier_index != previous_tier_index:
                    direction = "UPWARD" if current_tier_index > previous_tier_index else "DOWNWARD"
                    steps_moved = abs(current_tier_index - previous_tier_index)
                    
                    print(f"[CROSSING DETECTED] Index Shift Detected: {direction} By: {steps_moved} Tiers")
                    print(f"  Old Position: Step {previous_tier_index+1} (${grid_matrix[previous_tier_index]:,.4f})")
                    print(f"  New Position: Step {current_tier_index+1} (${tier_price_target:,.4f})")
                    
                    previous_tier_index = current_tier_index
                else:
                    print(f"[TICK CHECK #{loop_counter}] Steady inside Step {current_tier_index+1}/257. (Market Price: ${live_price:,.4f})")
            
            # Stable 3-second delay matching block production frequency
            await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[SYSTEM SHUTDOWN] Execution loops cleanly terminated by developer instruction.")
