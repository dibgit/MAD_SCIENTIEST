import struct

import numpy as np
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

from vtc_dex.config.settings import (
    ORACLE_FEEDS,
    ORACLE_STAGNANT_TRACKER,
    TOKEN_MINTS,
    jupiter_api_key,
)
from vtc_dex.integrations.jupiter_price import fetch_jupiter_price


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
                spot_price = fetch_jupiter_price(sol_mint, jupiter_api_key)
                if spot_price is not None:
                    print(
                        f"[JUPITER TICK] {pair_ticker} Active Working Value: "
                        f"${spot_price:.4f}"
                    )

                    ORACLE_STAGNANT_TRACKER["last_price"] = spot_price
                    ORACLE_STAGNANT_TRACKER["flatline_count"] = 0
                    print(
                        f"[JUPITER TICK] {pair_ticker} Active Working Value: "
                        f"${spot_price:,.4f}"
                    )
                    return spot_price
                    
        # Return the last known verified price out of our secure cache
        active_working_price = ORACLE_STAGNANT_TRACKER["last_price"]
        print(f"[ORACLE TICK] {pair_ticker} Active Working Value: ${active_working_price:,.4f}")
        return active_working_price
        
    except Exception as e:
        print(f"[ERROR] On-chain cryptograph execution failure: {e}")
        return None
