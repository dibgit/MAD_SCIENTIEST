import asyncio
import struct
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

# The official Pyth SOL/USD (or SOL/USDT) Price Account on Solana Mainnet
SOL_PRICE_FEED_ACCOUNT = "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
RPC_ENDPOINT = "https://api.mainnet-beta.solana.com" # Use a premium RPC for low-latency production

async def get_live_onchain_price():
    async with AsyncClient(RPC_ENDPOINT) as client:
        pubkey = Pubkey.from_string(SOL_PRICE_FEED_ACCOUNT)
        
        # 1. Fetch the raw contract storage account info
        response = await client.get_account_info(pubkey)
        if not response.value:
            print("Account not found.")
            return
            
        data = response.value.data
        
        # 2. Parse the Pyth v2 Binary Struct Layout
        # Pyth magic number is at byte 0, version at byte 4. 
        # The price info struct starts at byte 208.
        # Struct pattern: q (i64 price), Q (u64 conf), i (i32 exponent)
        price_offset = 208
        price_data = data[price_offset:price_offset + 20]
        
        raw_price, confidence, exponent = struct.unpack("<qQi", price_data)
        
        # 3. Convert the fixed-point storage structure to an actual USD value
        actual_price = raw_price * (10 ** exponent)
        actual_confidence = confidence * (10 ** exponent)
        
        print(f"--- On-Chain Storage State ---")
        print(f"Raw Price Integer: {raw_price}")
        print(f"Exponent Offset:   {exponent}")
        print(f"Live SOL/USDT Price: ${actual_price:,.4f} ± ${actual_confidence:,.4f}")

if __name__ == "__main__":
    asyncio.run(get_live_onchain_price())
