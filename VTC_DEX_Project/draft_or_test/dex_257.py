import os
import asyncio
from dotenv import load_dotenv
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient

# Load workspace infrastructure
load_dotenv()
rpc_url = os.getenv("SOLANA_RPC_URL")
wallet_address_str = os.getenv("MY_PUBLIC_WALLET")

# Universal Solana On-Chain Oracle Feeds (Pyth Network State Accounts)
# These read directly from live smart contract memory maps on Solana
ORACLE_FEEDS = {
    "SOL/USD": "H6ARLBOTw2xKiURmCvoGkh43bJ99G56VZa94UM33w1ay"
}

async def check_wallet_fuel(client: AsyncClient):
    """Asynchronously reads your live Phantom gas sandbox profile."""
    if not wallet_address_str:
        print("❌ Error: MY_PUBLIC_WALLET variable is missing from your .env file.")
        return
    public_key = Pubkey.from_string(wallet_address_str)
    balance_response = await client.get_balance(public_key)
    sol_balance = balance_response.value / 1_000_000_000
    print(f"📦 Laboratory Sandbox Fuel: {sol_balance} SOL")

async def fetch_onchain_price(client: AsyncClient, pair_ticker: str = "SOL/USD"):
    """
    Universally reads live price values directly from smart contract storage structures.
    Bypasses standard HTTP API endpoint rate-limits entirely.
    """
    feed_address = ORACLE_FEEDS.get(pair_ticker)
    if not feed_address:
        print(f"❌ Abort: Oracle config missing for {pair_ticker}")
        return None
        
    try:
        # THE FIX: Force the address to a clean, stripped string data type
        clean_address = str(feed_address).strip()
        pubkey = Pubkey.from_string(clean_address)
        
        # Fetch the complete binary data storage block of the account from your node
        account_info = await client.get_account_info(pubkey)
        data = account_info.value.data
        
        # Parse the custom Pyth cryptographic data structures natively from byte maps
        # Price offset resides at bytes 208-216, exponent follows at byte 216
        import struct
        price = struct.unpack("<q", data[208:216])[0]
        exponent = struct.unpack("<i", data[216:220])[0]
        
        final_price = price * (10 ** exponent)
        print(f"📈 Direct Node Matrix Feed -> {pair_ticker} = ${final_price:.2f} USDC/USDT")
        return final_price
    except Exception as e:
        print(f"⚠️ Chain Read Anomaly for {pair_ticker}. Reason: {e}")
        return None

async def main():
    print("--- 🔬 Starting Unlimited Async Laboratory Simulation ---")
    
    async with AsyncClient(rpc_url) as client:
        is_connected = await client.is_connected()
        if is_connected:
            print("🚀 Node Connection: ONLINE")
            await check_wallet_fuel(client)
            
            print("\n--- Testing Unlimited Direct-Node Feeds ---")
            await fetch_onchain_price(client, "SOL/USD")
        else:
            print("❌ Node Connection: OFFLINE. Verify your RPC string in .env")

if __name__ == "__main__":
    asyncio.run(main())
