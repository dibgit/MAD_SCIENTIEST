import asyncio
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey  # Modern pubkey import

async def main():
    # Use 'async with' to automatically open and clean up the client connection
    async with AsyncClient("https://api.mainnet-beta.solana.com") as client:
        # Await the response
        response = await client.get_balance(Pubkey.from_string("YOUR_WALLET_PUBKEY"))
        print(f"Balance: {response.value}")

asyncio.run(main())
