import numpy as np
from solana.rpc.async_api import AsyncClient
from solana.rpc.core import TokenAccountOpts
from solders.pubkey import Pubkey

from vtc_dex.config.settings import TOKEN_MINTS, wallet_address_str


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
