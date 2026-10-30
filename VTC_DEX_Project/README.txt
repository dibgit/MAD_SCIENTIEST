================================================================================

LABORATORY SPECIFICATION MANUAL: DECENTRALIZED ASYNC GRID TRADING ENGINE
Project Root Directory: this file's directory
Environment Target: Windows 11 (PowerShell Architecture)
================================================================================

1. DEPENDENCY ENVIRONMENT INSTALLATION
Install the runtime dependencies from the project environment:

  pip install requests solana solders python-dotenv numpy

Validation Verification Test String:
Run this import check to confirm the required libraries are available:

  python -c "import requests, solana, solders, dotenv, numpy; print('Dependencies loaded successfully!')"

[Troubleshooting Note]: If execution returns a 'python is not recognized' error, 
verify that the 'Add Python to PATH' setting configuration is selected within 
your Python deployment engine installation manager parameters.


2. PROJECT STRUCTURE DEFINITION
From the project directory, create a local environment file from the example:

  Copy-Item .env.example .env

3. SOLANA REMOTE PROCEDURE CALL (RPC) CONFIGURATION
Set the ladder rung count per ladder, plus the RPC endpoint and public wallet
address in the local '.env' file:

  NUMBER_OF_LADDER=257
  SOLANA_RPC_URL="YOUR_SOLANA_RPC_ENDPOINT"
  MY_PUBLIC_WALLET="YOUR_PUBLIC_WALLET_ADDRESS"

After changing NUMBER_OF_LADDER, restart the engine. Cached balances are remapped
to the new geometric rung prices while preserving total SOL and USDC inventory.
The prior single-ladder cache format is split across L1 and L2 during migration.

Each ladder maintains separate SOL and USDC rung arrays. Bucket overflow is swept
within that ladder only: excess SOL moves toward higher-price rungs; excess USDC
moves toward lower-price rungs. Amounts that pass the top or bottom boundary are
recorded in the shared overflow vault and are not automatically returned to either
ladder. If a ladder's cached cash baseline is zero, its USDC conveyor is disabled
so sale proceeds remain at the crossed rung instead of being swept into the vault.

4. ENGINE SCOPE AND PRICE FEED
The engine reads on-chain balances and oracle prices, with a Jupiter price fallback
when configured. Price crossings update local ladder accounting only; this version
does not build, sign, or broadcast DEX swap transactions. Set
`RUN_SIMULATION_PRICE_SHIFTER=true` in `.env` to apply a synthetic +/-30% price wave
to local accounting and initialize both ladders from the shared virtual budget
`INITIAL_ALLOCATION_USDC` (default $1,000), rather than wallet balances. The combined
L1+L2 portfolio starts with that budget, not $1,000 per ladder. Changing inventory
mode resets an incompatible cache so wallet-backed balances are not mistaken for
simulation funds. Buy and sell hooks currently use
`EXECUTION_MODE=MOCK`, report full mock fills for the identified ladder/rung, and
send no transactions. Live mode is not implemented or enabled.
================================================================================

5. RUNNING THE REORGANIZED PROJECT
The implementation is organized under 'src/vtc_dex/' by responsibility:

  config/          Environment-backed settings
  domain/          Mesh generation, accounting, and ladder rebalancing
  integrations/    Solana RPC, Pyth/Jupiter price reads, mock execution
  security/        Secret access and mock-only execution policy
  infrastructure/ State cache, CSV ledger, logging, and optional Git backup
  app/             Application startup, scheduling, and display

The original root-level launcher remains available for compatibility:

  python dex_grid.py

The launcher adds 'src/' to the import path and delegates to the modular
application. Importing 'dex_grid' also continues to expose the existing
functions and settings for scripts that used the monolithic module.
Live transaction signing, broadcasting, and DEX swaps remain unimplemented.
