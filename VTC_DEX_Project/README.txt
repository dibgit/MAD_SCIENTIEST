================================================================================
LABORATORY SPECIFICATION MANUAL: DECENTRALIZED ASYNC GRID TRADING ENGINE
Project Root Directory: D:\MAD_SCIENTIEST\VTC_DEX_Project
Environment Target: Windows 11 (PowerShell Architecture)
================================================================================

1. DEPENDENCY ENVIRONMENT INSTALLATION
Execute the following terminal command to deploy the core high-performance 
Web3 libraries and local environment isolation tools:

  pip install web3 solana solders ccxt python-dotenv numpy

Validation Verification Test String:
Execute this command path check to confirm the library paths are fully compiled:

  python -c "import web3; import solana; print('Web3 & Solana packages loaded successfully!')"

[Troubleshooting Note]: If execution returns a 'python is not recognized' error, 
verify that the 'Add Python to PATH' setting configuration is selected within 
your Python deployment engine installation manager parameters.


2. PROJECT STRUCTURE DEFINITION
Execute these commands to build the isolated laboratory directory tree:

  mkdir D:\MAD_SCIENTIEST\VTC_DEX_Project
  cd D:\MAD_SCIENTIEST\VTC_DEX_Project
  New-Item dex_grid.py
  New-Item .env
  New-Item .env.example
  New-Item .gitignore


3. ISOLATED BROWSER NODE CONFIGURATION
To insulate private trading sandbox keys from core desktop operational networks:

  3.1 Deploy Brave Browser to your system drive.
  3.2 Initialize an independent browser profile instance labeled: Python Sandbox
  3.3 Download and add the official Phantom Wallet extension into the profile:
      URL target: https://phantom.com
  3.4 Instantiate a new test account wallet profile profile labeled: VTC_SOL
  3.5 Capitalize this profile with a micro-tier funding layer (~0.03 SOL) to fuel 
      on-chain transaction testing and connection routines.


4. INDEPENDENT ON-CHAIN REMOTE PROCEDURE CALL (RPC) METRICS
To access unthrottled blockchain execution data structures directly:

  4.1 Establish a free infrastructural developer account via Alchemy.com.
  4.2 Navigate to the workspace control console panel and trigger: Create New App
  4.3 Hard-code your node rules to: Network: Solana | Chain: Mainnet
  4.4 Assign an arbitrary application label string (e.g., Mad_Scientist_Engine).
  4.5 Open the 'View Keys' sub-panel interface menu.
  4.6 Extract your dedicated HTTPS Endpoint String value. 
      Target pattern example:
      https://alchemy.com


5. HARDWARE CORE AND LOCAL ENVIRONMENT ARCHIVE RULES (.env)
Your private '.env' configurations must follow this layout index format exactly:

  SOLANA_RPC_URL="https://alchemy.com"
  MY_PUBLIC_WALLET="PASTE_YOUR_PUBLIC_PHANTOM_WALLET_ADDRESS_STRING"


6. SYSTEM DATA MODIFICATIONS AND ENGINE DEPRECATIONS (CRITICAL LOGS)
  6.1 JUPITER API DEPRECATION: 
      The initial Jupiter REST pricing configuration matrices require API key strings 
      and enforce a strict 1 Request-Per-Minute (1-RPM) usage restriction block on 
      free profiles. 
      DESTRUCTION NOTICE: All Jupiter integration code blocks have been fully 
      purged from the project space. Do not deploy the Jupiter CLI utility, curl 
      strings, or HTTP dependencies into 'dex_grid.py'.
      
  6.2 ON-CHAIN CONTRACT PIPELINE ADOPTION:
      The data ingestion pipeline has transitioned to reading raw binary byte layouts 
      directly out of Pyth Network v5 decentralized oracle accounts via your unlimited 
      Alchemy RPC nodes. This allows for unlimited price checking execution updates 
      without throttling limits.
================================================================================
