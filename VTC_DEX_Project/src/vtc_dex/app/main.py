"""Application startup and engine initialization."""

import numpy as np
from solana.rpc.async_api import AsyncClient

from vtc_dex.config.settings import (
    EXECUTION_MODE,
    INITIAL_ALLOCATION_USDC,
    INVENTORY_MODE,
    OVERFLOW_VAULT,
    RUN_SIMULATION_PRICE_SHIFTER,
    rpc_url,
)
from vtc_dex.domain.accounting import initialize_live_inventory_matrix
from vtc_dex.domain.mesh import (
    build_interleaved_event_mesh,
    generate_infinite_interleaved_matrix,
)
from vtc_dex.domain.rebalancing import rebalance_ladder_spillovers
from vtc_dex.infrastructure.logging_setup import (
    configure_logging,
    log_ladder_state_isolated,
)
from vtc_dex.infrastructure.state_store import (
    dump_engine_state_cache,
    load_engine_state_cache,
)
from vtc_dex.integrations.pyth_oracle import fetch_onchain_price
from vtc_dex.integrations.solana_rpc import fetch_live_token_balance
from vtc_dex.security.transaction_policy import validate_execution_mode
from vtc_dex.app.scheduler import run_engine_loop


async def main():
    validate_execution_mode(EXECUTION_MODE)
    configure_logging()

    print("[SYSTEM START] Starting the two-ladder SOL/USDC price monitor")
    print(f"[INVENTORY MODE] {INVENTORY_MODE}.")
    print("[ACCOUNTING MODE] Mock fills update local inventory only; no swaps are broadcast.")
    print(
        "[PRICE MODE] Synthetic price shifter is "
        f"{'enabled' if RUN_SIMULATION_PRICE_SHIFTER else 'disabled'}."
    )
    print("[EXECUTION MODE] MOCK fills only; no swap transactions are signed or sent.")

    async with AsyncClient(rpc_url) as client:
        if not await client.is_connected():
            print("[CRITICAL] RPC node is unavailable. The monitor will not start.")
            return

        print("[CONNECTION] RPC node status: online")
        
        # Query real on-chain spot pricing anchors out of the Pyth contract first
        calibrated_boot_price = await fetch_onchain_price(client, "SOL/USD")
        if calibrated_boot_price is None or not np.isfinite(calibrated_boot_price) or calibrated_boot_price <= 0:
            print("[CRITICAL] Initial price contract synchronization failed. Aborting initialization.")
            return

        # --- PHASE 1 ADAPTER BRIDGE ROW: UNIFIED LEDGER INTERROGATION ---
        if RUN_SIMULATION_PRICE_SHIFTER:
            # SHADOW SIMULATION MODE: Mimic true hardware wallets by populating an authentic asset base
            print(f"[INTERFACE ADAPTER] Engagement Mode: LABORATORY SHADOW WALLET SIMULATION.")
            live_usdc = INITIAL_ALLOCATION_USDC * np.float64(0.5)
            live_sol = (INITIAL_ALLOCATION_USDC * np.float64(0.5)) / calibrated_boot_price
        else:
            # LIVE PRODUCTION MODE: Execute absolute cryptograph balance scans straight from Web3 nodes
            print(f"[INTERFACE ADAPTER] Engagement Mode: LIVE PRODUCTION WALLET MONITORING.")
            live_sol = await fetch_live_token_balance(client, "SOL")
            live_usdc = await fetch_live_token_balance(client, "USDC")
            
            if live_sol <= 0 and live_usdc <= 0:
                print("[CRITICAL] Interrogated on-chain wallet balance holds 0 assets. Halting engine.")
                return
        # ----------------------------------------------------------------

        if calibrated_boot_price is None or not np.isfinite(calibrated_boot_price) or calibrated_boot_price <= 0:
            print("[CRITICAL] Price calibration failed; the ladders were not initialized.")
            return
        
        recovered_state = load_engine_state_cache(calibrated_boot_price)
        anchor_price = recovered_state["base_anchor_price"] if recovered_state is not None else calibrated_boot_price
        
        l1_grid, l2_grid = generate_infinite_interleaved_matrix(anchor_price, mode="geometric")
        merged_prices, merged_sources = build_interleaved_event_mesh(l1_grid, l2_grid)
        layer_grids = (l1_grid, l2_grid)

        if recovered_state is not None:
            print("[STATE RECOVERY] Restoring both ladder inventories from the local state cache.")
            loop_counter = recovered_state["loop_counter"]
            l1_sol_stock = recovered_state["l1_sol"]
            l1_usdc_stock = recovered_state["l1_usdc"]
            l2_sol_stock = recovered_state["l2_sol"]
            l2_usdc_stock = recovered_state["l2_usdc"]
            OVERFLOW_VAULT.clear()
            OVERFLOW_VAULT.update(recovered_state["overflow_vault"])
            cash_bucket_baselines = recovered_state["cash_bucket_baselines"]
        else:
            loop_counter = 0
            print("[INITIALIZATION] Seeding fresh split-ladder matrices natively out of unified client variables...")
            
            # Map available cash and tokens evenly across both interleaved horizons
            l1_sol_stock, l1_usdc_stock = initialize_live_inventory_matrix(l1_grid, calibrated_boot_price, live_sol_total=live_sol / 2.0, live_usdc_total=live_usdc / 2.0)
            l2_sol_stock, l2_usdc_stock = initialize_live_inventory_matrix(l2_grid, calibrated_boot_price, live_sol_total=live_sol / 2.0, live_usdc_total=live_usdc / 2.0)
            
            # Dynamically lock in baseline metrics to bound your conveyor ceilings accurately
            cash_bucket_baselines = (
                (live_usdc / 2.0) / max(1, int(np.count_nonzero(l1_grid < calibrated_boot_price))),
                (live_usdc / 2.0) / max(1, int(np.count_nonzero(l2_grid < calibrated_boot_price))),
            )
            
        layer_stocks = (l1_sol_stock, l1_usdc_stock, l2_sol_stock, l2_usdc_stock)

        rebalance_ladder_spillovers(
            l1_sol_stock,
            l1_usdc_stock,
            l1_grid,
            cash_bucket_baselines[0],
        )
        rebalance_ladder_spillovers(
            l2_sol_stock,
            l2_usdc_stock,
            l2_grid,
            cash_bucket_baselines[1],
        )
        layer_stocks = (l1_sol_stock, l1_usdc_stock, l2_sol_stock, l2_usdc_stock)
        previous_event_index = int(np.abs(merged_prices - calibrated_boot_price).argmin())
        print(
            f"[LADDERS READY] L1/L2 rungs: {len(l1_grid)} each; "
            f"combined crossing levels: {len(merged_prices)}; anchor: ${anchor_price:,.4f}."
        )
        print(
            f"[INVENTORY] L1+L2 totals: "
            f"{np.sum(l1_sol_stock, dtype=np.float64) + np.sum(l2_sol_stock, dtype=np.float64):.8f} SOL, "
            f"${np.sum(l1_usdc_stock, dtype=np.float64) + np.sum(l2_usdc_stock, dtype=np.float64):.4f} USDC."
        )

        dump_engine_state_cache(
            anchor_price,
            loop_counter,
            OVERFLOW_VAULT,
            layer_stocks,
            cash_bucket_baselines,
        )
        log_ladder_state_isolated(
            "ENGINE_BOOT_INITIALIZATION",
            previous_event_index,
            l1_grid,
            l2_grid,
            layer_stocks,
            calibrated_boot_price,
        )

        await run_engine_loop(
            client=client,
            calibrated_boot_price=calibrated_boot_price,
            anchor_price=anchor_price,
            loop_counter=loop_counter,
            l1_grid=l1_grid,
            l2_grid=l2_grid,
            merged_prices=merged_prices,
            merged_sources=merged_sources,
            layer_grids=layer_grids,
            layer_stocks=layer_stocks,
            cash_bucket_baselines=cash_bucket_baselines,
            previous_event_index=previous_event_index,
        )
