"""Price polling and crossing-event scheduling."""

import asyncio
import time

import numpy as np
from solana.rpc.async_api import AsyncClient

from vtc_dex.config.settings import OVERFLOW_VAULT, RUN_SIMULATION_PRICE_SHIFTER
from vtc_dex.domain.accounting import apply_interleaved_crossing
from vtc_dex.domain.mesh import get_simulated_price
from vtc_dex.domain.rebalancing import rebalance_ladder_spillovers
from vtc_dex.infrastructure.backup import execute_automated_git_vault
from vtc_dex.infrastructure.ledger import (
    analyze_and_suggest_ladder_expansion,
    log_transaction_to_csv,
)
from vtc_dex.infrastructure.logging_setup import log_ladder_state_isolated
from vtc_dex.infrastructure.state_store import dump_engine_state_cache
from vtc_dex.integrations.pyth_oracle import fetch_onchain_price
from vtc_dex.integrations.swap_executor import (
    execute_live_onchain_buy,
    execute_live_onchain_sell,
)
from vtc_dex.app.display import print_interleaved_ladder_state


def scan_price_crossings(
    live_price: float,
    previous_event_index: int,
    merged_prices: np.ndarray,
    loop_counter: int,
) -> int:
    current_event_index = previous_event_index
    if live_price > merged_prices[previous_event_index]:
        print(
            f"[TICK {loop_counter}] Price has risen above the previous level; "
            f"scanning upward for crossings. Live Price = ${live_price:,.4f}, "
            f"Previous Level = ${merged_prices[previous_event_index]:,.4f}"
        )
        while (
            current_event_index < len(merged_prices) - 1
            and live_price >= merged_prices[current_event_index + 1]
        ):
            current_event_index += 1
    elif live_price < merged_prices[previous_event_index]:
        print(
            f"[TICK {loop_counter}] Price has fallen below the previous level; "
            f"scanning downward for crossings. Live Price = ${live_price:,.4f}, "
            f"Previous Level = ${merged_prices[previous_event_index]:,.4f}"
        )
        while (
            current_event_index > 0
            and live_price <= merged_prices[current_event_index - 1]
        ):
            current_event_index -= 1
    return current_event_index


async def run_engine_loop(
    client: AsyncClient,
    calibrated_boot_price: float,
    anchor_price: float,
    loop_counter: int,
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
    merged_prices: np.ndarray,
    merged_sources: list[tuple[int, int, str]],
    layer_grids: tuple[np.ndarray, np.ndarray],
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    cash_bucket_baselines: tuple[float, float],
    previous_event_index: int,
) -> None:
    l1_sol_stock, l1_usdc_stock, l2_sol_stock, l2_usdc_stock = layer_stocks
    last_git_vault_timestamp = time.time()
    while True:
        loop_counter += 1
        live_price = await fetch_onchain_price(client, "SOL/USD")
        if live_price is None or not np.isfinite(live_price) or live_price <= 0:
            await asyncio.sleep(3)
            continue

        live_price = get_simulated_price(live_price, loop_counter)

        current_event_index = scan_price_crossings(
            live_price,
            previous_event_index,
            merged_prices,
            loop_counter,
        )

        if current_event_index == previous_event_index:
            print(
                f"[TICK {loop_counter}] Price ${live_price:,.4f} remains near combined level "
                f"{current_event_index + 1}/{len(merged_prices)}."
            )
            await asyncio.sleep(3)
            continue

        direction = "UPWARD" if current_event_index > previous_event_index else "DOWNWARD"
        crossed_indices = (
            range(previous_event_index + 1, current_event_index + 1)
            if direction == "UPWARD"
            else range(previous_event_index - 1, current_event_index - 1, -1)
        )
        steps_moved = abs(current_event_index - previous_event_index)
        print(
            f"[CROSSING] {direction}: {steps_moved} interleaved level(s), "
            f"combined index {previous_event_index + 1} -> {current_event_index + 1}."
        )

        # Convert crossed_indices to a list to safely capture the terminal tracking index
        crossed_list = list(crossed_indices)
        for event_index in crossed_list:
            layer_index, rung_index, target_order_type = merged_sources[event_index]

            # --- PRISTINE LIQUIDITY CONSUMPTION CALCULATOR ---
            if event_index == crossed_list[-1]:
                if direction == "UPWARD" and live_price > merged_prices[event_index]:
                    terminal_flag = False
                elif direction == "DOWNWARD" and live_price < merged_prices[event_index]:
                    terminal_flag = False
                else:
                    terminal_flag = True
            else:
                terminal_flag = False

            # Dynamic Type-Safe Filter Gate
            if direction == "UPWARD" and target_order_type != "SELL":
                continue
            if direction == "DOWNWARD" and target_order_type != "BUY":
                continue

            # DETHERMINISTIC TIMELINE ENGINE:
            # Executes and clears each crossed price milestone in exact sequential order
            apply_interleaved_crossing(
                direction=direction,
                layer_index=layer_index,
                rung_index=rung_index,
                rung_price=np.float64(merged_prices[event_index]),
                layer_grids=layer_grids,
                layer_stocks=layer_stocks,
                cash_bucket_baselines=cash_bucket_baselines,
                is_terminal_step=terminal_flag,
                sell_executor=execute_live_onchain_sell,
                buy_executor=execute_live_onchain_buy,
            )
        # GLOBAL INTERLEAVED PERSISTENCE SWEEP
        # Ensures any asset or cash surplus pushed past boundary caps cascades 
        # completely into permanent long-term vault archives unconditionally.
        rebalance_ladder_spillovers(l1_sol_stock, l1_usdc_stock, l1_grid, cash_bucket_baselines[0])
        rebalance_ladder_spillovers(l2_sol_stock, l2_usdc_stock, l2_grid, cash_bucket_baselines[1])

        total_sol = np.sum(l1_sol_stock, dtype=np.float64) + np.sum(
            l2_sol_stock, dtype=np.float64
        )
        total_usdc = np.sum(l1_usdc_stock, dtype=np.float64) + np.sum(
            l2_usdc_stock, dtype=np.float64
        )
        print(f"[PORTFOLIO MODEL] {total_sol:.8f} SOL + ${total_usdc:,.4f} USDC")
        print_interleaved_ladder_state(
            loop_counter,
            l1_grid,
            l2_grid,
            layer_stocks,
        )
        log_transaction_to_csv(
            loop_id=loop_counter,
            live_price=live_price,
            current_step=current_event_index + 1,
            direction=direction,
            steps_moved=steps_moved,
            layer_stocks=layer_stocks,
        )
        last_git_vault_timestamp = execute_automated_git_vault(
            tick_id=loop_counter,
            current_step=current_event_index + 1,
            market_price=live_price,
            last_vault_time=last_git_vault_timestamp,
        )
        # Resolve clean initialization focus row index parameter
        active_focal_rung = previous_event_index if previous_event_index is not None else 0

        log_ladder_state_isolated(
            "ENGINE_BOOT_INITIALIZATION",
            active_focal_rung,
            l1_grid,
            l2_grid,
            layer_stocks,
            calibrated_boot_price,
        )
        dump_engine_state_cache(
            anchor_price,
            loop_counter,
            OVERFLOW_VAULT,
            layer_stocks,
            cash_bucket_baselines,
        )
        analyze_and_suggest_ladder_expansion()
        previous_event_index = current_event_index
        if RUN_SIMULATION_PRICE_SHIFTER:
            # Fast-forward lab tests at 20 ticks per second to audit multi-day curves instantly
            await asyncio.sleep(0.05) 
        else:
            # Production Track: Paced threshold buffer to protect Alchemy RPC node limits
            await asyncio.sleep(3.0)
