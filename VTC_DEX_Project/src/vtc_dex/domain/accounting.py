import random
from collections.abc import Callable

import numpy as np

from vtc_dex.config.settings import DEX_EXECUTION_FEE_RATE, RUN_SIMULATION_PRICE_SHIFTER
from vtc_dex.domain.rebalancing import rebalance_ladder_spillovers


def initialize_live_inventory_matrix(grid_matrix: np.ndarray, current_price: float, live_sol_total: float, live_usdc_total: float) -> tuple[np.ndarray, np.ndarray]:
    """
    PRODUCTION INTERFACE INITIALIZER: Dynamically maps true on-chain ledger asset balances 
    across the active dual geometric scaling rungs relative to real-time spot entry points.
    """
    current_price = np.float64(current_price)
    live_sol_total = np.float64(live_sol_total)
    live_usdc_total = np.float64(live_usdc_total)
    total_steps = len(grid_matrix)
    
    sol_inventory = np.zeros(total_steps, dtype=np.float64)
    usdc_inventory = np.zeros(total_steps, dtype=np.float64)
    
    # Isolate rungs relative to market spot boundaries to build bidirectional runway channels
    below_market_indices = np.where(grid_matrix < current_price)[0]
    above_market_indices = np.where(grid_matrix >= current_price)[0]
    
    # 1. Symmetrically fund active buy-side tracks with cash weightings
    buy_steps_count = len(below_market_indices)
    if buy_steps_count > 0 and live_usdc_total > 0:
        usdc_per_step = live_usdc_total / np.float64(buy_steps_count)
        usdc_inventory[below_market_indices] = usdc_per_step
            
    # 2. Symmetrically fund active sell-side short lanes with asset stock
    sell_steps_count = len(above_market_indices)
    if sell_steps_count > 0 and live_sol_total > 0:
        sol_per_step = live_sol_total / np.float64(sell_steps_count)
        sol_inventory[above_market_indices] = sol_per_step
            
    print(f"  [INTERFACE ADAPTER] Mapped vector layer: {np.sum(sol_inventory):.17g} SOL ({sell_steps_count} asks) | ${np.sum(usdc_inventory):.17g} USDC ({buy_steps_count} bids)")
    return sol_inventory, usdc_inventory


def initialize_simulation_inventory(
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
    current_price: float,
    total_budget_usdc: float,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    tuple[float, float],
]:
    """Allocates one shared USD budget across both simulation ladders."""
    if not np.isfinite(current_price) or current_price <= 0:
        raise ValueError("Simulation price must be finite and positive.")
    if not np.isfinite(total_budget_usdc) or total_budget_usdc <= 0:
        raise ValueError("Simulation budget must be finite and positive.")
    if len(l1_grid) != len(l2_grid) or len(l1_grid) == 0:
        raise ValueError("Simulation ladders must have matching, non-empty price grids.")

    current_price = np.float64(current_price)
    total_budget_usdc = np.float64(total_budget_usdc)
    bucket_budget = total_budget_usdc / np.float64(len(l1_grid) + len(l2_grid))
    inventories = []
    cash_baselines = []
    for grid in (l1_grid, l2_grid):
        sol_inventory = np.zeros(len(grid), dtype=np.float64)
        usdc_inventory = np.zeros(len(grid), dtype=np.float64)
        buy_indices = grid < current_price
        usdc_inventory[buy_indices] = bucket_budget
        sol_inventory[~buy_indices] = bucket_budget / current_price
        inventories.extend((sol_inventory, usdc_inventory))
        cash_baselines.append(np.float64(bucket_budget))

    print(
        f"[SIMULATION INVENTORY] Allocated ${total_budget_usdc:,.17g} total across "
        f"L1+L2 at ${current_price:,.17g}; "
        f"${bucket_budget:,.17g} budget per rung."
    )
    return (*inventories, tuple(cash_baselines))


def apply_interleaved_crossing(
    direction: str,
    layer_index: int,
    rung_index: int,
    rung_price: float,
    layer_grids: tuple[np.ndarray, np.ndarray],
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    cash_bucket_baselines: tuple[float, float],
    is_terminal_step: bool,  # Passed from the main crossing event scanner
    sell_executor: Callable[[int, int, float, float], float] | None = None,
    buy_executor: Callable[[int, int, float, float], float] | None = None,
) -> None:
    """Updates one ladder's local inventory accounting for a crossed price level."""
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    if layer_index == 0:
        sol_stock, usdc_stock = l1_sol, l1_usdc
    elif layer_index == 1:
        sol_stock, usdc_stock = l2_sol, l2_usdc
    else:
        raise ValueError(f"Invalid ladder index: {layer_index}")
    if not 0 <= rung_index < len(sol_stock):
        raise IndexError(f"Invalid rung index {rung_index} for ladder {layer_index + 1}.")

    if direction == "UPWARD" and sol_stock[rung_index] > 0:
        available_sol = np.float64(sol_stock[rung_index])
        if RUN_SIMULATION_PRICE_SHIFTER:
            # check if this is the final event index in our current wave
            fill_fraction = 1.00 if not is_terminal_step else random.uniform(0.10, 1.00)
        else:
            if sell_executor is None:
                raise RuntimeError("A sell execution adapter must be configured.")
            fill_fraction = sell_executor(
                layer_index,
                rung_index,
                available_sol,
                rung_price,
            )
        if not np.isfinite(fill_fraction) or not 0.0 <= fill_fraction <= 1.0:
            raise ValueError(f"Sell adapter returned invalid fill fraction: {fill_fraction}")
        sol_sold = available_sol * fill_fraction
        if sol_sold == 0:
            return

        # REALIZATION PASS: Multiplies the base buy price by your spread factor 
        # to ensure the sell order executes exactly at your Option B profit target
        #explicit_sell_price = rung_price * (np.float64(1.0) + MIN_ROUND_TRIP_GAP_PCT)
        #usdc_received = sol_sold * explicit_sell_price * (1.0 - DEX_EXECUTION_FEE_RATE)
        usdc_received = sol_sold * rung_price * (1.0 - DEX_EXECUTION_FEE_RATE)
        sol_stock[rung_index] -= sol_sold
        usdc_stock[rung_index] += usdc_received
        print(
            f"  [FILL ACCOUNTING] L{layer_index + 1} rung {rung_index + 1} "
            f"sold {sol_sold:.17g} SOL at ${rung_price:.17g}; "
            f"credited ${usdc_received:.17g} USDC "
            f"({fill_fraction:.17g} fill fraction / {fill_fraction:.1%}; fee included)."
        )
    elif direction == "DOWNWARD" and usdc_stock[rung_index] > 0:
        available_usdc = np.float64(usdc_stock[rung_index])
        if RUN_SIMULATION_PRICE_SHIFTER:
            fill_fraction = 1.00 if not is_terminal_step else random.uniform(0.10, 1.00)
        else:
            if buy_executor is None:
                raise RuntimeError("A buy execution adapter must be configured.")
            fill_fraction = buy_executor(
                layer_index,
                rung_index,
                available_usdc,
                rung_price,
            )
        if not np.isfinite(fill_fraction) or not 0.0 <= fill_fraction <= 1.0:
            raise ValueError(f"Buy adapter returned invalid fill fraction: {fill_fraction}")
        usdc_spent = available_usdc * fill_fraction
        if usdc_spent == 0:
            return
        sol_received = (usdc_spent / rung_price) * (1.0 - DEX_EXECUTION_FEE_RATE)
        usdc_stock[rung_index] -= usdc_spent
        sol_stock[rung_index] += sol_received
        print(
            f"  [FILL ACCOUNTING] L{layer_index + 1} rung {rung_index + 1} "
            f"spent ${usdc_spent:.17g} at ${rung_price:.17g}; "
            f"credited {sol_received:.17g} SOL "
            f"({fill_fraction:.17g} fill fraction / {fill_fraction:.1%}; fee included)."
        )
    else:
        return

    cash_baseline = cash_bucket_baselines[layer_index]
    rebalance_ladder_spillovers(
        sol_stock,
        usdc_stock,
        layer_grids[layer_index],
        cash_baseline,
    )
