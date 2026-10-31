"""Console rendering for ladder state."""

import numpy as np

from vtc_dex.config.settings import MIN_ROUND_TRIP_GAP_PCT


def print_interleaved_ladder_state(
    event_id: int,
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
) -> None:
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    ladder_size = len(l1_grid)
    if any(
        len(values) != ladder_size
        for values in (l2_grid, l1_sol, l1_usdc, l2_sol, l2_usdc)
    ):
        raise ValueError("Both ladders and their inventory arrays must have equal lengths.")

    header = (
        "Rung | Buy price (USDC/SOL) | Buy fund (USDC) | "
        "Sell price (USDC/SOL) | Sell fund (SOL)"
    )
    print(f"[USDC/SOL LADDER AFTER EVENT {event_id:03d}]")
    print(f"{'L1':<2} {header} || {'L2':<2} {header}")
    for rung_index in range(ladder_size):
        l1_buy_price = np.float64(l1_grid[rung_index])
        l2_buy_price = np.float64(l2_grid[rung_index])
        l1_sell_price = l1_buy_price * (
            np.float64(1.0) + MIN_ROUND_TRIP_GAP_PCT
        )
        l2_sell_price = l2_buy_price * (
            np.float64(1.0) + MIN_ROUND_TRIP_GAP_PCT
        )
        l1_row = (
            f"{rung_index + 1:4d} | {l1_buy_price:>23.17g} | "
            f"{l1_usdc[rung_index]:>23.17g} | {l1_sell_price:>23.17g} | "
            f"{l1_sol[rung_index]:>23.17g}"
        )
        l2_row = (
            f"{rung_index + 1:4d} | {l2_buy_price:>23.17g} | "
            f"{l2_usdc[rung_index]:>23.17g} | {l2_sell_price:>23.17g} | "
            f"{l2_sol[rung_index]:>23.17g}"
        )
        print(f"{l1_row} || {l2_row}")
