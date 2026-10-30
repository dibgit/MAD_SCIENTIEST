import numpy as np

from vtc_dex.config.settings import (
    BUCKET_OVERFLOW_THRESHOLD,
    CONVEYOR_THRESHOLD_CAP_SOL,
    OVERFLOW_VAULT,
)


def execute_conveyor_bucket_spillover(
    step_index: int,
    sol_inventory: np.ndarray,
    usdc_inventory: np.ndarray,
    grid_matrix: np.ndarray,
    cash_bucket_baseline: float,
):
    """
    Moves SOL surplus toward higher-price rungs and USDC surplus toward lower-price
    rungs, vaulting only the amount that cannot fit within the ladder boundaries.
    """
    global OVERFLOW_VAULT

    if len(sol_inventory) != len(usdc_inventory) or len(sol_inventory) != len(grid_matrix):
        raise ValueError("SOL, USDC, and price-grid arrays must have matching lengths.")
    if not 0 <= step_index < len(grid_matrix):
        raise IndexError("Spillover step_index is outside the ladder.")

    # A zero baseline means this ladder has no funded cash-bucket target yet.
    # In that case, retain sale proceeds at their rung instead of draining them
    # through the ladder and into the vault.
    cash_bucket_baseline = np.float64(cash_bucket_baseline)

    rung_price = np.float64(grid_matrix[step_index])
    cash_limit = (
        CONVEYOR_THRESHOLD_CAP_SOL * rung_price * BUCKET_OVERFLOW_THRESHOLD
    )
    cash_index = step_index
    while usdc_inventory[cash_index] > cash_limit:
        excess_cash = np.float64(usdc_inventory[cash_index] - cash_limit)
        usdc_inventory[cash_index] = cash_limit
        if cash_index > 0:
            lower_step = cash_index - 1
            usdc_inventory[lower_step] += excess_cash
            print(
                f"[CASH CONVEYOR] Step {cash_index + 1} sent "
                f"${excess_cash:.4f} USDC down to Step {lower_step + 1}."
            )
            cash_index = lower_step
            # Recalculate cash limit for the lower step to keep the loop mathematically bounded
            cash_limit = CONVEYOR_THRESHOLD_CAP_SOL * np.float64(grid_matrix[cash_index]) * BUCKET_OVERFLOW_THRESHOLD
        else:
            OVERFLOW_VAULT["USDC_DUST"] += excess_cash
            print(
                f"[CASH FLOOR VAULTED] Step 1 reached the lower boundary; "
                f"vaulted ${excess_cash:.4f} USDC."
            )
            break

    # SOL surplus moves only toward higher-price rungs in this same ladder.
    asset_index = step_index
    asset_upper_limit = CONVEYOR_THRESHOLD_CAP_SOL * BUCKET_OVERFLOW_THRESHOLD
    while sol_inventory[asset_index] > asset_upper_limit:
        excess_sol = np.float64(sol_inventory[asset_index] - asset_upper_limit)
        sol_inventory[asset_index] = asset_upper_limit
        if asset_index < len(grid_matrix) - 1:
            upper_step = asset_index + 1
            sol_inventory[upper_step] += excess_sol
            print(
                f"[SOL CONVEYOR] Step {asset_index + 1} sent "
                f"{excess_sol:.8f} SOL up to Step {upper_step + 1}."
            )
            asset_index = upper_step
        else:
            OVERFLOW_VAULT["SOL_DUST"] += excess_sol
            print(
                f"[SOL CEILING VAULTED] Step {asset_index + 1} reached the upper "
                f"boundary; vaulted {excess_sol:.8f} SOL."
            )
            break


def rebalance_ladder_spillovers(
    sol_inventory: np.ndarray,
    usdc_inventory: np.ndarray,
    grid_matrix: np.ndarray,
    cash_bucket_baseline: float,
) -> None:
    """Sweep every rung of one ladder without accessing the other ladder's stocks."""
    for step_index in range(len(grid_matrix)):
        execute_conveyor_bucket_spillover(
            step_index,
            sol_inventory,
            usdc_inventory,
            grid_matrix,
            cash_bucket_baseline,
        )
