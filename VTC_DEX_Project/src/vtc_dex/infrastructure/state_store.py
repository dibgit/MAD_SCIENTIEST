import json
import os
from datetime import datetime

import numpy as np

from vtc_dex.config.settings import INVENTORY_MODE, MIN_ROUND_TRIP_GAP_PCT, NUMBER_OF_LADDER, STATE_CACHE_PATH
from vtc_dex.domain.mesh import generate_infinite_interleaved_matrix, generate_pure_geometric_mesh


def dump_engine_state_cache(
    base_anchor_price: float,
    loop_id: int,
    overflow_vault: dict,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    cash_bucket_baselines: tuple[float, float],
):
    """Serializes both discrete interleaved ladder states to prevent data overwrite collisions."""
    try:
        l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
        state_payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "base_anchor_price": float(base_anchor_price),
            "loop_counter": int(loop_id),
            "number_of_ladder": len(l1_sol),
            "inventory_mode": INVENTORY_MODE,
            "l1_sol_stock": l1_sol.tolist(),
            "l1_usdc_stock": l1_usdc.tolist(),
            "l2_sol_stock": l2_sol.tolist(),
            "l2_usdc_stock": l2_usdc.tolist(),
            "overflow_vault_state": overflow_vault,
            "cash_bucket_baselines": list(cash_bucket_baselines),
        }
        with open(STATE_CACHE_PATH, "w") as json_file:
            json.dump(state_payload, json_file, indent=4)
    except Exception as e:
        print(f"[CACHE WRITE ERROR] Dual-ladder serialization failure: {e}")


def load_engine_state_cache(current_price: float) -> dict | None:
    """Loads and price-maps cached inventory onto the configured interleaved ladders."""

    if not os.path.exists(STATE_CACHE_PATH):
        return None
    try:
        current_price = np.float64(current_price)
        with open(STATE_CACHE_PATH, "r") as json_file:
            state_data = json.load(json_file)

        if state_data.get("inventory_mode") != INVENTORY_MODE:
            cached_mode = state_data.get("inventory_mode", "unspecified/legacy")
            print(
                f"[STATE RESET] Cache inventory mode is {cached_mode}; active mode is "
                f"{INVENTORY_MODE}. Reinitializing from the active mode's allocation."
            )
            return None

        anchor_price = np.float64(state_data["base_anchor_price"])
        if not np.isfinite(anchor_price) or anchor_price <= 0:
            raise ValueError("Cached anchor price must be finite and positive.")

        new_l1_grid, new_l2_grid = generate_infinite_interleaved_matrix(
            anchor_price,
            mode="geometric",
        )

        def validate_stock(key_name):
            raw_arr = np.asarray(state_data[key_name], dtype=np.float64)
            if raw_arr.ndim != 1 or len(raw_arr) < 2:
                raise ValueError(f"Cached {key_name} must contain at least two rungs.")
            if not np.all(np.isfinite(raw_arr)) or np.any(raw_arr < 0):
                raise ValueError(f"Cached {key_name} contains invalid inventory values.")
            return raw_arr

        def map_stock_to_grid(stock, source_grid, target_grid):
            if len(stock) != len(source_grid):
                raise ValueError("Cached inventory length does not match its source grid.")
            mapped = np.zeros(len(target_grid), dtype=np.float64)
            right_indices = np.searchsorted(target_grid, source_grid)
            right_indices = np.clip(right_indices, 0, len(target_grid) - 1)
            left_indices = np.clip(right_indices - 1, 0, len(target_grid) - 1)
            use_right = (
                np.abs(target_grid[right_indices] - source_grid)
                <= np.abs(target_grid[left_indices] - source_grid)
            )
            target_indices = np.where(use_right, right_indices, left_indices)
            np.add.at(mapped, target_indices, stock)
            return mapped

        dual_layer_keys = (
            "l1_sol_stock",
            "l1_usdc_stock",
            "l2_sol_stock",
            "l2_usdc_stock",
        )
        if all(key in state_data for key in dual_layer_keys):
            old_l1_sol, old_l1_usdc, old_l2_sol, old_l2_usdc = (
                validate_stock(key) for key in dual_layer_keys
            )
            old_sizes = {
                len(old_l1_sol),
                len(old_l1_usdc),
                len(old_l2_sol),
                len(old_l2_usdc),
            }
            if len(old_sizes) != 1:
                raise ValueError("Cached ladder inventory arrays have inconsistent lengths.")
            old_size = old_sizes.pop()
            source_size = old_size
            old_l1_grid, old_l2_grid = generate_pure_geometric_mesh(
                anchor_price,
                old_size,
                MIN_ROUND_TRIP_GAP_PCT
            )
            l1_sol = map_stock_to_grid(old_l1_sol, old_l1_grid, new_l1_grid)
            l1_usdc = map_stock_to_grid(old_l1_usdc, old_l1_grid, new_l1_grid)
            l2_sol = map_stock_to_grid(old_l2_sol, old_l2_grid, new_l2_grid)
            l2_usdc = map_stock_to_grid(old_l2_usdc, old_l2_grid, new_l2_grid)
        elif "sol_stock_array" in state_data and "usdc_stock_array" in state_data:
            old_sol = validate_stock("sol_stock_array")
            old_usdc = validate_stock("usdc_stock_array")
            if len(old_sol) != len(old_usdc):
                raise ValueError("Cached single-ladder inventory arrays have inconsistent lengths.")
            source_size = len(old_sol)
            old_l1_grid, _ = generate_pure_geometric_mesh(
                anchor_price,
                len(old_sol),
                MIN_ROUND_TRIP_GAP_PCT
            )
            half_sol = old_sol * 0.5
            half_usdc = old_usdc * 0.5
            l1_sol = map_stock_to_grid(half_sol, old_l1_grid, new_l1_grid)
            l1_usdc = map_stock_to_grid(half_usdc, old_l1_grid, new_l1_grid)
            l2_sol = map_stock_to_grid(half_sol, old_l1_grid, new_l2_grid)
            l2_usdc = map_stock_to_grid(half_usdc, old_l1_grid, new_l2_grid)
            print(
                "[STATE MIGRATION] Mapped legacy single-ladder inventory across "
                "both interleaved ladders while preserving total balances."
            )
        else:
            raise ValueError("Cache does not contain a recognized ladder inventory schema.")

        # Identify if an environmental parameter change has been triggered
        size_changed = (source_size != NUMBER_OF_LADDER)

        if size_changed:
            print(
                f"[STATE MIGRATION] Remapped {source_size} cached rung(s) per source ladder "
                f"to {NUMBER_OF_LADDER} configured rung(s) per ladder."
            )
            # DYNAMIC SPREAD STEP NORMALIZER: Recalculate baselines to scale smoothly with your new ladder size
            reference_price = current_price if current_price > 0 else anchor_price

            # VARIABLY LAYER FIX: Extracts metrics from the verified old array layers 
            total_current_cash = np.sum(l1_usdc, dtype=np.float64) + np.sum(
                l2_usdc, dtype=np.float64
            )
            active_funded_buy_rungs = max(1, int(np.count_nonzero(new_l1_grid < reference_price)) + int(np.count_nonzero(new_l2_grid < reference_price)))
            
            # Computes an un-diluted baseline budget based on true incoming cash volumes
            shared_rung_budget = total_current_cash / active_funded_buy_rungs
            cash_baselines = (shared_rung_budget, shared_rung_budget)


            #cash_baselines = (
            #    np.sum(l1_usdc, dtype=np.float64) / max(1, int(np.count_nonzero(new_l1_grid < reference_price))),
            #    np.sum(l2_usdc, dtype=np.float64) / max(1, int(np.count_nonzero(new_l2_grid < reference_price))),
            #)
        else:
            cash_baselines = state_data.get(
                "cash_bucket_baselines",
                (np.float64(0.0), np.float64(0.0)),
            )

        if len(cash_baselines) != 2 or any(
            not np.isfinite(np.float64(value)) or np.float64(value) < 0
            for value in cash_baselines
        ):
            raise ValueError("Cached cash bucket baselines are invalid.")
        if "cash_bucket_baselines" not in state_data:
            reference_price = current_price if current_price > 0 else anchor_price
            cash_baselines = (
                np.sum(l1_usdc, dtype=np.float64)
                / max(1, int(np.count_nonzero(new_l1_grid < reference_price))),
                np.sum(l2_usdc, dtype=np.float64)
                / max(1, int(np.count_nonzero(new_l2_grid < reference_price))),
            )

        cached_vault = state_data.get(
            "overflow_vault_state",
            {"SOL_DUST": np.float64(0.0), "USDC_DUST": np.float64(0.0)},
        )
        return {
            "base_anchor_price": anchor_price,
            "loop_counter": int(state_data["loop_counter"]),
            "l1_sol": l1_sol,
            "l1_usdc": l1_usdc,
            "l2_sol": l2_sol,
            "l2_usdc": l2_usdc,
            "cash_bucket_baselines": tuple(
                np.float64(value) for value in cash_baselines
            ),
            "overflow_vault": {
                "SOL_DUST": np.float64(cached_vault.get("SOL_DUST", 0.0)),
                "USDC_DUST": np.float64(cached_vault.get("USDC_DUST", 0.0)),
            },
        }
    except Exception as e:
        print(f"[HOT BOOT ERROR] Dual-ladder recovery failure: {e}")
        return None
