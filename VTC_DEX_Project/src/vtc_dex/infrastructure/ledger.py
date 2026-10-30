import csv
import os
from datetime import datetime

import numpy as np

from vtc_dex.config.settings import CSV_LEDGER_PATH, NUMBER_OF_LADDER, OVERFLOW_VAULT
from vtc_dex.infrastructure.logging_setup import ladder_state_logger


def log_transaction_to_csv(
    loop_id: int,
    live_price: float,
    current_step: int,
    direction: str,
    steps_moved: int,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
):
    """Appends high-precision split-ladder metric snapshots directly to your Excel database file."""
    live_price = np.float64(live_price)
    file_exists = os.path.exists(CSV_LEDGER_PATH)
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    l1_sol_total = np.sum(l1_sol, dtype=np.float64)
    l1_usdc_total = np.sum(l1_usdc, dtype=np.float64)
    l2_sol_total = np.sum(l2_sol, dtype=np.float64)
    l2_usdc_total = np.sum(l2_usdc, dtype=np.float64)
    total_sol = l1_sol_total + l2_sol_total + OVERFLOW_VAULT["SOL_DUST"]
    total_usdc = l1_usdc_total + l2_usdc_total + OVERFLOW_VAULT["USDC_DUST"]
    portfolio_value = (total_sol * live_price) + total_usdc
    
    # Expanded database tracking headers matching your multi-ladder asset layers
    headers = [
        "timestamp", "tick_id", "market_price", "active_step", "trade_direction", "steps_crossed", 
        "l1_sol_total", "l1_usdc_total", "l2_sol_total", "l2_usdc_total", "vault_sol", "vault_usdc", "net_worth_usd"
    ]
    
    try:
        with open(CSV_LEDGER_PATH, mode="a", newline="") as csv_file:
            writer = csv.writer(csv_file)
            if not file_exists:
                writer.writerow(headers)
                
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                loop_id,
                f"{live_price:.4f}",
                current_step,
                direction,
                steps_moved,
                f"{l1_sol_total:.6f}",
                f"{l1_usdc_total:.4f}",
                f"{l2_sol_total:.6f}",
                f"{l2_usdc_total:.4f}",
                f"{OVERFLOW_VAULT['SOL_DUST']:.6f}",
                f"{OVERFLOW_VAULT['USDC_DUST']:.4f}",
                f"{portfolio_value:.4f}"
            ])
    except Exception as e:
        print(f"[ERROR] CSV Database Multi-Grid Write Failure: {e}")


def analyze_and_suggest_ladder_expansion():
    """
    Analyzes historical volatility crossings from the CSV database ledger.
    Generates a structured configuration advice snapshot to recommend safety window expansion parameters.
    """
    if not os.path.exists(CSV_LEDGER_PATH):
        return
        
    try:
        with open(CSV_LEDGER_PATH, mode="r") as csv_file:
            lines = list(csv.reader(csv_file))
            if len(lines) <= 5:
                return # Insufficient statistical track data collected
                
            # Extract historical step crossing tracks
            steps_logged = [int(row[3]) for row in lines[1:] if len(row) > 3 and row[3].strip().isdigit()]
            if not steps_logged:
                return
                
            min_step_reached = min(steps_logged)
            max_step_reached = max(steps_logged)
            
            # --- EVALUATE MATRIX EXHAUSTION THREAT LEVELS ---
            # If historical tracks show trades hitting within 1 step of your boundaries, recommend growth
            combined_rung_count = NUMBER_OF_LADDER * 2

            ladder_state_logger.info("=== [QUANTITATIVE GRID ANALYSIS REPORT] ===")
            ladder_state_logger.info(
                f"Historical Active Step Range Bounds: Min Step #{min_step_reached} | "
                f"Max Step #{max_step_reached} "
                f"(Per Ladder: {NUMBER_OF_LADDER}; Combined: {combined_rung_count})"
            )

            if min_step_reached <= 2 or max_step_reached >= (combined_rung_count - 1):
                recommended_per_ladder = int(np.ceil(NUMBER_OF_LADDER * 1.25))
                recommended_combined = recommended_per_ladder * 2

                ladder_state_logger.info("[ALERT: EXHAUSTION RISK] Price volatility has aggressively pushed near your ladder boundaries.")
                print("[ALERT: EXHAUSTION RISK] Price volatility has aggressively pushed near your ladder boundaries.")
                recommendation = (
                    f"Recommended Action Strategy: Set NUMBER_OF_LADDER in .env "
                    f"from {NUMBER_OF_LADDER} to {recommended_per_ladder} "
                    f"rungs per ladder ({combined_rung_count} to "
                    f"{recommended_combined} combined). Restart the engine to apply."
                )
                ladder_state_logger.info(recommendation)
                print(recommendation)
                ladder_state_logger.info("This adjustment will compress inter-rung delta gaps, boost localized trading volume capture, and shield assets from permanent vault lockout events.")
            else:
                ladder_state_logger.info("[STATUS: OPTIMAL] Current ladder configuration limits provide a safe structural cushioning barrier against immediate out-of-bounds market exposure.")
            ladder_state_logger.info("==========================================")
            
    except Exception as e:
        print(f"[RECONFIG ADVISOR ERROR] Failed to compute quantitative ledger metrics: {e}")
