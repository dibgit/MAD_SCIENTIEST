import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import numpy as np

from vtc_dex.config.settings import (
    BUY_SELL_RUNG_LOG_BASE_PATH,
    DATA_DIR,
    LOG_FILE_BASE_PATH,
    MAX_BUY_SELL_RUNG_LOG_BACKUPS,
    MAX_BUY_SELL_RUNG_LOG_SIZE_BYTES,
    MAX_LOG_BACKUPS,
    MAX_LOG_SIZE_BYTES,
    OVERFLOW_VAULT,
)


class MasterStreamLogger:
    def __init__(self, original_stream, file_logger):
        self.original_stream = original_stream
        self.file_logger = file_logger

    def write(self, message):
        # Stream raw text directly to native PowerShell window console lanes
        self.original_stream.write(message)
        self.original_stream.flush()
        
        # Filter terminal spacer newlines from cluttering clean text log strings
        cleaned_msg = message.strip()
        if cleaned_msg:
            self.file_logger.info(cleaned_msg)

    def flush(self):
        self.original_stream.flush()


class SequentialLogNamer(RotatingFileHandler):
    """Custom file naming strategy handler to enforce sequential index suffixes: messages0.log, messages1.log..."""
    def do_rollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
            
        # Dynamically isolate folder and string structures to shift suffixes cleanly
        dir_name, base_name = os.path.split(self.baseFilename)
        name_part, ext_part = os.path.splitext(base_name)
            
        # Execute downset shifting logic across historic logs sequentially up to limit boundaries
        for i in range(self.backupCount - 1, -1, -1):
            sfn = os.path.join(dir_name, f"{name_part}{i}{ext_part}" if i > 0 else base_name)
            dfn = os.path.join(dir_name, f"{name_part}{i+1}{ext_part}")
            if os.path.exists(sfn):
                if os.path.exists(dfn):
                    os.remove(dfn)
                os.rename(sfn, dfn)
                
        # Re-initialize clean working target log parameters
        dfn = os.path.join(dir_name, f"{name_part}1{ext_part}")
        if os.path.exists(self.baseFilename):
            if os.path.exists(dfn):
                os.remove(dfn)
            os.rename(self.baseFilename, dfn)
            
        if not self.delay:
            self.stream = self._open()


def setup_ladder_state_logger():
    """Instantiates a dedicated, isolated rotating file buffer exclusively for transaction events and ladder matrix states."""
    ladder_logger = logging.getLogger("LadderStateLogger")
    ladder_logger.setLevel(logging.INFO)
    ladder_logger.propagate = False  # Strict insulation: Stops this data from leaking into console stdout
    
    # Wrap with our sequential rolling logic using custom configuration parameters
    handler = SequentialLogNamer(
        BUY_SELL_RUNG_LOG_BASE_PATH, 
        maxBytes=MAX_BUY_SELL_RUNG_LOG_SIZE_BYTES, 
        backupCount=MAX_BUY_SELL_RUNG_LOG_BACKUPS
    )
    handler.setFormatter(logging.Formatter("%(asctime)s [GRID_MATRIX] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    
    ladder_logger.addHandler(handler)
    return ladder_logger


os.makedirs(DATA_DIR, exist_ok=True)
ladder_state_logger = setup_ladder_state_logger()


def configure_logging():
    file_handler = SequentialLogNamer(
        LOG_FILE_BASE_PATH,
        maxBytes=MAX_LOG_SIZE_BYTES,
        backupCount=MAX_LOG_BACKUPS,
    )
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    internal_logger = logging.getLogger("MatrixEngineLogger")
    internal_logger.setLevel(logging.INFO)
    if not any(
        getattr(handler, "baseFilename", None) == file_handler.baseFilename
        for handler in internal_logger.handlers
    ):
        internal_logger.addHandler(file_handler)
    else:
        file_handler.close()

    if not isinstance(sys.stdout, MasterStreamLogger):
        sys.stdout = MasterStreamLogger(sys.stdout, internal_logger)


def log_ladder_state_isolated(
    context_trigger: str,
    current_tier: int,
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
    layer_stocks: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    market_price: float,
):
    """
    Dumps both parallel interleaved grid ladder layers (L1 & L2) side by side 
    directly into the dedicated rotating log file without leaking to console.
    """
    l1_sol, l1_usdc, l2_sol, l2_usdc = layer_stocks
    total_steps = len(l1_grid)
    l1_sol_total = np.sum(l1_sol, dtype=np.float64)
    l1_usdc_total = np.sum(l1_usdc, dtype=np.float64)
    l2_sol_total = np.sum(l2_sol, dtype=np.float64)
    l2_usdc_total = np.sum(l2_usdc, dtype=np.float64)
    total_sol = l1_sol_total + l2_sol_total + OVERFLOW_VAULT["SOL_DUST"]
    total_usdc = l1_usdc_total + l2_usdc_total + OVERFLOW_VAULT["USDC_DUST"]
    market_price = np.float64(market_price)
    estimated_fund_usd = total_sol * market_price + total_usdc

    ladder_state_logger.info(f"=== [GRID_SNAPSHOT_START] Trigger: {context_trigger} ===")
    ladder_state_logger.info(f"Current Matrix Focus Index Layer: Step #{current_tier + 1}")
    ladder_state_logger.info(
        f"[PORTFOLIO AUDIT] Price=${market_price:.4f} | "
        f"L1={l1_sol_total:.8f} SOL / ${l1_usdc_total:.4f} USDC | "
        f"L2={l2_sol_total:.8f} SOL / ${l2_usdc_total:.4f} USDC | "
        f"Total incl. vault={total_sol:.8f} SOL / ${total_usdc:.4f} USDC | "
        f"Estimated fund=${estimated_fund_usd:.4f}"
    )
    ladder_state_logger.info(
        f"{'Step':<5} | "
        f"{'L1 Price':<11} | {'L1 SOL':<10} | {'L1 USDC':<10} || "
        f"{'L2 Price':<11} | {'L2 SOL':<10} | {'L2 USDC':<10}"
    )
    ladder_state_logger.info("-" * 95)
    
    for i in range(total_steps):
        # Tracking both structures cleanly using relative row indexing markers
        ladder_state_logger.info(
            f"#{i+1:03d} | "
            f"{l1_grid[i]:<11.4f} | {l1_sol[i]:<10.4f} | ${l1_usdc[i]:<9.2f} || "
            f"{l2_grid[i]:<11.4f} | {l2_sol[i]:<10.4f} | ${l2_usdc[i]:<9.2f}"
        )
    ladder_state_logger.info(
        f"Overflow Vault Account Balances: "
        f"{OVERFLOW_VAULT['SOL_DUST']:.6f} SOL | ${OVERFLOW_VAULT['USDC_DUST']:.4f} USDC"
    )
    ladder_state_logger.info("=== [GRID_SNAPSHOT_END] ===")
