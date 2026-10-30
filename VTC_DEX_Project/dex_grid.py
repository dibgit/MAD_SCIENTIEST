"""Compatibility launcher for the reorganized VTC DEX application."""

import asyncio
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SOURCE_ROOT = os.path.join(PROJECT_ROOT, "src")
if SOURCE_ROOT not in sys.path:
    sys.path.insert(0, SOURCE_ROOT)

from vtc_dex.config.settings import *  # noqa: F403
from vtc_dex.domain.accounting import (
    apply_interleaved_crossing as _apply_interleaved_crossing,
    initialize_live_inventory_matrix,
    initialize_simulation_inventory,
)
from vtc_dex.domain.mesh import (
    build_interleaved_event_mesh,
    generate_infinite_interleaved_matrix,
    generate_pure_arithmetic_mesh,
    generate_pure_geometric_mesh,
    get_simulated_price,
)
from vtc_dex.domain.rebalancing import (
    execute_conveyor_bucket_spillover,
    rebalance_ladder_spillovers,
)
from vtc_dex.infrastructure.backup import execute_automated_git_vault
from vtc_dex.infrastructure.ledger import (
    analyze_and_suggest_ladder_expansion,
    log_transaction_to_csv,
)
from vtc_dex.infrastructure.logging_setup import (
    MasterStreamLogger,
    SequentialLogNamer,
    configure_logging,
    ladder_state_logger,
    log_ladder_state_isolated,
    setup_ladder_state_logger,
)
from vtc_dex.infrastructure.state_store import (
    dump_engine_state_cache,
    load_engine_state_cache,
)
from vtc_dex.domain.models import CashBucketBaselines, LadderGrids, LadderStocks
from vtc_dex.integrations.pyth_oracle import fetch_onchain_price
from vtc_dex.integrations.solana_rpc import fetch_live_token_balance
from vtc_dex.integrations.swap_executor import (
    execute_live_onchain_buy,
    execute_live_onchain_sell,
)
from vtc_dex.app.display import print_interleaved_ladder_state
from vtc_dex.app.main import main


def apply_interleaved_crossing(
    direction: str,
    layer_index: int,
    rung_index: int,
    rung_price: float,
    layer_grids: LadderGrids,
    layer_stocks: LadderStocks,
    cash_bucket_baselines: CashBucketBaselines,
    is_terminal_step: bool,
) -> None:
    return _apply_interleaved_crossing(
        direction,
        layer_index,
        rung_index,
        rung_price,
        layer_grids,
        layer_stocks,
        cash_bucket_baselines,
        is_terminal_step,
        sell_executor=execute_live_onchain_sell,
        buy_executor=execute_live_onchain_buy,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM SHUTDOWN] Execution loop terminated by user.")
