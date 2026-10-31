def execute_live_onchain_sell(
    layer_index: int,
    rung_index: int,
    amount_in_sol: float,
    target_price: float,
) -> float:
    """Mock sell interface; replace its body with a confirmed DEX adapter later."""
    print(
        f"[MOCK EXECUTION] L{layer_index + 1} rung {rung_index + 1}: "
        f"SELL {amount_in_sol:.17g} SOL near ${target_price:.17g}; no transaction sent."
    )
    return 1.0


def execute_live_onchain_buy(
    layer_index: int,
    rung_index: int,
    amount_in_usdc: float,
    target_price: float,
) -> float:
    """Mock buy interface; replace its body with a confirmed DEX adapter later."""
    print(
        f"[MOCK EXECUTION] L{layer_index + 1} rung {rung_index + 1}: "
        f"BUY with ${amount_in_usdc:.17g} USDC near ${target_price:.17g}; "
        "no transaction sent."
    )
    return 1.0
