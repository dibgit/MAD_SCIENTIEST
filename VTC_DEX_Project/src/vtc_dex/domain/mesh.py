import numpy as np

from vtc_dex.config.settings import (
    MIN_ROUND_TRIP_GAP_PCT,
    NUMBER_OF_LADDER,
    RUN_SIMULATION_PRICE_SHIFTER,
)


def generate_pure_geometric_mesh(
    anchor_price: float,
    rung_count: int,
    gap_pct: float,
) -> tuple[np.ndarray, np.ndarray]:
    """
    REPLACES np.geomspace(): Generates a symmetrical interleaved dual-ladder grid 
    built directly out from a central anchor price using a fixed geometric trading spread.
    """
    anchor_price = np.float64(anchor_price)
    gap_pct = np.float64(gap_pct)
    
    # Establish your constant geometric multiplier ratio (e.g., 1.01362703 for Option B)
    # CORRECTED SYSTEM DEPENDENCY: Scales the buy-rung distance to exactly 
    # double your execution spread, preventing gridlock order collisions.
    step_multiplier = np.float64(1.0) + (gap_pct * np.float64(2.0))
    # Determine the number of tiers to deploy above and below your active working center anchor
    # Using an equal split gives your matrix a perfectly balanced bidirectional runway
    half_rungs = rung_count // 2
    
    # --- GENERATE LADDER 1 BASELINE CROSSINGS ---
    l1_grid = np.zeros(rung_count, dtype=np.float64)
    l1_grid[half_rungs] = anchor_price
    
    # Project prices downward for the lower buy rungs
    for i in range(half_rungs - 1, -1, -1):
        l1_grid[i] = l1_grid[i + 1] / step_multiplier
        
    # Project prices upward for the higher sell rungs
    for i in range(half_rungs + 1, rung_count):
        l1_grid[i] = l1_grid[i - 1] * step_multiplier

    # --- GENERATE LADDER 2 STAGGERED MIDZONE INTERLEAVE ---
    # To place Ladder 2 perfectly between Ladder 1's rungs geometrically, 
    # we multiply Ladder 1 by the square root of your step multiplier
    # half_step_ratio = np.sqrt(step_multiplier)
    half_step_ratio = np.sqrt(step_multiplier) * (np.float64(1.0) + (gap_pct * np.float64(0.5)))
    l2_grid = l1_grid * half_step_ratio

    return l1_grid, l2_grid


def generate_pure_arithmetic_mesh(
    anchor_price: float,
    rung_count: int,
    gap_pct: float,
) -> tuple[np.ndarray, np.ndarray]:
    """
    REPLACES np.linspace(): Generates a symmetrical interleaved dual-ladder grid 
    built directly out from a central anchor price using a localized linear volatility step size.
    """
    anchor_price = np.float64(anchor_price)
    gap_pct = np.float64(gap_pct)
    
    # Derive a static dollar increment bound right at your active center spot price
    # Example: If SOL is $100 and gap is 1.363%, your flat stepping increment is exactly $1.363
    linear_dollar_step = anchor_price * (gap_pct * np.float64(2.0))
    
    half_rungs = rung_count // 2
    
    # --- GENERATE LADDER 1 LINEAR PROGRESSIONS ---
    l1_grid = np.zeros(rung_count, dtype=np.float64)
    l1_grid[half_rungs] = anchor_price
    
    for i in range(half_rungs - 1, -1, -1):
        l1_grid[i] = l1_grid[i + 1] - linear_dollar_step
        
    for i in range(half_rungs + 1, rung_count):
        l1_grid[i] = l1_grid[i - 1] + linear_dollar_step

    # --- GENERATE LADDER 2 STAGGERED MIDZONE INTERLEAVE ---
    # To place Ladder 2 perfectly between Ladder 1's rungs arithmetically, 
    # we shift it upward by exactly half of your flat linear dollar step
    l2_grid = l1_grid + (linear_dollar_step / 2.0)

    return l1_grid, l2_grid


def generate_infinite_interleaved_matrix(
    low_price: float,  # This now acts as our central anchor spot price parameter input
    mode: str = "geometric",
) -> tuple[np.ndarray, np.ndarray]:
    """Generates the configured pair of interleaved price ladders using pure scaling mesh rules."""
    
    # Force direct execution routing straight to your pure scaling mesh structures
    if mode == "arithmetic":
        l1_grid, l2_grid = generate_pure_arithmetic_mesh(low_price, NUMBER_OF_LADDER, MIN_ROUND_TRIP_GAP_PCT)
    else:
        l1_grid, l2_grid = generate_pure_geometric_mesh(low_price, NUMBER_OF_LADDER, MIN_ROUND_TRIP_GAP_PCT)
        
    # Flatten arrays explicitly via .ravel() to guarantee string interpolation handles them as scalar layers
    l1_flat = l1_grid.ravel()
    l2_flat = l2_grid.ravel()

    print(
        f"[INTERLEAVE INITIALIZED] Ladder 1: {len(l1_flat)} rungs "
        f"(${float(l1_flat[0]):,.17g}-${float(l1_flat[-1]):,.17g})"
    )
    print(
        f"[INTERLEAVE INITIALIZED] Ladder 2: {len(l2_flat)} rungs "
        f"(${float(l2_flat[0]):,.17g}-${float(l2_flat[-1]):,.17g})"
    )
    return l1_grid, l2_grid


def build_interleaved_event_mesh(
    l1_grid: np.ndarray,
    l2_grid: np.ndarray,
) -> tuple[np.ndarray, list[tuple[int, int, str]]]:
    """
    REFACTORED COMPILER: Interleaves both BUY and profit-taking SELL milestones 
    for both ladders into a single chronological matrix to prevent scanner gridlock.
    """
    events = []
    
    # Map Ladder 1 Targets
    for idx, buy_p in enumerate(l1_grid):
        sell_p = buy_p * (np.float64(1.0) + MIN_ROUND_TRIP_GAP_PCT)
        events.append((np.float64(buy_p), 0, idx, "BUY"))
        events.append((np.float64(sell_p), 0, idx, "SELL"))
        
    # Map Ladder 2 Targets
    for idx, buy_p in enumerate(l2_grid):
        sell_p = buy_p * (np.float64(1.0) + MIN_ROUND_TRIP_GAP_PCT)
        events.append((np.float64(buy_p), 1, idx, "BUY"))
        events.append((np.float64(sell_p), 1, idx, "SELL"))
        
    # Sort everything chronologically by price
    events.sort(key=lambda event: event[0])
    
    prices = np.array([event[0] for event in events], dtype=np.float64)
    # Store the ladder index, rung index, and the specific order-type filter string
    sources = [(event[1], event[2], event[3]) for event in events]
    return prices, sources


def get_simulated_price(real_oracle_price: float, current_tick: int) -> float:
    """
    Apply the optional price wave or precise debugging toggle flags 
    for local ladder-accounting and leakage isolation tests.
    """
    real_oracle_price = np.float64(real_oracle_price)
    
    # STRUCTURAL DEBUG TOGGLE: Set to True to engage precision threshold switching
    DEBUG_0 = True

    if DEBUG_0:
        # Precision Switch Lane: Flips strictly between the exact Buy and Sell anchors of L1 Rung 1
        # Even ticks simulate an upward hit (Sell), odd ticks simulate a downward hit (Buy)
        if current_tick % 2 == 0:
            simulated_price = np.float64(101.468699) #target price for L1 rung 1 buy-->OK
            #simulated_price = np.float64(125.821555) #target price for L1 rung 9 buy-->OK
            #simulated_price = np.float64(103.542838)  #target price for L2 rung 1 buy-->OK
            #simulated_price = np.float64(128.393495)  #target price for L2 rung 9 buy--> OK
            #simulated_price = np.float64(136.391924)  #target price for L1 rung 12 buy--> OK
            #simulated_price = np.float64(139.179935)  #target price for L2 rung 12 buy--> OK
        else:
            #simulated_price = np.float64(101.468699) #target price for L1 rung 1 sell-->OK
            #simulated_price = np.float64(127.536132) #target price for L1 rung 9 sell-->OK
            #simulated_price = np.float64(104.953821)  #target price for L2 rung 1 sell-->OK
            #simulated_price = np.float64(130.143119)  #target price for L2 rung 9 sell--> OK
            #simulated_price = np.float64(138.250544)  #target price for L1 rung 12 sell--> OK
            simulated_price = np.float64(141.076547)  #target price for L2 rung 12 sell--> OK

        print(f"[DEBUG_0 ACTIVE] Loop Counter: {current_tick} | Forced Target Price: ${simulated_price:.17g}")
        return simulated_price

    else:
        # ---- STANDARD SIMULATION SINE WAVE TRACK (DEBUG_0 == FALSE) ----
        if not RUN_SIMULATION_PRICE_SHIFTER or current_tick <= 5:
            return real_oracle_price

        wave_frequency = np.float64(current_tick - 5) * (
            np.float64(2.0) * np.pi / np.float64(60.0)
        )
        simulated_price = real_oracle_price * (
            np.float64(2.0) + np.sin(wave_frequency) * np.float64(0.30)
        )
        simulated_price /= np.float64(2.0)

        print(f"[SIMULATION SHIFTER] Local accounting price: ${simulated_price:,.17g}")
        return simulated_price
