"""Shared type aliases for the grid's NumPy-backed domain data."""

import numpy as np

LadderGrids = tuple[np.ndarray, np.ndarray]
LadderStocks = tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]
CashBucketBaselines = tuple[float, float]
