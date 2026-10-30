"""Jupiter spot-price adapter used as an oracle fallback."""

import requests
import numpy as np


def fetch_jupiter_price(token_mint: str, api_key: str) -> np.float64 | None:
    url = "https://api.jup.ag/price/v3"
    params = {"ids": token_mint}
    headers = {"x-api-key": api_key.strip()}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=5)
        if response.status_code == 200:
            prices = response.json()
            return np.float64(prices[token_mint]["usdPrice"])
    except Exception as error:
        print(f"[WARNING] Jupiter V3 API fallback interface error: {error}")
    return None
