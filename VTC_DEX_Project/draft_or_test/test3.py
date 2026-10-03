import requests

def get_jupiter_token_prices():
    # Target endpoint for Jupiter Price V3 API
    url = "https://api.jup.ag/price/v3"
    
    # Solana Mint Addresses for the tokens you want to query
    # JUP: JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN
    # SOL: So11111111111111111111111111111111111111112
    token_mints = [
        "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
        "So11111111111111111111111111111111111111112"
    ]
    
    # Pass mint addresses as a comma-separated string parameter
    params = {
        "ids": ",".join(token_mints)
    }
    
    try:
        # Perform the GET request without passing an 'x-api-key' header
        response = requests.get(url, params=params)
        response.raise_for_status()
        print("Successfully fetched token prices.")
        data = response.json()
        prices_data = data.get("data", {})
        
        # Parse and display the results
        for mint, info in prices_data.items():
            token_id = info.get("id")
            price = info.get("price")
            # Fallback if your token's metadata isn't returned
            extra = f" (Mint: {mint})" if token_id != mint else ""
            print(f"Token: {token_id}{extra} | Price: ${float(price):.4f}")
            
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            print("Error: You are being rate-limited. Keyless access allows 30 requests per minute.")
        else:
            print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

if __name__ == "__main__":
    print("Starting Jupiter Token Price Fetcher...")
    get_jupiter_token_prices()
    print("Ending Jupiter Token Price Fetcher...")
