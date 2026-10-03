import requests

response = requests.get(
    'https://api.jup.ag/price/v3',
    params={'ids': 'So11111111111111111111111111111111111111112'},
    headers={'x-api-key': '<Here is my API key I have created in Jupiter>'}
)
response.raise_for_status()
prices = response.json()
sol_price = prices['So11111111111111111111111111111111111111112']['usdPrice']
print(f"SOL: ${sol_price}")


