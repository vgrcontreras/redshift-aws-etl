import requests
from dotenv import load_dotenv
import os
from typing import Any

load_dotenv()

X_CG_DEMO_API_KEY = os.getenv('X_CG_DEMO_API_KEY')

def extract_data() -> dict[str, Any]:
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'

    headers = {
        'accept': "application/json",
        'x-cg-demo-api-key': X_CG_DEMO_API_KEY
    }

    response = requests.get(url, headers=headers)
    return response.json()

