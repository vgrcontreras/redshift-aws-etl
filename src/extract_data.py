import requests
from dotenv import load_dotenv
import os
from typing import Any
from loguru import logger

load_dotenv()

X_CG_DEMO_API_KEY = os.getenv('X_CG_DEMO_API_KEY')

def extract_and_load_data() -> dict[str, Any]:
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'

    headers = {
        'accept': "application/json",
        'x-cg-demo-api-key': X_CG_DEMO_API_KEY
    }

    try:
        response = requests.get(url, headers=headers)
        logger.info(f'Data from API {url} retrieved successfully')

        return response.json()
    except Exception as e:
        logger.error(f'Error fetching data from API {url}: {e}')