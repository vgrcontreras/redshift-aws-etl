from datetime import datetime
import os
import json
from typing import Any


def transform_data_to_json(data: dict[str, Any]):
    now = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    folder_name = 'data'
    file_name = f'coins-with-market-data-{now}' 

    os.makedirs(folder_name, exist_ok=True)

    with open(f'{folder_name}/{file_name}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)