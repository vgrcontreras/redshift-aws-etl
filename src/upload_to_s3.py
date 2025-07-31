from datetime import datetime
import json
from utils.aws_clients import s3_client
from src.extract_data import extract_and_load_data
from typing import Any


def upload_data_to_s3(data: dict[str, Any], bucket_name: str) -> None:
    now = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    file_name = f'coins-with-market-data-{now}' 

    json_string = json.dumps(data)

    s3_client.Bucket(bucket_name).put_object(
        Key=file_name,
        Body=json_string
    )

if __name__ == '__main__':
    data = extract_and_load_data()

    upload_data_to_s3(data)
