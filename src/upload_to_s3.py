from datetime import datetime
import json
from utils.aws_clients import s3_resource
from src.extract_data import extract_and_load_data
from typing import Any
from loguru import logger


def upload_data_to_s3(data: dict[str, Any], bucket_name: str) -> None:
    now = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    file_name = f'coins-with-market-data-{now}.json' 

    json_string = json.dumps(data)

    try:
        s3_resource.Bucket(bucket_name).put_object(
            Key=file_name,
            Body=json_string
        )
        logger.info(f'{file_name} uploaded to {bucket_name}')
    except Exception as e:
        logger.error(f'Error uploading {file_name} to {bucket_name}')


if __name__ == '__main__':
    data = extract_and_load_data()

    upload_data_to_s3(data, bucket_name='coingecko-raw-data')
