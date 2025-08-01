from src.extract_data import extract_and_load_data
from src.upload_to_s3 import upload_data_to_s3
from utils.aws_clients import s3_client
from src.convert_to_parquet import convert_to_parquet
from src.read_s3_file import fetch_filenames_from_s3_bucket

from loguru import logger


def main():
    logger.info('Pipeline initialized')
    buckets_dict = {
        'raw_bucket': 'coingecko-raw-data', 
        'processed_bucket':'coingecko-processed'
        }

    for bucket in buckets_dict.values():
        try:
            s3_client.create_bucket(Bucket=bucket)
            logger.info(f'Bucket {bucket} created successfully')
        except Exception as e:
            logger.error(f'Error creating bucket {bucket}')


    data = extract_and_load_data()
    upload_data_to_s3(data, bucket_name=buckets_dict['raw_bucket'])

    buckets_filesname = fetch_filenames_from_s3_bucket(buckets_dict['raw_bucket'])
    convert_to_parquet(
        src_bucket_name=buckets_dict['raw_bucket'],
        dst_bucket_name=buckets_dict['processed_bucket'],
        buckets_filesname=buckets_filesname
    )

    logger.info('Pipeline')


if __name__ == '__main__':
    main()
