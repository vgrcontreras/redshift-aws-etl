from utils.aws_clients import s3_resource
import pandas as pd
import io
from pprint import pprint
import os
from loguru import logger


PROCESSED_FILE_PATH = 'data/processed.txt'


def convert_to_parquet(
    src_bucket_name: str,
    dst_bucket_name: str,
    buckets_filesname: dict
):
    # read file with processed file names
    try:
        os.makedirs(os.path.dirname)
        with open(PROCESSED_FILE_PATH, 'r') as f:
            processed_files = set(line.strip() for line in f.readlines())
    except FileNotFoundError:
        processed_files = set()

    # return only file names from raw bucket that has not been processed yet
    files_to_process = buckets_filesname - processed_files
    logger.info(f'{len(files_to_process)} new files to process')

    dst_bucket = s3_resource.Bucket(dst_bucket_name)

    newly_processed = set()

    for file in files_to_process:
        if not file.lower().endswith('.json'):
            logger.warning(f'Skipping non-JSON file: {file}')
            continue

        try:
            obj = s3_resource.Object(
                bucket_name=src_bucket_name, key=file
            )

            # get content and metadata from a s3 file
            response = obj.get()

            # read the content from the body metadata
            content = response['Body'].read() 

            # transform json binary content into a dataframe
            df = pd.read_json(io.BytesIO(content))

            # create an empty file-like object
            buffer = io.BytesIO()

            df.to_parquet(buffer, index=False, engine='pyarrow') # create in memory parquet file

            buffer.seek(0) # return pointer to the beginning of the file-like object

            base_name = os.path.splitext(file)[0]
            parque_file_name = f'{base_name}.parquet'

            dst_bucket.upload_fileobj(buffer, parque_file_name)

            newly_processed.add(file)

            logger.info(f'Finished converting and uploading: {file}')

        except Exception as e:
            logger.error(f'Failed to process {file}: {e}')

    with open(PROCESSED_FILE_PATH, 'a') as f:
        for file in newly_processed:
            f.write(f'{file}\n')


if __name__ == '__main__':
    convert_to_parquet('coingecko-raw-data', 'coingecko-processed')





