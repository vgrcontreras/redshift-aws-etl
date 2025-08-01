from utils.aws_clients import s3_resource
from loguru import logger


def fetch_filenames_from_s3_bucket(bucket_name: str) -> dict:
    try:
        bucket = s3_resource.Bucket(bucket_name)

        bucket_filenames = set(obj.key for obj in bucket.objects.all())
        logger.info(f'File names from {bucket_name} retrieved successfully')
        return bucket_filenames
    except Exception as e:
        logger.error(f'Error retrieving files from {bucket_name}: {e}')


if __name__ == '__main__':
    print(fetch_filenames_from_s3_bucket('coingecko-raw-data'))