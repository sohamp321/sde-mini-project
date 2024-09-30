import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Access the environment variables and store them in the configuration dictionary
configuration = {
    "AWS_ACCESS_KEY": os.getenv('AWS_ACCESS_KEY'),
    "AWS_SECRET_KEY": os.getenv('AWS_SECRET_KEY'),
    "AWS_BUCKET": os.getenv('AWS_BUCKET'),
    "MINIO_ENDPOINT": os.getenv('MINIO_ENDPOINT'),
    "MINIO_ACCESS_KEY": os.getenv('MINIO_ACCESS_KEY'),
    "MINIO_SECRET_KEY": os.getenv('MINIO_SECRET_KEY'),
    "MINIO_BUCKET": os.getenv('MINIO_BUCKET'),
}
