import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Access the environment variables and store them in the configuration dictionary
configuration = {
    "AWS_ACCESS_KEY": os.getenv('AWS_ACCESS_KEY'),
    "AWS_SECRET_KEY": os.getenv('AWS_SECRET_KEY')
}

# You can now access the AWS_ACCESS_KEY and AWS_SECRET_KEY from the configuration dictionary
