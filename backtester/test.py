import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("db_host")
name = os.getenv("db_name")
db_pass = os.getenv("db_pass")
user = os.getenv("db_user")

