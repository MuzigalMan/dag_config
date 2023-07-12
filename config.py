import os
import pymysql
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
global conn

#DB Connection
load_dotenv()

db = 'mysql'
user = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
schema = os.getenv('DB_SCHEMA')
port = os.getenv('DB_PORT')
host = os.environ.get('DB_HOST')

try:
    conn = create_engine(f"{db}+pymysql://{user}:{password}@{host}:{port}/{schema}")
except Exception as e:
    print(f'An Error has occured {e}')