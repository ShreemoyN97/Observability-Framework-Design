import pymysql
import os
from dotenv import load_dotenv


load_dotenv() 

host = os.getenv('host')
port = int(os.getenv('port'))
database = os.getenv('database')
username = os.getenv('username')
password = os.getenv('password')

try:
    # Attempt to establish a MySQL connection
    connection = pymysql.connect(host=host, user=username, password=password, db=database)
    print("Successfully connected to the database.")
    

finally:
    # Ensure the connection is closed properly
    if connection:
        connection.close()
