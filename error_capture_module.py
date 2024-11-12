import pymysql
import json
from datetime import datetime
import os
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('host')
DB_PORT = int(os.getenv('port'))
DB_DATABASE = os.getenv('database')
DB_USER = os.getenv('username')
DB_PASSWORD = os.getenv('password')


def connect_to_database():
    return pymysql.connect(
        host=DB_HOST, 
        user=DB_USER, 
        password=DB_PASSWORD, 
        db=DB_DATABASE,
        cursorclass=pymysql.cursors.DictCursor  # Use DictCursor to return results as dictionaries
    )

def convert_to_serializable(obj):
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def insert_record_error(process_file_id, record_row):
    record_row_serializable = convert_to_serializable(record_row)
    record_text = json.dumps(record_row_serializable)

    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO Record_Error_Report (Processing_File_ID, Record_Text) VALUES (%s, %s)", (process_file_id, record_text))
            record_id = cursor.lastrowid
            connection.commit()
            return record_id
    finally:
        connection.close()

def insert_column_error(record_id, processing_file_id, column_name, error_code):
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO Column_Error_Report (Record_ID, Processing_File_ID, Column_Name, Error_Code) VALUES (%s, %s, %s, %s)", (record_id, processing_file_id, column_name, error_code))
            connection.commit()
            print("Insert successful in Column_Error_Report")
    except pymysql.MySQLError as e:
        print(f"Error inserting into Column_Error_Report: {e}")
    finally:
        connection.close()

def get_error_code(description):
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT Error_Code FROM Error_Message_Reference WHERE Error_Message = %s", (description,))
            result = cursor.fetchone()
            return result['Error_Code'] if result else None
    finally:
        connection.close()