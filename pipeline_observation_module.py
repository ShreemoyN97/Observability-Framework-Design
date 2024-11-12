# pipeline_observation_module.py
import pymysql
from datetime import datetime
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv() 

DB_HOST = os.getenv('host')
DB_NAME = os.getenv('database')
DB_USER = os.getenv('username')
DB_PASSWORD = os.getenv('password')


def connect_to_database():
    return pymysql.connect(
        host=DB_HOST, 
        user=DB_USER, 
        password=DB_PASSWORD, 
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

def initialize_observation(process_file_id, process_start_time):
    """
    Initializes the observation metrics for a given process file.
    Sets the process start time in the Pipeline_Observability table.
    """
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE Pipeline_Observability SET Process_StartTime = %s WHERE Processing_File_ID = %s",
                (process_start_time, process_file_id)
            )
            connection.commit()
        print("Observation initialized with process start time.")
    finally:
        connection.close()

def finalize_observation(process_file_id, process_end_time, initial_count_of_records, processed_count, error_count, distinct_error_count):
    """
    Finalizes the observation metrics and logs them in the Pipeline_Observability table.
    Sets the process end time, total processed records, error count, and distinct errors.
    """
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE Pipeline_Observability
                SET Process_End_Time = %s,
                    Count_Of_Processed_Records = %s,
                    Count_Of_Error_Records = %s,
                    Count_of_Distinct_Errors = %s
                WHERE Processing_File_ID = %s
                """,
                (process_end_time, processed_count, error_count, distinct_error_count, process_file_id)
            )
            connection.commit()
        print("Observation finalized with process end time.")
    finally:
        connection.close()

def get_error_metrics(process_file_id):
    """
    Retrieves error metrics from Record_Error_Report and Column_Error_Report.
    Returns the count of error records and count of distinct errors.
    """
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS error_count FROM Record_Error_Report WHERE Processing_File_ID = %s", (process_file_id,))
            error_count = cursor.fetchone()['error_count']
            
            cursor.execute("SELECT COUNT(DISTINCT Error_Code) AS distinct_error_count FROM Column_Error_Report WHERE Processing_File_ID = %s", (process_file_id,))
            distinct_error_count = cursor.fetchone()['distinct_error_count']
            
            return error_count, distinct_error_count
    finally:
        connection.close()

def get_processed_count(output_path):
    """
    Count the number of records in the processed output files.
    :param output_path: Path to the output directory where processed CSV files are stored.
    :return: The count of processed records.
    """
    spark = SparkSession.builder \
        .appName("ProcessedCount") \
        .getOrCreate()
    
    df = spark.read.option("header", "true").csv(output_path)
    processed_count = df.count()
    spark.stop()
    
    return processed_count
