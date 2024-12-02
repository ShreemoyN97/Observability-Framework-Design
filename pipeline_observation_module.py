# pipeline_observation_module.py
import pymysql
from datetime import datetime
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv() 

DB_HOST = os.getenv('DB1_HOST')
DB_NAME = os.getenv('DB1_NAME')
DB_USER = os.getenv('DB1_USER')
DB_PASSWORD = os.getenv('DB1_PASSWORD')


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


from pyspark.sql import SparkSession

def get_processed_count(file_path: str) -> int:
    """
    Count the total number of records in the processed flat file.
    :param file_path: Path to the flat file containing the processed data.
    :return: The count of records in the file.
    """
    # Initialize a new Spark session
    spark = SparkSession.builder \
        .appName("Get Processed Count") \
        .getOrCreate()

    try:
        # Load the flat file into a Spark DataFrame
        processed_df = spark.read.option("header", "true").csv(file_path, inferSchema=True)

        # Calculate the total record count
        record_count = processed_df.count()
        return record_count

    except Exception as e:
        print(f"Error in get_processed_count: {e}")
        return 0

    finally:
        # Stop the Spark session
        spark.stop()
