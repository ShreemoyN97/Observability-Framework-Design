# etl_tasks.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import error_capture_module
import pymysql
import os

def perform_etl(file_category_name, input_path, process_file_id):
    if file_category_name == "EV":
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Data Aggregation") \
            .getOrCreate()
        
        # Load the dataset
        df = spark.read.option("header", "true").csv(input_path, inferSchema=True)
        
        # Collect rows that have no null values in any column
        valid_rows = []

        # Check for null values and log errors
        for row in df.collect():
            record_row = row.asDict()
            record_id = None  # Initialize record_id as None for each row
            has_null = False  # Flag to track if the row has any null values

            for col_name in df.columns:
                if row[col_name] is None:
                    has_null = True
                    # Insert record error only once per row
                    if record_id is None:
                        record_id = error_capture_module.insert_record_error(process_file_id, record_row)
                        print(f"Record ID after insert_record_error: {record_id}")
                    
                    # Insert a column error for each null column
                    error_description = "Null value encountered in non-nullable column"
                    error_code = error_capture_module.get_error_code(error_description)
                    print(f"Error code for '{error_description}': {error_code}")
                    
                    if error_code is not None and record_id is not None:
                        
                        error_capture_module.insert_column_error(record_id, process_file_id, col_name, error_code)
                    else:
                        print(f"Error in retrieving record ID or error code for description: {error_description}")
            
            # If no null values in the row, add it to valid_rows
            if not has_null:
                valid_rows.append(row)
        
        # Create a DataFrame with only the valid rows
        valid_df = spark.createDataFrame(valid_rows, df.schema)

        # Perform transformations
        df = valid_df.withColumn(
            "total_financials",
            col("compensation") +
            col("reimbursed_expenses") +
            col("expenses_less_than_75") +
            col("lobbying_expenses_for_non") +
            col("itemized_expenses")
        )
        
        # Write the transformed data to MySQL
        write_to_mysql(df)
        # Stop the Spark session
        spark.stop()



def write_to_mysql(df):
    # Connect to MySQL
    connection = pymysql.connect(
        host=os.getenv("DB1_HOST"),
        user=os.getenv("DB1_USER"),
        password=os.getenv("DB1_PASSWORD"),
        database=os.getenv("DB2_NAME"),
        cursorclass=pymysql.cursors.DictCursor
    )
    with connection.cursor() as cursor:
        for row in df.collect():
            cursor.execute(
                """
                INSERT INTO Processed_Lobbying_Data (
                    form_submission_id, reporting_year, filing_type, reporting_period,
                    principal_lobbyist_name, contractual_client_name, beneficial_client_name,
                    individual_lobbyist_name, compensation, reimbursed_expenses, expenses_less_than_75,
                    lobbying_expenses_for_non, itemized_expenses, expense_type, expense_paid_to,
                    expense_reimbursed_by_client, expense_purpose, expense_date, lobbying_subjects,
                    level_of_government, lobbying_focus_type, focus_identifying_number,
                    type_of_lobbying_communication, government_body, monitoring_only, party_name,
                    unique_id, total_financials
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['form_submission_id'], row['reporting_year'], row['filing_type'],
                    row['reporting_period'], row['principal_lobbyist_name'],
                    row['contractual_client_name'], row['beneficial_client_name'],
                    row['individual_lobbyist_name'], row['compensation'],
                    row['reimbursed_expenses'], row['expenses_less_than_75'],
                    row['lobbying_expenses_for_non'], row['itemized_expenses'],
                    row['expense_type'], row['expense_paid_to'], row['expense_reimbursed_by_client'],
                    row['expense_purpose'], row['expense_date'], row['lobbying_subjects'],
                    row['level_of_government'], row['lobbying_focus_type'], row['focus_identifying_number'],
                    row['type_of_lobbying_communication'], row['government_body'], row['monitoring_only'],
                    row['party_name'], row['unique_id'], row['total_financials']
                )
            )
    connection.commit()
    connection.close()