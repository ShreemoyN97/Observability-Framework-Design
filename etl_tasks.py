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
        

        # Count the number of records
        processed_count = df.count()
        # Write the transformed data to MySQL
        write_to_csvfile(df)
        # Stop the Spark session
        spark.stop()
        return processed_count

def write_to_csvfile(df):
    # Write the DataFrame to a CSV file
    df.write.option("header", "true").csv("output.csv")
