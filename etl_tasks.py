# etl_tasks.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import error_capture_module

def perform_etl(input_path, output_path, process_file_id):
    spark = SparkSession.builder \
        .appName("Data Aggregation") \
        .getOrCreate()

    df = spark.read.option("header", "true").csv(input_path, inferSchema=True)

    for col_name in df.columns:
        null_rows = df.filter(col(col_name).isNull())

        if null_rows.count() > 0:
            print(f"Null values detected in column: {col_name}")
            for row in null_rows.collect():
                record_row = row.asDict()
                
                record_id = error_capture_module.insert_record_error(process_file_id, record_row)
                print(f"Record ID after insert_record_error: {record_id}")

                error_description = "Null value encountered in non-nullable column"
                error_code = error_capture_module.get_error_code(error_description)
                print(f"Error code for '{error_description}': {error_code}")
                
                if error_code is not None and record_id is not None:
                    error_capture_module.insert_column_error(record_id, process_file_id, col_name, error_code)
                else:
                    print(f"Error in retrieving record ID or error code for description: {error_description}")

    df = df.withColumn(
        "total_financials",
        col("compensation") +
        col("reimbursed_expenses") +
        col("expenses_less_than_75") +
        col("lobbying_expenses_for_non") +
        col("itemized_expenses")
    )

    df.write.option("header", "true").mode("overwrite").csv(output_path)
    spark.stop()
