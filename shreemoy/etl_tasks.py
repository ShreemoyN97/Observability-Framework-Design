from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date, max, when, month, dayofweek, lower
import error_capture_module
import os
from dotenv import load_dotenv

load_dotenv()

def perform_etl(file_category_name, input_file_path, process_file_id):
    if file_category_name == "EVTRA":
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("ETL with Validation, Transformations, and Logging") \
            .getOrCreate()
        
        # Load the dataset
        df = spark.read.option("header", "true").csv(input_file_path, inferSchema=True)
        print(f"Initial record count: {df.count()}")

        # Step 1: Remove duplicates
        duplicate_count = df.count() - df.dropDuplicates().count()
        if duplicate_count > 0:
            record_id = error_capture_module.insert_record_error(
                process_file_id, {"error": "Duplicate records found"}
            )
            error_description = "Duplicate record detected"
            error_capture_module.insert_column_error(record_id, process_file_id, "N/A", error_capture_module.get_error_code(error_description))
        df = df.dropDuplicates()
        print(f"Record count after removing duplicates: {df.count()}")

        # Step 2: Validate Transaction Date and Sale Date
        if "Transaction Date" in df.columns and "Sale Date" in df.columns:
            invalid_date_rows = df.filter((to_date(col("Transaction Date"), "MMMM dd yyyy") > current_date()) |
                                          (to_date(col("Sale Date"), "MMMM dd yyyy") > current_date()))
            if invalid_date_rows.count() > 0:
                for row in invalid_date_rows.collect():
                    record_id = error_capture_module.insert_record_error(process_file_id, row.asDict())
                    error_description = "Invalid date format or future date"
                    error_capture_module.insert_column_error(record_id, process_file_id, "Transaction Date / Sale Date", error_capture_module.get_error_code(error_description))
                df = df.subtract(invalid_date_rows)
                print(f"Invalid date rows removed: {invalid_date_rows.count()}")

        # Step 3: Check for null values in mandatory columns
        mandatory_columns = ["County", "City", "State", "Postal Code", "Legislative District", "Model"]
        for col_name in mandatory_columns:
            if col_name in df.columns:
                null_rows = df.filter(col(col_name).isNull())
                if null_rows.count() > 0:
                    for row in null_rows.collect():
                        record_id = error_capture_module.insert_record_error(process_file_id, row.asDict())
                        error_description = "Missing required field"
                        error_capture_module.insert_column_error(record_id, process_file_id, col_name, error_capture_module.get_error_code(error_description))
                    df = df.subtract(null_rows)
                    print(f"Rows removed for null values in '{col_name}': {null_rows.count()}")
        
        # Final record count after all validations
        print(f"Record count after all validations: {df.count()}")

        # Transformation 1: Update Electric Range where it is 0
        if "Electric Range" in df.columns:
            max_range_df = df.groupBy("Model", "Make").agg(max("Electric Range").alias("Max_Electric_Range"))
            df = df.join(max_range_df, ["Model", "Make"], "left")
            df = df.withColumn(
                "Electric Range",
                when((col("Electric Range") == 0) & col("Max_Electric_Range").isNotNull(), col("Max_Electric_Range"))
                .otherwise(col("Electric Range"))
            ).drop("Max_Electric_Range")

        # Transformation 2: Correct Model Year if greater than 2025
        if "Model Year" in df.columns and "Year" in df.columns:
            df = df.withColumn(
                "Model Year",
                when(col("Model Year") > 2025, col("Year")).otherwise(col("Model Year"))
            )

        # Transformation 3: Replace missing Sale Date with Transaction Date for specific Transaction Types
        if "Transaction Type" in df.columns and "Sale Date" in df.columns and "Transaction Date" in df.columns:
            df = df.withColumn(
                "Sale Date",
                when(
                    col("Transaction Type").isin("Original Title", "Original Registration", "Title Transfer") & col("Sale Date").isNull(),
                    col("Transaction Date")
                ).otherwise(col("Sale Date"))
            )
        
        # Transformation 4: Extract Month and Day of the Week from Transaction Date
        if "Transaction Date" in df.columns:
            df = df.withColumn("Transaction Month", month(to_date(col("Transaction Date"), "MMMM dd yyyy")))
            df = df.withColumn("Transaction Day of Week", dayofweek(to_date(col("Transaction Date"), "MMMM dd yyyy")))

        # Transformation 5: Standardize Electric Vehicle Fee columns
        fee_columns = ["Electric Vehicle Fee Paid", "Transportation Electrification Fee Paid", "Hybrid Vehicle Electrification Fee Paid"]
        for fee_col in fee_columns:
            if fee_col in df.columns:
                df = df.withColumn(
                    fee_col,
                    when(lower(col(fee_col)).isNull() | lower(col(fee_col)).isin("not applicable", "no"), "no")
                    .when(lower(col(fee_col)).isin("yes"), "yes")
                    .otherwise(lower(col(fee_col)))
                )

        # Save the processed data to output
        output_file_path = os.getenv("OUTPUT_FILE_PATH")
        df.coalesce(1).write.csv(output_file_path, mode="overwrite", header=True)
        print(f"Processed data saved to: {output_file_path}")

        # Stop Spark session
        spark.stop()
