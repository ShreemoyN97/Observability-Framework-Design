import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, quarter, month, dayofweek, when, date_format

def generate_time_dim_flat_file(start_date, end_date):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Time Dimension Loader") \
        .config("spark.jars", "/Users/shreemoynanda/spark-jars/sqljdbc42.jar") \
        .master("local[3]").config("spark.driver.memory", "2g").config("spark.executor.memory", "3g").getOrCreate()

    # Retrieve output directory from environment variables
    output_directory = os.getenv("FLAT_FILE_DIR")
    if not output_directory:
        raise ValueError("Environment variable 'FLAT_FILE_DIR' is not set. Please set it to a valid directory path.")

    # Generate date range
    date_range = spark.range(
        start=int(start_date.replace("-", "")),
        end=int(end_date.replace("-", "")) + 1
    ).selectExpr("to_date(cast(id as string), 'yyyyMMdd') as full_date")

    # Format full_date to 'YYYY-MM-DD'
    time_dim = date_range.withColumn("full_date", date_format(col("full_date"), "yyyy-MM-dd"))

    # Extract year, quarter, month, and day of the week
    time_dim = time_dim.withColumn("year", year(col("full_date"))) \
                       .withColumn("quarter", quarter(col("full_date"))) \
                       .withColumn("month", month(col("full_date"))) \
                       .withColumn("day_of_week", dayofweek(col("full_date")))

    # Replace day_of_week integers with day names
    time_dim = time_dim.withColumn(
        "day_of_week",
        when(col("day_of_week") == 1, "Sunday")
        .when(col("day_of_week") == 2, "Monday")
        .when(col("day_of_week") == 3, "Tuesday")
        .when(col("day_of_week") == 4, "Wednesday")
        .when(col("day_of_week") == 5, "Thursday")
        .when(col("day_of_week") == 6, "Friday")
        .when(col("day_of_week") == 7, "Saturday")
    )

    # Drop NULL values in full_date
    time_dim = time_dim.filter(col("full_date").isNotNull())

    # Define output file path
    output_file_path = os.path.join(output_directory, "Time_Dim.csv")

    # Save as a CSV file
    try:
        time_dim.coalesce(1).write.csv(output_file_path, header=True, mode="overwrite")
        print(f"Flat file generated at: {output_file_path}")
    except Exception as e:
        print(f"Error while saving the flat file: {e}")

    return os.path.join(output_file_path, "part-*.csv")  # Return the path to the CSV file(s)

def load_time_dim_to_db(csv_file_path, table_name):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Time Dimension Loader") \
        .config("spark.jars", "/path/to/mssql-jdbc-9.4.1.jre8.jar") \
        .getOrCreate()

    # Read database connection parameters from environment variables
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    if not all([db_host, db_name, db_user, db_password]):
        raise ValueError("Database connection parameters are not fully set in environment variables.")

    jdbc_url = f"jdbc:sqlserver://{db_host}:1433;databaseName={db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Read the CSV file into a DataFrame
    time_dim = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Convert 'full_date' back to DateType
    from pyspark.sql.types import DateType
    time_dim = time_dim.withColumn("full_date", col("full_date").cast(DateType()))

    # Insert data into the database table
    try:
        print(f"Inserting records into {table_name}...")
        time_dim.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
        print(f"Inserted records into {table_name}.")
    except Exception as e:
        print(f"Error while inserting data into {table_name}: {e}")

# Example Usage
start_date = "2010-01-01"
end_date = "2025-12-31"
table_name = "ev_registration_activity.Time_Dim"

# Generate the flat file
csv_file_path = generate_time_dim_flat_file(start_date, end_date)

# Load the flat file into the database
load_time_dim_to_db(csv_file_path, table_name)
