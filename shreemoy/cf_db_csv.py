from pyspark.sql import SparkSession
import os

# Initialize Spark session with JDBC driver
spark = SparkSession.builder \
    .appName("Load_Dependent_Tables") \
    .config("spark.jars", "/Users/shreemoynanda/spark-jars/sqljdbc42.jar") \
    .master("local[3]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .getOrCreate()

# Directory path for the CSV file
csv_dir_path = "/Users/shreemoynanda/Desktop/Observability-Framework-Design/dataset_for_loading/"
table_name = "Vehicle_Transactions_Facts"  # Table name
csv_file_path = os.path.join(csv_dir_path, f"{table_name}.csv")  # Construct full file path
schema_name = "ev_registration_activity"  # Schema name

# JDBC database connection properties
jdbc_url = "jdbc:sqlserver://damg7374adg.database.windows.net:1433;databaseName=EVRTA"
connection_properties = {
    "user": "sn_admin",
    "password": "jejbe4-cutZym-nivvid",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Ensure the CSV file exists
if not os.path.exists(csv_file_path):
    raise FileNotFoundError(f"CSV file not found at: {csv_file_path}")

print(f"Loading CSV file: {csv_file_path}")
csv_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
csv_df = csv_df.toDF(*[c.strip().lower().replace(" ", "_") for c in csv_df.columns])  # Normalize column names
print(f"CSV Record Count: {csv_df.count()}")

# Schema name for the database table
schema_name = "ev_registration_activity"
fully_qualified_table_name = f"{schema_name}.{table_name}"  # Combine schema and table name

print(f"Loading table from database: {fully_qualified_table_name}")
db_df = spark.read.jdbc(url=jdbc_url, table=fully_qualified_table_name, properties=connection_properties)
print(f"DB Record Count: {db_df.count()}")

# Identify the columns to compare
common_columns = ["vehicle_id", "transaction_date"]

# Find records in DB but not in CSV
extra_in_db = db_df.join(csv_df, on=common_columns, how="left_anti")
print(f"Extra Records in Database: {extra_in_db.count()}")
extra_in_db.show(truncate=False)

# Find records in CSV but not in DB
extra_in_csv = csv_df.join(db_df, on=common_columns, how="left_anti")
print(f"Extra Records in CSV: {extra_in_csv.count()}")
extra_in_csv.show(truncate=False)
