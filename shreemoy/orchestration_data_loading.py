from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower, when
from pyspark.sql.types import IntegerType, DateType

# Step 1: Initialize Spark Session
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load_Dependent_Tables") \
    .master("local[3]") \
    .config("spark.jars", "/Users/shreemoynanda/spark-jars/sqljdbc42.jar") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .config("spark.cleaner.referenceTracking.blocking", "true") \
    .config("spark.storage.cleanupFilesAfterExecutorExit", "false") \
    .getOrCreate()

# Print configurations for confirmation
print(f"Master: {spark.sparkContext.master}")
print(f"Driver Memory: {spark.conf.get('spark.driver.memory')}")
print(f"Executor Memory: {spark.conf.get('spark.executor.memory')}")


# Step 2: Azure SQL Database Connection Parameters
jdbc_url = "jdbc:sqlserver://damg7374adg.database.windows.net:1433;databaseName=EVRTA"
connection_properties = {
    "user": "sn_admin",
    "password": "jejbe4-cutZym-nivvid",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Step 3: Define File Paths
TMP_FILE_DIR = "/Users/shreemoynanda/Desktop/Observability-Framework-Design/dataset_for_loading"

# Step 4: List of Independent Tables
independent_tables = {
    "Make_Dim.csv": ("ev_registration_activity.Make_Dim", ["make"]),
    "Model_Dim.csv": ("ev_registration_activity.Model_Dim", ["model"]),
    "Primary_Use_Dim.csv": ("ev_registration_activity.Primary_Use_Dim", ["primary_use"]),
    "Electric_Utility_Dim.csv": ("ev_registration_activity.Electric_Utility_Dim", ["electric_utility_name"]),
    "State_Dim.csv": ("ev_registration_activity.State_Dim", ["state"]),
    "Transaction_Type_Dim.csv": ("ev_registration_activity.Transaction_Type_Dim", ["transaction_type"]),
    "Fee_Dim.csv": ("ev_registration_activity.Fee_Dim", ["electric_vehicle_fee", "transportation_electrification_fee", "hybrid_vehicle_fee"])
}

# Step 5: Function to Load Independent Tables
def load_independent_table(file_path, table_name, unique_columns):
    try:
        # Read CSV into DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"Loaded {file_path} into DataFrame with {df.count()} records.")
        
        # Trim and normalize column names
        df = df.select([col(c).alias(c.strip().lower()) for c in df.columns])

        # Read existing data from SQL table
        try:
            db_table_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print(f"Loaded existing {table_name} from SQL with {db_table_df.count()} records.")
        except Exception as e:
            print(f"Table {table_name} not found or is empty: {e}")
            db_table_df = None

        # Filter out duplicates
        if db_table_df:
            df = df.join(db_table_df, on=unique_columns, how="left_anti")

        # Write to SQL table
        df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
        print(f"Inserted {df.count()} new records into {table_name}.")
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# Step 6: Function to Load City_Dim Table
def load_city_dim(file_path, table_name):
    try:
        # Read City_Dim CSV into DataFrame
        city_df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"Loaded {file_path} into DataFrame with {city_df.count()} records.")

        # Trim and normalize column names
        city_df = city_df.select([col(c).alias(c.strip().lower()) for c in city_df.columns])

        # Load State_Dim to map state_id
        state_table = "ev_registration_activity.State_Dim"
        state_df = spark.read.jdbc(url=jdbc_url, table=state_table, properties=connection_properties)

        # Join to get state_id
        city_df = city_df.join(state_df, city_df.state == state_df.state, "left").select(
            city_df.city, state_df.state_id
        )

        # Ensure all rows have valid state_id
        missing_state = city_df.filter(col("state_id").isNull())
        if missing_state.count() > 0:
            print(f"Rows with missing state_id:\n{missing_state.show()}")
            raise ValueError("Some cities in City_Dim.csv do not have a matching state in State_Dim.")

        # Load existing City_Dim data
        try:
            db_city_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print(f"Loaded existing {table_name} from SQL with {db_city_df.count()} records.")
        except Exception as e:
            print(f"Table {table_name} not found or is empty: {e}")
            db_city_df = None

        # Filter out duplicates
        if db_city_df:
            city_df = city_df.join(db_city_df, on=["city", "state_id"], how="left_anti")

        # Write to SQL table
        city_df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
        print(f"Inserted {city_df.count()} new records into {table_name}.")
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# Step 7: Function to Load Location_Dim Table
def load_location_dim(file_path, table_name):
    try:
        # Read Location_Dim CSV into DataFrame
        location_df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"Loaded {file_path} into DataFrame with {location_df.count()} records.")

        # Trim and normalize column names
        location_df = location_df.select([col(c).alias(c.strip().lower()) for c in location_df.columns])

        # Load City_Dim and State_Dim to map city_id and state_id
        city_table = "ev_registration_activity.City_Dim"
        state_table = "ev_registration_activity.State_Dim"

        city_df = spark.read.jdbc(url=jdbc_url, table=city_table, properties=connection_properties)
        state_df = spark.read.jdbc(url=jdbc_url, table=state_table, properties=connection_properties)

        # Join to map city_id and state_id
        location_df = location_df \
            .join(city_df, location_df.city == city_df.city, "left") \
            .join(state_df, location_df.state == state_df.state, "left") \
            .select(location_df.county, city_df.city_id, state_df.state_id)

        # Ensure all rows have valid city_id and state_id
        missing_mappings = location_df.filter(col("city_id").isNull() | col("state_id").isNull())
        if missing_mappings.count() > 0:
            print(f"Rows with missing city_id or state_id:\n{missing_mappings.show()}")
            raise ValueError("Some rows in Location_Dim.csv have unmatched city or state.")

        # Load existing Location_Dim data
        try:
            db_location_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print(f"Loaded existing {table_name} from SQL with {db_location_df.count()} records.")
        except Exception as e:
            print(f"Table {table_name} not found or is empty: {e}")
            db_location_df = None

        # Filter out duplicates
        if db_location_df:
            location_df = location_df.join(db_location_df, on=["county", "city_id", "state_id"], how="left_anti")

        # Write to SQL table
        location_df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
        print(f"Inserted {location_df.count()} new records into {table_name}.")
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# Step 8: Function to Load Vehicle_Dim Table
def load_vehicle_dim(file_path, table_name):
    try:
        # Step 1: Load Vehicle_Dim CSV into DataFrame
        vehicle_csv_df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"Loaded {file_path} into DataFrame with {vehicle_csv_df.count()} records.")
        print(f"Columns in Vehicle_Dim CSV: {vehicle_csv_df.columns}")

        # Step 2: Normalize column names
        vehicle_csv_df = vehicle_csv_df.toDF(*[c.strip().lower().replace(" ", "_") for c in vehicle_csv_df.columns])
        print(f"Normalized Vehicle_Dim CSV columns: {vehicle_csv_df.columns}")

        # Step 3: Load dependent dimensions
        make_df = spark.read.jdbc(
            url=jdbc_url, table="ev_registration_activity.Make_Dim", properties=connection_properties
        )
        model_df = spark.read.jdbc(
            url=jdbc_url, table="ev_registration_activity.Model_Dim", properties=connection_properties
        )
        primary_use_df = spark.read.jdbc(
            url=jdbc_url, table="ev_registration_activity.Primary_Use_Dim", properties=connection_properties
        )
        electric_utility_df = spark.read.jdbc(
            url=jdbc_url, table="ev_registration_activity.Electric_Utility_Dim", properties=connection_properties
        )

        # Step 4: Map foreign key IDs
        vehicle_csv_df = vehicle_csv_df \
            .join(make_df, vehicle_csv_df["make"] == make_df["make"], "left") \
            .withColumnRenamed("make_id", "make_id") \
            .drop("make") \
            .join(model_df, vehicle_csv_df["model"] == model_df["model"], "left") \
            .withColumnRenamed("model_id", "model_id") \
            .drop("model") \
            .join(primary_use_df, vehicle_csv_df["primary_use"] == primary_use_df["primary_use"], "left") \
            .withColumnRenamed("primary_use_id", "primary_use_id") \
            .drop("primary_use") \
            .join(electric_utility_df, vehicle_csv_df["electric_utility"] == electric_utility_df["electric_utility_name"], "left") \
            .withColumnRenamed("electric_utility_id", "electric_utility_id") \
            .drop("electric_utility", "electric_utility_name")

        # Step 5: Ensure all rows have valid IDs
        missing_ids = vehicle_csv_df.filter(
            col("make_id").isNull() | col("model_id").isNull() | 
            col("primary_use_id").isNull() | col("electric_utility_id").isNull()
        )
        if missing_ids.count() > 0:
            print("Rows with missing IDs:")
            missing_ids.show(truncate=False)
            raise ValueError("Some rows in Vehicle_Dim.csv do not have matching foreign keys.")

        # Step 6: Deduplicate within the CSV
        vehicle_csv_df = vehicle_csv_df.dropDuplicates(["dol_vehicle_id"])

        # Step 7: Load existing data from the SQL table
        try:
            vehicle_db_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print(f"Loaded existing {table_name} from SQL with {vehicle_db_df.count()} records.")

            # Step 8: Filter out rows already in the database
            vehicle_csv_df = vehicle_csv_df.join(
                vehicle_db_df.select("dol_vehicle_id"), 
                on="dol_vehicle_id", 
                how="left_anti"
            )
        except Exception as e:
            print(f"No existing records found in {table_name}, all rows will be inserted. {e}")

        # Step 9: Show a sample of the data to be inserted
        print("Sample data to be inserted into Vehicle_Dim:")
        vehicle_csv_df.show(5)

        # Step 10: Write to the database
        inserted_count = vehicle_csv_df.count()
        vehicle_csv_df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
        print(f"Inserted {inserted_count} new records into {table_name}.")
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# Step 10: Function to Load Vehicle_Dim Table
def load_vehicle_transactions_facts_spark(file_path, table_name):
    try:
        # Load the CSV file into a DataFrame
        vehicle_transactions_df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"Loaded {file_path} into DataFrame with {vehicle_transactions_df.count()} records.")

        # Normalize column names
        vehicle_transactions_df = vehicle_transactions_df.toDF(*[
            c.strip().lower().replace(" ", "_") for c in vehicle_transactions_df.columns
        ])

        # Alias the vehicle_id column to avoid ambiguity during joins
        vehicle_transactions_df = vehicle_transactions_df.withColumnRenamed("vehicle_id", "source_vehicle_id")

        # Load dependent dimensions
        vehicle_dim_df = spark.read.jdbc(url=jdbc_url, table="ev_registration_activity.Vehicle_Dim", properties=connection_properties)
        city_dim_df = spark.read.jdbc(url=jdbc_url, table="ev_registration_activity.City_Dim", properties=connection_properties)
        location_dim_df = spark.read.jdbc(url=jdbc_url, table="ev_registration_activity.Location_Dim", properties=connection_properties)
        transaction_type_dim_df = spark.read.jdbc(url=jdbc_url, table="ev_registration_activity.Transaction_Type_Dim", properties=connection_properties)
        fee_dim_df = spark.read.jdbc(url=jdbc_url, table="ev_registration_activity.Fee_Dim", properties=connection_properties)

        # Map Vehicle IDs
        vehicle_transactions_df = vehicle_transactions_df.join(
            vehicle_dim_df.select("vehicle_id", "dol_vehicle_id"),
            vehicle_transactions_df["source_vehicle_id"] == vehicle_dim_df["dol_vehicle_id"],
            "left"
        ).drop("dol_vehicle_id").withColumnRenamed("vehicle_id", "mapped_vehicle_id")

        # Map City IDs
        vehicle_transactions_df = vehicle_transactions_df.join(
            city_dim_df.select("city_id", "city"),
            vehicle_transactions_df["city"] == city_dim_df["city"],
            "left"
        ).drop("city")

        # Map Location IDs
        vehicle_transactions_df = vehicle_transactions_df.join(
            location_dim_df.select("location_id", "city_id"),
            vehicle_transactions_df["city_id"] == location_dim_df["city_id"],
            "left"
        ).drop("city_id")

        # Map Transaction Type IDs
        vehicle_transactions_df = vehicle_transactions_df.join(
            transaction_type_dim_df.select("transaction_type_id", "transaction_type"),
            vehicle_transactions_df["transaction_type"] == transaction_type_dim_df["transaction_type"],
            "left"
        ).drop("transaction_type")

        # Map Fee IDs
        vehicle_transactions_df = vehicle_transactions_df.join(
            fee_dim_df.select("fee_id", "electric_vehicle_fee", "transportation_electrification_fee", "hybrid_vehicle_fee"),
            (vehicle_transactions_df["electric_vehicle_fee"] == fee_dim_df["electric_vehicle_fee"]) &
            (vehicle_transactions_df["transportation_electrification_fee"] == fee_dim_df["transportation_electrification_fee"]) &
            (vehicle_transactions_df["hybrid_vehicle_fee"] == fee_dim_df["hybrid_vehicle_fee"]),
            "left"
        ).drop("electric_vehicle_fee", "transportation_electrification_fee", "hybrid_vehicle_fee")

        # Select only required columns
        vehicle_transactions_df = vehicle_transactions_df.select(
            vehicle_transactions_df["mapped_vehicle_id"].alias("vehicle_id"),
            "transaction_date", "sale_date", "location_id", "transaction_type_id",
            "base_msrp", "sale_price", "odometer_reading", "electric_range", "fee_id"
        )

        # Filter duplicates
        try:
            existing_transactions_df = spark.read.jdbc(
                url=jdbc_url, table=table_name, properties=connection_properties
            )
            vehicle_transactions_df = vehicle_transactions_df.join(
                existing_transactions_df.select("vehicle_id", "transaction_date"), 
                on=["vehicle_id", "transaction_date"], 
                how="left_anti"
            )
        except Exception:
            print(f"No existing records found in {table_name}, all rows will be inserted.")

        # Show data to be inserted
        print("Sample data to be inserted into Vehicle_Transactions_Facts:")
        vehicle_transactions_df.show(5)

        # Write to the table
        vehicle_transactions_df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
        print(f"Inserted {vehicle_transactions_df.count()} new records into {table_name}.")
    except Exception as e:
        print(f"Error processing {table_name}: {e}")


# Step 11: Load Tables (Independent)
for file_name, (table_name, unique_columns) in independent_tables.items():
    file_path = f"{TMP_FILE_DIR}/{file_name}"
    print(f"Processing {file_name}...")
    load_independent_table(file_path, table_name, unique_columns)
    

# Step 12: Process Dependent Tables
# Process City_Dim
print("Processing City_Dim.csv...")
city_file_path = f"{TMP_FILE_DIR}/City_Dim.csv"
load_city_dim(city_file_path, "ev_registration_activity.City_Dim")

# Process Location_Dim
print("Processing Location_Dim.csv...")
location_file_path = f"{TMP_FILE_DIR}/Location_Dim.csv"
load_location_dim(location_file_path, "ev_registration_activity.Location_Dim")

# Process Vehicle_Dim
print("Processing Vehicle_Dim.csv...")
vehicle_file_path = f"{TMP_FILE_DIR}/Vehicle_Dim.csv"
load_vehicle_dim(vehicle_file_path, "ev_registration_activity.Vehicle_Dim")

# Process Vehicle_Dim
print("Processing Vehicle_Transaction_Facts.csv...")
vehicle_transact_file_path = f"{TMP_FILE_DIR}/Vehicle_Transactions_Facts.csv"
load_vehicle_transactions_facts_spark(vehicle_transact_file_path, "ev_registration_activity.Vehicle_Transactions_Facts")
# Ensure spark.stop() is called after all operations
spark.stop()
