import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, lower, when
from pyspark.sql.types import IntegerType
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Azure SQL Database connection parameters
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# Set up the database connection string
connection_string = f"mssql+pyodbc://{db_user}:{db_password}@{db_host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_string)

# Start Spark session
spark = SparkSession.builder.appName("VehicleTransactionData").getOrCreate()

# Load the latest CSV file from the flat file directory
flat_file_dir = os.getenv("FLAT_FILE_DIR")
csv_files = [f for f in os.listdir(flat_file_dir) if f.endswith('.csv')]
latest_csv_file = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(flat_file_dir, x)))
file_path = os.path.join(flat_file_dir, latest_csv_file)

# Load dataset from CSV file
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Clean and format Transaction Date and Sale Date
df_cleaned = df.withColumn("Transaction Date", trim(col("Transaction Date"))) \
               .withColumn("Sale Date", trim(col("Sale Date"))) \
               .withColumn(
                   "transaction_date_formatted",
                   to_date(col("Transaction Date"), "MMMM dd yyyy")
               ).withColumn(
                   "sale_date_formatted",
                   to_date(col("Sale Date"), "MMMM dd yyyy")
               ) \
               .withColumnRenamed("DOL Vehicle ID", "dol_vehicle_id") \
               .withColumnRenamed("City", "city") \
               .withColumnRenamed("State", "state") \
               .withColumnRenamed("Electric Vehicle Fee Paid", "electric_vehicle_fee_paid") \
               .withColumnRenamed("Transportation Electrification Fee Paid", "transportation_electrification_fee_paid") \
               .withColumnRenamed("Hybrid Vehicle Electrification Fee Paid", "hybrid_vehicle_fee_paid") \
               .withColumnRenamed("Transaction Type", "transaction_type") \
               .withColumnRenamed("Base MSRP", "base_msrp") \
               .withColumnRenamed("Sale Price", "sale_price") \
               .withColumnRenamed("Odometer Reading", "odometer_reading") \
               .withColumnRenamed("Electric Range", "electric_range")

# Cast numeric columns to appropriate data types
df_cleaned = df_cleaned.withColumn("base_msrp", col("base_msrp").cast(IntegerType())) \
                       .withColumn("sale_price", col("sale_price").cast(IntegerType())) \
                       .withColumn("odometer_reading", col("odometer_reading").cast(IntegerType())) \
                       .withColumn("electric_range", col("electric_range").cast(IntegerType()))

# Normalize fee columns - handle null, 'Not Applicable', 'No' and other non-'Yes' values
df_cleaned = df_cleaned.withColumn("electric_vehicle_fee_paid",
                                   when(lower(trim(col("electric_vehicle_fee_paid"))) == "yes", "Yes")
                                   .otherwise("No")) \
                       .withColumn("transportation_electrification_fee_paid",
                                   when(lower(trim(col("transportation_electrification_fee_paid"))) == "yes", "Yes")
                                   .otherwise("No")) \
                       .withColumn("hybrid_vehicle_fee_paid",
                                   when(lower(trim(col("hybrid_vehicle_fee_paid"))) == "yes", "Yes")
                                   .otherwise("No"))

# Load necessary dimension tables from the database
with engine.connect() as conn:
    vehicle_dim_existing = pd.read_sql(text("SELECT vehicle_id, dol_vehicle_id FROM ev_registration_activity.Vehicle_Dim"), conn)
    location_dim_existing = pd.read_sql(text("SELECT location_id, city_id, state_id FROM ev_registration_activity.Location_Dim"), conn)
    fee_dim_existing = pd.read_sql(text("SELECT fee_id, electric_vehicle_fee, transportation_electrification_fee, hybrid_vehicle_fee FROM ev_registration_activity.Fee_Dim"), conn)
    city_dim_existing = pd.read_sql(text("SELECT city_id, city FROM ev_registration_activity.City_Dim"), conn)
    state_dim_existing = pd.read_sql(text("SELECT state_id, state FROM ev_registration_activity.State_Dim"), conn)

# Convert Pandas DataFrames to Spark DataFrames
vehicle_dim = spark.createDataFrame(vehicle_dim_existing)
location_dim = spark.createDataFrame(location_dim_existing)
fee_dim = spark.createDataFrame(fee_dim_existing)
city_dim = spark.createDataFrame(city_dim_existing)
state_dim = spark.createDataFrame(state_dim_existing)

# Trim fee_dim columns to match cleaned dataset columns
fee_dim = fee_dim.withColumn("electric_vehicle_fee", trim(col("electric_vehicle_fee"))) \
                 .withColumn("transportation_electrification_fee", trim(col("transportation_electrification_fee"))) \
                 .withColumn("hybrid_vehicle_fee", trim(col("hybrid_vehicle_fee")))

# Join city and state dimensions to match with the location dimension for the cleaned data
location_stage_df = df_cleaned \
    .join(city_dim, df_cleaned["city"] == city_dim["city"], "left") \
    .join(state_dim, df_cleaned["state"] == state_dim["state"], "left")

# Join the cleaned DataFrame with the location_dim DataFrame to create the final location dimension DataFrame
location_dim_df = location_stage_df.join(location_dim,
                                         (location_stage_df["city_id"] == location_dim["city_id"]) &
                                         (location_stage_df["state_id"] == location_dim["state_id"]),
                                         "left") \
    .select(
        col("dol_vehicle_id"),
        col("transaction_date_formatted").alias("transaction_date"),
        col("sale_date_formatted").alias("sale_date"),
        col("location_id"),
        col("transaction_type"),
        col("base_msrp"),
        col("sale_price"),
        col("odometer_reading"),
        col("electric_range"),
        col("electric_vehicle_fee_paid"),
        col("transportation_electrification_fee_paid"),
        col("hybrid_vehicle_fee_paid")
    )
# Normalize fee columns in fee_dim to lowercase
fee_dim = fee_dim.withColumn("electric_vehicle_fee", lower(trim(col("electric_vehicle_fee")))) \
                 .withColumn("transportation_electrification_fee", lower(trim(col("transportation_electrification_fee")))) \
                 .withColumn("hybrid_vehicle_fee", lower(trim(col("hybrid_vehicle_fee"))))

# Normalize fee columns in location_dim_df to lowercase
location_dim_df = location_dim_df.withColumn("electric_vehicle_fee_paid", lower(trim(col("electric_vehicle_fee_paid")))) \
                                 .withColumn("transportation_electrification_fee_paid", lower(trim(col("transportation_electrification_fee_paid")))) \
                                 .withColumn("hybrid_vehicle_fee_paid", lower(trim(col("hybrid_vehicle_fee_paid"))))

# Debugging: Show the columns and a few rows of fee_dim to understand the values being joined on after normalization
print("Normalized Fee Dimension DataFrame columns and sample rows:")
fee_dim.show(5)

# Debugging: Show the columns and a few rows of location_dim_df to understand the fee column values after normalization
print("Normalized Location Dimension DataFrame columns and sample rows:")
location_dim_df.select(
    "electric_vehicle_fee_paid",
    "transportation_electrification_fee_paid",
    "hybrid_vehicle_fee_paid"
).show(5)

# Join the transaction data with the dimensions
vehicle_transactions_facts_df = location_dim_df \
    .join(vehicle_dim, location_dim_df["dol_vehicle_id"] == vehicle_dim["dol_vehicle_id"], "left") \
    .join(fee_dim, (location_dim_df["electric_vehicle_fee_paid"] == fee_dim["electric_vehicle_fee"]) &
                   (location_dim_df["transportation_electrification_fee_paid"] == fee_dim["transportation_electrification_fee"]) &
                   (location_dim_df["hybrid_vehicle_fee_paid"] == fee_dim["hybrid_vehicle_fee"]), "left") \
    .select(
        col("vehicle_id"),
        col("transaction_date"),
        col("sale_date"),
        col("location_id"),
        col("transaction_type"),
        col("base_msrp"),
        col("sale_price"),
        col("odometer_reading"),
        col("electric_range"),
        col("fee_id")
    )

# Debugging: Show the resulting DataFrame after the join to see how fee_id is being matched
print("Vehicle Transactions Facts DataFrame after joining with fee_dim:")
vehicle_transactions_facts_df.show(5)


# Filter out records without a valid `location_id` or `vehicle_id`
vehicle_transactions_facts_df = vehicle_transactions_facts_df.filter(col("location_id").isNotNull() & col("vehicle_id").isNotNull())

# Convert Spark DataFrame to Pandas for final insertion
vehicle_transactions_facts_pd = vehicle_transactions_facts_df.toPandas()

# Debugging: Show the resulting DataFrame before filtering duplicates
print("Vehicle Transactions Facts DataFrame before filtering duplicates:")
print(vehicle_transactions_facts_pd)

# Insert data into Vehicle_Transactions_Facts table
try:
    with engine.connect() as conn:
        # Fetch existing values to prevent duplicates
        existing_values = pd.read_sql(text("SELECT vehicle_id, transaction_date, sale_date FROM ev_registration_activity.Vehicle_Transactions_Facts"), conn)

    # Standardize columns for consistency
    existing_values.columns = [col.lower() for col in existing_values.columns]
    vehicle_transactions_facts_pd.columns = [col.lower() for col in vehicle_transactions_facts_pd.columns]

    # Debugging: Show existing values from the database
    print("Existing values from the Vehicle_Transactions_Facts table:")
    print(existing_values)

    # Filter out the records that already exist in the database
    vehicle_transactions_facts_unique = vehicle_transactions_facts_pd.merge(existing_values, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)

    # Debugging: Show the filtered unique records
    print("New unique Vehicle Transactions Facts DataFrame to insert:")
    print(vehicle_transactions_facts_unique)

    # Insert the new unique records
    if not vehicle_transactions_facts_unique.empty:
        vehicle_transactions_facts_unique.to_sql("Vehicle_Transactions_Facts", schema="ev_registration_activity", con=engine, if_exists="append", index=False)
        print("Data inserted successfully into Vehicle_Transactions_Facts table.")
    else:
        print("No new data to insert into Vehicle_Transactions_Facts table.")
except Exception as e:
    print(f"Error reading or inserting data into Vehicle_Transactions_Facts: {e}")

print("Data processing completed for Vehicle_Transactions_Facts.")
