import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, quarter, month, dayofweek, when, lower, row_number, trim, to_date
from pyspark.sql.types import IntegerType
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pyspark.sql import Window

# Load environment variables from .env file
load_dotenv()

# Azure SQL Database connection parameters
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# Directory Path Containing Flat Files
flat_file_dir = os.getenv("FLAT_FILE_DIR")

# Set up the database connection string
connection_string = f"mssql+pyodbc://{db_user}:{db_password}@{db_host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_string)

# Set up Spark session
spark = SparkSession.builder.appName("EVRegistrationData").getOrCreate()

# Function to insert only unique data into the specified table
def insert_unique_data(df, table_name, unique_columns, engine, schema="ev_registration_activity"):
    # Convert Spark DataFrame to Pandas DataFrame
    df_pd = df.toPandas()

    # Standardize column names to lowercase to avoid mismatches
    df_pd.columns = [col.lower() for col in df_pd.columns]
    
    # Fetch existing values from the database
    try:
        with engine.connect() as conn:
            existing_values = pd.read_sql(text(f"SELECT {', '.join(unique_columns)} FROM {schema}.{table_name}"), conn)
    except Exception as e:
        print(f"Error reading existing values from {table_name}: {e}")
        return

    # Standardize existing values column names to lowercase to avoid mismatches
    existing_values.columns = [col.lower() for col in existing_values.columns]

    # Filter out records that already exist in the database
    condition = ~df_pd[unique_columns[0]].isin(existing_values[unique_columns[0]])
    for col_name in unique_columns[1:]:
        condition &= ~df_pd[col_name].isin(existing_values[col_name])
    df_pd_unique = df_pd[condition]

    # Insert the unique records into the table
    if not df_pd_unique.empty:
        try:
            df_pd_unique.to_sql(table_name, schema=schema, con=engine, if_exists="append", index=False)
            print(f"Data inserted successfully into {table_name} table.")
        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
    else:
        print(f"No new data to insert into {table_name} table.")

# Load the latest CSV file from the flat file directory
csv_files = [f for f in os.listdir(flat_file_dir) if f.endswith('.csv')]
latest_csv_file = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(flat_file_dir, x)))
file_path = os.path.join(flat_file_dir, latest_csv_file)

# Load the data into a Spark DataFrame
df = spark.read.option("header", True).csv(file_path)

# ---------- INSERT INTO Make_Dim TABLE ----------
make_dim = df.select("Make").dropDuplicates().filter(col("Make").isNotNull())
insert_unique_data(make_dim, "Make_Dim", ["make"], engine)

# ---------- INSERT INTO Model_Dim TABLE ----------
model_dim = df.select("Model").dropDuplicates().filter(col("Model").isNotNull())
insert_unique_data(model_dim, "Model_Dim", ["model"], engine)

# ---------- INSERT INTO Primary_Use_Dim TABLE ----------
primary_use_dim = df.select("Primary Use").dropDuplicates().filter(col("Primary Use").isNotNull())
primary_use_dim = primary_use_dim.withColumnRenamed("Primary Use", "primary_use")
insert_unique_data(primary_use_dim, "Primary_Use_Dim", ["primary_use"], engine)

# ---------- INSERT INTO Electric_Utility_Dim TABLE ----------
electric_utility_dim = df.select("Electric Utility").dropDuplicates().filter(col("Electric Utility").isNotNull())
electric_utility_dim = electric_utility_dim.withColumnRenamed("Electric Utility", "electric_utility_name")
insert_unique_data(electric_utility_dim, "Electric_Utility_Dim", ["electric_utility_name"], engine)

# ---------- INSERT INTO State_Dim TABLE ----------
state_dim = df.select("State").dropDuplicates().filter(col("State").isNotNull())
insert_unique_data(state_dim, "State_Dim", ["state"], engine)

# ---------- INSERT INTO Time_Dim TABLE ----------
start_date = "2010-01-01"
end_date = "2025-12-31"
date_range = pd.date_range(start=start_date, end=end_date)
date_df = pd.DataFrame(date_range, columns=["full_date"])

time_dim = spark.createDataFrame(date_df)
time_dim = time_dim.withColumn("year", year(col("full_date"))) \
                   .withColumn("quarter", quarter(col("full_date"))) \
                   .withColumn("month", month(col("full_date"))) \
                   .withColumn("day_of_week", dayofweek(col("full_date")))

# Use `when()` and `otherwise()` to replace day_of_week integers with day names
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

insert_unique_data(time_dim, "Time_Dim", ["full_date"], engine)

# ---------- INSERT INTO Transaction_Type_Dim TABLE ----------
transaction_type_dim = df.select("Transaction Type").dropDuplicates().filter(col("Transaction Type").isNotNull())
transaction_type_dim = transaction_type_dim.withColumnRenamed("Transaction Type", "transaction_type")
insert_unique_data(transaction_type_dim, "Transaction_Type_Dim", ["transaction_type"], engine)

unique_fees_combination_df = df.select(
    when(lower(col("Electric Vehicle Fee Paid")).isin("yes"), "yes").otherwise("no").alias("electric_vehicle_fee"),
    when(lower(col("Transportation Electrification Fee Paid")).isin("yes"), "yes").otherwise("no").alias("transportation_electrification_fee"),
    when(lower(col("Hybrid Vehicle Electrification Fee Paid")).isin("yes"), "yes").otherwise("no").alias("hybrid_vehicle_fee")
).dropDuplicates()

insert_unique_data(unique_fees_combination_df, "Fee_Dim", ["electric_vehicle_fee", "transportation_electrification_fee", "hybrid_vehicle_fee"], engine)

# ---------- INSERT INTO City_Dim TABLE ----------
city_dim = df.select("City", "State").dropDuplicates().filter(col("City").isNotNull() & col("State").isNotNull())
city_dim = city_dim.withColumnRenamed("City", "city").withColumnRenamed("State", "state")
city_dim_pd = city_dim.toPandas()

try:
    with engine.connect() as conn:
        state_dim_existing = pd.read_sql(text("SELECT state_id, state FROM ev_registration_activity.State_Dim"), conn)
        city_dim_existing = pd.read_sql(text("SELECT city, state_id FROM ev_registration_activity.City_Dim"), conn)

    city_dim_pd.columns = [col.lower() for col in city_dim_pd.columns]
    state_dim_existing.columns = [col.lower() for col in state_dim_existing.columns]
    city_dim_existing.columns = [col.lower() for col in city_dim_existing.columns]

    # Get `state_id` for each city-state combination
    city_dim_with_state_id = city_dim_pd.merge(state_dim_existing, how='left', on='state')
    city_dim_with_state_id = city_dim_with_state_id.dropna(subset=['state_id'])
    city_dim_unique = city_dim_with_state_id.merge(city_dim_existing, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not city_dim_unique.empty:
        city_dim_unique[['city', 'state_id']].to_sql("City_Dim", schema="ev_registration_activity", con=engine, if_exists="append", index=False)
        print("Data inserted successfully into City_Dim table.")
    else:
        print("No new data to insert into City_Dim table.")
except Exception as e:
    print(f"Error reading or inserting data into City_Dim: {e}")

print("Data processing completed for City_Dim.")

# ---------- INSERT INTO Vehicle_Dim TABLE ----------
vehicle_dim = df.select(
    "DOL Vehicle ID",
    "VIN (1-10)",
    "Model Year",
    "Make",
    "Model",
    "Primary Use",
    "Electric Utility"
).dropDuplicates().filter(
    col("DOL Vehicle ID").isNotNull() & 
    col("VIN (1-10)").isNotNull() & 
    col("Model Year").isNotNull() & 
    col("Make").isNotNull() & 
    col("Model").isNotNull()
)

vehicle_dim = vehicle_dim.withColumnRenamed("DOL Vehicle ID", "dol_vehicle_id") \
                         .withColumnRenamed("VIN (1-10)", "vin_prefix") \
                         .withColumnRenamed("Model Year", "model_year") \
                         .withColumnRenamed("Make", "make") \
                         .withColumnRenamed("Model", "model") \
                         .withColumnRenamed("Primary Use", "primary_use") \
                         .withColumnRenamed("Electric Utility", "electric_utility")

vehicle_dim_pd = vehicle_dim.toPandas()

try:
    with engine.connect() as conn:
        make_dim_existing = pd.read_sql(text("SELECT make_id, make FROM ev_registration_activity.Make_Dim"), conn)
        model_dim_existing = pd.read_sql(text("SELECT model_id, model FROM ev_registration_activity.Model_Dim"), conn)
        primary_use_dim_existing = pd.read_sql(text("SELECT primary_use_id, primary_use FROM ev_registration_activity.Primary_Use_Dim"), conn)
        electric_utility_dim_existing = pd.read_sql(text("SELECT electric_utility_id, electric_utility_name FROM ev_registration_activity.Electric_Utility_Dim"), conn)
        vehicle_dim_existing = pd.read_sql(text("SELECT dol_vehicle_id FROM ev_registration_activity.Vehicle_Dim"), conn)

    vehicle_dim_pd.columns = [col.lower() for col in vehicle_dim_pd.columns]
    make_dim_existing.columns = [col.lower() for col in make_dim_existing.columns]
    model_dim_existing.columns = [col.lower() for col in model_dim_existing.columns]
    primary_use_dim_existing.columns = [col.lower() for col in primary_use_dim_existing.columns]
    electric_utility_dim_existing.columns = [col.lower() for col in electric_utility_dim_existing.columns]
    vehicle_dim_existing.columns = [col.lower() for col in vehicle_dim_existing.columns]

    vehicle_dim_pd = vehicle_dim_pd.merge(make_dim_existing, how='left', on='make') \
                                   .merge(model_dim_existing, how='left', on='model') \
                                   .merge(primary_use_dim_existing, how='left', on='primary_use') \
                                   .merge(electric_utility_dim_existing, how='left', left_on='electric_utility', right_on='electric_utility_name')

    vehicle_dim_pd = vehicle_dim_pd.dropna(subset=['make_id', 'model_id', 'primary_use_id'])
    vehicle_dim_pd = vehicle_dim_pd.drop(columns=['make', 'model', 'primary_use', 'electric_utility', 'electric_utility_name'])
    vehicle_dim_unique = vehicle_dim_pd.merge(vehicle_dim_existing, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not vehicle_dim_unique.empty:
        vehicle_dim_unique.to_sql("Vehicle_Dim", schema="ev_registration_activity", con=engine, if_exists="append", index=False)
        print("Data inserted successfully into Vehicle_Dim table.")
    else:
        print("No new data to insert into Vehicle_Dim table.")
except Exception as e:
    print(f"Error reading or inserting data into Vehicle_Dim: {e}")

print("Data processing completed for Vehicle_Dim.")
# Load the City_Dim and State_Dim tables from SQL for joining
city_dim_spark = spark.createDataFrame(pd.read_sql("SELECT city_id, city FROM ev_registration_activity.City_Dim", con=engine))
state_dim_spark = spark.createDataFrame(pd.read_sql("SELECT state_id, state FROM ev_registration_activity.State_Dim", con=engine))

# Create Location_Dim DataFrame by joining City_Dim and State_Dim with the main DataFrame
location_dim = df.join(city_dim_spark.alias("city_dim"), df["City"] == col("city_dim.city"), "left") \
                 .join(state_dim_spark.alias("state_dim"), df["State"] == col("state_dim.state"), "left") \
                 .select(
                     col("city_dim.city_id").alias("city_id"),
                     col("state_dim.state_id").alias("state_id"),
                     col("County").alias("county")
                 ).dropDuplicates() \
                 .withColumn("location_id", row_number().over(Window.orderBy("city_id", "state_id")))

# Convert the Location DataFrame to Pandas DataFrame for insertion into SQL
location_dim_pd = location_dim.toPandas()

# Insert data into Location_Dim table in the SQL Database
try:
    # Fetch existing values from SQL to avoid duplicates
    with engine.connect() as conn:
        existing_location_values = pd.read_sql(text("SELECT city_id, state_id, county FROM ev_registration_activity.Location_Dim"), conn)
    
    # Standardize columns for consistency
    location_dim_pd.columns = [col.lower() for col in location_dim_pd.columns]
    existing_location_values.columns = [col.lower() for col in existing_location_values.columns]

    # Filter out records that already exist in the database (using city_id, state_id, and county as unique key)
    location_dim_unique = location_dim_pd.merge(existing_location_values, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)

    # Insert new unique records into Location_Dim
    if not location_dim_unique.empty:
        # Removing `location_id` column before insertion to avoid SQL error
        location_dim_unique = location_dim_unique.drop(columns=['location_id'])
        location_dim_unique.to_sql("Location_Dim", schema="ev_registration_activity", con=engine, if_exists="append", index=False)
        print("Data inserted successfully into Location_Dim table.")
    else:
        print("No new data to insert into Location_Dim table.")
except Exception as e:
    print(f"Error reading or inserting data into Location_Dim: {e}")

print("Data processing completed for Location_Dim.")
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

print("Data processing completed.")