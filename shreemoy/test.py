import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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

# Load the latest CSV file from the flat file directory
csv_files = [f for f in os.listdir(flat_file_dir) if f.endswith('.csv')]
latest_csv_file = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(flat_file_dir, x)))
file_path = os.path.join(flat_file_dir, latest_csv_file)

# Load the data into a Spark DataFrame
df = spark.read.option("header", True).csv(file_path)

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
