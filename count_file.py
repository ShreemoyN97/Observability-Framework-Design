from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Vehicle Transactions Count") \
    .getOrCreate()

# File path for the dataset
file_path = "/Users/shreemoynanda/Desktop/Observability-Framework-Design/dataset_for_loading/Vehicle_Transactions_Facts.csv"

# Read the dataset into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Count the number of records
current_count = df.count()
print(f"Current Record Count: {current_count}")

# .env file path
env_file_path = "/Users/shreemoynanda/Desktop/Observability-Framework-Design/.env"

# Function to read all environment variables from the .env file
def read_env_file(env_path):
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as file:
            for line in file:
                if line.strip() and not line.startswith("#"):  # Ignore comments and empty lines
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip()
    return env_vars

# Function to write environment variables back to the .env file
def write_env_file(env_path, env_vars):
    with open(env_path, "w") as file:
        for key, value in env_vars.items():
            file.write(f"{key}={value}\n")

# Read existing environment variables
env_vars = read_env_file(env_file_path)

# Get the previous count or default to 0
previous_count = int(env_vars.get("TOTAL_COUNT", 0))

# Calculate the new cumulative count
cumulative_count = previous_count + current_count
print(f"Cumulative Record Count: {cumulative_count}")

# Update the TOTAL_COUNT in the environment variables
env_vars["TOTAL_COUNT"] = str(cumulative_count)

# Write the updated environment variables back to the .env file
write_env_file(env_file_path, env_vars)

# Stop the Spark session
spark.stop()
