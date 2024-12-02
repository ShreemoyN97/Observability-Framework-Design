from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadAndMergeEVTRA") \
    .getOrCreate()

# Directory containing datasets
base_dir = "/Users/shreemoynanda/Library/CloudStorage/OneDrive-NortheasternUniversity/DAMG7374-Data_Governance/Datasets"

# Generate list of folder names matching Processed_EVTRA_YYYY
folders = [os.path.join(base_dir, f"Processed_EVTRA_{year}") for year in range(2010, 2025) if os.path.exists(os.path.join(base_dir, f"Processed_EVTRA_{year}"))]

# Define the common schema (adjust column names/types based on your dataset)
common_columns = [
    "Model", "Make", "Clean Alternative Fuel Vehicle Type", "VIN (1-10)", "DOL Vehicle ID",
    "Model Year", "Primary Use", "Electric Range", "Odometer Reading", "Odometer Reading Description",
    "New or Used Vehicle", "Sale Price", "Sale Date", "Base MSRP", "Transaction Type", "Transaction Date",
    "Year", "County", "City", "State", "Postal Code",
    "2019 HB 2042: Clean Alternative Fuel Vehicle (CAFV) Eligibility",
    "Meets 2019 HB 2042 Electric Range Requirement", "Meets 2019 HB 2042 Sale Date Requirement"
]

# Initialize an empty DataFrame
merged_df = None

# Loop through each folder
for folder in folders:
    if not os.path.exists(folder):
        continue

    # List all files in the folder
    files = [os.path.join(folder, file) for file in os.listdir(folder) if file.endswith(".csv")]

    # Check if files exist in the folder
    if not files:
        continue

    # Identify the latest file based on modification time
    latest_file = max(files, key=os.path.getmtime)

    # Read the latest file
    latest_df = spark.read.csv(latest_file, header=True, inferSchema=True)

    # Align schema with the common schema
    for col in common_columns:
        if col not in latest_df.columns:
            latest_df = latest_df.withColumn(col, lit(None))

    # Select only the common columns
    latest_df = latest_df.select(common_columns)

    # Merge the DataFrame with others
    if merged_df is None:
        merged_df = latest_df
    else:
        merged_df = merged_df.union(latest_df)

# Calculate the total row count in the merged DataFrame
if merged_df is not None:
    row_count = merged_df.count()
    print(f"Total number of rows in the merged DataFrame: {row_count}")
else:
    print("No files were found to process.")
