from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EVTRA Data Processing - Electric Range Update") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

# Load vehicle data
vehicle_file_path = "/Users/shreemoynanda/Library/CloudStorage/OneDrive-NortheasternUniversity/DAMG7374-Data_Governance/Datasets/EVTRA/EVTRA_2010_2014.csv"
vehicle_df = spark.read.csv(vehicle_file_path, header=True, inferSchema=True)

# Drop duplicates to ensure clean data
vehicle_df = vehicle_df.dropDuplicates()

# Capture the "before" state where Electric Range is 0
before_update_df = vehicle_df.filter(col("Electric Range") == 0).select(
    col("VIN (1-10)").alias("VIN"),
    col("DOL Vehicle ID").alias("Vehicle_ID"),
    col("Make"),
    col("Model"),
    col("Electric Range").alias("Electric_Range_Before")
)

print("Before Update (where Electric Range is 0):")
before_update_df.show(truncate=False)

# Group by Model and Make, and calculate the maximum Electric Range
max_range_df = vehicle_df.groupBy("Model", "Make").agg(
    max("Electric Range").alias("Max_Electric_Range")
)

# Join the original DataFrame with the max range DataFrame
updated_df = vehicle_df.join(
    max_range_df,
    ["Model", "Make"],
    "left"
)

# Replace Electric Range with Max_Electric_Range where Electric Range is 0
updated_df = updated_df.withColumn(
    "Updated_Electric_Range",
    when(
        (col("Electric Range") == 0) & col("Max_Electric_Range").isNotNull(),
        col("Max_Electric_Range")
    ).otherwise(col("Electric Range"))
)

# Capture the "after" state for rows where Electric Range was initially 0
after_update_df = updated_df.filter(col("Electric Range") == 0).select(
    col("VIN (1-10)").alias("VIN"),
    col("DOL Vehicle ID").alias("Vehicle_ID"),
    col("Make"),
    col("Model"),
    col("Electric Range").alias("Electric_Range_Before"),
    col("Updated_Electric_Range").alias("Electric_Range_After")
)

print("After Update (where Electric Range was 0):")
after_update_df.show(truncate=False)

# Drop the Max_Electric_Range column from the final DataFrame
final_df = updated_df.drop("Max_Electric_Range")

# Define output path
output_path = "/Users/shreemoynanda/Library/CloudStorage/OneDrive-NortheasternUniversity/DAMG7374-Data_Governance/Datasets/Final_Updated_Electric_Vehicle_Data.csv"

# Write the final DataFrame to a single CSV file
final_df.coalesce(1).write.csv(output_path, mode="overwrite", header=True)

print(f"Updated data has been saved to {output_path}")

# Stop Spark session
spark.stop()
