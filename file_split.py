import pandas as pd
import os

# Load the CSV file
file_path = "/Users/shreemoynanda/Library/CloudStorage/OneDrive-NortheasternUniversity/DAMG7374-Data_Governance/Datasets/Electric_Vehicle_Title_and_Registration_Activity.csv"
output_dir = "/Users/shreemoynanda/Library/CloudStorage/OneDrive-NortheasternUniversity/DAMG7374-Data_Governance/Datasets/EVTRA/Yearwise/"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file
df = pd.read_csv(file_path)

# Get unique years in the 'Year' column
unique_years = df['Year'].unique()

# Loop through each year, filter the data, and save it to a CSV file
for year in unique_years:
    # Filter data for the current year
    year_data = df[df['Year'] == year]
    
    # Define the output file path
    output_file = os.path.join(output_dir, f"EVTRA_{year}.csv")
    
    # Save the filtered data to a CSV file
    year_data.to_csv(output_file, index=False)
    print(f"Successfully wrote data for the year {year} to {output_file}.")
