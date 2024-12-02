import pandas as pd
import os

# Hardcoded paths
input_file_path = '/Users/shreemoynanda/Library/CloudStorage/OneDrive-NortheasternUniversity/DAMG7374-Data_Governance/Datasets/Processed_EVTRA_2024/part-00000-3beffb76-e489-4023-95b4-f3c00183cbb1-c000.csv'
output_directory = '/Users/shreemoynanda/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-b5fcfcc8-5b19-4db0-9b51-6b163537f887/import'

# Ensure the output directory exists, create if not
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
else:
    print("Output directory already exists:", output_directory)

# Check if the input file exists
if os.path.exists(input_file_path):
    # Load the dataset from the specified input file
    data = pd.read_csv(input_file_path)

    # Normalize the column names
    def normalize_column_name(col_name):
        # Remove leading/trailing whitespace
        col_name = col_name.strip()
        # Check if it starts with a number
        if col_name[0].isdigit():
            # Extract the leading number
            split_index = 0
            while split_index < len(col_name) and col_name[split_index].isdigit():
                split_index += 1
            number_part = col_name[:split_index]
            rest_part = col_name[split_index:].strip()
            # Reformat the column name
            col_name = f"{rest_part}_{number_part}"
        # Replace spaces with underscores
        col_name = col_name.replace(" ", "_").lower()
        # Remove special characters
        col_name = ''.join(e if e.isalnum() or e == '_' else '' for e in col_name)
        return col_name

    data.columns = [normalize_column_name(col) for col in data.columns]

    # Print normalized column names to confirm changes
    print("Normalized column names:", data.columns)

    # Save the modified DataFrame back to CSV in the output directory
    normalized_file_path = os.path.join(output_directory, 'normalized_EVTRA_2024.csv')
    data.to_csv(normalized_file_path, index=False)
    print("File saved successfully:", normalized_file_path)
else:
    print("Input file does not exist:", input_file_path)
