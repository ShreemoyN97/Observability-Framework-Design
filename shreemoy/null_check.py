import pandas as pd

# Path to your CSV file
file_path = '/Users/shreemoynanda/Library/CloudStorage/OneDrive-NortheasternUniversity/DAMG7374-Data_Governance/Datasets/Processed_EVTRA_2024/part-00000-3beffb76-e489-4023-95b4-f3c00183cbb1-c000.csv'

# Load the CSV file into a DataFrame
data = pd.read_csv(file_path)

# Check for null values in each column
null_counts = data.isnull().sum()

# Display the result
print("Null values in each column:")
print(null_counts)

