import os
import pandas as pd
from glob import glob
from dotenv import load_dotenv

# Reset environment variables and reload
def reset_env_variables():
    os.environ.pop("FLAT_FILE_DIR", None)
    os.environ.pop("TMP_FILE_DIR", None)

    # Reload environment variables
    load_dotenv()

    # Fetch new variables
    flat_file_dir = os.getenv("FLAT_FILE_DIR")
    tmp_file_dir = os.getenv("TMP_FILE_DIR", "/Users/shreemoynanda/Desktop/Observability-Framework-Design/dataset_for_loading")

    # Ensure they are valid
    if not flat_file_dir:
        raise EnvironmentError("FLAT_FILE_DIR environment variable is not set.")
    return flat_file_dir, tmp_file_dir

# Get the latest file from a directory
def get_latest_file(directory, extension="*.csv"):
    files = glob(os.path.join(directory, extension))
    if not files:
        raise FileNotFoundError(f"No files with extension '{extension}' found in directory '{directory}'.")
    latest_file = max(files, key=os.path.getctime)
    return latest_file

# Create synthetic time dimension
def create_time_dim(output_dir):
    date_range = pd.date_range(start="2010-01-01", end="2025-12-31", freq="D")
    time_dim = pd.DataFrame({
        'full_date': date_range,
        'year': date_range.year,
        'quarter': date_range.quarter,
        'month': date_range.month,
        'day_of_week': date_range.day_name()
    })
    time_dim.to_csv(os.path.join(output_dir, "Time_Dim.csv"), index=False)
    print("Synthetic Time_Dim file created successfully.")

# Create vehicle-related tables
def create_vehicle_related_tables(df, tmp_file_dir):
    if not os.path.exists(tmp_file_dir):
        os.makedirs(tmp_file_dir)

    make_dim = df[['Make']].drop_duplicates().rename(columns={'Make': 'make'})
    make_dim.to_csv(os.path.join(tmp_file_dir, "Make_Dim.csv"), index=False)
    print("Make_Dim file created successfully.")

    model_dim = df[['Model']].drop_duplicates().rename(columns={'Model': 'model'})
    model_dim.to_csv(os.path.join(tmp_file_dir, "Model_Dim.csv"), index=False)
    print("Model_Dim file created successfully.")

    primary_use_dim = df[['Primary Use']].drop_duplicates().rename(columns={'Primary Use': 'primary_use'})
    primary_use_dim.to_csv(os.path.join(tmp_file_dir, "Primary_Use_Dim.csv"), index=False)
    print("Primary_Use_Dim file created successfully.")

    electric_utility_dim = df[['Electric Utility']].drop_duplicates().rename(columns={'Electric Utility': 'electric_utility_name'})
    electric_utility_dim.to_csv(os.path.join(tmp_file_dir, "Electric_Utility_Dim.csv"), index=False)
    print("Electric_Utility_Dim file created successfully.")

    vehicle_dim = df[['DOL Vehicle ID', 'VIN (1-10)', 'Model Year', 'Make', 'Model', 'Primary Use', 'Electric Utility']].drop_duplicates()
    vehicle_dim.rename(columns={
        'DOL Vehicle ID': 'dol_vehicle_id',
        'VIN (1-10)': 'vin_prefix',
        'Model Year': 'model_year'
    }, inplace=True)
    vehicle_dim.to_csv(os.path.join(tmp_file_dir, "Vehicle_Dim.csv"), index=False)
    print("Vehicle_Dim file created successfully.")

# Create location-related tables
def create_location_related_tables(df, tmp_file_dir):
    state_dim = df[['State']].drop_duplicates().rename(columns={'State': 'state'})
    state_dim.to_csv(os.path.join(tmp_file_dir, "State_Dim.csv"), index=False)
    print("State_Dim file created successfully.")

    city_dim = df[['City', 'State']].drop_duplicates().rename(columns={'City': 'city'})
    city_dim.to_csv(os.path.join(tmp_file_dir, "City_Dim.csv"), index=False)
    print("City_Dim file created successfully.")

    location_dim = df[['City', 'State', 'County']].drop_duplicates()
    location_dim.rename(columns={'County': 'county'}, inplace=True)
    location_dim.to_csv(os.path.join(tmp_file_dir, "Location_Dim.csv"), index=False)
    print("Location_Dim file created successfully.")

# Create transaction-related tables
def create_transaction_related_tables(df, tmp_file_dir):
    transaction_type_dim = df[['Transaction Type']].drop_duplicates().rename(columns={'Transaction Type': 'transaction_type'})
    transaction_type_dim.to_csv(os.path.join(tmp_file_dir, "Transaction_Type_Dim.csv"), index=False)
    print("Transaction_Type_Dim file created successfully.")

    fee_dim = df[['Electric Vehicle Fee Paid', 'Transportation Electrification Fee Paid', 'Hybrid Vehicle Electrification Fee Paid']].drop_duplicates()
    fee_dim.rename(columns={
        'Electric Vehicle Fee Paid': 'electric_vehicle_fee',
        'Transportation Electrification Fee Paid': 'transportation_electrification_fee',
        'Hybrid Vehicle Electrification Fee Paid': 'hybrid_vehicle_fee'
    }, inplace=True)
    fee_dim.to_csv(os.path.join(tmp_file_dir, "Fee_Dim.csv"), index=False)
    print("Fee_Dim file created successfully.")

    transactions_facts = df[[
        'DOL Vehicle ID', 'Transaction Date', 'Sale Date', 'City', 'State', 'County',
        'Transaction Type', 'Base MSRP', 'Sale Price', 'Odometer Reading', 'Electric Range',
        'Electric Vehicle Fee Paid', 'Transportation Electrification Fee Paid', 'Hybrid Vehicle Electrification Fee Paid'
    ]].copy()

    transactions_facts['Transaction Date'] = pd.to_datetime(transactions_facts['Transaction Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    transactions_facts['Sale Date'] = pd.to_datetime(transactions_facts['Sale Date'], errors='coerce').dt.strftime('%Y-%m-%d')

    transactions_facts.rename(columns={
        'DOL Vehicle ID': 'vehicle_id',
        'Transaction Date': 'transaction_date',
        'Sale Date': 'sale_date',
        'Electric Vehicle Fee Paid': 'electric_vehicle_fee',
        'Transportation Electrification Fee Paid': 'transportation_electrification_fee',
        'Hybrid Vehicle Electrification Fee Paid': 'hybrid_vehicle_fee'
    }, inplace=True)

    transactions_facts.to_csv(os.path.join(tmp_file_dir, "Vehicle_Transactions_Facts.csv"), index=False)
    print("Vehicle_Transactions_Facts file created successfully.")

# Update TOTAL_COUNT in the .env file
def update_total_count(file_path):
    """
    Update the TOTAL_COUNT environment variable with the count of records in the given file.
    Ensures other variables in the .env file are preserved.
    """
    env_path = os.getenv("ENV_FILE", "/Users/shreemoynanda/Desktop/Observability-Framework-Design/.env")
    load_dotenv(dotenv_path=env_path)

    # Read all environment variables from the .env file
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as file:
            for line in file:
                if "=" in line and not line.strip().startswith("#"):
                    key, value = line.strip().split("=", 1)
                    env_vars[key] = value

    # Ensure TOTAL_COUNT exists and calculate the updated count
    try:
        total_count = int(env_vars.get("TOTAL_COUNT", 0))
    except ValueError:
        print("Warning: TOTAL_COUNT in the .env file is not valid. Resetting to 0.")
        total_count = 0

    # Count the records in the file
    try:
        df = pd.read_csv(file_path)
        new_count = len(df)
        updated_count = total_count + new_count
        env_vars["TOTAL_COUNT"] = str(updated_count)  # Update TOTAL_COUNT
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: File {file_path} is empty or not a valid CSV.")
        return

    # Write all variables back to the .env file
    with open(env_path, "w") as file:
        for key, value in env_vars.items():
            file.write(f"{key}={value}\n")

    print(f"Updated TOTAL_COUNT in .env to: {updated_count}")

# Main script
if __name__ == "__main__":
    try:
        flat_file_dir, tmp_file_dir = reset_env_variables()
        print(f"FLAT_FILE_DIR: {flat_file_dir}")
        print(f"TMP_FILE_DIR: {tmp_file_dir}")

        latest_file = get_latest_file(flat_file_dir)
        print(f"Latest file selected: {latest_file}")

        df = pd.read_csv(latest_file)

        create_vehicle_related_tables(df, tmp_file_dir)
        create_location_related_tables(df, tmp_file_dir)
        create_transaction_related_tables(df, tmp_file_dir)
        create_time_dim(tmp_file_dir)

        vehicle_transactions_path = os.path.join(tmp_file_dir, "Vehicle_Transactions_Facts.csv")
        update_total_count(vehicle_transactions_path)

        print(f"Vehicle_Transactions_Facts file processed. Path: {vehicle_transactions_path}")
    except Exception as e:
        print(f"Error occurred: {e}")
