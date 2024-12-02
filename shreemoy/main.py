from datetime import datetime
from data_capture_module import process_file
import pipeline_observation_module
import os
from dotenv import load_dotenv
from etl_tasks import perform_etl
from pyspark.sql import SparkSession
import platform
import subprocess

def clear_terminal_memory():
    """
    Clears the terminal memory based on the operating system.
    """
    os_name = platform.system()
    try:
        if os_name == "Windows":
            subprocess.run("cls", shell=True)
        elif os_name in ["Linux", "Darwin"]:  # Darwin is macOS
            subprocess.run("clear", shell=True)
        print("Terminal memory cleared.")
    except Exception as e:
        print(f"Error clearing terminal memory: {e}")

def reset_and_reload_env():
    """
    Unsets specific environment variables and reloads the .env file.
    """
    # Unset specific environment variables
    for var in ["INPUT_FILE_PATH", "OUTPUT_FILE_PATH"]:
        if var in os.environ:
            del os.environ[var]  # Unset the variable
    
    # Reload variables from .env
    dotenv_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path, override=True)

    # Debugging: Print reloaded variables
    input_file_path = os.getenv("INPUT_FILE_PATH")
    output_file_path = os.getenv("OUTPUT_FILE_PATH")
    print(f"Reloaded INPUT_FILE_PATH: {input_file_path}")
    print(f"Reloaded OUTPUT_FILE_PATH: {output_file_path}")

    return input_file_path, output_file_path

def main():
    # Clear terminal memory
    clear_terminal_memory()

    # Reset and reload environment variables
    input_file_path, output_file_path = reset_and_reload_env()

    if not input_file_path or not output_file_path:
        print("Error: Missing INPUT_FILE_PATH or OUTPUT_FILE_PATH in the environment variables.")
        return

    # Start pipeline
    process_start_time = datetime.now()
    print("Starting the data pipeline...")

    # Capture file information
    try:
        process_file_id, file_category = process_file(input_file_path)
        if not process_file_id or not file_category:
            print("Error: Missing process_file_id or file_category.")
            return
    except Exception as e:
        print(f"Error during file processing: {e}")
        return

    # Initialize observation
    try:
        pipeline_observation_module.initialize_observation(process_file_id, process_start_time)
    except Exception as e:
        print(f"Error initializing observation: {e}")
        return

    try:
        # Perform ETL
        perform_etl(file_category, input_file_path, process_file_id)
        
        # Capture process end time
        process_end_time = datetime.now()

        # Get error metrics
        try:
            error_count, distinct_error_count = pipeline_observation_module.get_error_metrics(process_file_id)
            print(f"Error Count: {error_count}, Distinct Error Count: {distinct_error_count}")
        except Exception as e:
            print(f"Error fetching error metrics: {e}")
            error_count = distinct_error_count = 0

        # Get processed record count
        try:
            processed_count = pipeline_observation_module.get_processed_count(output_file_path)
            print(f"Processed Record Count: {processed_count}")
        except Exception as e:
            print(f"Error fetching processed record count: {e}")
            processed_count = 0

        # Finalize observation
        pipeline_observation_module.finalize_observation(
            process_file_id, process_end_time, 0, processed_count, error_count, distinct_error_count
        )
    except Exception as e:
        print(f"Pipeline execution failed: {e}")

    print("Data pipeline completed.")

if __name__ == '__main__':
    main()
