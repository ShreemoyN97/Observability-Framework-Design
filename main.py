# main.py
from datetime import datetime  # Import datetime
from data_capture_module import process_file
from etl_tasks import perform_etl
import pipeline_observation_module

def main():
    input_file_path = '/Users/shreemoynanda/Desktop/NY_Lobbying_data_1.csv'
    output_file_path = '/Users/shreemoynanda/Desktop/output_with_aggregated_values.csv'
    
    # Capture the process start time
    process_start_time = datetime.now()
    print("Starting the data pipeline...")
    
    # Record the start time in Pipeline_Observability
    process_file_id = process_file(input_file_path)
    
    if process_file_id:
        # Initialize observation with process start time
        pipeline_observation_module.initialize_observation(process_file_id, process_start_time)
        
        # Perform ETL tasks
        perform_etl(input_file_path, output_file_path, process_file_id)
        
        # Capture the process end time
        process_end_time = datetime.now()
        
        # Calculate metrics and finalize observation
        error_count, distinct_error_count = pipeline_observation_module.get_error_metrics(process_file_id)
        processed_count = pipeline_observation_module.get_processed_count(output_file_path)
        
        # Finalize observation with process end time
        pipeline_observation_module.finalize_observation(
            process_file_id,
            process_end_time,
            initial_count_of_records=0,  # Update with actual initial record count if available
            processed_count=processed_count,
            error_count=error_count,
            distinct_error_count=distinct_error_count
        )
        
    print("Data pipeline completed.")

if __name__ == '__main__':
    main()