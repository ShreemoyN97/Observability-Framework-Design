# test_insert_column_error.py
import error_capture_module

# Replace these values with test data that should logically fit your database structure
record_id = 1  # Use an existing Record_ID from Record_Error_Report
processing_file_id = 1  # Use an existing Processing_File_ID from Pipeline_Observability
column_name = "sample_column"
error_code = 1002  # Assuming this error code exists in Error_Message_Reference

# Attempt to insert a test entry
error_capture_module.insert_column_error(record_id, processing_file_id, column_name, error_code)

print("Test entry inserted into Column_Error_Report.")
