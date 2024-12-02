# data_capture_module.py
import csv
import pymysql
from datetime import datetime
import os
from dotenv import load_dotenv


def connect_to_database():
    load_dotenv() 

    host = os.getenv('DB1_HOST')
    database = os.getenv('DB1_NAME')
    username = os.getenv('DB1_USER')
    password = os.getenv('DB1_PASSWORD')

    return pymysql.connect(host=host, user=username, password=password, db=database)

def process_file(file_path):
    file_name = file_path.split('/')[-1]
    file_category_name = file_name.split('_')[0]
    
    # Opening the CSV file
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)
            schema_text = ', '.join(header)
            initial_count_of_records = sum(1 for row in csv_reader)
            # Find the size of the file
            input_file_size = os.path.getsize(file_path)
        time_of_arrival = datetime.now()

        connection = connect_to_database()
        try:
            with connection.cursor() as cursor:
                # Check if file_category_name exists in File_Schema
                cursor.execute("SELECT COUNT(*) FROM File_Schema WHERE File_Category_Name = %s", (file_category_name,))
                category_exists = cursor.fetchone()[0]

                if category_exists == 0:
                    print("Category doesn't exist.")
                    raise ValueError(f"File category '{file_category_name}' does not exist in File_Schema table.")
                else:
                    print("Category does exist.")
                    # Find the file schema ID
                    cursor.execute("SELECT File_Schema_ID FROM File_Schema WHERE File_Category_Name = %s", (file_category_name,))
                    file_schema_id = cursor.fetchone()[0]
                    print(f"File schema ID: {file_schema_id}")
                    cursor.execute("INSERT INTO File_Name (Processing_file_name) VALUES (%s)", (file_name,))
                    processing_file_id = cursor.lastrowid
                    cursor.execute("""
                        INSERT INTO Pipeline_Observability (
                            Processing_File_ID, 
                            File_Schema_ID, 
                            Time_Of_Arrival, 
                            Process_StartTime, 
                            Process_End_Time, 
                            Input_File_Size, 
                            Initial_Count_Of_Records, 
                            Count_Of_Processed_Records, 
                            Count_Of_Error_Records,
                            Count_of_Distinct_Errors
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        processing_file_id, 
                        file_schema_id, 
                        time_of_arrival, 
                        time_of_arrival, 
                        time_of_arrival, 
                        input_file_size, 
                        initial_count_of_records, 
                        0,  # Assuming 0 processed records initially
                        0,  # Assuming 0 error records initially
                        0   # Assuming 0 distinct errors initially
                    ))

                    connection.commit()
                    print("Data captured and stored successfully.")
                    # Return the processing_file_id and file_category
                    return processing_file_id, file_category_name
            
        finally:
            if connection:
                connection.close()
                
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None  # Return None if there's an error
    

def main(file_path):
    return process_file(file_path)  # Return process_file_id from main
