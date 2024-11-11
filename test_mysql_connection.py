import pymysql

# Database connection parameters
host = 'localhost'  # Adjust as necessary, if your server isn't running locally
user = 'root'  # Replace with your MySQL username
password = 'ASDf090120@' 
database = 'DAMG_7473_ADG_Framework_Database'  # Replace with your database name

try:
    # Attempt to establish a MySQL connection
    connection = pymysql.connect(host=host, user=user, password=password, db=database)
    print("Successfully connected to the database.")
    
    # Perform any database operations here
    # For example, to fetch data from a table:
    # cursor = connection.cursor()
    # cursor.execute("SELECT * FROM your_table_name")
    # results = cursor.fetchall()
    # for row in results:
    #     print(row)

finally:
    # Ensure the connection is closed properly
    if connection:
        connection.close()
