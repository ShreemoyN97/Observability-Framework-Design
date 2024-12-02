import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_azure_sql_connection():
    try:
        # Fetch database credentials from .env
        server = os.getenv("DB_HOST")
        database = os.getenv("DB_NAME")
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        # Create a connection string
        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        print(f"Connecting to Azure SQL at {server}...")

        # Initialize SQLAlchemy engine
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect() as connection:
            print("Connection successful!")
            # Execute a simple query to verify
            result = connection.execute("SELECT GETDATE() AS CurrentDateTime")
            for row in result:
                print(f"Server Date/Time: {row['CurrentDateTime']}")
        engine.dispose()
    except Exception as e:
        print(f"Error connecting to Azure SQL: {e}")

if __name__ == "__main__":
    test_azure_sql_connection()
