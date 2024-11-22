import sys
from environment_manager import EnvironmentManager
from database_connection import DatabaseConnection
from csv_handler import CSVHandler
from staging_table_manager import StagingTableManager

class CSVToStagingDB:
    """
    Main application class for importing CSV files into SQL Server staging tables.

    This class orchestrates the process of:
    - Verifying required environment variables
    - Establishing database connections
    - Creating staging tables based on CSV structure
    - Bulk inserting CSV data into staging tables
    """

    def __init__(self):
        self.env_manager = EnvironmentManager()
        self.db_connection = DatabaseConnection()
        self.csv_handler = CSVHandler()
        
    def run(self):
        print("""
Before running this script, please ensure the following environment variables are set:
- SQL_SERVER_INSTANCE: The SQL Server instance name
- SQL_SERVER_DATABASE: The target database name  
- CSV_FILEPATH: The directory containing your CSV files
""")
        try:
            # Get and verify environment variables
            env_vars = self.env_manager.verify_env_variables()
            
            # Connect to database
            conn, cursor = self.db_connection.connect(
                server=env_vars["SQL_SERVER_INSTANCE"],
                database=env_vars["SQL_SERVER_DATABASE"]
            )
            if not conn or not cursor:
                sys.exit(1)
                
            # Initialize staging manager
            staging_manager = StagingTableManager(self.db_connection)
            
            # Create staging table
            target_table, csv_files = staging_manager.create_staging_table(
                self.csv_handler, 
                env_vars["CSV_FILEPATH"]
            )
            if not target_table:
                sys.exit(1)
                
            # Insert data
            files_inserted = staging_manager.insert_csv_data(
                target_table, 
                csv_files, 
                env_vars["CSV_FILEPATH"]
            )
            
            print(f"{files_inserted} files inserted." if files_inserted else "No files were inserted.")
            
        except Exception as e:
            print("An error has occurred.")
            print(e)
        finally:
            self.db_connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    CSVToStagingDB().run()