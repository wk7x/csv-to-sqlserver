import os
import sys
import time
import threading
import pyodbc

class StagingTableManager:
    """
    Manages creation and population of staging tables for CSV data import.
    """

    def __init__(self, db_connection):
        """
        Initialize staging table manager.

        Args:
            db_connection: DatabaseConnection object for database operations
        """
        self.db = db_connection

    def create_staging_table(self, csv_handler, csv_directory):
        """
        Create a staging table based on CSV file headers.

        Args:
            csv_handler: CSVHandler object to process CSV files
            csv_directory: Directory path containing CSV files to process

        Returns:
            tuple: (table_name, csv_files) where:
                - table_name (str): Name of created staging table, None if creation failed
                - csv_files (list): List of CSV files to be processed, None if creation failed

        Notes:
            - Gets CSV files and headers from CSVHandler
            - Prompts user for table name (alphanumeric only)
            - Creates table with VARCHAR(255) columns matching CSV headers
            - Uses transaction for rollback on failure
        """
        # Get CSV files and column names from handler
        csv_files, column_names = csv_handler.get_csv_files(csv_directory)
        
        # Get valid table name from user
        while True:
            target_table = input("Enter name of target staging table: ")
            if target_table.isalnum():
                break
            print("Table name can only contain alphanumeric characters.")

        # Specify column dtypes for CREATE TABLE
        columns = ", ".join(f"[{col}] VARCHAR(255)" for col in column_names)
        try:
            # Create table if it doesn't exist
            self.db.cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{target_table}')
                BEGIN 
                    CREATE TABLE {target_table}({columns})
                END
            """)
            self.db.conn.commit()
            print(f"Success: {target_table} is ready")
            return target_table, csv_files
        except pyodbc.Error as error:
            self.db.conn.rollback()
            print(f"Error: {error}")
            return None, None

    def insert_csv_data(self, target_table, csv_files, csv_directory):
        """
        Bulk insert CSV data into staging table with progress tracking.

        Args:
            target_table: Name of target staging table
            csv_files: List of CSV files to insert
            csv_directory: Directory containing CSV files

        Returns:
            int: Number of files successfully inserted, 0 if operation failed

        Notes:
            - Uses SQL Server BULK INSERT
            - Shows animated progress indicator during insertion
            - Handles interrupts gracefully
            - Maintains transaction integrity with XACT_ABORT
        """
        file_counter = 0  # Track number of files processed
        row_counter = {}  # Track rows inserted per file
        
        # Enable transaction abort on error
        self.db.cursor.execute("SET XACT_ABORT ON;")
        
        # Flag for progress animation thread
        inserting_in_progress = False

        def print_progress():
            """Animated progress indicator function run in separate thread"""
            while inserting_in_progress:
                for dots in ['Inserting.', 'Inserting..', 'Inserting...', 'Inserting   ']:
                    sys.stdout.write(f"\r{dots}")
                    sys.stdout.flush()
                    time.sleep(1)
                    sys.stdout.write("\r   ")
                    sys.stdout.flush()
                    time.sleep(0.2)

        try:
            # Process each CSV file
            for csv_file in csv_files:
                file_path = os.path.join(csv_directory, csv_file)
                
                # Count rows to insert (excluding header)
                with open(file_path, 'r') as f:
                    next(f)  # Skip header
                    rows_to_insert = sum(1 for _ in f)
                
                print(f"Total lines to insert for {csv_file}: {rows_to_insert}")

                # Start progress animation in separate thread
                inserting_in_progress = True
                progress_thread = threading.Thread(target=print_progress)
                progress_thread.start()

                # Execute bulk insert
                self.db.cursor.execute(f"""
                    BULK INSERT {target_table}
                    FROM '{file_path}'
                    WITH (
                        FIELDTERMINATOR = ',', 
                        ROWTERMINATOR = '0x0a',
                        FIRSTROW = 2
                    );
                """)

                file_counter += 1
                print(f"\n{csv_file} has finished inserting.")
                row_counter[csv_file] = rows_to_insert

                # Stop progress animation
                inserting_in_progress = False
                progress_thread.join()

        except KeyboardInterrupt:
            print("\nProcess interrupted by keyboard input.")
            inserting_in_progress = False
            progress_thread.join()
            self.db.conn.rollback()
            sys.exit(1)
        except pyodbc.Error as error:
            print(f"Fail on insert: {error}")
            self.db.conn.rollback()
            return 0

        # Commit transaction and cleanup
        self.db.conn.commit()
        self.db.cursor.execute("SET XACT_ABORT OFF;")
        print(f"Rows inserted: {row_counter}")
        return file_counter