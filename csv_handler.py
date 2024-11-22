import os

class CSVHandler:
    def get_csv_files(self, csv_directory):
        """
        Get CSV files from directory and verify they have uniform headers.

        Args:
            csv_directory (str): Directory path containing CSV files to process

        Returns:
            tuple: (files, headers) where:
                - files (list): List of CSV filenames if headers are uniform, None if no files or non-uniform headers
                - headers (tuple): Tuple of column names from CSV headers if uniform, None if no files or non-uniform headers

        Notes:
            - Checks for .csv file extension
            - Reads first line of each file as header
            - Verifies all files have identical headers
            - Prints status messages about file discovery and header uniformity
        """
        # Get list of all CSV files in directory
        files = [f for f in os.listdir(csv_directory) if f.endswith(".csv")]
        if not files:
            print("No CSV files in directory")
            return None, None

        print(f"CSV files in folder: {files}")
        
        # Read headers from all CSV files
        all_headers = []
        for csv_file in files:
            # Open each file and get first line as header
            with open(os.path.join(csv_directory, csv_file), 'r') as f:
                # Split header line into column names
                header = f.readline().strip().split(',')
                all_headers.append(header)

        # Check if all headers match the first file's headers
        if all(header == all_headers[0] for header in all_headers):
            print("Success: Headers are uniform")
            print(all_headers[0])
            # Return files list and headers tuple if uniform
            return files, tuple(all_headers[0])
        
        print("Headers are not uniform")
        # Return None if headers don't match
        return None, None 