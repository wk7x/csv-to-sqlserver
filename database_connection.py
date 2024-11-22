import pyodbc

class DatabaseConnection:
    """
    Manages database connections to SQL Server.

    This class handles establishing connections to SQL Server databases,
    managing cursors, and properly closing connections when finished.
    """

    def __init__(self):
        """
        Initialize database connection object.

        The connection and cursor are initially set to None until connect() is called.
        """
        self.conn = None
        self.cursor = None

    def connect(self, server, database, username=None, password=None, is_local=True):
        """
        Connect to SQL Server database.

        Args:
            server: SQL Server instance name
            database: Target database name
            username: SQL Server authentication username (optional)
            password: SQL Server authentication password (optional) 
            is_local: If True, use Windows authentication, else SQL Server auth (default: True)

        Returns:
            tuple: (connection object, cursor object) if successful, (None, None) if failed

        Notes:
            - Uses Windows authentication by default
            - Disables autocommit for transaction control
            - Prints connection status message
        """
        conn_str = (f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};'
                   f'{"Trusted_Connection=yes" if is_local else f"UID={username};PWD={password}"}')
        try:
            self.conn = pyodbc.connect(conn_str)
            self.cursor = self.conn.cursor()
            self.conn.autocommit = False
            print(f"Connected to: {self.conn.getinfo(pyodbc.SQL_SERVER_NAME)}")
            return self.conn, self.cursor
        except pyodbc.Error as error:
            print(f"Connect failed: {error}")
            return None, None

    def close(self):
        """
        Close database connection and cursor.

        Closes the cursor and connection if they exist.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close() 