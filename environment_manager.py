import os
import sys

class EnvironmentManager:
    """
    Manages environment variable verification.
    """

    def __init__(self):
        self.required_env_vars = ["SQL_SERVER_INSTANCE", "SQL_SERVER_DATABASE", "CSV_FILEPATH"]

    def verify_env_variables(self):
        """
        Check if all required environment variables exist and return their values.

        Returns:
            dict: Dictionary mapping environment variable names to their values

        Raises:
            SystemExit: If any required environment variables are missing
        """
        missing_vars = [var for var in self.required_env_vars if not os.getenv(var)]
        if missing_vars:
            print(f"Fail: Missing environment variables: {', '.join(missing_vars)}")
            print("Adjust environment variables and restart")
            sys.exit(1)
        return {var: os.getenv(var) for var in self.required_env_vars}