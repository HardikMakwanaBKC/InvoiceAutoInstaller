# import logging
# import os
# from datetime import datetime

# class CLogUtility:
#     def __init__(self):
#         # Initialize log file path
#         self.log_file = None

#         # Set up logging
#         self.setup_logging()

#     def setup_logging(self):
#         # Create a directory for logs if it doesn't exist
#         log_dir = "logs"
#         if not os.path.exists(log_dir):
#             os.makedirs(log_dir)

#         # Get today's date
#         today_date = datetime.now().strftime('%Y-%m-%d')

#         # Set log file path
#         self.log_file = os.path.join(log_dir, f"{today_date}.log")

#         # Create a logging file handler
#         file_handler = logging.FileHandler(self.log_file)

#         # Create a logging format
#         formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
#         file_handler.setFormatter(formatter)

#         # Set up logging level to INFO
#         file_handler.setLevel(logging.INFO)

#         # Add the file handler to the root logger
#         logging.getLogger().addHandler(file_handler)

#     def check_date_and_update_log_file(self):
#         # Get today's date
#         today_date = datetime.now().strftime('%Y-%m-%d')

#         # Check if current log file is for today's date
#         if not self.log_file.endswith(f"{today_date}.log"):
#             # If not, set up logging again to create a new log file
#             self.setup_logging()

#     def logInfo(self, message):
#         self.check_date_and_update_log_file()
#         logging.info(message)

#     def logWarning(self, message):
#         self.check_date_and_update_log_file()
#         logging.warning(message)

#     def logError(self, message):
#         self.check_date_and_update_log_file()
#         logging.error(message)

#     def logDebug(self, message):
#         self.check_date_and_update_log_file()
#         logging.debug(message)

import os
import logging
from datetime import datetime

class CLogUtility:
    def __init__(self):
        self.setup_logging()

    def setup_logging(self, log_dir="logs", log_level=logging.INFO):
        """
        Set up logging configuration.
        
        Args:
            log_dir (str): Directory path where log files will be saved.
            log_level (int): Logging level. Default is logging.INFO.
        """
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_file = os.path.join(log_dir, f"log_{datetime.now().strftime('%Y-%m-%d')}.log")

        logging.basicConfig(level=log_level,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            filename=log_file,
                            filemode='a')

    def logInfo(self, message):
        """
        Log information message.
        
        Args:
            message (str): Information message to be logged.
        """
        logging.info(message)

    def logError(self, message):
        """
        Log error message.
        
        Args:
            message (str): Error message to be logged.
        """
        logging.error(message)

# # Example usage:
# if __name__ == "__main__":
#     setup_logging()
#     log_info("This is an information message.")
#     log_error("This is an error message.")
