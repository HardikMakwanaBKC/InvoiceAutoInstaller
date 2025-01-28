
# import os
# import logging
# from datetime import datetime

# class CLogUtility:
#     def __init__(self):
#         self.setup_logging()

#     def setup_logging(self, log_dir="logs", log_level=logging.INFO):
#         """
#         Set up logging configuration.
        
#         Args:
#             log_dir (str): Directory path where log files will be saved.
#             log_level (int): Logging level. Default is logging.INFO.
#         """
#         if not os.path.exists(log_dir):
#             os.makedirs(log_dir)

#         log_file = os.path.join(log_dir, f"log_{datetime.now().strftime('%Y-%m-%d')}.log")

#         logging.basicConfig(level=log_level,
#                             format='%(asctime)s - %(levelname)s - %(message)s',
#                             datefmt='%Y-%m-%d %H:%M:%S',
#                             filename=log_file,
#                             filemode='a')
import logging
import sys

class CLogUtility:
    def __init__(self):
        self.logger = logging.getLogger("AppLogger")
        self.logger.setLevel(logging.DEBUG)  # Adjust the level as needed
        self.setup_logging()

    def setup_logging(self):
        # Remove existing handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Log to stdout instead of a file
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)  # Adjust the level as needed

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)

        self.logger.addHandler(console_handler)

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

# Example usage:
if __name__ == "__main__":
    log_util = CLogUtility()
    log_util.setup_logging()
    log_util.logInfo("This is an information message.")
