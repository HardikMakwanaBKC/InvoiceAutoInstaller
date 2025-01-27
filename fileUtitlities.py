import os
import time
import winreg
import shutil
import glob
import pandas as pd

from logUtility import CLogUtility

objLogger = CLogUtility()

class CFileUtility:
    def MGetFilePathsOfPayroll(directory, liFileNames):
        """
        Dynamically selects file paths based on specific criteria from a directory.

        Args:
            directory (str): The path to the directory containing the files.
            liFileNames (list): A list of file names to look for in the directory.

        Returns:
            list: A list containing the full paths of the specified files.
        """
        liFilePaths = [os.path.join(directory, f"Input_{strFileName}") for strFileName in liFileNames]
        return liFilePaths


    def MKeepSpecificColumns(strInputExcelPath, strOutputExcelPath, lsColumnsToKeep):
        """
        Purpose : Keep only specific columns from an Excel file and save it to a new file.

        Inputs:
            1) strInputExcelPath (str)  : The file path to the input Excel file.
            2) strOutputExcelPath (str) : The file path to save the output Excel file.
            3) lsColumnsToKeep (list)   : A list of column names to keep.

        Outputs:
            None
        """
        try:
            # Read the Excel file
            df = pd.read_excel(strInputExcelPath)
            
            # Keep only the specified columns
            df_filtered = df[lsColumnsToKeep]
            
            # Save the filtered DataFrame to a new Excel file
            df_filtered.to_excel(strOutputExcelPath, index=False)
        except Exception as e:
            print(f"Error occurred: {e}")


    def copyFilesToFolder(folder_path, strFolderName):
        original_data_folder = os.path.join(folder_path, strFolderName)
        
        # Create Original Data folder if it doesn't exist
        if not os.path.exists(original_data_folder):
            os.makedirs(original_data_folder)
        
        # List files in the specified directory (without walking through subdirectories)
        for file in os.listdir(folder_path):
            if file.endswith('.xlsx') and not file.startswith('~$'):
                file_path = os.path.join(folder_path, file)
                # Copy the file to Original Data folder
                shutil.copy(file_path, os.path.join(original_data_folder, file))
                print(f"Copied file to Original Data folder: {file}")

def get_chrome_download_path(bDebug = False):
    try:
        # Open the Windows Registry key where Chrome settings are stored
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders")

        # Query the value of the Chrome download directory
        download_path = winreg.QueryValueEx(key, '{374DE290-123F-4565-9164-39C4925E467B}')[0]

        # Resolve environment variables in the path
        download_path = os.path.expandvars(download_path)

        return download_path
    
    except Exception as e:
        # objLogger.logInfo(f"Error occurred while getting Chrome download path: {e}")
        if bDebug:
            print(f"Error occurred while getting Chrome download path: {e}")
        return None
# # Example usage:
# directory = r'C:\Hardik\Payrun Listing\Input'
# file_paths = get_file_paths(directory)

# print("File Paths:")
# for file_path in file_paths:
#     print(file_path)


def move_latest_excel_files(download_folder, destination_folder, num_files=6, bDebug = False):
    """
    Move the latest Excel files from the download folder to the destination folder.
    """
    # Get the current time
    current_time = time.time()

    # List all files in the download folder
    all_files = glob.glob(os.path.join(download_folder, '*'))

    # # Filter Excel files
    # excel_files = [file for file in all_files if file.endswith('.xlsx')]
    # Filter Excel files modified in the last 10 minutes
    excel_files = [
        file for file in all_files
        if file.endswith('.xlsx') and (current_time - os.path.getmtime(file)) <= 600
    ]

    # Sort files by modification time (latest first)
    excel_files.sort(key=os.path.getmtime, reverse=True)

    # Move the latest Excel files to the destination folder
    for file in excel_files[:num_files]:
        shutil.move(file, destination_folder)
        objLogger.logInfo(f"Latest Excel file :  {file} moved to {destination_folder} successfully.")
        if bDebug:
            print(f"Latest Excel file :  {file} moved to {destination_folder} successfully.")

    

def get_latest_file_in_folder(folder_path, bDebug = False):
    try:
        # Get a list of all files in the folder
        files = glob.glob(os.path.join(folder_path, '*'))

        # Sort the files by modification time
        files.sort(key=os.path.getmtime, reverse=True)

        # Return the path of the latest file
        if files:
            return files[0]
        else:
            return None
    except Exception as e:
        # objLogger.logInfo(f"Error occurred while getting latest file in folder: {e}")
        if bDebug:
            print(f"Error occurred while getting latest file in folder: {e}")
        return None



def rename_latest_file_in_folder(folder_path, new_name, bDebug=False, timeout=60):
    """
    Rename the latest file in the folder to the new name.
    
    Parameters:
        folder_path (str): The path to the folder containing the files.
        new_name (str): The new name for the latest file.
        bDebug (bool, optional): If True, print debug information. Default is False.
        timeout (int, optional): The maximum time in seconds to wait for the download to complete. Default is 60 seconds.
    
    Returns:
        str: The new file path if the renaming was successful, None otherwise.
    """
    try:
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Get the path of the latest file
            latest_file_path = get_latest_file_in_folder(folder_path)

            if latest_file_path:
                # Check if the latest file is still downloading
                if latest_file_path.endswith('.crdownload'):
                    if bDebug:
                        print(f"The latest file {latest_file_path} is still downloading. Waiting...")
                    time.sleep(1)
                    continue

                # Check if the file actually exists
                if os.path.exists(latest_file_path):
                    # Split the file path into directory and filename
                    directory, filename = os.path.split(latest_file_path)

                    # Ensure new name does not conflict with an existing file
                    new_file_path = os.path.join(directory, new_name)
                    if os.path.exists(new_file_path):
                        if bDebug:
                            print(f"A file with the name {new_name} already exists in the directory.")
                        return None

                    # Rename the file with the new name
                    os.rename(latest_file_path, new_file_path)
                    if bDebug:
                        print(f"Renamed file from {latest_file_path} to {new_file_path}")
                    return new_file_path
                else:
                    if bDebug:
                        print(f"The file {latest_file_path} does not exist.")
                    return None
            else:
                if bDebug:
                    print("No files found in the folder.")
                return None
        
        if bDebug:
            print("Timeout reached. Download may not have completed.")
        return None

    except Exception as e:
        if bDebug:
            print(f"Error occurred while renaming the latest file in the folder: {e}")
        return None

# Function to wait for a new file to appear in the download directory
def wait_for_new_file(download_path, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Get the list of files in the download directory
        files = os.listdir(download_path)
        # Check if any new file has been created after the start time
        new_files = [file for file in files if os.path.getctime(os.path.join(download_path, file)) > start_time]
        if new_files:
            return os.path.join(download_path, new_files[0])  # Return the path of the first new file found
        time.sleep(1)  # Wait for 1 second before checking again
    return None  # Timeout reached, no new file found


def get_chrome_user_data_path():
    try:    
        app_data_path = os.getenv('LOCALAPPDATA')
        chrome_user_data_path = os.path.join(app_data_path, 'Google', 'Chrome', 'User Data')
        return chrome_user_data_path
    except Exception as e:
        print(f"Error occurred while getting Chrome user data path: {e}")
        return None