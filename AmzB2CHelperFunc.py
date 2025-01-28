import pandas as pd
import requests
import pycountry
from concurrent.futures import ThreadPoolExecutor, as_completed
# from bs4 import BeautifulSoup
from urllib.parse import quote
from ensure import ensure_annotations
from datetime import datetime, timedelta
from logUtility import CLogUtility

objLogger  = CLogUtility()

class CAmzB2CHelperFunc:
    """
    The CHelperFunctions class provides various static methods to handle date-related operations,
    exchange rate retrieval tasks, data processing, and geographic information retrieval.

    Methods:
        - MGetLastDayOfPreviousMonth
        - MGetFirstDayOfPreviousMonth
        - MFillMissingDates
        - MGetHistoricalExchangeRates
        - MConvertDateKeys
        - MGetPreferredMonthDates
        - MGetLastMonthDates
        - MGetExchangeRatesLastMonth
        - MGetLastMonthName
        - MGetLastMonthYear
        - MGetCountry
        - MCheckTaxColsAndDropZeroSumCols
        - MGetCountryAndStateName
        - MGetCountryAndState
        - fetch_and_store
        - MGetAllCountriesAndStates
        - MGetExchangeRatesFinalDict
        - MVerifySums
        - MProcessCsvTillOrderFilter
    """
    
    @staticmethod
    @ensure_annotations
    def MGetLastDayOfPreviousMonth():
        """
        Purpose: Get the last day of the previous month.

        Outputs:
            1) str : The last day of the previous month in "YYYY-MM-DD" format.
        """
        # Get today's date
        today = datetime.today()

        # Get the first day of this month
        firstDayThisMonth = today.replace(day=1)

        # Get the last day of the previous month by subtracting one day from the first day of this month
        lastDayLastMonth = firstDayThisMonth - timedelta(days=1)

        # Format the last day of the previous month as "YYYY-MM-DD"
        strformattedLastDayLastMonth = lastDayLastMonth.strftime("%Y-%m-%d")

        return strformattedLastDayLastMonth


    @staticmethod
    @ensure_annotations
    def MGetFirstDayOfPreviousMonth():
        """
        Purpose: Get the first day of the previous month.

        Outputs:
            1) str : The first day of the previous month in "YYYY-MM-DD" format.
        """
        # Get today's date
        today = datetime.today()

        # Get the first day of this month
        firstDayThisMonth = today.replace(day=1)

        # Get the first day of the previous month by subtracting one day from the first day of this month
        firstDayLastMonth = (firstDayThisMonth - timedelta(days=1)).replace(day=1)

        # Format the first day of the previous month as "YYYY-MM-DD"
        strformattedFirstDayLastMonth = firstDayLastMonth.strftime("%m/%d/%Y")

        return strformattedFirstDayLastMonth

    @staticmethod
    @ensure_annotations
    def MFillMissingDates(liExchangeRates: dict, dates: list):
        """
        Purpose: Fill missing exchange rates in the provided dates with the last known or next known exchange rate.

        Inputs:
            1) liExchangeRates (list): List of tuples containing dates and their respective exchange rates.
            2) dates (list): List of dates for which exchange rates are required.

        Outputs:
            1) dict: Dictionary with dates as keys and exchange rates as values.
        """
        # Convert the list of tuples to a dictionary for easy access
        exchangeRatesDict = dict(liExchangeRates)
        dictfilledExchangeRates = {}
        
        lastKnownRate = None
        
        # Pre-compute next available rates
        nextKnownRates = {}
        sortedDates = sorted(dates)
        for i, dateStr in enumerate(sortedDates):
            if dateStr in exchangeRatesDict:
                rate = exchangeRatesDict[dateStr]
                for j in range(i - 1, -1, -1):
                    if sortedDates[j] not in exchangeRatesDict:
                        nextKnownRates[sortedDates[j]] = rate
                    else:
                        break
        
        # Fill missing rates
        for dateStr in dates:
            if dateStr in exchangeRatesDict:  # Check if date present in exchangeRatesDict
                lastKnownRate = exchangeRatesDict[dateStr]
            elif dateStr in nextKnownRates:
                lastKnownRate = nextKnownRates[dateStr]
            if lastKnownRate is not None:
                dictfilledExchangeRates[dateStr] = lastKnownRate
            else:
                print(f"No available exchange rate for {dateStr} and no previous or next data to use.")
        
        return dictfilledExchangeRates
    
    # @staticmethod
    # @ensure_annotations
    # def MFillMissingDates(liExchangeRates : dict, dates : list):
    #     """
    #     Purpose: Fill missing exchange rates in the provided dates with the last known exchange rate.

    #     Inputs:
    #         1) liExchangeRates (list): List of tuples containing dates and their respective exchange rates.
    #         2) dates (list)        : List of dates for which exchange rates are required.

    #     Outputs:
    #         1) dict: Dictionary with dates as keys and exchange rates as values.
    #     """
    #     # Convert the list of tuples to a dictionary for easy access
    #     exchangeRatesDict = dict(liExchangeRates)
    #     dictfilledExchangeRates = {}
        
    #     lastKnownRate = None
    #     # loop through all dates of previous month
    #     for dateStr in dates:
    #         if dateStr in exchangeRatesDict:    # Check if date present in exchangeRatesDict
    #             lastKnownRate = exchangeRatesDict[dateStr]
    #         if lastKnownRate is not None:   
    #             dictfilledExchangeRates[dateStr] = lastKnownRate
    #         else:
    #             print(f"No available exchange rate for {dateStr} and no previous data to use.")
        
    #     return dictfilledExchangeRates


    # @staticmethod
    # @ensure_annotations
    # def MGetHistoricalExchangeRates(dates : list):
    #     """
    #     Purpose: Get historical exchange rates for USD to CAD from Yahoo Finance.

    #     Inputs:
    #         1) dates (list): List of dates for which exchange rates are required.

    #     Outputs:
    #         1) dict: Dictionary with dates as keys and exchange rates as values.
    #     """
    #     # Initialize an empty dictionary to store exchange rates
    #     dictexchangeRates = {}
        
    #     # Construct the URL with proper encoding
    #     tickerSymbol = 'USDCAD=X'
    #     url = f'https://finance.yahoo.com/quote/{quote(tickerSymbol)}/history?p={quote(tickerSymbol)}'

    #     # Send a GET request to the URL with a User-Agent header
    #     headers = {
    #         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    #     }
    #     response = requests.get(url, headers=headers)

    #     # Parse the HTML content of the response
    #     soup = BeautifulSoup(response.content, 'html.parser')

    #     # Find the table containing the historical data
    #     table = soup.find('table')

    #     # If the table exists, proceed to extract exchange rates for each date
    #     if table:
    #         # Find the table body
    #         tbody = table.find('tbody')
    #         if tbody:
    #             # Iterate over table rows
    #             for row in tbody.find_all('tr'):
    #                 # Extract table cells
    #                 cells = row.find_all('td')
    #                 # Check if the row has data
    #                 if len(cells) >= 7:
    #                     # Extract the date in the format 'Month Day, Year'
    #                     rowDate = cells[0].text.strip()
    #                     # Extract the exchange rate for the date
    #                     exchangeRate = cells[4].text.strip().replace(',', '')
    #                     dateObj = datetime.strptime(rowDate, '%b %d, %Y')
    #                     dateObj = str(dateObj.strftime('%b %d, %Y'))
    #                     # Store the exchange rate in the dictionary if the date matches
    #                     if dateObj in dates:
    #                         dictexchangeRates[dateObj] = exchangeRate
    #     return dictexchangeRates


    @staticmethod
    @ensure_annotations
    def MConvertDateKeys(datesDict : dict):
        """
        Purpose: Convert date keys in the dictionary to the format '%d-%m-%Y'.

        Inputs:
            1) datesDict (dict): Dictionary with dates as keys and exchange rates as values.

        Outputs:
            1) dict: Dictionary with formatted date keys and exchange rates as values.
        """
        convertedDict = {}
        for dateStr, value in datesDict.items():
            # Convert the date string to a datetime object
            dateObj = datetime.strptime(dateStr, '%b %d, %Y')
            # Format the datetime object to dd-mm-yyyy format
            formattedDate = dateObj.strftime('%d-%m-%Y')
            # Add the formatted date as key to the new dictionary
            convertedDict[formattedDate] = value
        return convertedDict

    @staticmethod
    @ensure_annotations  # Assuming this is a decorator for type hints
    def MGetPreferredMonthDates(preferred_month: int) -> list:
        """
        Purpose: Get a list of dates for each day of a specified month.

        Args:
            preferred_month (int): The desired month (1-12).

        Outputs:
            list: A list of date strings in the format '%d-%m-%Y'.
        """

        if not 1 <= preferred_month <= 12:
            raise ValueError("Invalid month. Please enter a value between 1 and 12.")

        today = datetime.today()
        year = today.year  # Use current year as default

        import calendar

        firstDayOfPreferredMonth = datetime(year, preferred_month, 1)
        lastDayOfPreferredMonth = firstDayOfPreferredMonth + timedelta(days = (calendar.monthrange(year, preferred_month)[1]) - 1)

        dateList = []
        currentDate = firstDayOfPreferredMonth
        while currentDate <= lastDayOfPreferredMonth:
            dateList.append(currentDate.strftime('%d-%m-%Y'))
            currentDate += timedelta(days=1)

        return dateList

    @staticmethod
    @ensure_annotations
    def MGetLastMonthDates(strStartDate, strEndDate):
        """
        Purpose: Get a list of dates for each day of the last month.

        Outputs:
            1) list: A list of date strings in the format '%d-%m-%Y'.
        """
        startDate = datetime.strptime(strStartDate, '%d-%m-%Y')
        endDate = datetime.strptime(strEndDate, '%d-%m-%Y')

        dateList = []
        currentDate = startDate
        while currentDate <= endDate:
            dateList.append(currentDate.strftime('%d-%m-%Y'))
            currentDate += timedelta(days=1)

        return dateList


    @staticmethod
    @ensure_annotations
    def MGetExchangeRatesLastMonth(apiKey, strOrg, strStartDate, strEndDate):
        """
        Purpose: Get the exchange rates for each day of the last month in a single API request.

        Inputs:
            1) apiKey (str): Your Alpha Vantage API key.
            2) strOrg (str): The organization, either 'mexico', 'canada', or 'other'.

        Outputs:
            1) dict: A dictionary containing dates as keys (in the format '%d-%m-%Y') and exchange rates as values.
        """
        if strOrg.lower() == 'mexico':
            url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=MXN&to_symbol=USD&apikey={apiKey}"
        elif strOrg.lower() == 'usa':
            # For USD to USD, the exchange rate is always 1.
            url = None
        elif strOrg.lower() == 'canada':
            url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=CAD&to_symbol=USD&apikey={apiKey}"

        dictexchangeRates = {}
        lastMonthDates = CAmzB2CHelperFunc.MGetLastMonthDates(strStartDate, strEndDate)

        if strOrg.lower() == 'usa':
            # For 'usa', directly assign an exchange rate of 1 for all dates
            for date in lastMonthDates:
                dictexchangeRates[date] = 1.0
            return dictexchangeRates
        else:
            # If not 'usa', proceed with the API request
            response = requests.get(url)
            data = response.json()

            timeSeries = data.get("Time Series FX (Daily)", {})

            for date in lastMonthDates:
                # Convert date to the required format for the API response
                dateObj = datetime.strptime(date, '%d-%m-%Y')
                apiDateFormat = dateObj.strftime('%Y-%m-%d')
                
                if apiDateFormat in timeSeries:
                    exchangeRate = timeSeries[apiDateFormat]['4. close']
                    dictexchangeRates[date] = float(exchangeRate)

        return dictexchangeRates



    @staticmethod
    @ensure_annotations
    def MGetLastMonthName(strDate):
        """
        Get the name of the month and year from a given date in 'dd/mm/yyyy' format.

        Args:
            strDate (str): Date in the format 'dd/mm/yyyy'.

        Returns:
            tuple: A tuple containing the month (in words) and the year as separate strings.
        """
        try:
            # Convert the string date into a datetime object
            date_obj = datetime.strptime(strDate, '%d-%m-%Y')
            
            # Extract the month name and year as separate strings
            month_name = date_obj.strftime('%B')
            year = date_obj.strftime('%Y')
            
            return month_name, year
        
        except ValueError as e:
            print(f"Error: {e}. Please ensure the date is in 'dd/mm/yyyy' format.")
    
    @staticmethod
    @ensure_annotations
    def MGetLastMonthYear():
        """
        Get the year of the last month in YYYY format.

        Returns:
            str: Year of the last month in YYYY format.
        """
        today = datetime.today()
        last_month = today.replace(day=1) - timedelta(days=1)
        return last_month.strftime('%Y')

    @staticmethod
    @ensure_annotations
    def MCheckTaxColsAndDropZeroSumCols(df, tax_columns, tolerance):
        """
        Processes a DataFrame by dropping numeric columns with a sum of zero and checks the sum of specified tax columns.

        Args:
            df (pd.DataFrame): The input DataFrame.
            tax_columns (list): A list of tax column names to check.
            tolerance (float): The tolerance value for the sum check of tax columns.

        Returns:
            bool: True if the sum of tax columns is within the tolerance, False otherwise.
        """
        # Identify numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns

        # Identify columns where all values are 0
        cols_to_drop = [col for col in numeric_cols if (df[col] == 0).all()]
        
        # Drop cols with a zero sum 
        df.drop(columns=cols_to_drop, inplace=True)

        # Check if all specified tax columns exist in the DataFrame and remove missing columns from the list
        tax_columns = [col for col in tax_columns if col in df.columns]
        missing_columns = [col for col in tax_columns if col not in df.columns]
        if missing_columns:
            print(f"The following columns are missing and will be excluded from the check: {missing_columns}")
        # Check the tax columns sum if > tolerance then print message and return False
        if abs(df[tax_columns].sum().sum()) > tolerance:
            print("Kindly check the sum of 'product sales tax', 'shipping credits tax', 'giftwrap credits tax', 'marketplace withheld tax' columns ")
            return df, False
        print("Done and dusted.......")
        return df, True
    

    def MGetCountryAndStateName(state_code):
        """
        Given a state code (e.g., 'US-CA' for California, USA), return the full country name and state name.

        Args:
            state_code (str): The state code in the format 'country-subdivision' (e.g., 'US-CA').

        Returns:
            tuple: A tuple containing the country name and full state name.
        """
        try:
            # Ensure state_code is a string
            if not isinstance(state_code, str):
                return None, None

            # Lookup the subdivision (state) using the state code
            subdivision = pycountry.subdivisions.lookup(f'US-{state_code}')

            # Get the country code from the subdivision
            country_code = subdivision.country_code

            # Get the full country name using the country code
            country = pycountry.countries.get(alpha_2=country_code)
            if country is None:
                return None, None
            country_name = country.name

            # Get the full state name
            state_name = subdivision.name

            return country_name, state_name

        except LookupError:
            return None, None
    
    @staticmethod
    def MGetCountryAndState(city: str, state_code: str) -> tuple[str, str]:
        """
        This function retrieves country and full state name using Nominatim API.

        Inputs :
            city: The city name (string).
            state_code: The state code (string).

        Returns:
            A tuple with country and full state name, or (None, None) if not found.
        """
        if city == "Ramat Hasharon":
            return "Israel", "Ramat-Hasharon"
        if city == "Varna":
            return "Bulgaria", "Varna"
        if city == "Suncheon City":
            return "South Korea", "Jeollanam-do"
        if city == "Kennedy Town" and state_code == 'HK Island':
            return "Hong Kong", "Hong Kong Island"
        if city == "Scarborouugh" and state_code == 'Ontario':
            return "Canada", "Ontario"

        if len(state_code) == 2:
            country, state = CAmzB2CHelperFunc.MGetCountryAndStateName(state_code)
            if country == None and state == None: # if country and state
                # Combine city and state code for location query
                location = f"{city}, {state_code}"
                url = f"https://nominatim.openstreetmap.org/search?q={location}&format=json&limit=1"
                headers = {
                    "User-Agent": "YourApplicationName/Version (e.g., MyGeocoder/1.0)"
                }
                try:
                    response = requests.get(url, headers=headers)
                    data = response.json()

                    if data and len(data) > 0:
                        # Extract country and state from the address components (if available)
                        place = data[0]
                        display_name = place["display_name"]
                        parts = display_name.split(",")

                        state = parts[1].strip()
                        country = parts[-1].strip()
                        return country, state
                    else:
                        return None, None
                except Exception as e:
                    print(f"Error occurred during geocoding: {e}")
                    return None, None
            return country, state

        else:
            # Combine city and state code for location query
            location = f"{city}, {state_code}"
            url = f"https://nominatim.openstreetmap.org/search?q={location}&format=json&limit=1"
            headers = {
                "User-Agent": "YourApplicationName/Version (e.g., MyGeocoder/1.0)"
            }
            try:
                response = requests.get(url, headers=headers)
                data = response.json()

                if data and len(data) > 0:
                    # Extract country and state from the address components (if available)
                    place = data[0]
                    display_name = place["display_name"]
                    parts = display_name.split(",")

                    # state = parts[2].strip()
                    country = parts[-1].strip()
                    return country, state_code
                else:
                    return None, None
            except Exception as e:
                print(f"Error occurred during geocoding: {e}")
                return None, None


    @staticmethod
    def fetch_and_store(city: str, state_code: str, results: dict[tuple[str, str], tuple[str, str]]) -> None:
        """Helper function to fetch and store the country and state for a city and state code."""
        results[(city, state_code)] = CAmzB2CHelperFunc.MGetCountryAndState(city, state_code)
    
    @staticmethod
    def MGetAllCountriesAndStates(df: pd.DataFrame) -> pd.DataFrame:
        # Ensure all values are strings and handle missing values
        df['order city'] = df['order city'].fillna('').astype(str)
        df['order state'] = df['order state'].fillna('').astype(str)
        
        # Create a dictionary to store the results
        results = {}
        
        # Get unique city and state combinations
        unique_city_state = df[['order city', 'order state']].drop_duplicates().values.tolist()
        
        # Use ThreadPoolExecutor to fetch data concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(CAmzB2CHelperFunc.fetch_and_store, city, state_code, results): (city, state_code)
                for city, state_code in unique_city_state
            }
            
            # Ensure all futures complete
            for future in as_completed(futures):
                future.result()
        
        # Map the results back to the original DataFrame
        df['country'] = df.apply(lambda row: results[(row['order city'], row['order state'])][0], axis=1)
        df['state'] = df.apply(lambda row: results[(row['order city'], row['order state'])][1], axis=1)
        
        return df
    

    @staticmethod
    def MGetExchangeRatesFinalDict(strOrg, strStartDate, strEndDate):
        """
        Get the final dictionary of exchange rates for the previous month, filling in any missing dates.

        Outputs:
            dict: A dictionary with dates as keys and exchange rates as values, including filled missing dates.
        """
        # Get the previous months dates
        liLastMonthDates = CAmzB2CHelperFunc.MGetLastMonthDates(strStartDate, strEndDate)
        # Get the exchange rates
        dictExchangeRates = CAmzB2CHelperFunc.MGetExchangeRatesLastMonth('DXEBI58OSLKIBOQT', strOrg, strStartDate, strEndDate)
        # Fill the missing dates (Saturday-Sunday) in the exchange rate data with the previous friday's ex rate
        dictExchangeRates = CAmzB2CHelperFunc.MFillMissingDates(dictExchangeRates, liLastMonthDates)

        return dictExchangeRates

    @staticmethod
    @ensure_annotations
    def MVerifySums(df : pd.DataFrame, cols_to_sum : list, tolerance : float = 1e-10):
        """
        Verify if the sum of specified columns matches the sum of the 'total' column within a given tolerance.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            cols_to_sum (list): List of column names to sum.
            tolerance (float): The tolerance level for comparison. Default is 1e-10.

        Returns:
            bool: True if the sums match within the given tolerance, False otherwise.
        """
        if not df.empty:
            # Convert specified columns to numeric, coercing errors to NaN
            df[cols_to_sum] = df[cols_to_sum].apply(pd.to_numeric, errors='coerce')

            # Calculate the sum of all specified columns across all rows
            total_sum_of_columns = df[cols_to_sum].sum().sum()

            # Convert 'total' column to numeric, coercing errors to NaN
            df['total'] = df['total'].apply(pd.to_numeric, errors='coerce')

            # Calculate the sum of the 'total' column
            total_sum_of_total_col = df['total'].sum().sum()

            # Verify if the difference between the sums is within the tolerance
            return abs(total_sum_of_columns - total_sum_of_total_col) < tolerance
        else:
            print("No data available.")
            # If the DataFrame is empty, return False
    
    
    def MProcessCsvTillOrderFilter(strDateRangeFilePath, strDateColName, strSettleIdColName, strOrderIdColName, strOrg, cols_to_sum):
        """
        Process the CSV file by performing various operations like skipping rows, 
        formatting dates, converting columns to strings, and filtering data.

        Args:
            strDateRangeFilePath (str): The file path to the CSV file.
            strDateColName (str): The name of the date column.
            strSettleIdColName (str): The name of the settle ID column.
            strOrderIdColName (str): The name of the order ID column.
            strOrg (str): The organization name.
            cols_to_sum (list): List of column names to sum.

        Returns:
            pd.DataFrame: The processed DataFrame.
        """
        # Check Organization (Mexico, Canada, USA)
        if strOrg.lower() == 'mexico':
            # Custom date parser function
            # Function to clean and parse dates
            def custom_date_parser(date_str):
                try:
                    # Replace localized Spanish abbreviations (if present)
                    date_str = date_str.replace("a.m.", "AM").replace("p.m.", "PM")
                    # Parse the date using a fixed format
                    return datetime.strptime(date_str, "%d %b %Y %I:%M:%S %p %Z%z")
                except Exception as e:
                    print(f"Failed to parse date: {date_str}. Error: {e}")
                    return pd.NaT
            df = pd.read_csv(strDateRangeFilePath, skiprows=7, parse_dates=['fecha/hora'], date_parser = custom_date_parser)

            df.rename(columns={'fecha/hora':'date/time', 'Id. de liquidación':'settlement id', 'tipo':'type', 'Id. del pedido':'order id', 'sku':'sku', 'descripción':'description', 'cantidad':'quantity', 'marketplace':'marketplace', 'cumplimiento':'fulfillment', 'ciudad del pedido':'order city', 'estado del pedido':'order state', 'código postal del pedido':'order postal', 'modelo de recaudación de impuestos':'tax collection model', 'ventas de productos':'product sales', 'impuesto de ventas de productos':'product sales tax', 'créditos de envío':'shipping credits', 'impuesto de abono de envío':'shipping credits tax', 'créditos por envoltorio de regalo':'gift wrap credits', 'impuesto de créditos de envoltura':'giftwrap credits tax', 'Tarifa reglamentaria':'Regulatory Fee', 'Impuesto sobre tarifa reglamentaria':'Tax On Regulatory Fee', 'descuentos promocionales':'promotional rebates', 'impuesto de reembolsos promocionales':'promotional rebates tax', 'impuesto de retenciones en la plataforma':'marketplace withheld tax', 'tarifas de venta':'selling fees', 'tarifas fba':'fba fees', 'tarifas de otra transacción':'other transaction fees', 'otro':'other', 'total':'total'}, inplace=True)
        else:
            # Read the CSV file, skipping the first 7 rows
            df = pd.read_csv(strDateRangeFilePath, skiprows=7, parse_dates=[strDateColName])
        
        if strOrg.lower() == 'canada':
            df.rename(columns = {'gift wrap credits tax' : 'giftwrap credits tax', 'Regulatory fee' : 'Regulatory Fee', 'Tax on regulatory fee' : 'Tax On Regulatory Fee'}, inplace = True)
            # Check if the column is of string type before replacing text
            if pd.api.types.is_string_dtype(df[strDateColName]):
                # Preprocess the date column to replace 'a.m.' and 'p.m.' and remove the time zone
                df[strDateColName] = df[strDateColName].str.replace("a.m.", "AM").str.replace("p.m.", "PM").str.replace(" PDT", "").str.replace(" PST", "").str.strip()
                try:
                    # Convert the cleaned column to datetime
                    df[strDateColName] = pd.to_datetime(df[strDateColName], format="%b %d, %Y %I:%M:%S %p")
                except:
                    print("not a %b %d, %Y %I:%M:%S %p format")
                    pass
            elif pd.api.types.is_datetime64_any_dtype(df[strDateColName]):
                pass
            else:
                print("Invalid date format in column: ", strDateColName)
        
        if strOrg.lower() == 'mexico':
            df['type'] = df['type'].str.replace('Pedido', 'Order')
            # Remove commas and convert to float
            try:
                df['product sales'] = df['product sales'].str.replace(',', '').astype(float)
                df['total'] = df['total'].str.replace(',', '').astype(float)
            except:
                df['product sales'] = df['product sales'].astype(float)
                df['total'] = df['total'].str.replace(',', '').astype(float)
        
        # Format date column to ‘dd-mm-yyyy’
        # Format the strDateColName column to dd-mm-yyyy format
        df[strDateColName] = df[strDateColName].dt.strftime('%d-%m-%Y')

        # Verify Sums If Other cols - total col = 0?
        # Calculate the sum of all specified columns(cols_to_sum) - total column
        isZero = CAmzB2CHelperFunc.MVerifySums(df, cols_to_sum)
        objLogger.logInfo('Verifying the sum of all specified columns(cols_to_sum) - total column sum is zero')
        # Check if tax columns sum is zero
        tolerance = 1e-10
        if isZero == True:
            # Convert columns to string
            # Convert strSettleIdColName and 'order id' columns to strings
            df[strSettleIdColName] = df[strSettleIdColName].astype(str)
            df[strOrderIdColName] = df[strOrderIdColName].astype(str)
            
            # Create ‘Invoice Number’ (Settlement ID - Order ID)
            # Combine strSettleIdColName and strOrderIdColName columns with '-' separator and create a new column 'Invoice Number'
            df['Invoice Number'] = df[strSettleIdColName] + '-' + df[strOrderIdColName]

            # Filter rows where ‘type’ column value = ‘Order’
            # Filter the DataFrame by keeping only the rows where the 'type' column has the value 'Order'
            df = df[df['type'] == 'Order']

            # Return processed dataframe
            return df
        else:
            objLogger.logInfo("Error: The sum of the specified columns does not match the sum of the 'total' column.")
            print("Error: The sum of the specified columns does not match the sum of the 'total' column.")
            return None

        
    
