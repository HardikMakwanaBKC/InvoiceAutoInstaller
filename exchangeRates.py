import requests
from datetime import datetime, timedelta

class CExchangeRatesHelper:
    @staticmethod
    def MGetLastMonthDates():
        """
        Purpose: Get a list of dates for each day of the last month.

        Outputs:
            1) list: A list of date strings in the format '%m-%d-%Y'.
        """
        today = datetime.today()
        first_day_this_month = today.replace(day=1)
        last_day_last_month = first_day_this_month - timedelta(days=1)
        first_day_last_month = last_day_last_month.replace(day=1)

        dateList = []
        currentDate = first_day_last_month
        while currentDate <= last_day_last_month:
            dateList.append(currentDate.strftime('%m-%d-%Y'))
            currentDate += timedelta(days=1)

        return dateList

    @staticmethod
    def MGetExchangeRatesLastMonth(apiKey):
        """
        Purpose: Get the exchange rates for each day of the last month in a single API request.

        Inputs:
            1) apiKey (str): Your Alpha Vantage API key.
            2) strOrg (str): The organization, either 'mexico', 'canada', or 'other'.

        Outputs:
            1) dict: A dictionary containing dates as keys (in the format '%d-%m-%Y') and exchange rates as values.
        """
        url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol=CAD&apikey={apiKey}"

        dictexchangeRates = {}
        lastMonthDates = CExchangeRatesHelper.MGetLastMonthDates()
        
        # If not 'usa', proceed with the API request
        response = requests.get(url)
        data = response.json()

        timeSeries = data.get("Time Series FX (Daily)", {})

        for date in lastMonthDates:
            # Convert date to the required format for the API response
            dateObj = datetime.strptime(date, '%m-%d-%Y')
            apiDateFormat = dateObj.strftime('%Y-%m-%d')
            
            if apiDateFormat in timeSeries:
                exchangeRate = timeSeries[apiDateFormat]['4. close']
                dictexchangeRates[date] = float(exchangeRate)

        return dictexchangeRates
    
    @staticmethod
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
    
    @staticmethod
    def MGetExchangeRatesFinalDict():
        """
        Get the final dictionary of exchange rates for the previous month, filling in any missing dates.

        Outputs:
            dict: A dictionary with dates as keys and exchange rates as values, including filled missing dates.
        """
        # Get the previous months dates
        liLastMonthDates = CExchangeRatesHelper.MGetLastMonthDates()
        # Get the exchange rates
        dictExchangeRates = CExchangeRatesHelper.MGetExchangeRatesLastMonth('DXEBI58OSLKIBOQT')
        # Fill the missing dates (Saturday-Sunday) in the exchange rate data with the previous friday's ex rate
        dictExchangeRates = CExchangeRatesHelper.MFillMissingDates(dictExchangeRates, liLastMonthDates)

        return dictExchangeRates
    
# dictExchangeRates = CExchangeRatesHelper.MGetExchangeRatesFinalDict()
# print(dictExchangeRates)