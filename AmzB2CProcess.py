import pandas as pd
from ensure import ensure_annotations
from AmzB2CHelperFunc import CAmzB2CHelperFunc
from logUtility import CLogUtility

objLogger  = CLogUtility()

class CAMZB2C:
    """
    CAMZB2C class provides methods to process sales orders, invoices, and credit notes for Amazon USA.

    Methods:
        - MProcessSalesOrderCsv(strDateRangeFilePath: str) -> pd.DataFrame:
            Process sales orders from a CSV file and generates a CSV file with processed data.

        - MProcessInvoiceCsv(strDateRangeFilePath: str) -> pd.DataFrame:
            Process invoices from a CSV file and generates a CSV file with processed data.

        - MProcessCreditNoteCsv(strDateRangeFilePath: str) -> pd.DataFrame:
            Process credit notes from a CSV file and generates a CSV file with processed data.
    """

    @staticmethod
    @ensure_annotations
    def MProcessSalesOrderCsv(strDateRangeFilePath : str, strOutputFolderPath : str, dictExchangeRates : dict, cols_to_sum : list, liColsToDrop : list, tax_columns : list, dictSKUMapping : dict, strOrg : str):
        """
        Process sales orders from a CSV file and generates a CSV file with processed data.

        Inputs:
            strDateRangeFilePath (str): The file path to the CSV file (Date Range).
            dictExchangeRates (dict): A dictionary mapping dates to exchange rates.
            cols_to_sum (list): A list of column names whose sums need to be verified.
            liColsToDrop (list): A list of column names to drop from the DataFrame.
            tax_columns (list): A list of tax column names to be checked against a tolerance value.

        Outputs:
            pd.DataFrame: A formatted DataFrame with processed sales order data.
            
        Returns:
            pd.DataFrame: A formatted DataFrame with processed sales order data if the conditions are met.
            str: A message indicating issues with the tax columns sum if the conditions are not met.
        """
        try:
            objLogger.logInfo('Processing sales orders.....')
            # Processing data
            df = CAmzB2CHelperFunc.MProcessCsvTillOrderFilter(strDateRangeFilePath, 'date/time', 'settlement id', 'order id', strOrg = strOrg, cols_to_sum = cols_to_sum)

            # If DataFrame is not None, then process the data
            if df is not None:
                # Filter Rows Are product sales = 0?
                # Eliminating data from the product sales column that is zero in value
                df = df[df['product sales'] != 0 ]
                objLogger.logInfo('Eliminating data from the product sales column that is zero in value')

                # Convert the 'other' and 'total' columns to string to remove commas from the values
                df['product sales'] = df['product sales'].astype('str')
                df['other'] = df['other'].astype('str')
                df['total'] = df['total'].astype('str')

                tolerance = 1e-10
                objLogger.logInfo('Verifying the sum of tax columns is zero')
                # Verify Sums Are tax columns sum Zero?
                # Drop cols whose sum zero and check sum of tax cols is zero
                df, bIsZero = CAmzB2CHelperFunc.MCheckTaxColsAndDropZeroSumCols(df, tax_columns, tolerance)

                # if sum of tax cols is not zero then return msg
                if bIsZero == False:
                    objLogger.logInfo('The sum of columns(product sales tax, shipping credits tax, giftwrap credits tax, marketplace withheld tax) is not zero')
                    return "Kindly check the sum of 'product sales tax', 'shipping credits tax', 'giftwrap credits tax', 'marketplace withheld tax' columns"
                objLogger.logInfo('The sum of columns(product sales tax, shipping credits tax, giftwrap credits tax, marketplace withheld tax) is zero')

                # Get states and country
                # Apply the function to each row and update the DataFrame by adding country and state columns
                df = CAmzB2CHelperFunc.MGetAllCountriesAndStates(df)
                objLogger.logInfo('Applying the function to each row and updating the DataFrame by adding country and state columns')
                
                # Get exchange rates of the provided date range
                if dictExchangeRates:
                    # Map the exchange rate data to the existing
                    # Insert the new column at the specified index
                    df['Exchange Rate'] = df['date/time'].map(dictExchangeRates)
                    objLogger.logInfo('Mapping the exchange rate data to the existing')
                else:
                    objLogger.logError('No exchange rate data available for the specified dates')
                    print("No exchange rate data available for the specified dates")
                if 'promotional rebates' not in df.columns:
                    df['promotional rebates'] = 0
                if 'shipping credits' not in df.columns:
                    df['shipping credits'] = 0
                # 'shipping credits' col = 'shipping credits' + 'promotional rebates'
                df['shipping credits'] = df['shipping credits'] + df['promotional rebates']

                # Insert the columns in order
                # Renaming the date/time col
                df.rename({'date/time':'Date'}, axis=1, inplace=True)
                df.insert(1, 'Shipment Date', df['Date'])
                df.insert(2, 'Sales Order Number', df['Invoice Number'])
                df.insert(3, 'Status', 'Confirmed')
                if strOrg.lower() == 'canada':
                    df.insert(4, 'Customer Name', 'Amazon CA')
                elif strOrg.lower() == 'mexico':
                    df.insert(4, 'Customer Name', 'Amazon Mexico')
                else:
                    df.insert(4, 'Customer Name', 'Amazon USA')
                df.insert(5, 'Sales Order Level Tax', '')
                df.insert(6, 'Sales Order Level Tax %', '')
                df.insert(7, 'Sales Order Level Tax Authority', 'Canada Revenue Agency')
                df.insert(8, 'Sales Order Level Tax Exemption Reason', 'EXPORT')
                df.insert(9, 'PurchaseOrder', '')
                df.insert(10, 'Template Name', 'Standard Template')
                if strOrg.lower() == 'canada':
                    df.insert(11, 'Currency Code', 'CAD')
                elif strOrg.lower() == 'mexico':
                    df.insert(11, 'Currency Code', 'MXN')
                else:
                    df.insert(11, 'Currency Code', 'USD')

                # To move Exchange Rate
                liOrderNumber = df['Exchange Rate']
                # Remove the column from its original position
                df.drop(columns=['Exchange Rate'], inplace=True)
                # Insert the column at the desired index
                df.insert(12, 'Exchange Rate', liOrderNumber)

                df.insert(13, 'Discount Type', '')
                df.insert(14, 'Is Discount BeforeTax', '')
                df.insert(15, 'Entity Discount Percent', '')
                df.insert(16, 'Item Name', df['sku'] + '-AMZUS')
                df.insert(17, 'SKU', df['sku'] + '-AMZUS')
                df.insert(18, 'Item Desc', df['description'])
                df.insert(19, 'Quantity', df['quantity'])
                df.insert(20, 'Warehouse Name', 'Amazon FBA US')
                df.insert(21, 'Usage unit', '')
                df.insert(22, 'Item Price', pd.to_numeric(df['product sales'], errors='coerce') / df['Quantity'])
                df.insert(23, 'Item Type', 'Goods')    
                df.insert(24, 'Discount', '')    							
                df.insert(25, 'Discount Amount', '')    
                df.insert(26, 'Item Tax', '')    
                df.insert(27, 'Item Tax %', '')    
                df.insert(28, 'Item Tax Authority', '')    
                df.insert(29, 'Item Tax Exemption Reason', '')    
                df.insert(30, 'Shipping Charge', '')    
                df.insert(31, 'Adjustment', '')    
                df.insert(32, 'Adjustment Description', '')    
                df.insert(33, 'Sales Person', '')
                df.insert(34, 'Notes', '')
                df.insert(35, 'Terms & Conditions', '')
                df.insert(36, 'Sales Channel', 'Amazon US')
                df.insert(37, 'Department', 'Sales')
                df.insert(38, 'Products', df['sku'])
                df.insert(39, 'Ship City', df['order city'])
                df.insert(40, 'Ship State', df['state'])
                df.insert(41, 'Ship Country', df['country'])

                # Replacing "United States" to "U.S.A" in Country col
                df['Ship Country'] = df['Ship Country'].str.replace("United States", "U.S.A")

                df.insert(42, 'Billing City', df['Ship City'])
                df.insert(43, 'Billing State', df['Ship State'])
                df.insert(44, 'Billing Country', df['Ship Country'])
                df.insert(45, 'Custom Field Value9', '')
                df.insert(46, 'Custom Field Value10', '')
                df.insert(47, 'Project Name', '')

                # removing Invoice Number col
                df.drop(columns=['Invoice Number'], inplace=True)

                # Map values in 'Item Name' column using SKU mapping dictionary
                # Mapping dictSKUMapping into SKU col
                df['Item Name'] = df['Item Name'].map(dictSKUMapping)
                
                # if 'shipping credits' != 0: add line items having “Shipping and Handling (Outbound)“ value in columns ”Item Name”, ”SKU”, ”Description”
                # Check if the 'shipping credits' column exists in the DataFrame
                if 'shipping credits' in df.columns:
                    # Add new rows where 'shipping credits' non zero
                    for i, row in df.iterrows():
                        if row['shipping credits'] != 0:
                            objLogger.logInfo("Processing shipping credits")
                            dictShippingSalesOrder = {
                                'Date': row['Date'],
                                'Shipment Date': row['Shipment Date'],
                                'Sales Order Number': row['Sales Order Number'],
                                'Status': 'Confirmed',
                                'Customer Name': None,
                                'Sales Order Level Tax Authority': 'Canada Revenue Agency',
                                'Sales Order Level Tax Exemption Reason': 'EXPORT',
                                'Template Name': 'Standard Template',
                                'Currency Code': None,
                                'Exchange Rate': row['Exchange Rate'],
                                'Item Name': 'Shipping and Handling (Outbound)',
                                'SKU': 'Shipping and Handling (Outbound)',
                                'Item Desc': 'Shipping and Handling (Outbound)',
                                'Quantity' : '1',
                                'Warehouse Name': 'Amazon FBA US',
                                'Item Price': row['shipping credits'],
                                'Item Type': 'Service',
                                'Sales Channel': 'Amazon US',
                                'Department': 'Sales',
                                'Products': row['sku'],
                                'Ship City': row['Ship City'],
                                'Ship State': row['Ship State'],
                                'Ship Country' : row['Ship Country'],
                                'Billing City': row['Ship City'],
                                'Billing State': row['Ship State'],
                                'Billing Country': row['Ship Country']
                            }
                            # Set 'Customer Name' based on the condition
                            if strOrg.lower() == 'canada':
                                dictShippingSalesOrder['Customer Name'] = 'Amazon CA'
                                dictShippingSalesOrder['Currency Code'] = 'CAD'
                            elif strOrg.lower() == 'mexico':
                                dictShippingSalesOrder['Customer Name'] = 'Amazon Mexico'
                                dictShippingSalesOrder['Currency Code'] = 'MXN'
                            else:
                                dictShippingSalesOrder['Customer Name'] = 'Amazon USA'
                                dictShippingSalesOrder['Currency Code'] = 'USD'
                            df = pd.concat([df.iloc[:i + 1], pd.DataFrame([dictShippingSalesOrder]), df.iloc[i + 1:]]).reset_index(drop=True)
                
                # if 'gift wrap credits' != 0: add line items having “gift wrap credits” va;lue in columns “Item Name”, “SKU”, “Description”
                # Check if the 'gift wrap credits' column exists in the DataFrame
                if 'gift wrap credits' in df.columns:
                    # Getting df where gift wrap credits not NAN
                    giftWrapDf = df[df['gift wrap credits'].notna()]
                    # Add new rows where 'gift wrap credits' is non zero
                    for i, row in giftWrapDf.iterrows():
                        if row['gift wrap credits'] != 0:
                            objLogger.logInfo("Processing gift wrap credits")
                            new_row = {
                                'Date': row['Date'],
                                'Shipment Date': row['Shipment Date'],
                                'Sales Order Number': row['Sales Order Number'],
                                'Status': 'Confirmed',
                                'Customer Name': None,
                                'Sales Order Level Tax Authority': 'Canada Revenue Agency',
                                'Sales Order Level Tax Exemption Reason': 'EXPORT',
                                'Template Name': 'Standard Template',
                                'Currency Code': None,
                                'Exchange Rate': row['Exchange Rate'],
                                'Item Name': 'Gift Wrap - Amz',
                                'SKU': 'Gift Wrap - Amz',
                                'Item Desc': 'Gift Wrap - Amz',
                                'Quantity' : '1',
                                'Warehouse Name': 'Amazon FBA US',
                                'Item Price': row['gift wrap credits'],
                                'Item Type': 'Service',
                                'Sales Channel': 'Amazon US',
                                'Department': 'Sales',
                                'Products': row['sku'],
                                'Ship City': row['Ship City'],
                                'Ship State': row['Ship State'],
                                'Ship Country' : row['Ship Country'],
                                'Billing City': row['Ship City'],
                                'Billing State': row['Ship State'],
                                'Billing Country': row['Ship Country']
                            }
                            # Set 'Customer Name' based on the condition
                            if strOrg.lower() == 'canada':
                                new_row['Customer Name'] = 'Amazon CA'
                                new_row['Currency Code'] = 'CAD'
                            elif strOrg.lower() == 'mexico':
                                new_row['Customer Name'] = 'Amazon Mexico'
                                new_row['Currency Code'] = 'MXN'
                            else:
                                new_row['Customer Name'] = 'Amazon USA'
                                new_row['Currency Code'] = 'USD'
                            df = pd.concat([df.iloc[:i + 1], pd.DataFrame([new_row]), df.iloc[i + 1:]]).reset_index(drop=True)

                # Check if the specified columns exist in the DataFrame
                existing_cols_to_drop = [col for col in liColsToDrop if col in df.columns]

                # Drop the unnecessary columns
                # Drop the existing columns
                if existing_cols_to_drop:
                    df.drop(columns=existing_cols_to_drop, inplace=True)
                    objLogger.logInfo(f"Dropping the following columns: {existing_cols_to_drop}")
                    print(f"Dropped the following columns: {existing_cols_to_drop}")
                else:
                    objLogger.logInfo("No columns to drop exist in the DataFrame.")
                    print("No columns to drop from the specified list exist in the DataFrame.")
                
                # Sort data by 'Date', 'Invoice Number'
                df_sorted = df.sort_values(by=['Date', 'Sales Order Number'])
                
                df_sorted.reset_index(drop=True, inplace=True)

                # getting first value of 'Date' column
                first_value = df_sorted['Date'].iloc[0]
                strMonth, strYear = CAmzB2CHelperFunc.MGetLastMonthName(first_value)

                strOutputFilePath = rf'{strOutputFolderPath}\{strMonth} Sales Order {strYear}.csv'
                # strOutputFolderPath = r'2_Data\2_Output'
                # Save the final DataFrame to the csv file.
                df_sorted.to_csv(strOutputFilePath, index=False)
                objLogger.logInfo(f"The output CSV file has been saved at: {strOutputFilePath}")
                return df_sorted, strOutputFilePath
            else:
                objLogger.logInfo("Error: The sum of the specified columns does not match the sum of the 'total' column.")
                print("Error: The sum of the specified columns does not match the sum of the 'total' column.")
                print("No data available.")
                return None, None
        except Exception as e:
            objLogger.logError(f"An error occurred: {e}")
            print(f"An error occurred: {e}")


    @staticmethod
    @ensure_annotations
    def MProcessInvoiceCsv(strDateRangeFilePath : str, strOutputFolderPath : str, dictExchangeRates : dict, cols_to_sum : list, liColsToDrop : list, tax_columns : list, dictSKUMapping : dict, strOrg : str):
        """
        Process invoices from a CSV file and generates a CSV file with processed data.

        Inputs:
            strDateRangeFilePath (str): The file path to the CSV file (Date Range).
            dictExchangeRates (dict): A dictionary mapping dates to exchange rates.
            cols_to_sum (list): A list of column names whose sums need to be verified.
            liColsToDrop (list): A list of column names to drop from the DataFrame.
            tax_columns (list): A list of tax column names to be checked against a tolerance value.

        Outputs:
            pd.DataFrame: A formatted DataFrame with processed invoice data.
            
        Returns:
            pd.DataFrame: A formatted DataFrame with processed invoice data if the conditions are met.
            str: A message indicating issues with the tax columns sum if the conditions are not met.
        """
        try:
            objLogger.logInfo("Processing invoice csv.....")
            # Processing data
            df = CAmzB2CHelperFunc.MProcessCsvTillOrderFilter(strDateRangeFilePath, 'date/time', 'settlement id', 'order id', strOrg = strOrg, cols_to_sum = cols_to_sum)

            # If DataFrame is not None, then process the data
            if df is not None:
                # Filter Rows Are product sales = 0?
                # Eliminating data from the product sales column that is zero in value
                df = df[df['product sales'] != 0 ]
                objLogger.logInfo("Eliminating data from the product sales column that is zero in value")
                
                # Convert the 'other' and 'total' columns to string
                df['product sales'] = df['product sales'].astype('str')
                df['other'] = df['other'].astype('str')
                df['total'] = df['total'].astype('str')

                tolerance = 1e-10
                objLogger.logInfo('Verifying the sum of tax columns is zero')
                # Verify Sums Are tax columns sum Zero?
                # Drop cols whose sum zero and check sum of tax cols is zero
                df, bIsZero = CAmzB2CHelperFunc.MCheckTaxColsAndDropZeroSumCols(df, tax_columns, tolerance)

                # if sum of tax cols is not zero then return msg
                if bIsZero == False:
                    objLogger.logInfo('The sum of columns(product sales tax, shipping credits tax, giftwrap credits tax, marketplace withheld tax) is not zero')
                    return "Kindly check the sum of 'product sales tax', 'shipping credits tax', 'giftwrap credits tax', 'marketplace withheld tax' columns"
                objLogger.logInfo('The sum of columns(product sales tax, shipping credits tax, giftwrap credits tax, marketplace withheld tax) is zero')

                # Get states and country
                # Apply the function to each row and update the DataFrame by adding country and state columns
                df = CAmzB2CHelperFunc.MGetAllCountriesAndStates(df)
                objLogger.logInfo('Applying the function to each row and updating the DataFrame by adding country and state columns')

                # Get exchange rates of the provided date range
                if dictExchangeRates:
                    # Map the exchange rate data to the existing 
                    # Insert the new column at the specified index
                    df['Exchange Rate'] = df['date/time'].map(dictExchangeRates)
                    objLogger.logInfo('Mapping the exchange rate data to the existing')
                else:
                    objLogger.logError('No exchange rate data available for the specified dates')
                    print("No exchange rate data available for the specified dates")

                if 'promotional rebates' not in df.columns:
                    df['promotional rebates'] = 0
                if 'shipping credits' not in df.columns:
                    df['shipping credits'] = 0
                # 'shipping credits' col = 'shipping credits' + 'promotional rebates'
                df['shipping credits'] = df['shipping credits'] + df['promotional rebates']

                # Renaming the date/time col
                df.rename({'date/time' : 'Invoice Date'}, axis=1, inplace=True)
                
                # To move Invoice Number
                liInvoiceNum = df['Invoice Number']
                # Remove the column from its original position
                df.drop(columns=['Invoice Number'], inplace=True)
                # Insert the column at the desired index
                df.insert(1, 'Invoice Number', liInvoiceNum)

                df.insert(2, 'Estimate Number', df['Invoice Number'])
                df.insert(3, 'Invoice Status', 'Open')
                if strOrg.lower() == 'canada':
                    df.insert(4, 'Customer Name', 'Amazon CA')
                elif strOrg.lower() == 'mexico':
                    df.insert(4, 'Customer Name', 'Amazon Mexico')
                else:
                    df.insert(4, 'Customer Name', 'Amazon USA')
                df.insert(5, 'Due Date', '')
                df.insert(6, 'PurchaseOrder', df['Estimate Number'])
                df.insert(7, 'Template Name', 'Standard Template')

                if strOrg.lower() == 'canada':
                    df.insert(8, 'Currency Code', 'CAD')
                elif strOrg.lower() == 'mexico':
                    df.insert(8, 'Currency Code', 'MXN')
                else:
                    df.insert(8, 'Currency Code', 'USD')

                # To move Exchange Rate
                liOrderNumber = df['Exchange Rate']
                # Remove the column from its original position
                df.drop(columns=['Exchange Rate'], inplace=True)
                # Insert the column at the desired index
                df.insert(9, 'Exchange Rate', liOrderNumber)

                df.insert(10, 'Item Name', df['sku'] + '-AMZUS')
                df.insert(11, 'SKU', df['sku'] + '-AMZUS')
                df.insert(12, 'Item Desc', df['description'])
                df.insert(13, 'Quantity', df['quantity'])
                df.insert(14, 'Item Price', pd.to_numeric(df['product sales'], errors='coerce') / df['quantity'])
                df.insert(15, 'Item Type', 'Goods')
                df.insert(16, 'Discount(%)', '')
                df.insert(17, 'Item Tax', '')
                df.insert(18, 'Item Tax %', '')
                df.insert(19, 'Item Tax Authority', 'Canada')    
                df.insert(20, 'Item Tax Exemption Reason', 'Export')  
                df.insert(21, 'Notes', '')    
                df.insert(22, 'Terms & Conditions', '')    
                df.insert(23, 'Invoice Level Tax', '')    
                df.insert(24, 'Invoice Level Tax %', '')
                df.insert(25, 'Invoice Level Tax Authority', 'Canada')    
                df.insert(26, 'Invoice Level Tax Exemption Reason', 'Export') 
                df.insert(27, 'Sales Channel', 'Amazon US') 
                df.insert(28, 'Department', 'Sales')
                df.insert(29, 'Products', df['sku'])
                df.insert(30, 'Shipping City', df['order city'])    
                df.insert(31, 'Shipping State', df['state'])
                df.insert(32, 'Shipping Country', df['country'])
                df['Shipping Country'] = df['Shipping Country'].str.replace("United States", "U.S.A")
                df.insert(33, 'Billing City', df['Shipping City'])    
                df.insert(34, 'Billing State', df['Shipping State'])
                df.insert(35, 'Billing Country', df['Shipping Country'])
                df.insert(36, 'Warehouse Name', 'Amazon FBA US')
                
                # Map values in 'Item Name' column using SKU mapping dictionary
                # Mapping dictSKUMapping into SKU col
                df['Item Name'] = df['Item Name'].map(dictSKUMapping)

                # if 'shipping credits' != 0: add line items having “Shipping and Handling (Outbound)“ value in columns ”Item Name”, ”SKU”, ”Description”
                # Check if the 'shipping credits' column exists in the DataFrame
                if 'shipping credits' in df.columns:
                    # Add new rows where 'shipping credits' non zero
                    for i, row in df.iterrows():
                        if row['shipping credits'] != 0:
                            objLogger.logInfo("Processing shipping credits")
                            new_row = {
                                'Invoice Date': row['Invoice Date'],
                                'Invoice Number': row['Invoice Number'],
                                'Estimate Number': row['Estimate Number'],
                                'Invoice Status': 'Open',
                                'Customer Name': None,
                                'PurchaseOrder': row['Estimate Number'],
                                'Template Name': 'Standard Template',
                                'Currency Code': None,
                                'Exchange Rate': row['Exchange Rate'],
                                'Item Name': 'Shipping and Handling (Outbound)',
                                'SKU': 'Shipping and Handling (Outbound)',
                                'Item Desc': 'Shipping and Handling (Outbound)',
                                'Quantity': '1',
                                'Item Price': row['shipping credits'],
                                'Item Type': 'Service',
                                'Item Tax Authority': 'Canada',
                                'Item Tax Exemption Reason': 'Export',
                                'Invoice Level Tax Authority': 'Canada',
                                'Invoice Level Tax Exemption Reason': 'Export',
                                'Sales Channel': 'Amazon US',
                                'Department': 'Sales',
                                'Products': row['sku'],
                                'Shipping City': row['Shipping City'],
                                'Shipping State': row['Shipping State'],
                                'Shipping Country': row['Shipping Country'],
                                'Billing City': row['Shipping City'],
                                'Billing State': row['Shipping State'],
                                'Billing Country': row['Shipping Country'],
                                'Warehouse Name': 'Amazon FBA US',
                            }
                            # Set 'Customer Name' based on the condition
                            if strOrg.lower() == 'canada':
                                new_row['Customer Name'] = 'Amazon CA'
                            elif strOrg.lower() == 'mexico':
                                new_row['Customer Name'] = 'Amazon Mexico'
                            else:
                                new_row['Customer Name'] = 'Amazon USA'
                            
                            # Set 'Customer Name' based on the condition
                            if strOrg.lower() == 'canada':
                                new_row['Currency Code'] = 'CAD'
                            elif strOrg.lower() == 'mexico':
                                new_row['Currency Code'] = 'MXN'
                            else:
                                new_row['Currency Code'] = 'USD'
                            df = pd.concat([df.iloc[:i + 1], pd.DataFrame([new_row]), df.iloc[i + 1:]]).reset_index(drop=True)

                # if 'gift wrap credits' != 0: add line items having “gift wrap credits” va;lue in columns “Item Name”, “SKU”, “Description”
                # Check if the 'gift wrap credits' column exists in the DataFrame
                if 'gift wrap credits' in df.columns:
                    # Getting df where gift wrap credits not NAN
                    giftWrapDf = df[df['gift wrap credits'].notna()]
                    # Add new rows where 'gift wrap credits' is non zero
                    for i, row in giftWrapDf.iterrows():
                        if row['gift wrap credits'] != 0:
                            objLogger.logInfo("Processing gift wrap credits")
                            new_row = {
                                'Invoice Date': row['Invoice Date'],
                                'Invoice Number': row['Invoice Number'],
                                'Estimate Number': row['Estimate Number'],
                                'Invoice Status': 'Open',
                                'Customer Name': None,
                                'PurchaseOrder': row['Estimate Number'],
                                'Template Name': 'Standard Template',
                                'Currency Code': None,
                                'Exchange Rate': row['Exchange Rate'],
                                'Item Name': 'Gift Wrap - Amz',
                                'SKU': 'Gift Wrap - Amz',
                                'Item Desc': 'Gift Wrap - Amz',
                                'Quantity': '1',
                                'Item Price': row['gift wrap credits'],
                                'Item Type': 'Service',
                                'Item Tax Authority': 'Canada',
                                'Item Tax Exemption Reason': 'Export',
                                'Invoice Level Tax Authority': 'Canada',
                                'Invoice Level Tax Exemption Reason': 'Export',
                                'Sales Channel': 'Amazon US',
                                'Department': 'Sales',
                                'Products': row['sku'],
                                'Shipping City': row['Shipping City'],
                                'Shipping State': row['Shipping State'],
                                'Shipping Country': row['Shipping Country'],
                                'Billing City': row['Shipping City'],
                                'Billing State': row['Shipping State'],
                                'Billing Country': row['Shipping Country'],
                                'Warehouse Name': 'Amazon FBA US',
                            }
                            # Set 'Customer Name' based on the condition
                            if strOrg.lower() == 'canada':
                                new_row['Customer Name'] = 'Amazon CA'
                            elif strOrg.lower() == 'mexico':
                                new_row['Customer Name'] = 'Amazon Mexico'
                            else:
                                new_row['Customer Name'] = 'Amazon USA'
                            
                            # Set 'Customer Name' based on the condition
                            if strOrg.lower() == 'canada':
                                new_row['Currency Code'] = 'CAD'
                            elif strOrg.lower() == 'mexico':
                                new_row['Currency Code'] = 'MXN'
                            else:
                                new_row['Currency Code'] = 'USD'
                            df = pd.concat([df.iloc[:i + 1], pd.DataFrame([new_row]), df.iloc[i + 1:]]).reset_index(drop=True)

                # Check if the specified columns exist in the DataFrame
                existing_cols_to_drop = [col for col in liColsToDrop if col in df.columns]

                # Drop the unnecessary columns
                # Drop the existing columns
                if existing_cols_to_drop:
                    df.drop(columns=existing_cols_to_drop, inplace=True)
                    print(f"Dropped the following columns: {existing_cols_to_drop}")
                    objLogger.logInfo(f"Dropped the following columns: {existing_cols_to_drop}")
                else:
                    objLogger.logInfo("No columns to drop exist in the DataFrame.")
                    print("No columns to drop from the specified list exist in the DataFrame.")
                
                # Sort data by 'Date', 'Invoice Number'
                df_sorted = df.sort_values(by=['Invoice Date', 'Invoice Number'])

                df_sorted.reset_index(drop=True, inplace=True)

                # getting first value of 'Date' column
                first_value = df_sorted['Invoice Date'].iloc[0]
                strMonth, strYear = CAmzB2CHelperFunc.MGetLastMonthName(first_value)

                strOutputFilePath = rf'{strOutputFolderPath}\{strMonth} Invoice {strYear}.csv'
                
                # Save the final DataFrame to the csv file.
                df_sorted.to_csv(strOutputFilePath, index=False)
                objLogger.logInfo(f"The output CSV file has been saved at: {strOutputFilePath}")
                return df_sorted, strOutputFilePath
            else:
                objLogger.logInfo("The sum of the specified columns does not match the sum of the 'total' column.")
                print("Error: The sum of the specified columns does not match the sum of the 'total' column.")
                print("No data available.")
                return None, None
                
        except Exception as e:
            objLogger.logError(f"An error occurred: {e}")
            print(f"An error occurred: {e}")    

    @staticmethod
    @ensure_annotations
    def MProcessCreditNoteCsv(strDateRangeFilePath : str, strOutputFolderPath : str, dictExchangeRates : dict, cols_to_sum : list, liColsToDrop : list, tax_columns : list, strOrg : str):
        """
        Process credit notes from a CSV file and generates a CSV file with processed data.

        Inputs:
            strDateRangeFilePath (str): The file path to the CSV file (Date Range).
            dictExchangeRates (dict): A dictionary mapping dates to exchange rates.
            cols_to_sum (list): A list of column names whose sums need to be verified.
            liColsToDrop (list): A list of column names to drop from the DataFrame.
            tax_columns (list): A list of tax column names to be checked against a tolerance value.

        Outputs:
            pd.DataFrame: A formatted DataFrame with processed credit notes.
            
        Returns:
            pd.DataFrame: A formatted DataFrame with processed credit notes if the conditions are met.
            str: A message indicating issues with the tax columns sum if the conditions are not met.
        """
        try:
            objLogger.logInfo("Processing Credit Notes.....")
            # Processing data
            df = CAmzB2CHelperFunc.MProcessCsvTillOrderFilter(strDateRangeFilePath, 'date/time', 'settlement id', 'order id', strOrg = strOrg, cols_to_sum = cols_to_sum)

            # If DataFrame is not None, then process the data
            if df is not None:
                # Convert the 'other' and 'total' columns to string
                df['product sales'] = df['product sales'].astype('str')
                df['other'] = df['other'].astype('str')
                df['total'] = df['total'].astype('str')
                # Remove commas from strings before conversion (assuming ',' is the thousands separator)
                df['other'] = df['other'].str.replace(',', '')
                df['total'] = df['total'].str.replace(',', '')

                # Convert the 'other' and 'total' columns to float64
                df[['other', 'total']] = df[['other', 'total']].astype('float64')
            
                tolerance = 1e-10
                objLogger.logInfo('Verifying the sum of tax columns is zero')
                # Verify Sums Are tax columns sum Zero?
                # Drop cols whose sum zero and check sum of tax cols is zero
                df, bIsZero = CAmzB2CHelperFunc.MCheckTaxColsAndDropZeroSumCols(df, tax_columns, tolerance)

                # if sum of tax cols is not zero then return msg
                if bIsZero == False:
                    objLogger.logInfo('The sum of columns(product sales tax, shipping credits tax, giftwrap credits tax, marketplace withheld tax) is not zero')
                    return "Kindly check the sum of 'product sales tax', 'shipping credits tax', 'giftwrap credits tax', 'marketplace withheld tax' columns"
                objLogger.logInfo('The sum of columns(product sales tax, shipping credits tax, giftwrap credits tax, marketplace withheld tax) is zero')

                # Get states and country
                # Apply the function to each row and update the DataFrame by adding country and state columns
                df = CAmzB2CHelperFunc.MGetAllCountriesAndStates(df)
                objLogger.logInfo('Applying the function to each row and updating the DataFrame by adding country and state columns')
                
                # taking copy of df
                df = df.copy()

                # Dataframe of selling fees having selling fees col values without 0
                dfSellingFees = df[df['selling fees'] != 0]
                # Adding Description and Item Price col
                dfSellingFees['Description'] = 'Amazon Selling fees'
                dfSellingFees['Item Price'] = df['selling fees']
                dfSellingFees['SKU'] = ''

                # Dataframe of fba fees having fba fees col values without 0
                dfFBAFees = df[df['fba fees'] != 0]
                # Adding Description and Item Price col
                dfFBAFees['Description'] = 'Amazon FBA Fees'
                dfFBAFees['Item Price'] = df['fba fees']
                dfFBAFees['SKU'] = ''

                # Dataframe of other transaction fees col valeues without 0
                dfOtherFees = df[df['other transaction fees'] != 0]
                # Adding Description and Item Price col
                dfOtherFees['Description'] = 'Amazon Selling fees'
                dfOtherFees['Item Price'] = df['other transaction fees']

                # Append all of the dataframes
                # Appending FBA df to Selling fees df
                df = dfSellingFees._append(dfFBAFees)

                # Appending Other fees df to df
                df = df._append(dfOtherFees)

                # Making Item Price column absolute to remove minus sign
                # to remove minus sign
                df['Item Price'] = df['Item Price'].abs()

                # Get exchange rates of the provided date range
                if dictExchangeRates:
                    # Map the exchange rate data to the existing
                    # Insert the new column at the specified index
                    df['Exchange Rate'] = df['date/time'].map(dictExchangeRates)
                    objLogger.logInfo('Mapping the exchange rate data to the existing')
                else:
                    objLogger.logInfo("No exchange rate data available for the specified dates")
                    print("No exchange rate data available for the specified dates")

                # Renaming the date/time col
                df.rename({'date/time' : 'Credit Note Date'}, axis=1, inplace=True)

                # Insert the column at the desired index
                df.insert(1, 'Credit Note Number', df['Invoice Number'])
                df.insert(2, 'Applied Invoice Number', df['Credit Note Number'])
                df.insert(3, 'Applied Invoice Date', df['Credit Note Date'])
                df.insert(4, 'Amount to be Applied to Invoice', df['Item Price'])
                df.insert(5, 'Credit Note Status', 'Open')
                if strOrg.lower() == 'canada':
                    df.insert(6, 'Customer Name', 'Amazon CA')
                    df.insert(7, 'Currency Code', 'CAD')
                elif strOrg.lower() == 'mexico':
                    df.insert(6, 'Customer Name', 'Amazon Mexico')
                    df.insert(7, 'Currency Code', 'MXN')
                else:
                    df.insert(6, 'Customer Name', 'Amazon USA')
                    df.insert(7, 'Currency Code', 'USD')
                    
                # To move Exchange Rate col
                liOrderNumber = df['Exchange Rate']
                # Remove the column from its original position
                df.drop(columns=['Exchange Rate'], inplace=True)
                # Insert the column at the desired index
                df.insert(8, 'Exchange Rate', liOrderNumber)

                df.insert(9, 'Reference#', '')
                df.insert(10, 'Template Name', 'Standard Template')
                
                # To move Description col
                liDescription = df['Description']
                # Remove the column from its original position
                df.drop(columns=['Description'], inplace=True)
                # Insert the column at the desired index
                df.insert(11, 'Description', liDescription)

                # To move SKU col
                liSKU = df['SKU']
                # Remove the column from its original position
                df.drop(columns=['SKU'], inplace=True)
                # Insert the column at the desired index
                df.insert(12, 'SKU', liSKU)

                df.insert(13, 'Account', df['Description'])
                df.insert(14, 'Quantity', '1')

                # To move Item Price col
                liItemPrice = df['Item Price']
                # Remove the column from its original position
                df.drop(columns=['Item Price'], inplace=True)
                # Insert the column at the desired index
                df.insert(15, 'Item Price', liItemPrice)

                df.insert(16, 'Item Tax', '')
                df.insert(17, 'Item Tax %', '')
                df.insert(18, 'Item Tax Authority', 'Canada') 
                df.insert(19, 'Item Tax Exemption Reason', 'Export')
                df.insert(20, 'Notes', '')
                df.insert(21, 'Terms & Conditions', '')
                df.insert(22, 'Credit Note Level Tax', '')
                df.insert(23, 'Credit Note Level Tax %', '')
                df.insert(24, 'Credit Note Level Tax Authority', 'Canada')    
                df.insert(25, 'Credit Note Level Tax Exemption Reason', 'Export') 
                df.insert(26, 'Sales Channel', 'Amazon US') 
                df.insert(27, 'Products', df['sku'])
                df.insert(28, 'Department', 'Sales')
                df.insert(29, 'City', df['order city'])    
                df.insert(30, 'State', df['state'])
                df.insert(31, 'Country', df['country'])
                df['Country'] = df['Country'].str.replace("United States", "U.S.A")
                df.insert(32, 'Billing City', df['City'])
                df.insert(33, 'Billing State', df['State'])
                df.insert(34, 'Billing Country', df['Country'])
                df.insert(35, 'Warehouse Name', '')

                # drop the Invoice Number column
                df.drop(columns=['Invoice Number'], inplace=True)

                # Check if the specified columns exist in the DataFrame
                existing_cols_to_drop = [col for col in liColsToDrop if col in df.columns]

                # Drop the unnecessary columns
                # Drop the existing columns
                if existing_cols_to_drop:
                    df.drop(columns=existing_cols_to_drop, inplace=True)
                    print(f"Dropped the following columns: {existing_cols_to_drop}")
                    objLogger.logInfo(f"Dropped the following columns: {existing_cols_to_drop}")
                else:
                    print("No columns to drop from the specified list exist in the DataFrame.")
                    objLogger.logInfo("No columns to drop from the specified list exist in the DataFrame.")
                
                # Sort data by 'Date', 'Invoice Number'
                df_sorted = df.sort_values(by=['Credit Note Date', 'Credit Note Number'])

                df_sorted.reset_index(drop=True, inplace=True)
                # getting first value of 'Date' column
                first_value = df_sorted['Credit Note Date'].iloc[0]
                strMonth, strYear = CAmzB2CHelperFunc.MGetLastMonthName(first_value)

                strOutputFilePath = rf'{strOutputFolderPath}\{strMonth} Credit Notes {strYear}.csv'
                
                # Save the final DataFrame to the csv file.
                df_sorted.to_csv(strOutputFilePath, index=False)
                objLogger.logInfo(f"The output CSV file has been saved at: {strOutputFilePath}")
                return df_sorted, strOutputFilePath
            
            else:
                objLogger.logInfo("The sum of the specified columns does not match the sum of the 'total' column.")
                print("Error: The sum of the specified columns does not match the sum of the 'total' column.")
                print("No data available.")
                return None, None

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    strOrg = 'Mexico'

    dictExchangeRates = CAmzB2CHelperFunc.MGetExchangeRatesFinalDict(strOrg, strStartDate='01-08-2024', strEndDate='30-09-2024')
    tax_columns = ['product sales tax', 'shipping credits tax', 'giftwrap credits tax', 'marketplace withheld tax']
    cols_to_sum = ['product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'giftwrap credits tax', 'Regulatory Fee', 'Tax On Regulatory Fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other']
    liColsToDrop = ['settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace', 'account type', 'fulfillment', 'order city', 'order state', 'order postal', 'tax collection model', 'product sales', 'shipping credits', 'gift wrap credits', 'giftwrap credits tax', 'promotional rebates', 'selling fees', 'fba fees', 'total', 'state', 'country', 'product sales tax', 'shipping credits tax', 'marketplace withheld tax']
    dictSKUMapping = {
        'MOSWZ70-RG-AMZUS': 'Moto Watch 70 - Rose Gold (Amazon US)',
        'MOSWZ40-RG-AMZUS': 'Moto Watch 40 - Rose Gold (Amazon US)',
        'MOSWZ40-PB-AMZUS': 'Moto Watch 40 - Phantom Black (Amazon US)',
        'MOSWZ70-PB-AMZUS': 'Moto Watch 70 - Phantom Black (Amazon US)',
    }
    strDateRangeFilePath = r"C:\Users\Hardik Makwana\Downloads\2024Aug1-2024Sep20 Date Range report (Maxico) (1).csv"
    strOutputFolderPath = r'C:\Hardik\Project\ReportGienie\ReportGenie\output'
    CAMZB2C.MProcessSalesOrderCsv(strDateRangeFilePath, strOutputFolderPath, dictExchangeRates, cols_to_sum, liColsToDrop, tax_columns, dictSKUMapping, strOrg)
    CAMZB2C.MProcessInvoiceCsv(strDateRangeFilePath, strOutputFolderPath, dictExchangeRates, cols_to_sum, liColsToDrop, tax_columns, dictSKUMapping, strOrg)
    CAMZB2C.MProcessCreditNoteCsv(strDateRangeFilePath, strOutputFolderPath, dictExchangeRates, cols_to_sum, liColsToDrop, tax_columns, strOrg)