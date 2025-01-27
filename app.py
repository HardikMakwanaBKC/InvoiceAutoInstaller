from flask import Flask, request, jsonify, send_file, render_template
import os
import zipfile
from werkzeug.utils import secure_filename
from AmzB2CHelperFunc import CAmzB2CHelperFunc
from AmzB2CProcess import CAMZB2C

# Initialize Flask app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
app.config['OUTPUT_FOLDER'] = '/tmp/output'
app.secret_key = 'your_secret_key'


def create_folders():
    """
    Create required folders for uploads and outputs if they don't exist.
    """
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)


def clear_output_folder() -> None:
    """
    Clear all files from the output folder before processing.
    """
    output_folder = app.config['OUTPUT_FOLDER']
    for filename in os.listdir(output_folder):
        file_path = os.path.join(output_folder, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
        except Exception as e:
            print(f"Error occurred while clearing output folder: {e}")


@app.route('/')
def index():
    """
    Serve the HTML page for the forms.
    """
    return render_template('index.html')


@app.route('/processAmzDateRangeCsv', methods=['POST'])
def process_amz_date_range_csv():
    """
    Process an Amazon date range CSV file and return a ZIP file with processed outputs.

    Inputs:
    - file: CSV file
    - strOrg: Organization (AMZUS or AMZCA)
    - startdate: Start date (dd-mm-yyyy)
    - enddate: End date (dd-mm-yyyy)

    Outputs:
    - ZIP file containing processed sales, invoice, and credit note CSV files.
    """
    try:
        create_folders()  # Ensure folders exist
        clear_output_folder()  # Clear old outputs

        # Handle file upload
        uploaded_file = request.files.get('file')
        if not uploaded_file:
            return jsonify({'error': 'No file uploaded'}), 400

        # Secure the uploaded file name and save it
        file_name = secure_filename(uploaded_file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
        uploaded_file.save(file_path)

        # Get form inputs
        strOrg = request.form.get('strOrg')
        strStartDate = request.form.get('startdate')
        strEndDate = request.form.get('enddate')

        if not all([strOrg, strStartDate, strEndDate]):
            return jsonify({'error': 'Missing required form fields'}), 400

        # Format dates (ensure consistent 'dd-mm-yyyy' format)
        strStartDate = '-'.join(reversed(strStartDate.split('-')))
        strEndDate = '-'.join(reversed(strEndDate.split('-')))

        # Fetch exchange rates for the given date range
        dictExchangeRates = CAmzB2CHelperFunc.MGetExchangeRatesFinalDict(
            strOrg=strOrg, strStartDate=strStartDate, strEndDate=strEndDate
        )

        # Columns to sum, drop, and other settings
        tax_columns = [
            'product sales tax', 'shipping credits tax', 'giftwrap credits tax', 'marketplace withheld tax'
        ]
        cols_to_sum = [
            'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 
            'gift wrap credits', 'giftwrap credits tax', 'Regulatory Fee', 'Tax On Regulatory Fee',
            'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 
            'selling fees', 'fba fees', 'other transaction fees', 'other'
        ]
        liColsToDrop = [
            'settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace', 
            'account type', 'fulfillment', 'order city', 'order state', 'order postal', 
            'tax collection model', 'product sales', 'shipping credits', 'gift wrap credits', 
            'giftwrap credits tax', 'promotional rebates', 'selling fees', 'fba fees', 'total', 
            'state', 'country', 'product sales tax', 'shipping credits tax', 'marketplace withheld tax', 
            'other transaction fees', 'other'
        ]
        dictSKUMapping = {
            'MOSWZ70-RG-AMZUS': 'Moto Watch 70 - Rose Gold (Amazon US)',
            'MOSWZ40-RG-AMZUS': 'Moto Watch 40 - Rose Gold (Amazon US)',
            'MOSWZ40-PB-AMZUS': 'Moto Watch 40 - Phantom Black (Amazon US)',
            'MOSWZ70-PB-AMZUS': 'Moto Watch 70 - Phantom Black (Amazon US)',
            'MOSWZ70-BG-AMZUS': 'Moto Watch 70 - Bright Gold (Amazon US)',
            'MOSWZ120-PB-AMZUS': 'Moto Watch 120 - Phantom Black (Amazon US)',
            'MOSWZ120-RG-AMZUS': 'Moto Watch 120 - Rose Gold (Amazon US)',
            'MOSWZ120-SL-AMZUS': 'Moto Watch 120 - Silver (Amazon US)',
        }

        # Process files using your custom class methods
        strSalesOutputFilePath = CAMZB2C.MProcessSalesOrderCsv(
            strDateRangeFilePath=file_path, strOutputFolderPath=app.config['OUTPUT_FOLDER'],
            dictExchangeRates=dictExchangeRates, cols_to_sum=cols_to_sum, liColsToDrop=liColsToDrop,
            tax_columns=tax_columns, dictSKUMapping=dictSKUMapping, strOrg=strOrg
        )[1]

        strInvoiceOutputFilePath = CAMZB2C.MProcessInvoiceCsv(
            strDateRangeFilePath=file_path, strOutputFolderPath=app.config['OUTPUT_FOLDER'],
            dictExchangeRates=dictExchangeRates, cols_to_sum=cols_to_sum, liColsToDrop=liColsToDrop,
            tax_columns=tax_columns, dictSKUMapping=dictSKUMapping, strOrg=strOrg
        )[1]

        strCreditNoteOutputFilePath = CAMZB2C.MProcessCreditNoteCsv(
            strDateRangeFilePath=file_path, strOutputFolderPath=app.config['OUTPUT_FOLDER'],
            dictExchangeRates=dictExchangeRates, cols_to_sum=cols_to_sum, liColsToDrop=liColsToDrop,
            tax_columns=tax_columns, strOrg=strOrg
        )[1]

        # Validate processed file paths
        output_files = [strSalesOutputFilePath, strInvoiceOutputFilePath, strCreditNoteOutputFilePath]
        if not all(output_files) or not all(os.path.exists(f) for f in output_files):
            return jsonify({'error': 'One or more output files are missing'}), 404

        # Create a ZIP file containing the output files
        zip_file_path = os.path.join(app.config['OUTPUT_FOLDER'], 'AMZB2COutput.zip')
        with zipfile.ZipFile(zip_file_path, 'w') as zipf:
            for file in output_files:
                zipf.write(file, os.path.basename(file))

        return send_file(zip_file_path, as_attachment=True)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Export app for Vercel
app = app
