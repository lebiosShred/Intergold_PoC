import os
import json
import io
from datetime import date
import pandas as pd
from flask import Flask, jsonify, request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

app = Flask(__name__)
SCOPES = ['https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

def load_credentials_from_env():
    try:
        if 'GOOGLE_CREDENTIALS_JSON' in os.environ:
            if not os.path.exists(CREDENTIALS_FILE):
                with open(CREDENTIALS_FILE, 'w') as f:
                    f.write(os.environ['GOOGLE_CREDENTIALS_JSON'])
        if 'GOOGLE_TOKEN_JSON' in os.environ:
            if not os.path.exists(TOKEN_FILE):
                with open(TOKEN_FILE, 'w') as f:
                    f.write(os.environ['GOOGLE_TOKEN_JSON'])
    except:
        return False
    return os.path.exists(CREDENTIALS_FILE) and os.path.exists(TOKEN_FILE)

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            return None
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except HttpError:
        return None

def find_file_id_by_name(service, file_name):
    search_query = f"name contains '{file_name}' and trashed=false"
    try:
        results = service.files().list(q=search_query, pageSize=5, fields="files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            return None, f"File not found: '{file_name}'"
        if len(items) > 1:
            return None, f"Multiple files found with name: '{file_name}'. Please use a unique name."
        return items[0]['id'], None
    except HttpError as error:
        return None, f"An error occurred searching for file: {error}"

def load_dataframe_from_drive(service, file_id, file_name, usecols=None, parse_dates=None, skiprows=None):
    file_content_request = service.files().get_media(fileId=file_id)
    content = file_content_request.execute()
    if file_name.lower().endswith('.csv'):
        df = pd.read_csv(io.BytesIO(content), usecols=usecols, parse_dates=parse_dates, skiprows=skiprows)
        return {'Sheet1': df}
    else:
        try:
            df_sheets = pd.read_excel(io.BytesIO(content), sheet_name=None, engine='openpyxl', usecols=usecols, skiprows=skiprows)
            return df_sheets
        except Exception:
            df = pd.read_csv(io.BytesIO(content), usecols=usecols, parse_dates=parse_dates, skiprows=skiprows)
            return {'Sheet1': df}

@app.route('/')
def index():
    return jsonify({"status": "ok", "message": "Google Drive connector is running."})

@app.route('/files', methods=['GET'])
def list_files():
    if not load_credentials_from_env():
        return jsonify({"error": "Server is not configured with Google credentials."}), 500
    service = get_drive_service()
    if not service:
        return jsonify({"error": "Could not authenticate with Google Drive."}), 500
    try:
        results = service.files().list(pageSize=20, fields="nextPageToken, files(id, name, mimeType)").execute()
        items = results.get('files', [])
        if not items:
            return jsonify({"message": "No files found."})
        file_list = [{"name": item['name'], "id": item['id'], "type": item['mimeType']} for item in items]
        return jsonify({"files": file_list})
    except HttpError as error:
        return jsonify({"error": str(error)}), 500

@app.route('/check_headers', methods=['GET'])
def check_headers():
    if not load_credentials_from_env():
        return jsonify({"error": "Server is not configured with Google credentials."}), 500
    service = get_drive_service()
    if not service:
        return jsonify({"error": "Could not authenticate with Google Drive."}), 500
    file_name = request.args.get('fileName')
    if not file_name:
        return jsonify({"error": "You must provide a 'fileName' parameter."}), 400
    
    # Optional parameter to skip rows before header
    skip_rows = request.args.get('skipRows', 0)
    try:
        skip_rows = int(skip_rows)
    except:
        skip_rows = 0
    
    file_id, err = find_file_id_by_name(service, file_name)
    if err:
        return jsonify({"error": err}), 404
    try:
        content = service.files().get_media(fileId=file_id).execute()
        # Attempt CSV first with skiprows parameter
        df = pd.read_csv(io.BytesIO(content), skiprows=skip_rows, nrows=5)
    except Exception:
        try:
            # Fallback to Excel if CSV fails
            df = pd.read_excel(io.BytesIO(content), sheet_name=0, engine='openpyxl', skiprows=skip_rows, nrows=5)
        except Exception as ex:
            return jsonify({"error": f"Could not load file to inspect headers: {str(ex)}"}), 500
    
    cols = df.columns.tolist()
    
    # Check if we got bad headers (all Unnamed or mostly numeric)
    unnamed_count = sum(1 for c in cols if str(c).startswith('Unnamed:'))
    numeric_count = sum(1 for c in cols if str(c).replace('.', '').replace('-', '').isdigit())
    
    warning = None
    if unnamed_count > len(cols) * 0.3 or numeric_count > len(cols) * 0.3:
        warning = "Warning: Many columns appear unnamed or numeric. The file may have headers in a different row. Try using skipRows parameter (e.g., ?skipRows=1)"
    
    # Also return first few rows as preview to help identify header location
    preview = df.head(3).to_dict('records') if len(df) > 0 else []
    
    result = {
        "columns": cols,
        "columnCount": len(cols),
        "preview": preview
    }
    
    if warning:
        result["warning"] = warning
    
    return jsonify(result)

@app.route('/query', methods=['GET'])
def query_data():
    if not load_credentials_from_env():
        return jsonify({"error": "Server is not configured with Google credentials."}), 500
    service = get_drive_service()
    if not service:
        return jsonify({"error": "Could not authenticate with Google Drive."}), 500
    file_name_to_query = request.args.get('fileName')
    if not file_name_to_query:
        return jsonify({"error": "You must provide a 'fileName' parameter."}), 400
    file_id, error = find_file_id_by_name(service, file_name_to_query)
    if error:
        return jsonify({"error": error}), 404
    query_params = request.args
    requested_date_col_raw = query_params.get('dateColumn', 'OrdDate')
    requested_group_by_raw = query_params.get('groupBy', 'SOType')
    
    # Add skipRows parameter support
    skip_rows = query_params.get('skipRows', None)
    if skip_rows is not None:
        try:
            skip_rows = int(skip_rows)
        except:
            skip_rows = None
    
    usecols = None  # until columns matched
    sheets = load_dataframe_from_drive(service, file_id, file_name_to_query, usecols=None, parse_dates=None, skiprows=skip_rows)
    df = next(iter(sheets.values())).copy()
    cols = df.columns.tolist()
    norm = {c.strip().lower(): c for c in cols}
    req_date_key = requested_date_col_raw.strip().lower()
    req_group_key = requested_group_by_raw.strip().lower()
    if req_date_key not in norm or req_group_key not in norm:
        return jsonify({"error": f"Could not find required columns. Available: {cols}"}), 400
    date_col = norm[req_date_key]
    group_by = norm[req_group_key]
    df = df[[date_col, group_by]].copy()
    try:
        df[date_col] = pd.to_datetime(df[date_col])
    except Exception as e:
        return jsonify({"error": f"Could not parse date column '{date_col}': {str(e)}"}), 400
    today = date.today()
    if today.month <= 3:
        last_quarter_start = date(today.year - 1, 10, 1)
    else:
        q = ((today.month - 1) // 3)
        last_quarter_start = date(today.year, q * 3 - 2, 1)
    last_quarter_end_month = (last_quarter_start.month + 2)
    last_quarter_end = date(last_quarter_start.year, last_quarter_end_month,
                             pd.Period(f"{last_quarter_start.year}-{last_quarter_start.month}").asfreq('Q').month_end.day)
    mask = (df[date_col].dt.date >= last_quarter_start) & (df[date_col].dt.date <= last_quarter_end)
    df_q = df.loc[mask]
    result = df_q.groupby(group_by).size().to_dict()
    return jsonify(result)

if __name__ == '__main__':
    app.run(port=5000, debug=True)
