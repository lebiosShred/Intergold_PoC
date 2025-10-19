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

def detect_header_row(content, file_name, max_rows_to_check=5):
    try:
        for skip in range(max_rows_to_check):
            if file_name.lower().endswith('.csv'):
                df = pd.read_csv(io.BytesIO(content), skiprows=skip, nrows=3)
            else:
                df = pd.read_excel(io.BytesIO(content), sheet_name=0, engine='openpyxl', skiprows=skip, nrows=3)
            cols = df.columns.tolist()
            unnamed_count = sum(1 for c in cols if str(c).startswith('Unnamed:'))
            numeric_count = sum(1 for c in cols if str(c).replace('.', '').replace('-', '').isdigit())
            empty_count = sum(1 for c in cols if not str(c).strip())
            text_count = len(cols) - unnamed_count - numeric_count - empty_count
            if text_count > len(cols)*0.5 and (unnamed_count + numeric_count) < len(cols)*0.3 and len(cols) > 3:
                return skip, cols
        return 0, None
    except:
        return 0, None

def load_dataframe_from_drive(service, file_id, file_name, usecols=None, parse_dates=None, skiprows=None, auto_detect=False):
    file_content_request = service.files().get_media(fileId=file_id)
    content = file_content_request.execute()
    detected_skip = None
    detected_cols = None
    if auto_detect and skiprows is None:
        detected_skip, detected_cols = detect_header_row(content, file_name)
        skiprows = detected_skip
    if file_name.lower().endswith('.csv'):
        df = pd.read_csv(io.BytesIO(content), usecols=usecols, parse_dates=parse_dates, skiprows=skiprows)
        result = {'Sheet1': df}
    else:
        try:
            df_sheets = pd.read_excel(io.BytesIO(content), sheet_name=None, engine='openpyxl', usecols=usecols, skiprows=skiprows)
            result = df_sheets
        except:
            df = pd.read_csv(io.BytesIO(content), usecols=usecols, parse_dates=parse_dates, skiprows=skiprows)
            result = {'Sheet1': df}
    if auto_detect and detected_skip is not None:
        result['_metadata'] = {'auto_detected_skiprows': detected_skip, 'detected_columns': detected_cols}
    return result

@app.route('/')
def index():
    return jsonify({"status": "ok", "message": "Google Drive connector is running with auto-header detection."})

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
    auto_detect = request.args.get('autoDetect', 'true').lower() == 'true'
    skip_rows = request.args.get('skipRows', None)
    if skip_rows is not None:
        try:
            skip_rows = int(skip_rows)
            auto_detect = False
        except:
            skip_rows = None
    file_id, err = find_file_id_by_name(service, file_name)
    if err:
        return jsonify({"error": err}), 404
    try:
        content = service.files().get_media(fileId=file_id).execute()
        if auto_detect and skip_rows is None:
            detected_skip, detected_cols = detect_header_row(content, file_name)
            skip_rows = detected_skip
            was_auto = True
        else:
            detected_cols = None
            was_auto = False
        if skip_rows is None:
            skip_rows = 0
        if file_name.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(content), skiprows=skip_rows, nrows=5)
        else:
            try:
                df = pd.read_excel(io.BytesIO(content), sheet_name=0, engine='openpyxl', skiprows=skip_rows, nrows=5)
            except:
                df = pd.read_csv(io.BytesIO(content), skiprows=skip_rows, nrows=5)
        cols = df.columns.tolist()
        unnamed_count = sum(1 for c in cols if str(c).startswith('Unnamed:'))
        numeric_count = sum(1 for c in cols if str(c).replace('.', '').replace('-', '').isdigit())
        warning = None
        if unnamed_count > len(cols)*0.3 or numeric_count > len(cols)*0.3:
            warning = f"Warning: Many columns appear unnamed or numeric at row {skip_rows}. Headers may be in a different row."
        preview = df.head(3).to_dict('records')
        result = {"columns": cols, "columnCount": len(cols), "preview": preview, "skipRowsUsed": skip_rows, "autoDetected": was_auto}
        if warning:
            result["warning"] = warning
        return jsonify(result)
    except Exception as ex:
        return jsonify({"error": f"Could not load file to inspect headers: {str(ex)}"}), 500

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
    auto_detect = query_params.get('autoDetect', 'true').lower() == 'true'
    skip_rows = query_params.get('skipRows', None)
    if skip_rows is not None:
        try:
            skip_rows = int(skip_rows)
            auto_detect = False
        except:
            skip_rows = None
    sheets = load_dataframe_from_drive(service, file_id, file_name_to_query, usecols=None, parse_dates=None, skiprows=skip_rows, auto_detect=auto_detect)
    metadata = sheets.pop('_metadata', None)
    df = next(iter(sheets.values())).copy()
    cols = df.columns.tolist()
    norm = {c.strip().lower(): c for c in cols}
    req_date_key = requested_date_col_raw.strip().lower()
    req_group_key = requested_group_by_raw.strip().lower()
    if req_date_key not in norm or req_group_key not in norm:
        error_msg = f"Could not find required columns. Available: {cols}"
        if metadata:
            error_msg += f" (Auto-detected header at row {metadata.get('auto_detected_skiprows')})"
        return jsonify({"error": error_msg}), 400
    date_col = norm[req_date_key]
    group_by = norm[req_group_key]
    df = df[[date_col, group_by]].copy()
    df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
    total_rows = len(df)
    invalid_dates = df[date_col].isna().sum()
    valid_rows = total_rows - invalid_dates
    if valid_rows == 0:
        return jsonify({"error": f"No valid dates found in column '{date_col}' after parsing"}), 400
    today = date.today()
  if today.month <= 3:
    last_quarter_start = date(today.year - 1, 10, 1)
    last_quarter_end = date(today.year - 1, 12, 31)
else:
    q = ((today.month - 1) // 3)
    last_quarter_start_month = (q * 3) - 2
    last_quarter_start = date(today.year, last_quarter_start_month, 1)
    # end month = start_month + 2
    last_quarter_end_month = last_quarter_start_month + 2
    # determine last day of that month:
    # by building date of (next month 1) minus 1 day
    if last_quarter_end_month == 12:
        next_month = date(last_quarter_start.year + 1, 1, 1)
    else:
        next_month = date(last_quarter_start.year, last_quarter_end_month + 1, 1)
    last_quarter_end = next_month - pd.Timedelta(days=1)

    mask = (df[date_col].dt.date >= last_quarter_start) & (df[date_col].dt.date <= last_quarter_end)
    df_q = df.loc[mask]
    group_counts = df_q.groupby(group_by).size().to_dict()
    response = {
        "data": group_counts,
        "metadata": {
            "quarterStart": last_quarter_start.isoformat(),
            "quarterEnd": last_quarter_end.isoformat(),
            "totalRecords": total_rows,
            "validDateRecords": valid_rows,
            "invalidDateRecords": invalid_dates,
            "filteredRecords": len(df_q),
            "dateColumn": date_col,
            "groupByColumn": group_by,
            "dateFormat": "DD/MM/YYYY"
        }
    }
    if metadata:
        response["metadata"]["autoDetectedSkipRows"] = metadata.get('auto_detected_skiprows')
    return jsonify(response)

if __name__ == '__main__':
    app.run(port=5000, debug=True)

