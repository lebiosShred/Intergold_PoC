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
    except Exception as e:
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

def load_dataframe_from_drive(service, file_id, file_name):
    file_content_request = service.files().get_media(fileId=file_id)
    content = file_content_request.execute()
    if file_name.lower().endswith(('.xlsx', '.xls')):
        df_sheets = pd.read_excel(io.BytesIO(content), sheet_name=None)
        return df_sheets
    else:
        try:
            df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(io.BytesIO(content), encoding='latin1')
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

@app.route('/sheets', methods=['GET'])
def list_sheets():
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
    sheets = load_dataframe_from_drive(service, file_id, file_name_to_query)
    return jsonify({"sheets": list(sheets.keys())})

@app.route('/query', methods=['GET'])
def query_data():
    if not load_credentials_from_env():
        return jsonify({"error": "Server is not configured with Google credentials."}), 500
    service = get_drive_service()
    if not service:
        return jsonify({"error": "Could not authenticate with Google Drive."}), 500
    query_params = request.args
    file_name_to_query = query_params.get('fileName')
    if not file_name_to_query:
        return jsonify({"error": "You must provide a 'fileName' parameter."}), 400
    file_id, error = find_file_id_by_name(service, file_name_to_query)
    if error:
        return jsonify({"error": error}), 404
    sheets = load_dataframe_from_drive(service, file_id, file_name_to_query)
    sheet_name = query_params.get('sheetName')
    if sheet_name and sheet_name in sheets:
        df = sheets[sheet_name].copy()
    else:
        df = next(iter(sheets.values())).copy()
    date_col = query_params.get('dateColumn', 'OrderDate')
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
    operation = query_params.get('operation', '').lower()
    group_by = query_params.get('groupBy')
    if operation == 'group_count' and group_by and group_by in df_q.columns:
        result = df_q.groupby(group_by).size().to_dict()
        return jsonify(result)
    return jsonify({"error": "Unsupported operation or missing parameters"}), 400

if __name__ == '__main__':
    app.run(port=5000, debug=True)
