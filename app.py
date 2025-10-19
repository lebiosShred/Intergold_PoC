import os
import json
import io
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
        print(f"Error loading credentials from environment: {e}")
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
            print("No valid credentials.")
            return None
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except HttpError as error:
        print(f'An error occurred building the service: {error}')
        return None

def find_file_id_by_name(service, file_name):
    search_query = f"name='{file_name}' and trashed=false"
    try:
        results = service.files().list(
            q=search_query,
            pageSize=5,
            fields="files(id, name)"
        ).execute()
        
        items = results.get('files', [])

        if not items:
            return None, f"File not found: '{file_name}'"
        if len(items) > 1:
            return None, f"Multiple files found with name: '{file_name}'. Please use a unique name."
        
        return items[0]['id'], None 
    except HttpError as error:
        return None, f"An error occurred searching for file: {error}"

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
        results = service.files().list(
            pageSize=20,
            fields="nextPageToken, files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])
        if not items:
            return jsonify({"message": "No files found."})
        file_list = [
            {"name": item['name'], "id": item['id'], "type": item['mimeType']}
            for item in items
        ]
        return jsonify({"files": file_list})
    except HttpError as error:
        return jsonify({"error": str(error)}), 500

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

    try:
        file_id_to_query, error = find_file_id_by_name(service, file_name_to_query)
        if error:
            return jsonify({"error": error}), 404

        file_content_request = service.files().get_media(fileId=file_id_to_query)
        file_content = file_content_request.execute()
        
        df = pd.read_csv(io.BytesIO(file_content))
        
        filter_params = query_params.to_dict()
        filter_params.pop('fileName', None) 
        
        filters = []
        
        for key, value in filter_params.items():
            if key in df.columns:
                try:
                    df[key] = df[key].astype(type(value))
                    filters.append(df[key] == value)
                except Exception:
                    try:
                        df[key] = df[key].astype(float)
                        filters.append(df[key] == float(value))
                    except (ValueError, TypeError):
                        df[key] = df[key].astype(str)
                        filters.append(df[key].str.lower() == str(value).lower())

        if filters:
            combined_filter = pd.Series(True, index=df.index)
            for f in filters:
                combined_filter = combined_filter & f
            results_df = df.loc[combined_filter]
        else:
            results_df = df
        
        if 'operation' in filter_params:
            op = filter_params['operation'].lower()
            if op == 'count':
                return jsonify({"count": len(results_df)})
            if 'column' in filter_params and op == 'most_produced':
                most_produced = results_df[filter_params['column']].mode()[0]
                return jsonify({"most_produced": most_produced})

        results_json = results_df.to_json(orient='records')
        return jsonify(json.loads(results_json))

    except HttpError as error:
        return jsonify({"error": str(error)}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred during data processing: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)
