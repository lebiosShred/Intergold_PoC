import os
import json
from flask import Flask, jsonify
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

app = Flask(__name__)

# --- Google API Scopes ---
# This defines what your app is allowed to do.
# 'readonly' is safest for just listing files.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

# --- Credential Loading ---
# These filenames are what the Google library expects.
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

def load_credentials_from_env():
    """
    Writes credentials from Render's environment variables to files.
    This is the secure way to handle secrets on a server.
    """
    try:
        # 1. Load the main 'credentials.json' (from OAuth setup)
        if 'GOOGLE_CREDENTIALS_JSON' in os.environ:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"Writing {CREDENTIALS_FILE} from environment variable...")
                with open(CREDENTIALS_FILE, 'w') as f:
                    f.write(os.environ['GOOGLE_CREDENTIALS_JSON'])
        
        # 2. Load the 'token.json' (from local auth step)
        if 'GOOGLE_TOKEN_JSON' in os.environ:
            if not os.path.exists(TOKEN_FILE):
                print(f"Writing {TOKEN_FILE} from environment variable...")
                with open(TOKEN_FILE, 'w') as f:
                    f.write(os.environ['GOOGLE_TOKEN_JSON'])
    
    except Exception as e:
        print(f"Error loading credentials from environment: {e}")
        return False
    
    return os.path.exists(CREDENTIALS_FILE) and os.path.exists(TOKEN_FILE)

def get_drive_service():
    """
    Authenticates with the Google Drive API and returns a service object.
    """
    creds = None
    
    # Check if token.json exists (it should, after load_credentials_from_env)
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # If no valid credentials, try to refresh (if refresh_token is present)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing Google credentials...")
            creds.refresh(Request())
        else:
            # This block should not be hit on Render if env vars are set.
            # It's here for a fallback, but would fail on a server.
            print("No valid credentials. Run get_token.py locally first.")
            return None
        
        # Save the credentials for the next run
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except HttpError as error:
        print(f'An error occurred building the service: {error}')
        return None

# --- API Endpoints ---

@app.route('/')
def index():
    """A simple health-check endpoint."""
    return jsonify({"status": "ok", "message": "Google Drive connector is running."})

@app.route('/files', methods=['GET'])
def list_files():
    """
    The main API endpoint that Watsonx will call.
    Lists the first 10 files and folders from Google Drive.
    """
    
    # Ensure credentials are loaded from environment
    if not load_credentials_from_env():
        return jsonify({"error": "Server is not configured with Google credentials."}), 500

    service = get_drive_service()
    if not service:
        return jsonify({"error": "Could not authenticate with Google Drive."}), 500

    try:
        # Call the Drive v3 API
        results = service.files().list(
            pageSize=20,  # Get up to 20 items
            fields="nextPageToken, files(id, name, mimeType)"
        ).execute()
        
        items = results.get('files', [])

        if not items:
            return jsonify({"message": "No files found."})
        
        # Format the output nicely for Watsonx
        file_list = [
            {"name": item['name'], "id": item['id'], "type": item['mimeType']}
            for item in items
        ]
        
        return jsonify({"files": file_list})

    except HttpError as error:
        print(f'An error occurred: {error}')
        return jsonify({"error": str(error)}), 500

if __name__ == '__main__':
    # This is only for local testing, not for production on Render.
    # Render will use Gunicorn (defined in render.yaml).
    app.run(port=5000, debug=True)
