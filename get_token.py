import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# This MUST match the scope in app.py
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

def main():
    """
    Runs the local, one-time authentication flow.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens.
    if os.path.exists(TOKEN_FILE):
        print(f"{TOKEN_FILE} already exists. Delete it to re-authenticate.")
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing credentials...")
            creds.refresh(Request())
        else:
            print(f"No {TOKEN_FILE} found. Starting new authentication flow...")
            # This will open a browser window for you to log in
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES)
            # Use a specific port that matches your GCP redirect URI
            creds = flow.run_local_server(port=8080)
        
        # Save the credentials for the server to use
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
        
        print(f"Authentication successful. {TOKEN_FILE} saved.")
        print("\n--- IMPORTANT ---")
        print(f"You must now copy the *contents* of {TOKEN_FILE} and {CREDENTIALS_FILE}")
        print("into your Render.com environment variables.")

if __name__ == '__main__':
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"Error: {CREDENTIALS_FILE} not found.")
        print("Please download it from the Google Cloud Console and place it in this directory.")
    else:
        main()