import os
import dropbox
from flask import Flask, request, jsonify

# --- Configuration ---
# Initialize the Flask application
app = Flask(__name__)

# Retrieve the Dropbox Access Token from an environment variable for security.
# This will be set in the Render dashboard.
DROPBOX_ACCESS_TOKEN = os.environ.get('DROPBOX_ACCESS_TOKEN')

# --- Helper Functions ---

def initialize_dropbox_client():
    """Initializes and returns a Dropbox client instance.
    
    Returns:
        dropbox.Dropbox: An authenticated Dropbox client instance.
        None: If the access token is not configured.
    """
    if not DROPBOX_ACCESS_TOKEN:
        print("Error: The DROPBOX_ACCESS_TOKEN environment variable is not set.")
        return None
    try:
        dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
        # Test the connection by getting current account info
        dbx.users_get_current_account()
        print("Successfully connected to Dropbox.")
        return dbx
    except dropbox.exceptions.AuthError:
        print("Error: Invalid Dropbox access token. Please check your token.")
        return None
    except Exception as e:
        print(f"An error occurred during Dropbox initialization: {e}")
        return None

def list_folders_in_root():
    """Lists all folders in the root directory of the Dropbox account.
    
    Returns:
        dict: A dictionary containing a list of folder names or an error message.
    """
    dbx = initialize_dropbox_client()
    if not dbx:
        return {"error": "Dropbox client could not be initialized."}
        
    try:
        folders = []
        # List items in the root folder (path="")
        for entry in dbx.files_list_folder(path="").entries:
            if isinstance(entry, dropbox.files.FolderMetadata):
                folders.append(entry.name)
        return {"folders": folders}
    except Exception as e:
        return {"error": f"Failed to list Dropbox folders: {e}"}

def find_file(file_name):
    """Searches for a file in the Dropbox account.
    
    Args:
        file_name (str): The name of the file to search for.
    
    Returns:
        dict: A dictionary containing details of the found file or an error message.
    """
    dbx = initialize_dropbox_client()
    if not dbx:
        return {"error": "Dropbox client could not be initialized."}

    try:
        # Search for files matching the query
        search_result = dbx.files_search_v2(file_name).matches
        if not search_result:
            return {"message": f"No file matching '{file_name}' was found."}
        
        # Get the metadata for the first match
        first_match = search_result[0].metadata.get_metadata()
        
        file_details = {
            "name": first_match.name,
            "path": first_match.path_display,
            "size_mb": round(first_match.size / (1024 * 1024), 2),
            "last_modified": first_match.client_modified.strftime("%Y-%m-%d %H:%M:%S")
        }
        return {"file_found": file_details}
    except Exception as e:
        return {"error": f"An error occurred while searching for the file: {e}"}

# --- Flask API Endpoint ---

@app.route('/invoke', methods=['POST'])
def invoke_skill():
    """
    This is the main endpoint that Watson Orchestrate will call.
    It parses the user's prompt and triggers the corresponding Dropbox action.
    """
    try:
        data = request.get_json()
        if not data or 'prompt' not in data:
            return jsonify({"error": "Invalid request. 'prompt' is required."}), 400

        prompt = data.get('prompt', '').lower().strip()
        print(f"Received prompt: '{prompt}'")

        # --- REVISED LOGIC ---
        # We must check for the most *specific* intent first.
        # "show me file" is more specific than "show me... folders".
        
        if "show me file" in prompt or "find file" in prompt:
            # This is the "find file" intent
            query = prompt.replace("show me file", "").replace("find file", "").strip()
            if not query:
                response = {"error": "Please specify a file name to search for."}
            else:
                response = find_file(query)

        elif "all folders" in prompt or "list folders" in prompt:
            # This is the "list folders" intent
            response = list_folders_in_root()
            
        else:
            # This is the catch-all error
            response = {"error": "Sorry, I can only 'show all folders' or 'find file <name>'."}

        return jsonify(response)

    except Exception as e:
        return jsonify({"error": f"An unexpected server error occurred: {e}"}), 500

# --- Main Execution ---

if __name__ == '__main__':
    # This block is for local development.
    if not DROPBOX_ACCESS_TOKEN:
        print("\nFATAL ERROR: 'DROPBOX_ACCESS_TOKEN' is not set.")
        print("Please set it as an environment variable before running the script.")
    else:
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)
