import os
from fastapi import FastAPI, HTTPException, Body
from typing import List, Dict
import boxsdk
from processing import process_intergold_query  # Correctly imports from your processing.py file
from dotenv import load_dotenv

# Load environment variables from a .env file for local testing
load_dotenv()

# --- Configuration ---
# Securely fetch the Box token from environment variables (this is what Render will use)
BOX_DEVELOPER_TOKEN = os.getenv("BOX_DEVELOPER_TOKEN")
# Define the exact filename to search for in the Box folder
TARGET_FILE_NAME = "InterGold_Report.xlsx" 

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Inter Gold Data Automation API",
    description="An API to query Inter Gold's operational data from files stored in Box."
)

# --- Box Integration Logic ---
def get_file_content_from_box(folder_id: str, file_name: str) -> bytes:
    """Searches for a file by name within a specific Box folder and returns its content."""
    if not BOX_DEVELOPER_TOKEN:
        raise HTTPException(status_code=500, detail="Server is missing BOX_DEVELOPER_TOKEN configuration.")
        
    try:
        auth = boxsdk.OAuth2(client_id=None, client_secret=None, access_token=BOX_DEVELOPER_TOKEN)
        client = boxsdk.Client(auth)
        items = client.folder(folder_id).get_items()
        
        target_file = None
        for item in items:
            if item.type == 'file' and item.name == file_name:
                target_file = item
                break

        if not target_file:
            raise HTTPException(status_code=404, detail=f"File '{file_name}' not found in Box folder ID '{folder_id}'.")

        return target_file.content()

    except boxsdk.exception.BoxAPIException as e:
        raise HTTPException(status_code=400, detail=f"Box API Error: {e.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

# --- API Endpoint ---
@app.post("/query-from-box")
async def run_box_query(
    box_folder_id: str = Body(..., examples=["1234567890"], description="The ID of the folder in Box to search."),
    metric: str = Body(..., examples=["Total Bag Bal", "Bal To Prod Qty"], description="The column name for the metric to summarize."),
    order_type: str = Body(..., examples=["LGD", "Mined"], description="Filter for 'LGD' or 'Mined' orders."),
    attributes: List[str] = Body(..., examples=[["Dsg Ctg", "KT"]], description="List of columns to group the results by.")
) -> Dict:
    """
    Fetches an Excel file from a specific Box folder, runs a query, and returns the result.
    """
    excel_file_content = get_file_content_from_box(
        folder_id=box_folder_id, 
        file_name=TARGET_FILE_NAME
    )
    
    json_result = process_intergold_query(
        file_content=excel_file_content,
        metric_column=metric,
        order_type=order_type,
        group_by_columns=attributes
    )
    
    return {"status": "success", "source_folder": box_folder_id, "data": json_result}