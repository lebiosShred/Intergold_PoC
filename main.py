from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import os
import logging
from processing import load_excel_from_bytes, classify_order_type, reclassify_kt, process_query, fetch_file_from_box

app = FastAPI()
logging.basicConfig(level=logging.INFO)

# Allowed values
ALLOWED_METRICS = ["Total Bag Bal","Bal To Prod Qty","Bal To Exp Qty","BalToMfg","CastBal"]
ALLOWED_ATTRIBUTES = ["SO type","Dsg Ctg","KT","Set Type","Factory"]
ALLOWED_ORDER_TYPES = ["LGD","Mined"]

class QueryRequest(BaseModel):
    box_folder_id: str = Field(..., example="123456789")
    filename: str = Field(..., example="InterGold_Report.xlsx")
    metric: str = Field(..., example="Total Bag Bal")
    order_type: str = Field(..., example="LGD")
    attributes: list[str] = Field(..., example=["Dsg Ctg","Set Type"])
    top_n: int | None = Field(None, example=3)

@app.post("/query-from-box")
def query_from_box(req: QueryRequest):
    logging.info(f"Received request: {req}")
    if req.metric not in ALLOWED_METRICS:
        raise HTTPException(status_code=400, detail=f"Invalid metric: {req.metric}")
    if req.order_type not in ALLOWED_ORDER_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid order_type: {req.order_type}")
    if not req.attributes:
        raise HTTPException(status_code=400, detail="Must supply at least one attribute")
    bad_attrs = [a for a in req.attributes if a not in ALLOWED_ATTRIBUTES]
    if bad_attrs:
        raise HTTPException(status_code=400, detail=f"Invalid attributes: {bad_attrs}")

    token = os.getenv("BOX_DEVELOPER_TOKEN")
    if not token:
        raise HTTPException(status_code=500, detail="Box developer token not configured")

    try:
        file_bytes = fetch_file_from_box(req.box_folder_id, req.filename, token)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logging.error("Error fetching from Box", exc_info=e)
        raise HTTPException(status_code=500, detail="Error fetching file from Box")

    try:
        df = load_excel_from_bytes(file_bytes)
        df = classify_order_type(df)
        df = reclassify_kt(df)
        result = process_query(
            df,
            req.order_type,
            req.metric,
            req.attributes,
            top_n=req.top_n
        )
        return result
    except Exception as e:
        logging.error("Error processing query", exc_info=e)
        raise HTTPException(status_code=500, detail="Error processing data")

@app.get("/health")
def health():
    return {"status":"OK"}
