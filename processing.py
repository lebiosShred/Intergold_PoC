import io
import os
import pandas as pd
from typing import List
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException

# business-rule mapping for KT (example)
KT_MAPPING = {
    # e.g. "KT_old1": "KT_new1",
    # add client’s mapping here
}

PPC_ORDER = ['Overdue', 'Due']  # followed by weeks descending

def load_excel_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    return df

def classify_order_type(df: pd.DataFrame) -> pd.DataFrame:
    df['order_type_classified'] = df['SO Description'].apply(
        lambda x: 'LGD' if isinstance(x, str) and 'LGD' in x else 'Mined'
    )
    return df

def reclassify_kt(df: pd.DataFrame) -> pd.DataFrame:
    df['KT'] = df['KT'].map(KT_MAPPING).fillna(df['KT'])
    return df

def order_ppc_delivery_period(df: pd.DataFrame) -> pd.DataFrame:
    weeks = sorted(
        [v for v in df['PPC Delivery Period'].unique() if v not in ('Overdue','Due')],
        key=lambda x: int(x.split()[0]) if isinstance(x, str) and x.split()[0].isdigit() else 0,
        reverse=True
    )
    cat_order = ['Overdue','Due'] + weeks
    df['PPC Delivery Period'] = pd.Categorical(
        df['PPC Delivery Period'],
        categories=cat_order,
        ordered=True
    )
    return df

def process_query(
    df: pd.DataFrame,
    order_type: str,
    metric: str,
    attributes: List[str],
    top_n: int = None
) -> list:
    missing = [a for a in attributes if a not in df.columns]
    if missing:
        raise ValueError(f"Missing attributes in data: {missing}")

    # Filter by order_type
    df = df[df['order_type_classified'] == order_type]

    group_cols = attributes + ['PPC Delivery Period']
    agg = (
        df
        .groupby(group_cols)[metric]
        .sum()
        .reset_index()
        .rename(columns={metric: 'total'})
    )

    agg = order_ppc_delivery_period(agg)
    agg = agg.sort_values(by=['PPC Delivery Period','total'], ascending=[True, False])

    if top_n is not None:
        agg = agg.groupby('PPC Delivery Period').head(top_n).reset_index(drop=True)

    return agg.to_dict(orient='records')

def fetch_file_from_box(folder_id: str, filename: str) -> bytes:

    client_id = os.getenv('BOX_CLIENT_ID')
    client_secret = os.getenv('BOX_CLIENT_SECRET')
    access_token = os.getenv('BOX_ACCESS_TOKEN')

    if not (client_id and client_secret and access_token):
        raise RuntimeError("Box credentials are not all configured (BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_ACCESS_TOKEN)")

    oauth2 = OAuth2(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token
    )
    client = Client(oauth2)

    try:
        items = client.folder(folder_id).get_items()
    except BoxAPIException as e:
        raise RuntimeError(f"Error listing folder {folder_id}: {e}")

    file_obj = None
    for item in items:
        if item.name == filename:
            file_obj = item
            break

    if not file_obj:
        raise FileNotFoundError(f"File '{filename}' not found in folder '{folder_id}'")

    try:
        content_bytes = client.file(file_obj.id).content()
    except BoxAPIException as e:
        raise RuntimeError(f"Error downloading file {filename} (id {file_obj.id}): {e}")

    return content_bytes
