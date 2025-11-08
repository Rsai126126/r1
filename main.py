import io
import datetime
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI(title="Missing 945 API", version="1.0.0")

def load_csv_bytes(b: bytes) -> pd.DataFrame:
    """
    Read CSV from uploaded bytes. Tries a couple of common encodings, but
    otherwise keeps behavior close to your local script.
    """
    for enc in ("utf-8", "utf-8-sig", "utf-16", "latin1"):
        try:
            return pd.read_csv(io.BytesIO(b), encoding=enc, low_memory=False)
        except Exception:
            continue
    # last attempt (let pandas guess)
    try:
        return pd.read_csv(io.BytesIO(b), low_memory=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV read error: {e}")

def df_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    buf.seek(0)
    return buf.getvalue()

@app.post("/reconcile")
async def reconcile(
    shipment_history: UploadFile = File(..., description="Shipment_History___Total-*.csv"),
    edib2bi: UploadFile = File(..., description="EDIB2BiReportV2*.csv"),
    edi940: UploadFile = File(..., description="EDI940Report_withCostV2.0*.csv"),
):
    # 1) Read the three CSVs
    df1 = load_csv_bytes(await shipment_history.read())
    df2 = load_csv_bytes(await edib2bi.read())
    df3 = load_csv_bytes(await edi940.read())

    # 2) Match your script: trim headers
    df1.columns = df1.columns.astype(str).str.strip()
    df2.columns = df2.columns.astype(str).str.strip()
    df3.columns = df3.columns.astype(str).str.strip()

    # 3) Hard required columns EXACTLY as in your code
    for name, df, col in [
        ("Shipment_History___Total", df1, "Pickticket"),
        ("EDIB2BiReportV2", df2, "AXReferenceID"),
        ("EDI940Report_withCostV2.0", df3, "PickRoute"),
    ]:
        if col not in df.columns:
            raise HTTPException(status_code=400, detail=f"Missing column '{col}' in {name}. Available: {list(df.columns)}")

    # 4) Merge logic — identical to your reference
    merged_df = pd.merge(df1, df2, how='left',
                         left_on=['Pickticket'], right_on=['AXReferenceID'])
    merged_df.columns = merged_df.columns.str.strip()

    merged_df = merged_df[[
        c for c in [
            'Warehouse', 'Pickticket', 'Order', 'Drop Date', 'Ship Date', 'Ship To',
            'Ship State', 'Zip Code', 'Customer PO', 'Ship Via', 'Load ID',
            'Weight', 'SKU', 'Units', 'Price', 'Size Type', 'Size', 'Product Type',
            'InvoiceNumber', 'StatusSummary', 'ERRORDESCRIPTION'
        ] if c in merged_df.columns
    ]]

    final_merge_df = pd.merge(merged_df, df3, how='left',
                              left_on=['Pickticket'], right_on=['PickRoute'])

    final_merge_df = final_merge_df[[
        c for c in [
            'Pickticket','Warehouse','Order','Drop Date','Ship Date','Ship To',
            'Ship State','Zip Code','Customer PO','Ship Via','Load ID',
            'Weight','SKU','Units','Price','Size Type','Size','Product Type',
            'InvoiceNumber','StatusSummary','ERRORDESCRIPTION',
            'PickRoute','SalesHeaderStatus','SalesHeaderDocStatus',
            'PickModeOfDelivery','PickCreatedDate','DeliveryDate'
        ] if c in final_merge_df.columns
    ]]

    final_merge_df = final_merge_df.rename(columns={
        'InvoiceNumber': 'Received in EDI?',
        'StatusSummary': 'EDI Processing Status',
        'ERRORDESCRIPTION': 'EDI Message',
        'PickRoute': 'Found in AX DATa?'
    })

    # 5) Filter + dedupe — same as your script
    if 'SalesHeaderDocStatus' in final_merge_df.columns and 'EDI Processing Status' in final_merge_df.columns:
        filtered_df = final_merge_df[
            final_merge_df['SalesHeaderDocStatus'].isin(['Picking List']) &
            final_merge_df['EDI Processing Status'].isin(['AX Load Failure'])
        ]
    else:
        filtered_df = final_merge_df

    if 'Pickticket' in filtered_df.columns:
        filtered_df = filtered_df.drop_duplicates(subset=['Pickticket'])

    # 6) Return Excel file
    data = df_to_xlsx_bytes(filtered_df)
    stamp = datetime.datetime.now().strftime("%m%d%y")
    # Your original script produced a name like "MISSING_945__<date>.xlsx" (double underscore).
    filename = f"MISSING_945_{stamp}.xlsx"
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
