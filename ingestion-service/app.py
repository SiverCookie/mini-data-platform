from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
from pathlib import Path
import uuid

app = FastAPI(title="Ingestion Service")

RAW_DATA_DIR = Path("/data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


@app.post("/ingest/csv")
async def ingest_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        df = pd.read_csv(file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV file: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded CSV is empty")

    # Minimal schema sanity check (generic on purpose)
    if df.shape[1] < 2:
        raise HTTPException(
            status_code=400,
            detail="CSV must contain at least 2 columns"
        )

    file_id = uuid.uuid4().hex
    output_path = RAW_DATA_DIR / f"{file_id}.csv"
    df.to_csv(output_path, index=False)

    return {
        "status": "success",
        "rows": len(df),
        "columns": list(df.columns),
        "file_id": file_id,
        "stored_at": str(output_path)
    }


@app.get("/health")
def health_check():
    return {"status": "ok"}