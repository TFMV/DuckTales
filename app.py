import duckdb
from fsspec import filesystem
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

app = FastAPI()

# Register the GCS filesystem with DuckDB
fs = filesystem('gcs')
duckdb.register_filesystem(fs)

class QueryRequest(BaseModel):
    query: str

@app.post("/query")
def query_db(request: QueryRequest):
    try:
        result = duckdb.sql(request.query).fetchall()
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
