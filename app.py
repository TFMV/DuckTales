import adbc_driver_postgresql.dbapi as pgdbapi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import pyarrow as pa
import logging
import time
from typing import Any, Dict

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseConfig(BaseModel):
    type: str
    host: str
    port: int
    user: str
    password: str
    database: str

class MigrationRequest(BaseModel):
    source: DatabaseConfig
    target: DatabaseConfig

class TableMigrationRequest(BaseModel):
    source: DatabaseConfig
    target: DatabaseConfig
    table_name: str

class ExportParquetRequest(BaseModel):
    config: DatabaseConfig
    target_directory: str

def get_pg_uri(db_config: DatabaseConfig) -> str:
    return f"postgresql://{db_config.user}:{db_config.password}@" \
           f"{db_config.host}:{db_config.port}/{db_config.database}"

def format_mysql_connection_string(config: DatabaseConfig) -> str:
    return f"host={config.host} user={config.user} port={config.port} database={config.database} password={config.password}"

def format_postgres_connection_string(config: DatabaseConfig) -> str:
    return get_pg_uri(config)

def initialize_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL mysql;")
    con.execute("INSTALL postgres;")
    con.execute("LOAD mysql;")
    con.execute("LOAD postgres;")
    return con

def attach_database(con: duckdb.DuckDBPyConnection, config: DatabaseConfig, alias: str):
    if config.type.lower() == "mysql":
        conn_string = format_mysql_connection_string(config)
    elif config.type.lower() == "postgres":
        conn_string = format_postgres_connection_string(config)
    else:
        raise ValueError(f"Unsupported database type: {config.type}")
    logger.info(f"Attaching {config.type} database: {conn_string}")
    con.execute(f"ATTACH DATABASE '{conn_string}' AS {alias} (TYPE {config.type.upper()});")

def detach_database(con: duckdb.DuckDBPyConnection, alias: str):
    try:
        logger.info(f"Detaching database alias: {alias}")
        con.execute(f"DETACH DATABASE {alias};")
    except Exception as e:
        logger.warning(f"Could not detach database alias {alias}: {e}")

def ingest_to_database(conn, table_name: str, arrow_table: pa.Table):
    with conn.cursor() as cur:
        cur.adbc_ingest(table_name, arrow_table, mode="create_append")
    conn.commit()

def summarize_arrow_table(arrow_table: pa.Table) -> Dict[str, Any]:
    summary = {
        "num_rows": arrow_table.num_rows,
        "num_columns": arrow_table.num_columns,
        "schema": str(arrow_table.schema),
    }
    return summary

con = initialize_duckdb()

@app.post("/migrate_simple")
async def migrate_simple(request: MigrationRequest):
    source_alias = "source_db"
    target_alias = "target_db"

    try:
        attach_database(con, request.source, source_alias)
        attach_database(con, request.target, target_alias)
        
        logger.info("Starting database copy operation")
        con.execute(f"COPY FROM DATABASE {source_alias} TO {target_alias};")
        logger.info("Database copy operation completed")
        
        return {"message": "Data migration completed successfully using COPY FROM DATABASE"}
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        detach_database(con, source_alias)
        detach_database(con, target_alias)

@app.post("/migrate_table")
async def migrate_table(request: TableMigrationRequest):
    source_alias = "source_db"

    try:
        attach_database(con, request.source, source_alias)
        
        logger.info(f"Fetching table: {request.table_name} as Arrow table")
        start_time = time.time()
        arrow_table = con.execute(f"SELECT * FROM {source_alias}.{request.table_name}").arrow()
        fetch_time = time.time() - start_time
        
        summary = summarize_arrow_table(arrow_table)
        logger.info(f"Fetched table summary: {summary}")
        
        target_conn_string = get_pg_uri(request.target)
        logger.info(f"Connecting to target PostgreSQL database: {target_conn_string}")
        
        start_time = time.time()
        with pgdbapi.connect(target_conn_string) as target_conn:
            logger.info(f"Ingesting table: {request.table_name} into PostgreSQL")
            ingest_to_database(target_conn, request.table_name, arrow_table)
        ingest_time = time.time() - start_time
        
        return {
            "message": "Table migration completed successfully",
            "fetch_time_seconds": fetch_time,
            "ingest_time_seconds": ingest_time,
            "summary": summary
        }
    except Exception as e:
        logger.error(f"Error during table migration: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        detach_database(con, source_alias)

@app.post("/migrate_pg_to_mysql")
async def migrate_pg_to_mysql(request: TableMigrationRequest):
    source_alias = "source_db"
    target_alias = "target_db"

    try:
        if request.source.type.lower() != "postgres" or request.target.type.lower() != "mysql":
            raise HTTPException(status_code=400, detail="Source must be PostgreSQL and target must be MySQL")

        attach_database(con, request.source, source_alias)
        attach_database(con, request.target, target_alias)
        
        logger.info("Starting CTAS operation from PostgreSQL to MySQL")
        con.execute(f"CREATE TABLE {target_alias}.{request.table_name} AS SELECT * FROM {source_alias}.{request.table_name}")
        logger.info("CTAS operation completed")
        
        return {"message": "Data migration completed successfully using CTAS"}
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        detach_database(con, source_alias)
        detach_database(con, target_alias)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
