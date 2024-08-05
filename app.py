import adbc_driver_postgresql.dbapi as pgdbapi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import pyarrow as pa

app = FastAPI()

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

def format_mysql_connection_string(config: DatabaseConfig) -> str:
    return f"host={config.host} user={config.user} port={config.port} database={config.database} password={config.password}"

def format_postgres_connection_string(config: DatabaseConfig) -> str:
    return f"host={config.host} port={config.port} user={config.user} password={config.password} dbname={config.database}"

def initialize_duckdb():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL mysql;")
    con.execute("INSTALL postgres;")
    con.execute("LOAD mysql;")
    con.execute("LOAD postgres;")
    return con

con = initialize_duckdb()

@app.post("/migrate_simple")
async def migrate_simple(request: MigrationRequest):
    try:
        source_conn_string = (
            format_mysql_connection_string(request.source)
            if request.source.type == "mysql"
            else format_postgres_connection_string(request.source)
        )
        target_conn_string = (
            format_mysql_connection_string(request.target)
            if request.target.type == "mysql"
            else format_postgres_connection_string(request.target)
        )
        
        # Attach source and target databases with unique aliases
        source_alias = "source_db"
        target_alias = "target_db"
        
        # Detach databases if already attached
        try:
            con.execute(f"DETACH DATABASE {source_alias}")
        except Exception:
            pass
        try:
            con.execute(f"DETACH DATABASE {target_alias}")
        except Exception:
            pass
        
        con.execute(f"ATTACH DATABASE '{source_conn_string}' AS {source_alias} (TYPE {request.source.type.upper()});")
        con.execute(f"ATTACH DATABASE '{target_conn_string}' AS {target_alias} (TYPE {request.target.type.upper()});")
        
        # Copy the entire source database to the target database
        con.execute(f"COPY FROM DATABASE {source_alias} TO {target_alias};")
        
        # Detach databases to clean up
        con.execute(f"DETACH DATABASE {source_alias};")
        con.execute(f"DETACH DATABASE {target_alias};")

        return {
            "message": "Data migration completed successfully using COPY FROM DATABASE"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def attach_database(con, config: DatabaseConfig, alias: str):
    if config.type.lower() == "mysql":
        conn_string = format_mysql_connection_string(config)
        con.execute(f"ATTACH DATABASE '{conn_string}' AS {alias} (TYPE MYSQL);")
    elif config.type.lower() == "postgres":
        conn_string = format_postgres_connection_string(config)
        con.execute(f"ATTACH DATABASE '{conn_string}' AS {alias} (TYPE POSTGRES);")
    else:
        raise ValueError(f"Unsupported database type: {config.type}")

def ingest_to_database(conn, table_name: str, arrow_table: pa.Table):
    with conn.cursor() as cur:
        cur.adbc_ingest(table_name, arrow_table, mode="create_append")
    conn.commit()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
