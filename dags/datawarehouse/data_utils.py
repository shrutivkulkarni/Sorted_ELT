from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import RealDictCursor

table = "yt_api"


def get_conn_cursor():
    #conn_id: defined in docker-compose.yaml & database in .env file for username & pw 
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db") 
    conn = hook.get_conn() 
    #outputs data from the SQL query as a Python dict and not a default tuple
    cur = conn.cursor(cursorfor_factory=RealDictCursor) 
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()

    schema_sql_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql_query)
    
    conn.commit()

    close_conn_cursor(conn,cur)

def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql_query = f""" 
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "video_title" TEXT NOT NULL,
                "upload_date" TIMESTAMP NOT NULL,
                "duration" VARCHAR(20) NOT NULL,
                "video_views" INT,
                "likes_count" INT,
                "comments_count" INT
            );
        """
    else:
        table_sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "video_title" TEXT NOT NULL,
                    "upload_date" TIMESTAMP NOT NULL,
                    "duration" TIME NOT NULL,
                    "video_type" VARCHAR(10) NOT NULL,
                    "video_views" INT,
                    "likes_count" INT,
                    "comments_count" INT 
                );
        """

    cur.execute(table_sql_query)
    
    conn.commit()

    close_conn_cursor(conn,cur)

def get_video_ids(cur, schema):
    cur.execute(f""" SELECT video_ID FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row['video_ID'] for row in ids]
    # cur.close()

    return video_ids
        
