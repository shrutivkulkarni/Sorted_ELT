from datawarehouse.data_utils import  get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import parse_duration, transform_data


import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "sorted_api"

@task
def staging_table():
    schema = 'staging'

    conn,cur = None, None

    try:
        conn,cur = get_conn_cursor()

        Sorted_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        for row in Sorted_data:
            if len(table_ids) == 0: #first ever insert only
                insert_rows(cur, conn, schema, row)
            else:
                if row['video_ID'] in table_ids: #update existing video_id for newer commentcount or viewcount or likecount
                    update_rows(cur, conn, schema, row)
                else: 
                    insert_rows(cur, conn, schema, row)
                
        ids_in_json = {row['video_ID'] for row in Sorted_data}

        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.exception(f"An error occurred during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor (conn, cur)

@task
def core_table():
    schema = 'core'

    conn,cur = None, None

    try:
        conn,cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()  #this will not work when teh table contains 1000's of rows and will batch processing

        for row in rows:
            current_video_ids.add(row['video_ID'])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            else:
                transformed_row = transform_data(row)
                if transformed_row['video_ID'] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)
        
        ids_to_delete = set(table_ids) - current_video_ids
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.exception(f"An error occurred during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor (conn, cur)