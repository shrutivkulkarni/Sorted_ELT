import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):
    
    try:
        if schema == 'staging':
            video_id = 'video_id'

            cur.execute(f""" 
                INSERT INTO {schema}.{table} ("video_ID", "video_title", "upload_date", "duration", "video_views", "likes_count", "comments_count")
                VALUES(%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
            """, row
            )
        #for core layer, the values are from the staging layer
        else:
            video_id = 'video_ID'

            cur.execute(f""" 
                INSERT INTO {schema}.{table} ("video_ID", "video_title", "upload_date", "duration", "video_views", "likes_count", "comments_count")
                VALUES(%(video_ID)s, %(video_title)s, %(upload_date)s, %(duration)s, %(video_views)s, %(likes_count)s, %(comments_count)s);
            """, row
            )
        conn.commit()

        logger.info(f"Inserted row for video_id: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error occurred in inserting row for video_id: {row[video_id]}")
        raise e

def update_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"

        #core table
        else:
            video_id = "video_ID"
            upload_date = "upload_date"
            video_title = "video_title"
            video_views = "video_views"
            likes_count = "likes_count"
            comments_count = "comments_count"

        cur.execute(
            f"""UPDATE  {schema}.{table}  
                SET "video_title" = %({video_title})s,
                    "video_views" = %({video_views})s,
                    "likes_count" = %({likes_count})s,
                    "comments_count" = %({comments_count})s 
                WHERE "video_id" = %({video_id})s AND "upload_date" = %({upload_date})s;
            """, row
            )
        
        conn.commit()

        logger.info(f"Updated row for video_id: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error occurred in updating row for video_id: {row[video_id]}")
        raise e
    

def delete_rows(cur, conn, schema, ids_to_delete):

    try:
        #there could be one or multiple videos that the channel could decide to delete, hence this join
        ids_to_delete = f"""({', '.join(f"'{id}' for id in ids_to_delete:")})"""

        cur.execute(
            f"""DELETE FROM {schema}.{table} 
                WHERE "video_id" IN {ids_to_delete};"""
        )
        conn.commit()

        logger.info(f"Deleted rows with video_ids: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error occurred in deleting rows with video_ids: {ids_to_delete}")
        raise e
    
                    

            



            


