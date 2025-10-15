from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_playlist_id, get_video_ids, extract_video_details, save_dict_to_json

#define localtimezone
local_tz = pendulum.timezone('America/Los_Angeles')
print(local_tz)


# Default Args
default_args = {
    "owner": "analyticsengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "analytics@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description= 'DAG to produce json file using extracted data',
    schedule='0 14 * * *',
    catchup=False
) as dag:

    #Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_video_stats = extract_video_details(video_ids)
    save_data_to_json = save_dict_to_json(extract_video_stats)

    #Define dependencies
    playlist_id >> video_ids >> extract_video_stats >> save_data_to_json

