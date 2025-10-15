import requests
import json
from datetime import date

# import os
# from dotenv import load_dotenv 
# load_dotenv(dotenv_path="./.env")
# API_KEY = os.getenv("API_KEY")
# CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")


from airflow.decorators import task
from airflow.models import Variable


API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")

maxResults = 50 #as per youtube's api guide


#from the channel handle, get the channel's unique identifier for videos
@task
def get_playlist_id():
    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        #print(response)
        response.raise_for_status()

        data = response.json()

        # print(json.dumps(data, indent=4))

        #separating the output for readability
        #data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        channel_items = data['items'][0]
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']

        # print(channel_playlistId) 
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e
    
# if __name__== "__main__":
#     get_playlist_id()


# playlist_id = get_playlist_id()

#from the channel's unique identifier for video, get list and count of all the uploaded videos
@task
def get_video_ids(playlist_id):

    video_ids = []
    page_token = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={API_KEY}"
    
    try:
        while True:
            url = base_url

            if page_token:
                url = url + f"&pageToken={page_token}"   #set the url to base_url of no nextPageToken was found in the response

            response = requests.get(url)

            #print(response)
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            
            page_token = data.get('nextPageToken')

            #no more nextPageToken found
            if not page_token:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e


""" this is super helpful to process the request to the API in chunks because we have 2694 videos and 
iterating each video one by one will just cause our request and error out after reaching the established quota by YT API guideline, these are reasons for batching the request
"""
# def batch_size_helper(videoid_list, batch_size):
#     for videoid in range(0,len(videoid_list),batch_size):
#         yield videoid_list[videoid: videoid + batch_size]


@task
def extract_video_details(video_ids):
    extracted_details = []

    def batch_size_helper(videoid_list, batch_size):
        for videoid in range(0,len(videoid_list),batch_size):
            yield videoid_list[videoid: videoid + batch_size]

    try:
        for video_id_batches in batch_size_helper(video_ids, maxResults):
            video_ids_str = ",".join(video_id_batches)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)

            #print(response)
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
            
                #from here I want specific info from each of these keys
                video_dict = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount', None), #similar to COELSECE IN SQL, if no counts replace with none as some videos may not have any of the stats
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None)
                }

                extracted_details.append(video_dict)

        return extracted_details
    
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_dict_to_json(extracted_details):
    file_path = f"./data/SORTED_YT_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(extracted_details, f, indent=4)


def main():
    print("Getting playlistid for the handle..")
    playlist_id = get_playlist_id()
    print(f"Returned playlist_id:{playlist_id}")

    print("Get the videos from the channel") # as of oct 2 2025, there are 2694 videos for this channel
    videoid_list = get_video_ids(playlist_id)
    print(f"Total number of videos so far: {len(videoid_list)}")
    print(videoid_list)

    print("Getting details of each video from the list in the batches of 50")
    video_detail_dict = extract_video_details(videoid_list)
    # print(video_detail_dict)

    print("Save the video details to a JSON file")
    save_dict_to_json(video_detail_dict)


if __name__ == "__main__":
    main()

