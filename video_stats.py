import requests
import json

import os
from dotenv import load_dotenv


load_dotenv(dotenv_path="./.env") #current directory's .env file

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "SortedFood"

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
    except requests.exception.RequestException as e:
        raise e
    
# if __name__== "__main__":
#     get_playlist_id()

def main():
    print("Getting playlistid for the handle..")
    playlist_id = get_playlist_id()
    print(f"Returned playlist_id:{playlist_id}")

if __name__ == "__main__":
    main()

