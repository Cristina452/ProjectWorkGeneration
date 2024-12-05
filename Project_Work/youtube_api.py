import googleapiclient.discovery

api_key = 'AIzaSyADXCZVvZUB23vasq9espFea6woJDdEOyk'


def get_youtube_service():
    api_service_name = "youtube"
    api_version = "v3"

    return googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key
    )


def get_channel_info(channel_username):
    youtube = get_youtube_service()

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        forUsername=channel_username
    )
    response = request.execute()

    return response


def get_channel_videos(playlist_id, max_results=5):
    youtube = get_youtube_service()

    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=max_results,
        playlistId=playlist_id
    )
    response = request.execute()

    return response


def get_video_statistics(video_id):
    youtube = get_youtube_service()

    request = youtube.videos().list(
        part="snippet,statistics",
        id=video_id
    )
    response = request.execute()

    return response