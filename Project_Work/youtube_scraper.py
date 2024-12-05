import googleapiclient.discovery
from googleapiclient.errors import HttpError


class YoutubeScraper:
    """This class can:
        a) return a list of videos by category;
            a.1) get the information associated with the channels of these videos;
        b) return the results of a YouTube search for videos/channels, much like in the actual app;
            b.1) get the detailed information of the videos/channel based on this search.
        c) returns the 50 most popular videos from a channel"""

    def __init__(self):
        self.api_key = "AIzaSyADXCZVvZUB23vasq9espFea6woJDdEOyk"
        self.youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=self.api_key)

    def get_video_by_channel(self, channel_id):
        request = self.youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,
            order="viewCount"
        )
        response = request.execute()
        videos = []
        videos_det=[]
        for element in response["items"]:
            video = {
                'videoId': element["id"]["videoId"],
                'videoTitle': element["snippet"]["title"],
                'description': element["snippet"]["description"],
                'publishedAt': element["snippet"]["publishedAt"]
            }
            videos.append(video)
        # for element in videos:
        #     if element["videoId"]:
        videoIds = [vid["videoId"] for vid in videos]
        request = self.youtube.videos().list(
            part="snippet,statistics,contentDetails,topicDetails",
            id=",".join(videoIds)
        )
        response = request.execute()
        for a in response["items"]:
            video = {
                'videoId': a['id'],
                'title': a['snippet']['title'],
                'description': a['snippet']['description'],
                'publishedAt': a['snippet']['publishedAt'],
                'duration': a['contentDetails']["duration"],
                'channelId': a['snippet']['channelId'],
                'channelTitle': a['snippet']['channelTitle'],
                'thumbnail_url': a['snippet']['thumbnails']['default']['url'],
                'viewCount': a["statistics"]["viewCount"],
                'likeCount': a["statistics"]["likeCount"] if "likeCount" in a["statistics"] else 0,
                'commentCount': a["statistics"]["commentCount"] if "commentCount" in a["statistics"] else 0,
                'categoryId': a["snippet"]["categoryId"],
                'topics': ",".join(a["topicDetails"]["topicCategories"]) if "topicDetails" in a else ""
            }
            videos_det.append(video)
        return videos_det

    def get_videos_by_category(self, max_results, region, category="0"):
        """returns a list with the top n videos in the region by category"""
        request = self.youtube.videos().list(
            part="snippet,contentDetails,statistics,topicDetails",
            chart="mostPopular",
            videoCategoryId=category,
            regionCode=region,
            maxResults=max_results
        )
        response = request.execute()
        videos = []
        for element in response["items"]:
            video = {
                'videoId': element['id'],
                'title': element['snippet']['title'],
                'description': element['snippet']['description'],
                'publishedAt': element['snippet']['publishedAt'],
                'duration': element['contentDetails']['duration'],
                'channelId': element['snippet']['channelId'],
                'channelTitle': element['snippet']['channelTitle'],
                'thumbnail_url': element['snippet']['thumbnails']['default']['url'],
                'viewCount': element["statistics"]["viewCount"],
                'likeCount': element["statistics"]["likeCount"] if "likeCount" in element["statistics"] else 0,
                'commentCount': element["statistics"]["commentCount"] if "commentCount" in element["statistics"] else 0,
                'categoryId': category,
                'topics': ",".join(element["topicDetails"]["topicCategories"]) if "topicDetails" in element else ""
            }
            videos.append(video)
        while len(videos) < response["pageInfo"]["totalResults"]:
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics,topicDetails",
                chart="mostPopular",
                videoCategoryId=category,
                regionCode=region,
                maxResults=max_results,
                pageToken=response["nextPageToken"] if "nextPageToken" in response else ""
            )
            response = request.execute()
            for element in response["items"]:
                video = {
                    'videoId': element['id'],
                    'title': element['snippet']['title'],
                    'description': element['snippet']['description'],
                    'publishedAt': element['snippet']['publishedAt'],
                    'duration': element['contentDetails']['duration'],
                    'channelId': element['snippet']['channelId'],
                    'channelTitle': element['snippet']['channelTitle'],
                    'thumbnail_url': element['snippet']['thumbnails']['default']['url'],
                    'viewCount': element["statistics"]["viewCount"],
                    'likeCount': element["statistics"]["likeCount"] if "likeCount" in element["statistics"] else 0,
                    'commentCount': element["statistics"]["commentCount"] if "commentCount" in element[
                        "statistics"] else 0,
                    'categoryId': category,
                    'topics': ",".join(element["topicDetails"]["topicCategories"])if "topicDetails" in element else ""
                }
                videos.append(video)
        return videos

    def get_channels_details(self, resource_list: list, keyword=None):
        """returns the channel details from the resource list"""
        channels = []
        # channelIds = set([a["channelId"] for a in resource_list])
        for element in resource_list:
            if element["channelId"]:
                request = self.youtube.channels().list(
                    part="snippet,statistics,topicDetails",
                    id=element["channelId"]
                )
                response = request.execute()
                for a in response["items"]:
                    channel = {
                        'channelId': a["id"],
                        'channelTitle': a["snippet"]["title"],
                        'createdAt': a["snippet"]["publishedAt"],
                        'totViews': a["statistics"]["viewCount"] if "viewCount" in a["statistics"] else 0,
                        'subscribers': a["statistics"]["subscriberCount"] if "subscriberCount" in a[
                            "statistics"] else 0,
                        'videoCount': a["statistics"]["videoCount"] if "videoCount" in a["statistics"] else 0,
                        'topicsIds': ",".join(a["topicDetails"]["topicIds"]) if "topicDetails" in a and "topicIds" in a["topicDetails"] else "",
                        'topicCat': ",".join(a["topicDetails"]["topicCategories"])if "topicDetails" in a and "topicCategories" in a["topicDetails"] else ""
                    }
                    if keyword:
                        channel.update({"keyword": keyword})
                    channels.append(channel)

        return channels

    def get_channels(self, max_results, region_code, keyword):
        """returns a simple list of the top channels in the region by keyword"""
        keyword = keyword
        request = self.youtube.search().list(
            part="snippet",
            type="channel",
            order="relevance",
            maxResults=max_results,
            regionCode=region_code,
            q=keyword
        )
        response = request.execute()
        channels = []
        for element in response["items"]:
            channel = {
                'channelId': element["id"]["channelId"],
                'channelTitle': element["snippet"]["title"],
                'description': element["snippet"]["description"],
                'createdAt': element["snippet"]["publishedAt"]
            }
            channels.append(channel)
        return channels, keyword

    def get_videos(self, max_results, region_code, keyword):
        # returns the same results as a YouTube search
        keyword = keyword
        request = self.youtube.search().list(
            part="snippet",
            type="video",
            order="viewCount",
            maxResults=max_results,
            regionCode=region_code,
            q=keyword
        )
        response = request.execute()
        videos = []
        for element in response["items"]:
            video = {
                'videoId': element["id"]["videoId"],
                'videoTitle': element["snippet"]["title"],
                'description': element["snippet"]["description"],
                'publishedAt': element["snippet"]["publishedAt"]
            }
            videos.append(video)
        return videos, keyword

    def get_video_details(self, resource_list, keyword):
        videos = []
        for element in resource_list:
            if element["videoId"]:
                request = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=element["videoId"]
                )
                response = request.execute()
                for a in response["items"]:
                    video = {
                        'videoId': a['id'],
                        'title': a['snippet']['title'],
                        'description': a['snippet']['description'],
                        'publishedAt': a['snippet']['publishedAt'],
                        'duration': a['contentDetails']["duration"],
                        'channelId': a['snippet']['channelId'],
                        'channelTitle': a['snippet']['channelTitle'],
                        'thumbnail_url': a['snippet']['thumbnails']['default']['url'],
                        'viewCount': a["statistics"]["viewCount"],
                        'likeCount': a["statistics"]["likeCount"] if "likeCount" in a["statistics"] else 0,
                        'commentCount': a["statistics"]["commentCount"] if "commentCount" in a["statistics"] else 0,
                        'categoryId': a["snippet"]["categoryId"],
                        'keyword': keyword
                    }
                    videos.append(video)
            else:
                raise KeyError("Key 'videoId' not found.")

        return videos
