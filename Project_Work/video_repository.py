from youtube_scraper import YoutubeScraper
from youtube_dao import *
from datetime import datetime
import pandas as pd
from db_setup import *


class VideoRepository:
    def __init__(self, dao: VideoDAO, scraper: YoutubeScraper):
        self.dao = dao
        self.scraper = scraper
        self.last_update = None

    def search_and_store_videos(self, max_results, region_code, keyword):
        try:
            tmp, kw = self.scraper.get_videos(max_results, region_code, keyword)
            videos = self.scraper.get_video_details(tmp, kw)

            for video in videos:
                existing_video = self.dao.get_video_by_id(video['videoId'])
                if not existing_video:
                    self.dao.add_video(video)
                    print(f"Video {video['title']} aggiunto al database.")
                else:
                    print(f"Video {video['title']} già presente nel database.")

            self.last_update = datetime.now()

            df = pd.DataFrame(videos,
                              columns=["videoId", "title", "description", "publishedAt", "duration", "channelId",
                                       "channelTitle", "thumbnail_url", "viewCount", "likes", "comments",
                                       "categoryId", "keyword"])
            return df

        except Exception as e:
            print(f"Errore durante la ricerca e l'inserimento dei video: {e}")
            return None

    def update_existing_videos(self, max_results, region_code, keyword=None,category=None):
        self.drop()
        if keyword:
            df = self.search_and_store_videos(max_results, region_code, keyword)
            return df
        elif category:
            df = self.list_video_by_cat(max_results, region_code,category)
            return df
        elif keyword and category:
            raise Exception("Non puoi cercare sia con keyword che categoria!")


    def drop(self):
        try:
            self.dao.drop_table()
            create_videos()
            print("Tabella video eliminata con successo.")
        except Exception as e:
            print(f"Errore durante l'eliminazione della tabella video: {e}")

    def list_video_by_cat(self,max_results, region_code,category="0"):
        videos = self.scraper.get_videos_by_category(max_results,region_code,category)
        for video in videos:
            existing_video = self.dao.get_video_by_id(video['videoId'])
            if not existing_video:
                self.dao.add_video_no_kw(video)
                print(f"Video {video['title']} aggiunto al database.")
            else:
                print(f"Video {video['title']} già presente nel database.")

        self.last_update = datetime.now()

        df = pd.DataFrame(videos,
                          columns=["videoId", "title", "description", "publishedAt", "duration", "channelId",
                                   "channelTitle", "thumbnail_url", "viewCount", "likeCount", "commentCount",
                                   "categoryId","topics"])
        return df



