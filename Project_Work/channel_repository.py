from youtube_scraper import YoutubeScraper
from youtube_dao import ChannelDAO
from datetime import datetime
import pandas as pd
from db_setup import *


class ChannelRepository:
    def __init__(self, dao: ChannelDAO, scraper: YoutubeScraper):
        self.dao = dao
        self.scraper = scraper
        self.last_update = None

    def search_and_store_channels(self, max_results, region_code, keyword):
        try:
            tmp, kw = self.scraper.get_channels(max_results, region_code, keyword)
            channels = self.scraper.get_channels_details(keyword, tmp)
            for channel in channels:
                existing_channel = self.dao.get_channel_by_id(channel['channelId'])
                if not existing_channel:
                    self.dao.add_channel(channel)
                    print(f"Canale {channel['channelTitle']} aggiunto al database.")
                else:
                    print(f"Canale {channel['channelTitle']} già presente nel database.")

            self.last_update = datetime.now()

            df = pd.DataFrame(channels, columns=["channelId", "channelTitle", "createdAt", "totViews", "subscribers",
                                                 "videoCount", "topicIds", "topicCat", "keyword"])
            return df

        except Exception as e:
            print(f"Errore durante la ricerca e l'inserimento dei canali: {e}")
            return None

    def update_existing_channels(self, max_results, region_code, keyword=None, category=None):
        self.drop()
        if keyword:
            df = self.search_and_store_channels(max_results, region_code, keyword)
            return df
        elif category:
            df = self.list_channels_by_cat(max_results, region_code, category)
            return df
        elif keyword and category:
            raise Exception("Non puoi fare la ricerca con entrambi i parametri..")

    def drop(self):
        try:
            self.dao.drop_table()
            create_channels()
            print("Tabella canali eliminata con successo.")
        except Exception as e:
            print(f"Errore durante l'eliminazione della tabella canali: {e}")

    def list_channels_by_cat(self, max_results, region, category="0"):
        videos = self.scraper.get_videos_by_category(max_results, region, category)
        channels = self.scraper.get_channels_details(videos)
        for channel in channels:
            existing_channel = self.dao.get_channel_by_id(channel['channelId'])
            if not existing_channel:
                self.dao.add_channel_no_kw(channel)
                print(f"Canale {channel['channelTitle']} aggiunto al database.")
            else:
                print(f"Canale {channel['channelTitle']} già presente nel database.")

        self.last_update = datetime.now()

        df = pd.DataFrame(channels, columns=["channelId", "channelTitle", "createdAt", "totViews", "subscribers",
                                             "videoCount", "topicsIds", "topicCat"])

        return df


