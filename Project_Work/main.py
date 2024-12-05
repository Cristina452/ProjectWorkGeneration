import os
import pandas as pd
from youtube_scraper import YoutubeScraper
from youtube_dao import ChannelDAO, VideoDAO
from channel_repository import ChannelRepository
from video_repository import VideoRepository
import db_setup
import csv_saver as saver
from connector import Connection

scraper = YoutubeScraper()
channel_dao = ChannelDAO()
video_dao = VideoDAO()
connect = Connection()
conn = connect.get_conn()
channel_repo = ChannelRepository(channel_dao, scraper)
video_repo = VideoRepository(video_dao, scraper)


categories = {
    "film_and_animation": 1,
    "sports": 17,
    "gaming": 20,
    "science_and_tech": 28
}

max_results = 50
region_code = 'US'


for category_name, category_id in categories.items():
    try:
        channels_df = channel_repo.list_channels_by_cat(max_results, region_code, category_id)
        channel_csv_path = f"channels_{category_name}.csv"
        saver.save_csv(channels_df, channel_csv_path)
    except Exception as e:
        print(f"Errore durante la generazione dei canali per {category_name}: {e}")

    try:
        video_df = video_repo.list_video_by_cat(max_results, region_code, category_id)
        video_csv_path = f"videos_{category_name}.csv"
        saver.save_csv(video_df, video_csv_path)
    except Exception as e:
        print(f"Errore durante la generazione dei video per {category_name}: {e}")

print("Processo completato per tutte le categorie.")


query_topics = "SELECT * FROM topics"
saver.save_csv_from_query(query_topics, "topics.csv",conn)

query_categories = "SELECT * FROM categories"
saver.save_csv_from_query(query_categories, "categories.csv",conn)

saver.save_top_channel_dfs('C:/Users/omen/PycharmProjects/REPOS/Project_work_Verde/ACCENTRATORE.xlsx',5,"Temp")
