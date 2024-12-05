from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)


class ChannelDAO:
    @staticmethod
    def add_channel(channel:list):
        with engine.connect() as connection:
            connection.execute(
                text("""INSERT INTO channels (channel_id, channel_title, created, tot_views,
                 subscribers, video_count, topic_ids, topic_cat, keyword)
                  VALUES (:channelId, :channelTitle, :createdAt, :totViews,
                   :subscribers, :videoCount, :topicIds, :topicCat, :keyword)"""),
                channel
            )
            connection.commit()
            connection.close()
        return "Channels added successfully"

    @staticmethod
    def add_channel_no_kw(channel: list):
        with engine.connect() as connection:
            connection.execute(
                text("""INSERT INTO channels (channel_id, channel_title, created, tot_views,
                     subscribers, video_count, topic_ids, topic_cat)
                      VALUES (:channelId, :channelTitle, :createdAt, :totViews,
                       :subscribers, :videoCount, :topicsIds, :topicCat)"""),
                channel
            )
            connection.commit()
            connection.close()
        return "Channels added successfully"
    @staticmethod
    def get_channel_by_id(channel_id):
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT * FROM channels WHERE channel_id = :channel_id"),
                {"channel_id": channel_id}
            )
            return result.fetchone()

    @staticmethod
    def update_channel(channel_id, subscribers):
        with engine.connect() as connection:
            connection.execute(
                text("UPDATE channels SET subscribers = :subscribers WHERE channel_id = :channel_id"),
                {"subscribers": subscribers, "channel_id": channel_id}
            )

    @staticmethod
    def delete_channel(channel_id):
        with engine.connect() as connection:
            connection.execute(
                text("DELETE FROM channels WHERE channel_id = :channel_id"),
                {"channel_id": channel_id}
            )

    @staticmethod
    def drop_table():
        with engine.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS channels"))
        return "Table 'channels' dropped."


class VideoDAO:
    @staticmethod
    def add_video(video: dict):
        with engine.connect() as connection:
            connection.execute(
                text("""INSERT INTO videos (video_id, title, description, published_at, duration, view_count, like_count, 
                         comment_count, channelTitle, channel_id, thumbnail_url, category, keyword) 
                         VALUES (:videoId, :title, :description, :publishedAt, :duration, :viewCount, :likeCount, 
                         :commentCount, :channelTitle, :channelId, :thumbnail_url, :categoryId, :keyword)"""),
                video
            )
            connection.commit()
            connection.close()
        return "Videos added successfully."

    @staticmethod
    def add_video_no_kw(video: dict):
        with engine.connect() as connection:
            connection.execute(
                text("""INSERT INTO videos (video_id, title, description, published_at, duration, view_count, like_count, 
                             comment_count, channelTitle, channel_id, thumbnail_url, category,topic_categories) 
                             VALUES (:videoId, :title, :description, :publishedAt, :duration, :viewCount, :likeCount, 
                             :commentCount, :channelTitle, :channelId, :thumbnail_url, :categoryId, :topics)"""),
                video
            )
            connection.commit()
            connection.close()
        return "Videos added successfully."


    @staticmethod
    def get_video_by_id(video_id):
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT * FROM videos WHERE video_id = :video_id"),
                {"video_id": video_id}
            )
            return result.fetchone()

    @staticmethod
    def update_video(video_id, data: dict):
        with engine.connect() as connection:
            update_query = "UPDATE videos SET "
            update_query += ", ".join([f"{key} = :{key}" for key in data.keys()])
            update_query += " WHERE video_id = :video_id"
            data["video_id"] = video_id
            connection.execute(
                text(update_query),
                data
            )
        return f"Video {video_id} updated successfully."

    @staticmethod
    def delete_video(video_id):
        with engine.connect() as connection:
            connection.execute(
                text("DELETE FROM videos WHERE video_id = :video_id"),
                {"video_id": video_id}
            )
        return f"Video {video_id} deleted successfully."

    @staticmethod
    def drop_table():
        with engine.connect() as connection:
            query = "DROP TABLE IF EXISTS videos"
            connection.execute(text(query))
        return "Table 'videos' dropped."
