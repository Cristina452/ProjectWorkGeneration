import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from connector import Connection

load_dotenv()


def drop_schema(cursor):
    cursor.execute("""DROP SCHEMA IF EXISTS youtube;""")


def create_schema(cursor):
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS youtube;""")


def create_channels(cursor):

    cursor.execute("CREATE TABLE IF NOT EXISTS channels (\
         id INT(20) PRIMARY KEY AUTO_INCREMENT,\
         channel_id VARCHAR (60) UNIQUE NOT NULL,\
         channel_title VARCHAR(100) UNIQUE,\
         created VARCHAR(255),\
         tot_views BIGINT,\
         subscribers BIGINT,\
         video_count INT,\
         topic_ids VARCHAR(800),\
         topic_cat VARCHAR(800),\
         keyword VARCHAR(255) NULL\
         );")


def create_videos(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS videos (\
    id INT PRIMARY KEY AUTO_INCREMENT,\
    video_id VARCHAR(60) NOT NULL,\
    title VARCHAR(255) NOT NULL,\
    description TEXT,\
    published_at VARCHAR(255),\
    duration VARCHAR(100),\
    view_count INT,\
    like_count INT,\
    comment_count INT,\
    channelTitle VARCHAR(100),\
    channel_id VARCHAR(30),\
    thumbnail_url VARCHAR(70),\
    category INT,\
    keyword VARCHAR(255) NULL,\
    topic_categories VARCHAR(800)\
    );")


def create_topics(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS topics (\
    id INT PRIMARY KEY AUTO_INCREMENT,\
    topic_id VARCHAR (60) UNIQUE NOT NULL,\
    topic VARCHAR(255) NOT NULL\
    );")


def create_categories(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS categories (\
    id INT PRIMARY KEY AUTO_INCREMENT,\
    category_id VARCHAR (60) UNIQUE NOT NULL,\
    category VARCHAR(255) NOT NULL\
    );")


def insert_topics(cursor):

    topics_data = [
        ('/m/04rlf', 'Music (parent topic)'),
        ('/m/02mscn', 'Christian music'),
        ('/m/0ggq0m', 'Classical music'),
        ('/m/01lyv', 'Country'),
        ('/m/02lkt', 'Electronic music'),
        ('/m/0glt670', 'Hip hop music'),
        ('/m/05rwpb', 'Independent music'),
        ('/m/03_d0', 'Jazz'),
        ('/m/028sqc', 'Music of Asia'),
        ('/m/0g293', 'Music of Latin America'),
        ('/m/064t9', 'Pop music'),
        ('/m/06cqb', 'Reggae'),
        ('/m/06j6l', 'Rhythm and blues'),
        ('/m/06by7', 'Rock music'),
        ('/m/0gywn', 'Soul music'),
        ('/m/0bzvm2', 'Gaming (parent topic)'),
        ('/m/025zzc', 'Action game'),
        ('/m/02ntfj', 'Action-adventure game'),
        ('/m/0b1vjn', 'Casual game'),
        ('/m/02hygl', 'Music video game'),
        ('/m/04q1x3q', 'Puzzle video game'),
        ('/m/01sjng', 'Racing video game'),
        ('/m/0403l3g', 'Role-playing video game'),
        ('/m/021bp2', 'Simulation video game'),
        ('/m/022dc6', 'Sports game'),
        ('/m/03hf_rm', 'Strategy video game'),
        ('/m/06ntj', 'Sports (parent topic)'),
        ('/m/0jm_', 'American football'),
        ('/m/018jz', 'Baseball'),
        ('/m/018w8', 'Basketball'),
        ('/m/01cgz', 'Boxing'),
        ('/m/09xp_', 'Cricket'),
        ('/m/02vx4', 'Football'),
        ('/m/037hz', 'Golf'),
        ('/m/03tmr', 'Ice hockey'),
        ('/m/01h7lh', 'Mixed martial arts'),
        ('/m/0410tth', 'Motorsport'),
        ('/m/07bs0', 'Tennis'),
        ('/m/07_53', 'Volleyball'),
        ('/m/02jjt', 'Entertainment (parent topic)'),
        ('/m/09kqc', 'Humor'),
        ('/m/02vxn', 'Movies'),
        ('/m/05qjc', 'Performing arts'),
        ('/m/066wd', 'Professional wrestling'),
        ('/m/0f2f9', 'TV shows'),
        ('/m/019_rr', 'Lifestyle (parent topic)'),
        ('/m/032tl', 'Fashion'),
        ('/m/027x7n', 'Fitness'),
        ('/m/02wbm', 'Food'),
        ('/m/03glg', 'Hobby'),
        ('/m/068hy', 'Pets'),
        ('/m/041xxh', 'Physical attractiveness [Beauty]'),
        ('/m/07c1v', 'Technology'),
        ('/m/07bxq', 'Tourism'),
        ('/m/07yv9', 'Vehicles'),
        ('/m/098wr', 'Society (parent topic)'),
        ('/m/09s1f', 'Business'),
        ('/m/0kt51', 'Health'),
        ('/m/01h6rj', 'Military'),
        ('/m/05qt0', 'Politics'),
        ('/m/06bvp', 'Religion'),
        ('/m/01k8wb', 'Knowledge')
    ]

    cursor.executemany("INSERT INTO topics (topic_id, topic) VALUES (%s, %s)",topics_data)


def insert_categories(cursor):
    categories_data = [
        (1, 'Film & Animation'),
        (2, 'Autos & Vehicles'),
        (10, 'Music'),
        (15, 'Pets & Animals'),
        (17, 'Sports'),
        (18, 'Short Movies'),
        (19, 'Travel & Events'),
        (20, 'Gaming'),
        (21, 'Videoblogging'),
        (22, 'People & Blogs'),
        (23, 'Comedy'),
        (24, 'Entertainment'),
        (25, 'News & Politics'),
        (26, 'Howto & Style'),
        (27, 'Education'),
        (28, 'Science & Technology'),
        (29, 'Nonprofits & Activism'),
        (30, 'Movies'),
        (31, 'Anime/Animation'),
        (32, 'Action/Adventure'),
        (33, 'Classics'),
        (34, 'Comedy'),
        (35, 'Documentary'),
        (36, 'Drama'),
        (37, 'Family'),
        (38, 'Foreign'),
        (39, 'Horror'),
        (40, 'Sci-Fi/Fantasy'),
        (41, 'Thriller'),
        (42, 'Shorts'),
        (43, 'Shows'),
        (44, 'Trailers')
    ]

    cursor.executemany("INSERT INTO categories (category_id, category) VALUES (%s, %s)",categories_data)


if __name__ == "main":
    connection = Connection()
    cursore = connection.first_connection()

    drop_schema(cursore)

    connection.commit()
    connection.close()

    cursore = connection.first_connection()

    create_schema(cursore)
    connection.commit()
    connection.close()

    cursore = connection.connect_to_db()
    create_channels(cursore)
    create_videos(cursore)
    create_topics(cursore)
    create_categories(cursore)
    insert_topics(cursore)
    insert_categories(cursore)

    connection.commit()
    connection.close()

