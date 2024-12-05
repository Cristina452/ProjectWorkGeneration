import mysql.connector
import os
from dotenv import load_dotenv
from connector import Connection
from youtube_scraper import YoutubeScraper
import pandas as pd

scraper = YoutubeScraper()
load_dotenv()
connection = Connection()
connection.connect_to_db()
conn = connection.get_conn()

def save_top_channel_dfs(path: str, head: int, sheet: str = None):
    df = pd.read_excel(path, sheet)
    tops = df.sort_values("subscribers", ascending=False).head(head)
    lista = []
    for id in tops["channel_ids.channelId"]:
        vids = scraper.get_video_by_channel(id)
        lista.append(vids)
    subs = []
    for element in lista:
        df = pd.DataFrame(element,
                          columns=["videoId", "title", "description", "publishedAt", "duration", "channelId",
                                   "channelTitle", "thumbnail_url", "viewCount", "likeCount", "commentCount",
                                   "categoryId", "topics"])
        subs.append(df)
    for dataframe in subs:
        dataframe.to_csv(f"{dataframe['channelTitle'][1]}.csv", index=False)


def save_csv(df, filename):
    try:
        if df is not None and not df.empty:
            df.to_csv(filename, index=False)
            print(f"File salvato con successo: {filename}")
        else:
            print(f"Nessun dato da salvare per: {filename}")
    except Exception as e:
        print(f"Errore durante il salvataggio del file {filename}: {e}")


def save_csv_from_query(query, csv_filename, conn):
    try:
        df = pd.read_sql(query, conn)

        df.to_csv(csv_filename, index=False)
        print(f"File salvato con successo: {csv_filename}")
    except mysql.connector.Error as err:
        print(f"Errore durante il recupero dei dati: {err}")
    finally:
        conn.close()


def save_sheet_from_xlsx(filename):
    xl = pd.ExcelFile(filename)
    sheets = len(xl.sheet_names)
    for i in range(sheets):
        df = pd.read_excel(filename, i)
        df.to_csv(f"{xl.sheet_names[i]}.csv",index=False,encoding="utf-8-sig",errors="ignore")
    return print("Done")

save_sheet_from_xlsx("C:/Users/omen/PycharmProjects/REPOS/Project_work_Verde/ACCENTRATORE.xlsx")