import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()


class Connection:
    def __init__(self):
        self._connection = None
        self._cursor = None

    def first_connection(self):
        self._connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        self._cursor = self._connection.cursor()
        return self._cursor

    def connect_to_db(self):
        self._connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        self._cursor = self._connection.cursor()
        return self._cursor

    def commit(self):
        self._connection.commit()

    def close(self):
        self._connection.commit()
        self._connection.close()
        self._cursor.close()

    def get_conn(self):
        return self._connection