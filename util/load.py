import pandas as pd
import psycopg2
import pathlib
import numpy as np
import os
from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, AsIs)


class Load:
    def __init__(self, hostname, dbname, username, password):
        self.hostname = hostname
        self.dbname = dbname
        self.username = username
        self.password = password

    def insert_rec_played(self):
        conn = psycopg2.connect(
            host=self.hostname,
            dbname=self.dbname,
            user=self.username,
            password=self.password,
        )

        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS RECENTLY_PLAYED;")
            cur.execute(
                """CREATE TABLE RECENTLY_PLAYED (TRACK_PLAYED_AT DATE, TRACK_ID VARCHAR(255),
                TRACK_NAME VARCHAR(255),TRACK_DURATION_MIN NUMERIC, TRACK_POPULARITY INT, PRIMARY_ARTIST VARCHAR(255),
                PRIMARY_ARTIST_ID VARCHAR(255), ALBUM_ID VARCHAR(255), ALBUM_NAME VARCHAR(255),
                ALBUM_RELEASE_YEAR DATE, PRIMARY_GENRE VARCHAR(255), TRACK_DANCEABILITY NUMERIC,
                TRACK_ENERGY NUMERIC, TRACK_KEY INT, TRACK_LOUDNESS NUMERIC, TRACK_MODE INT, TRACK_SPEECHINESS NUMERIC,
                TRACK_ACOUSTICNESS NUMERIC, TRACK_INSTRUMENTALNESS NUMERIC, TRACK_LIVENESS NUMERIC, TRACK_VALENCE NUMERIC,
                TRACK_TEMPO NUMERIC, POPULARITY INT, FOLLOWERS BIGINT,_ETL_LOADED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"""
            )

            lf = max(pathlib.Path("./extract/rp").glob("*"), key=os.path.getmtime)
            lf = str(lf)

            df = pd.read_csv(lf)

            tracks = list(df.to_records(index=False))

            cur.executemany(
                """INSERT INTO RECENTLY_PLAYED (TRACK_PLAYED_AT,TRACK_ID,TRACK_NAME,TRACK_DURATION_MIN,TRACK_POPULARITY,
                PRIMARY_ARTIST,PRIMARY_ARTIST_ID,ALBUM_ID,ALBUM_NAME,ALBUM_RELEASE_YEAR,PRIMARY_GENRE,TRACK_DANCEABILITY,
                TRACK_ENERGY,TRACK_KEY,TRACK_LOUDNESS,TRACK_MODE,TRACK_SPEECHINESS,TRACK_ACOUSTICNESS,TRACK_INSTRUMENTALNESS,TRACK_LIVENESS,
                TRACK_VALENCE,TRACK_TEMPO,POPULARITY,FOLLOWERS,_ETL_LOADED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP) VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
                tracks,
            )

            conn.commit()

        except Exception as e:
            print(e)

        finally:
            conn.close()
            print("inserted recently played tracks")

    def insert_top_artists(self):
        conn = psycopg2.connect(
            host=self.hostname,
            dbname=self.dbname,
            user=self.username,
            password=self.password,
        )

        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS TOP_ARTISTS;")
            cur.execute(
                """CREATE TABLE TOP_ARTISTS (ARTIST_ID VARCHAR(255), ARTIST_NAME VARCHAR(255), ARTIST_POPULARITY INT,
                ARTIST_FOLLOWERS BIGINT, PRIMARY_GENRE VARCHAR(255), _ETL_LOADED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"""
            )

            lf = max(pathlib.Path("./extract/ta").glob("*"), key=os.path.getmtime)
            lf = str(lf)

            df = pd.read_csv(lf)

            artists = list(df.to_records(index=False))

            cur.executemany(
                """INSERT INTO TOP_ARTISTS (ARTIST_ID,ARTIST_NAME,ARTIST_POPULARITY,ARTIST_FOLLOWERS,PRIMARY_GENRE) VALUES (%s,%s,%s,%s,%s,%s);""",
                artists,
            )

            conn.commit()

        except Exception as e:
            print(e)

        finally:
            conn.close()
            print("inserted top artists")

    def insert_top_tracks(self):
        conn = psycopg2.connect(
            host=self.hostname,
            dbname=self.dbname,
            user=self.username,
            password=self.password,
        )

        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS TOP_TRACKS;")
            cur.execute(
                """CREATE TABLE TOP_TRACKS (TRACK_ID VARCHAR(255),TRACK_NAME VARCHAR(255),TRACK_DURATION_MIN NUMERIC,
                TRACK_POPULARITY INT,PRIMARY_ARTIST VARCHAR(255),PRIMARY_ARTIST_ID VARCHAR(255), ALBUM_ID VARCHAR(255), ALBUM_NAME VARCHAR(255),
                ALBUM_RELEASE_YEAR DATE, PRIMARY_GENRE VARCHAR(255), TRACK_DANCEABILITY NUMERIC,
                TRACK_ENERGY NUMERIC, TRACK_KEY INT, TRACK_LOUDNESS NUMERIC, TRACK_MODE INT, TRACK_SPEECHINESS NUMERIC,
                TRACK_ACOUSTICNESS NUMERIC, TRACK_INSTRUMENTALNESS NUMERIC, TRACK_LIVENESS NUMERIC, TRACK_VALENCE NUMERIC,
                TRACK_TEMPO NUMERIC, POPULARITY INT, FOLLOWERS BIGINT, _ETL_LOADED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"""
            )

            lf = max(pathlib.Path("./extract/tt").glob("*"), key=os.path.getmtime)
            lf = str(lf)

            df = pd.read_csv(lf)

            top_tracks = list(df.to_records(index=False))

            cur.executemany(
                """INSERT INTO TOP_TRACKS (TRACK_ID,TRACK_NAME,TRACK_DURATION_MIN,TRACK_POPULARITY,PRIMARY_ARTIST,PRIMARY_ARTIST_ID,
                ALBUM_ID,ALBUM_NAME,ALBUM_RELEASE_YEAR,PRIMARY_GENRE,TRACK_DANCEABILITY,TRACK_ENERGY,TRACK_KEY,TRACK_LOUDNESS,TRACK_MODE,
                TRACK_SPEECHINESS,TRACK_ACOUSTICNESS,TRACK_INSTRUMENTALNESS,TRACK_LIVENESS,TRACK_VALENCE,TRACK_TEMPO,POPULARITY,FOLLOWERS) VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
                top_tracks,
            )

            conn.commit()

        except Exception as e:
            print(e)

        finally:
            conn.close()
            print("inserted top tracks")

    def insert_all_tracks(self):
        conn = psycopg2.connect(
            host=self.hostname,
            dbname=self.dbname,
            user=self.username,
            password=self.password,
        )

        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS ALL_TRACKS;")
            cur.execute(
                """CREATE TABLE ALL_TRACKS (ID VARCHAR(255), ARTISTS VARCHAR(255), TRACKNAME VARCHAR(255), ALBUM VARCHAR(255),
                _ETL_LOADED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"""
            )

            lf = max(pathlib.Path("./extract/all").glob("*"), key=os.path.getmtime)
            lf = str(lf)

            df = pd.read_csv(lf)

            all_tracks = list(df.to_records(index=False))

            cur.executemany(
                """INSERT INTO ALL_TRACKS (ID,ARTISTS,TRACKNAME,ALBUM) VALUES (%s,%s,%s,%s,%s);""",
                all_tracks,
            )

            conn.commit()

        except Exception as e:
            print(e)

        finally:
            conn.close()
            print("inserted all tracks")
