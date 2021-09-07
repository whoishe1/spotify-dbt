from util.spotify import SpotifyTracks
from util.alltracks import ReturnPlaylist
from util.filter import edit_columns
from util.load import Load
import os
from dotenv import dotenv_values
from datetime import datetime
import pandas as pd


config = dotenv_values(".env")


def get_spotify():

    spotify_obj = SpotifyTracks(
        username=config["USERNAME"],
        client_id=config["CLIENT_ID"],
        client_secret=config["CLIENT_SECRET"],
    )

    top_artists = spotify_obj.current_top_artists("user-top-read")
    recently_played = spotify_obj.rec_played_or_top_tracks("user-read-recently-played")
    top_tracks = spotify_obj.rec_played_or_top_tracks("user-top-read")

    top_artists.to_csv(os.path.join("./extract", "top_artists.csv"), index=False, encoding="utf-8")
    recently_played.to_csv(
        os.path.join("./extract", "recentlyplayed.csv"), index=False, encoding="utf-8"
    )
    top_tracks.to_csv(os.path.join("./extract", "top_tracks.csv"), index=False, encoding="utf-8")


def get_alltracks():
    date = datetime.today().strftime("%Y-%m-%d")

    alltracks_obj = ReturnPlaylist(
        username=config["USERNAME"],
        scope="playlist-read-private",
        client_id=config["CLIENT_ID"],
        client_secret=config["CLIENT_SECRET"],
    )

    pl = ["lp", "the six", "the three", "the two", "the one"]

    output = []
    for i in pl:
        df = alltracks_obj.get_playlists(i)
        output.append(df)

    all_df = pd.concat(output).reset_index(drop=True)

    all_df.to_csv(
        os.path.join("./extract/all", f"spotify_playlist_{date}.csv"),
        index=False,
        encoding="utf-8",
    )


def load_data():
    load_obj = Load("localhost", "spotify", "postgres", "postgres")

    load_obj.insert_rec_played()
    load_obj.insert_all_tracks()
    load_obj.insert_top_artists()
    load_obj.insert_top_tracks()


if __name__ == "__main__":
    get_spotify()
    get_alltracks()
    edit_columns()
    load_data()
