import pandas as pd
from datetime import datetime


def edit_columns():
    date = datetime.now().strftime("%Y-%m-%d")

    rp = pd.read_csv("./extract/recentlyplayed.csv")
    ta = pd.read_csv("./extract/top_artists.csv")
    tt = pd.read_csv("./extract/top_tracks.csv")

    rp = rp[
        [
            "track_played_at",
            "track_id",
            "track_name",
            "track_duration_min",
            "track_popularity",
            "primary_artist",
            "primary_artist_id",
            "album_id",
            "album_name",
            "album_release_year",
            "primary_genre",
            "track_danceability",
            "track_energy",
            "track_key",
            "track_loudness",
            "track_mode",
            "track_speechiness",
            "track_acousticness",
            "track_instrumentalness",
            "track_liveness",
            "track_valence",
            "track_tempo",
            "popularity",
            "followers",
        ]
    ]

    ta = ta[
        [
            "artist_id",
            "artist_name",
            "artist_popularity",
            "artist_followers",
            "primary_genre",
        ]
    ]

    tt = tt[
        [
            "track_id",
            "track_name",
            "track_duration_min",
            "track_popularity",
            "primary_artist",
            "primary_artist_id",
            "album_id",
            "album_name",
            "album_release_year",
            "primary_genre",
            "track_danceability",
            "track_energy",
            "track_key",
            "track_loudness",
            "track_mode",
            "track_speechiness",
            "track_acousticness",
            "track_instrumentalness",
            "track_liveness",
            "track_valence",
            "track_tempo",
            "popularity",
            "followers",
        ]
    ]

    rp.to_csv(f"./extract/rp/recentlyplayed_{date}.csv", index=False)
    tt.to_csv(f"./extract/tt/top_tracks_{date}.csv", index=False)
    ta.to_csv(f"./extract/ta/top_artists_{date}.csv", index=False)
