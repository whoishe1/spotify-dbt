import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple
import os

# use the following scopes to access data in the categories ->
# user-read-recently-played, user-top-read
# Methods used in spotify, current_user_recently_played, audio_features, artists

# when you use the scopes in the spotify, it will filter what to look at (played recently, users top artists, users top tracks).  After that you use the spotipy methods to obtain that data in that category.
class SpotifyTracks:
    """
    Using Spotipy to access Spotify API

    Attribute:
        username: spotify username
        client_id: spotify client id for app
        client_secret: spotify clientsecret for app
        # redirect_uri: spotify client redirect uri for app
    """

    def __init__(self, username: str, client_id: str, client_secret: str):
        self.username = username
        self.client_id = client_id
        self.client_secret = client_secret
        # self.redirect_uri = redirect_uri

    def connection(self, scope: str) -> spotipy.client.Spotify:
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
        return sp

    def parse_artist(self, artists: List[Dict]) -> Tuple[str, str, str, str]:
        """
        parse primary and other artists
        """
        primary_artist, secondary_artists = self.parse_primary_other(
            [artist["name"] for artist in artists]
        )
        primary_artist_id, secondary_artists_id = self.parse_primary_other(
            [artist["id"] for artist in artists]
        )
        return (
            primary_artist,
            secondary_artists,
            primary_artist_id,
            secondary_artists_id,
        )

    def parse_primary_other(self, parse_list: List[str]) -> Tuple[str, str]:
        """
        Parses primary and other values for lists
        """
        parse_list = parse_list.copy()
        try:
            primary = parse_list.pop(0)
        except IndexError:
            primary = None

        others = ", ".join(parse_list)

        return primary, others

    def spread_columns(
        self, df: pd.core.frame.DataFrame, col: str
    ) -> pd.core.frame.DataFrame:
        """
        spread columns for atomicity
        """

        cols = df[col].str.split(",", expand=True)
        num_of_cols = len(cols.columns)
        output = [str(f"{col}_{i}") for i in range(num_of_cols)]

        df[output] = df[col].str.split(",", expand=True)

        return df

    def rec_played_or_top_tracks(self, scope: str) -> pd.core.frame.DataFrame:
        sp = self.connection(scope)

        if scope == "user-read-recently-played":
            results = sp.current_user_recently_played(limit=50)
            trackfeatures = {
                "played_at": "track_played_at",
                "track.id": "track_id",
                "track.name": "track_name",
                "track.artists": "artists",
                "track.duration_ms": "track_duration_min",
                "track.popularity": "track_popularity",
                "track.album.id": "album_id",
                "track.album.name": "album_name",
                "track.album.release_date": "album_release_year",
            }
        elif scope == "user-top-read":
            results = sp.current_user_top_tracks(limit=50)
            trackfeatures = {
                "id": "track_id",
                "name": "track_name",
                "artists": "artists",
                "duration_ms": "track_duration_min",
                "popularity": "track_popularity",
                "album.id": "album_id",
                "album.name": "album_name",
                "album.release_date": "album_release_year",
            }
        else:
            raise ("Wrong spotify scope")

        # recently played/user top tracks

        results_method = results.get("items")
        results_method = pd.json_normalize(results_method).reset_index()
        results_method = results_method[trackfeatures.keys()].rename(
            columns=trackfeatures
        )

        results_method["track_duration_min"] = round(
            (results_method["track_duration_min"] / 60000), 2
        )

        if scope == "user-read-recently-played":
            results_method["track_played_at"] = (
                pd.to_datetime(results_method["track_played_at"])
                .dt.tz_convert("America/Los_Angeles")
                .dt.tz_localize(None)
            )

        (
            results_method["primary_artist"],
            results_method["secondary_artists"],
            results_method["primary_artist_id"],
            results_method["secondary_artists_id"],
        ) = zip(*results_method["artists"].apply(self.parse_artist))

        # audio features
        results_audio_features = sp.audio_features(
            tracks=results_method["track_id"].tolist()
        )

        audiofeatures = {
            "danceability": "track_danceability",
            "energy": "track_energy",
            "key": "track_key",
            "loudness": "track_loudness",
            "mode": "track_mode",
            "speechiness": "track_speechiness",
            "acousticness": "track_acousticness",
            "instrumentalness": "track_instrumentalness",
            "liveness": "track_liveness",
            "valence": "track_valence",
            "tempo": "track_tempo",
            "id": "track_id",
        }

        audio = pd.json_normalize(results_audio_features)
        audio = audio[audiofeatures.keys()].rename(columns=audiofeatures)
        audio = audio.drop_duplicates(subset="track_id").reset_index(drop=True)

        # artist info
        results_artists = sp.artists(
            artists=results_method["primary_artist_id"].tolist()
        )
        artists = pd.json_normalize(results_artists["artists"])

        artists_info = {
            "genres": "genres",
            "id": "primary_artist_id",
            "popularity": "popularity",
            "followers.total": "followers",
        }
        artists = artists[artists_info.keys()].rename(columns=artists_info)

        artists["primary_genre"], artists["other_genre"] = zip(
            *artists["genres"].apply(self.parse_primary_other)
        )

        artists = artists.drop_duplicates(subset="primary_artist_id").reset_index(
            drop=True
        )

        df = pd.merge(results_method, audio, how="left", on="track_id").merge(
            artists, how="left", on="primary_artist_id"
        )

        df = self.spread_columns(df=df, col="secondary_artists")
        df = self.spread_columns(df=df, col="secondary_artists_id")
        df = self.spread_columns(df=df, col="other_genre")

        df = df.drop(columns=["artists", "genres"])

        return df

    def current_top_artists(self, scope: str) -> pd.core.frame.DataFrame:
        sp = self.connection(scope)

        topartists_col = {
            "id": "artist_id",
            "name": "artist_name",
            "genres": "artist_genres",
            "popularity": "artist_popularity",
            "followers.total": "artist_followers",
        }

        topartists = sp.current_user_top_artists(limit=50)
        topartists = topartists.get("items")
        topartists = pd.json_normalize(topartists)

        topartists = topartists[topartists_col.keys()].rename(columns=topartists_col)
        topartists["primary_genre"], topartists["other_genre"] = zip(
            *topartists["artist_genres"].apply(self.parse_primary_other)
        )

        topartists = self.spread_columns(df=topartists, col="other_genre")
        topartists = topartists.drop(columns=["artist_genres", "other_genre"])

        return topartists


# username = os.environ.get("SPOTIPY_USERNAME")
# client_id = os.environ.get("SPOTIPY_CLIENT_ID")
# client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")

# obj1 = SpotifyTracks(
#     username=username, client_id=client_id, client_secret=client_secret
# )

# # obj1.top_playlists("user-read-recently-played")
# aaa = obj1.current_top_artists("user-top-read")
# bbb = obj1.rec_played_or_top_tracks("user-read-recently-played")
# ccc = obj1.rec_played_or_top_tracks("user-top-read")
