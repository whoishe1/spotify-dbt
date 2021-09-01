from spotify import SpotifyTracks
import os
from dotenv import dotenv_values


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

    top_artists.to_csv(
        os.path.join("./data", "top_artists.csv"), index=False, encoding="utf-8"
    )
    recently_played.to_csv(
        os.path.join("./data", "recentlyplayed.csv"), index=False, encoding="utf-8"
    )
    top_tracks.to_csv(
        os.path.join("./data", "top_tracks.csv"), index=False, encoding="utf-8"
    )


# if __name__ == '__main__':
#     main()
