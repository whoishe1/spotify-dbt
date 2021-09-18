import boto3
import os
import datetime
import pathlib
from dotenv import dotenv_values

config = dotenv_values(".env")


def to_s3():

    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config["AWS_ID"],
        aws_secret_access_key=config["AWS_SECRET"],
        region_name=config["AWS_LOCATION"],
    )

    date = (datetime.datetime.now()).strftime("%Y-%m-%d")

    try:
        at = str(max(pathlib.Path("./extract/alltracks").glob("*"), key=os.path.getmtime))
        rp = "./extract/recentlyplayed.csv"
        ta = "./extract/top_artists.csv"
        tt = "./extract/top_tracks.csv"

        s3.meta.client.upload_file(at, "dbtspotify", f"spotifyhistory/alltracks/{date}.csv")
        s3.meta.client.upload_file(rp, "dbtspotify", f"spotifyhistory/recentlyplayed/{date}.csv")
        s3.meta.client.upload_file(ta, "dbtspotify", f"spotifyhistory/topartists/{date}.csv")
        s3.meta.client.upload_file(tt, "dbtspotify", f"spotifyhistory/toptracks/{date}.csv")

        print(
            "Uploaded alltracks, recentlyplayed, topartists and toptracks to spotifyhistory s3 bucket"
        )

    except Exception as e:
        print(e)
