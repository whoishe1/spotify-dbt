select track_id, track_name, primary_artist_id, primary_artist, album_name, track_popularity, popularity from {{ ref('staging_top_tracks') }}
where track_popularity > 50
