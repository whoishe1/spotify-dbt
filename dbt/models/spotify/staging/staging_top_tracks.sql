select * from {{ source('spotify','top_tracks') }}
