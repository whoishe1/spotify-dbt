select * from {{ source('spotify','all_tracks') }}
