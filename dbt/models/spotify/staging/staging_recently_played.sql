select * from {{ source('spotify','recently_played') }}
