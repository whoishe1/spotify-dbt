select id,artists,trackname,album from {{ref('staging_all_tracks')}} where artists in ('Atu','Dpat','Sonder','Brent Faiyaz','Amber Olivier') order by artists
