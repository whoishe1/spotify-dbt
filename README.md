# Spotify User playlists / analytics

- Expose your sacred music playlists to the public.
- Provides entire selected playlists, top played artists, top played songs and recent tracks listened.
- Uses postgresql, dbt and powerbi

## Instructions

- Setup up a virtual environment in your root directory
  - `virtualenv ./venv`
  - `./venv/scripts/activate`

- Install requirements
  - `pip install -r requirements.txt`

- Create an application on https://developer.spotify.com/
- Get your Client ID, Client Secret and make sure to setup your Redirect URI

- Make sure to also install postgres
  - https://www.enterprisedb.com/downloads/postgres-postgresql-downloads

- Run main script to fetch the data from spotify and insert data into local database
  - `python main.py`

- Running dbt
  - `dbt init dbt`: Creates the project folder
  - `dbt debug`: Checks the connection with the Postgres database
  - `dbt deps`: Installs the test dependencies
  - `dbt seed`: Loads the CSV files into staging tables in the database in postgres
  - `dbt run`: Runs the transformations and loads the data into the database
  - `dbt docs generate`: Generates the documentation of the dbt project
  - `dbt docs serve`: Serves the documentation on a webserver

- Once data is ready, use PowerBI to connect to local database and procede to create visualizations
