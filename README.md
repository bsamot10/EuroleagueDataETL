# EuroleagueDataETL
Python tools for Extraction, Transformation and Loading of Euroleague and Eurocup data. 

## Description of existing folders

### api_requests
A Python tool that fetches _json_ files from the responses of the API with endpoint https://live.euroleague.net/api/, 
and URI parameters _Header_, _Boxscore_, _ShootingGraphic_, _Points_, _Comparison_ and _PlaybyPlay_.

### data
Folders that include samples of the _json_ files that are fetched by the _api_requests_ tool.

Folders that include samples of the _csv_ files that are created by the _tables_to_csv_ tool.
The complete _csv_ files exist in Kaggle and are regularly updated https://www.kaggle.com/datasets/babissamothrakis/euroleague-datasets

### docs
A few images for the documentation of the project.

### postgres_etl
A Python tool that loads the fetched _json_ files of the API responses to a PostgreSQL database.

### tables_to_csv
A Python tool that creates one csv file for each available table of the PostgreSQL database.

