# EuroleagueDataETL
Python tools for Extraction, Transformation and Loading of Euroleague data. 

### Brief description of existing folders

##### euroleague_api_etl
A Python tool that fetches _json_ files from up to 7 Euroleague APIs.

##### euroleague_json_data
A sample of the _json_ files that are fecthed by the _euroleague_api_etl_ tool.

##### euroleague_postgres_etl
A Python tool that loads the fetched _json_ files of the Euroleague APIs to a PostgreSQL database.

##### euroleague_tables_to_csv
A Python tool that creates one csv file for each available table of the PostgreSQL database.

##### euroleague_csv_data
A sample for each _csv_ file that is created by the _euroleague_tables_to_csv_ tool.
The complete _csv_ files exist in Kaggle and are regularly updated https://www.kaggle.com/datasets/babissamothrakis/euroleague-datasets
