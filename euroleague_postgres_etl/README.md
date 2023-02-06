### Description

A Python tool that loads the fetched _json_ files of the Euroleague APIs to a PostgreSQL database.
It is compatible with the paths and filenames that the _euroleague_api_etl_ creates.

### Requirements

The present program has been implemented with the following basic dependencies:

* postgreSQL 15
* Python 3.8.10
* posycopg2 2.9.5
* pandas 1.5.1
* numpy 1.23.4

### Connection to PostgreSQL, creation of database and table loading

After installing PostgreSQL, a connection to the PostgreSQL server is necessary, and a PostgreSQL database should be created manually.

The tables are created and loaded with the use of the present program. In that case, the connection to the database is handled by the _psycopg2_ python module.

The relevant configuration file is _config_connection.json_ and it has the following arguments:

* database
* user
* password
* host
* port

### Command line arguments

* --season_code, -sc
* --table, -tb

More details may be found in the _utils.py_ file (class _ConfigParser_, instance method _set_arguments_).
  
### Logs

A simple logfile that captures all the _print_ results is available in the _simple_logs_ folder that is created in the current working directory.

### Program interruption and update

If the program is interrupted it can be re-started from the season of interruption. 
In such case, the _season_code_ argument should start from the season of interruption.
The tables can be also upated similarly.

It should be noted, that the rows which are already loaded in the interrupted season, will not be re-loaded. 
In fact, every table has a unique identifier, which is useful for the avoidance of duplicate inputs.

### Brief description of the PostgreSQL tables

The column names and data types of each table can be found in the _table_schema_ module, and more specifically,
in the _table_column_names_ instance variable of the class _SchemaLoader_. 
The instance methods _set__{table}__columns_ of the same class, determine the aforementioned variable.

All tables are dependent from the _json_ files of the _Header_ API. 
The full dependencies of each table and the name of the columns that uniquely identify a record (_index_), are presented below:

* **header**
  * API dependencies: _Header_, _Boxscore_
  * Index column: _game_id_
* **comparison**
  * API dependencies: _Header_, _Comparison_, _ShootingGraphic_
  * Index column: _game_id_
* **box_score**
  * API dependencies: _Header_, _Boxscore_
  * Index column: _game_player_id_
* **players**
  * API dependencies: _Header_, _Boxscore_
  * Index column: _season_player_id_
* **teams**
  * API dependencies: _Header_, _Boxscore_
  * Index column: _season_team_id_
* **points**
  * API dependencies: _Header, Points_
  * Index column: _game_point_id_
* **play_by_play**
  * API dependencies: _Header_, _PlaybyPlay_
  * Index column: _game_play_id_

The _players_ and _teams_ tables are created from the _box_score_ table and they are not directly dependent from the _Header_ and _Boxscore_ APIs. They are small tables containing the aggregated season stats of each player and team, and they are created once-off for all seasons every time the _box_score_ table is requested.


### Example 1 (default execution)

Loaing all tables by extracting data from the available _json_ files of the latest Euroleague season: 

```python main.py```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/euroleague_postgres_etl_example_1.png)

### Example 2

Loading _header_, _play_by_lay_ and _box_score_ (and consequently _players_ and _teams_) tables from the Euroleague seasons _2007-08_ until _2022-23_: 

```python main.py -tb header play_by_play box_score -sc 2007-2022```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/euroleague_postgres_etl_example_2.png)
