### Description

A Python tool that loads the fetched _json_ files of the API responses to a PostgreSQL database.

It is compatible with the paths and filenames that the _api_requests_ creates.

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

* --competition, -cp
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

All tables are dependent from the _json_ files of the _Header_ URI. 
The full dependencies of each table and the name of the columns that uniquely identify a record (_primary key_), are presented below:

* **header**
  * URI dependencies: _Header_, _Boxscore_
  * Primary key: _game_id_
* **comparison**
  * URI dependencies: _Header_, _Comparison_, _ShootingGraphic_
  * Primary key: _game_id_
* **box_score**
  * URI dependencies: _Header_, _Boxscore_
  * Primary key: _game_player_id_
* **players**
  * URI dependencies: _Header_, _Boxscore_
  * Primary key: _season_player_id_
* **teams**
  * URI dependencies: _Header_, _Boxscore_
  * Primary key: _season_team_id_
* **points**
  * URI dependencies: _Header, Points_
  * Primary key: _game_point_id_
* **play_by_play**
  * URI dependencies: _Header_, _PlaybyPlay_
  * Primary key: _game_play_id_

The _players_ and _teams_ tables are created from the _box_score_ table and they are not directly dependent from the _Header_ and _Boxscore_ URIs. They are small tables containing the aggregated season stats of each player and team, and they are created once-off for all seasons every time the _box_score_ table is requested.


### Example 1 (default execution)

Loading all tables by extracting data from the available _json_ files of the latest Euroleague season: 

```python main.py```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/postgres_etl_example_1.png)

### Example 2

Loading _header_ and _comparison_ tables from the Eurocup seasons _2020-21_ until _2022-23_: 

```python main.py -cp eurocup -tb header comparison -sc 2020-2022```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/postgres_etl_example_2.png)
