### Description

A Python program that loads the fetched _json_ files of the Euroleague APIs to a postgreSQL database.
It is compatible with the paths and filenames that the _euroleague_data_loader_ creates.

### Requirements
The present program has been implemented with the following basic dependencies:

* postgreSQL 15
* Python 3.8.10
* posycopg2 2.9.5
* pandas 1.5.1
* numpy 1.23.4

### Command line arguments
* --season_code, -sc
* --table, -tb

More details may be found in the _utils.py_ file (class _ConfigParser_, instance function _set_arguments_).
  
### Results

### Logs

A simple logfile that captures all the _print_ results is available in the _simple_logs_ folder of the current working directory.

### Program interruption



### Example 1 (default execution)

Loaing all tables by extracting data from the available _json_ files of the latest Euroleague season: _python main.py_

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueProject/blob/main/docs/images/euroleague_postgres_etl_example_1.png)

### Example 2

Loading _header_, _play_by_lay_ and _box_score_ tables from the Euroleague seasons _2007-08_ until _2022-23_: _python main.py -tb header play_by_play box_score -sc 2007-2022_

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueProject/blob/main/docs/images/euroleague_postgres_etl_example_2.png)
