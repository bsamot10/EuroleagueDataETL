### Description

A simple Python program that fetches _json_ files from 7 possible Euroleague APIs.
To my knowledge in the above APIs no data exist before the 2007-2008 season.

### Example data
URLs for each available API from the 1st game (_gamecode=1_) of the 2022-2023 Euroleague season (_seasoncode=E2022_):

* API: Header           ---  URL: https://live.euroleague.net/api/Header?gamecode=1&seasoncode=E2022
* API: Points           ---  URL: https://live.euroleague.net/api/Points?gamecode=1&seasoncode=E2022
* API: BoxScore         ---  URL: https://live.euroleague.net/api/BoxScore?gamecode=1&seasoncode=E2022
* API: Evolution        ---  URL: https://live.euroleague.net/api/Evolution?gamecode=1&seasoncode=E2022
* API: PlaybyPlay       ---  URL: https://live.euroleague.net/api/PlaybyPlay?gamecode=1&seasoncode=E2022
* API: Comparison       ---  URL: https://live.euroleague.net/api/Comparison?gamecode=1&seasoncode=E2022
* API: ShootingGraphic  ---  URL: https://live.euroleague.net/api/ShootingGraphic?gamecode=1&seasoncode=E2022

### Command line arguments
* --season_code, -sc
* --euroleague_api, -api
* --failed_extractions_limit, -fel
* --game_code_start, -gcs

More details may be found in the _utils.py_ file (class _ConfigParser_, instance function _set_arguments_).
  
### Results

The results are saved in the EuroleagueProject directory under a folder with name _euroleague_data_ and they are classified per season.

For each season's directory there exist two sub-directories: one for successful requests (dir name: _success_) and one for failed requests (dir name: _failure_).

All the successfully loaded _json_ files from the Euroleague APIs are placed in the _success_ sub-directories.

All the failed URLs are included in text files inside the _failure_ sub-directories.

### Meta data and logs

For each season's directory a meta data _json_ file is created that holds some useful information of the process.

A simple logfile that captures all the _print_ results is also available in the _simple_logs_ folder of the current working directory.

### Program interruption

If the program is interrupted there is an option for continuing from the point of interruption.
Information for that is available in the _game_code_counter_ parameter of the meta_data _json_ file. 

The process should then be re-started manually from the season of interruption (_season_code_ argument should be updated accordingly), by adding the appropriate value in the command line argument of _game_code_start_ (should be equal to _game_code_counter_).

### Example 1 (default execution)

Fetching all the available _json_ files from the APIs of the latest Euroleague season: 

_python main.py_

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueProject/blob/main/docs/images/euroleague_data_loader_example_1.png)

### Example 2

Fetching all the available _json_ files from the _Header_, _PlaybyPlay_ and _Boxscore_ APIs of the Euroleague seasons _2007-08_ until _2022-23_: 

_python main.py -api PlaybyPlay Boxscore -sc 2007-2022_

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueProject/blob/main/docs/images/euroleague_data_loader_example_2.png)
