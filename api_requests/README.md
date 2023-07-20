### Description

A Python tool that fetches _json_ files from the responses of the API with endpoint https://live.euroleague.net/api/, 
and URI parameters _Header_, _Boxscore_, _ShootingGraphic_, _Points_, _Comparison_ and _PlaybyPlay_.

To my knowledge in the API no data exists before the 2007-2008 season.

### Example URLs
URLs for each available URI parameter from the 1st game (_gamecode=1_) of the 2022-2023 Euroleague season (_seasoncode=E2022_):

* URI: Header           ---  URL: https://live.euroleague.net/api/Header?gamecode=1&seasoncode=E2022
* URI: Points           ---  URL: https://live.euroleague.net/api/Points?gamecode=1&seasoncode=E2022
* URI: BoxScore         ---  URL: https://live.euroleague.net/api/BoxScore?gamecode=1&seasoncode=E2022
* URI: Evolution        ---  URL: https://live.euroleague.net/api/Evolution?gamecode=1&seasoncode=E2022
* URI: PlaybyPlay       ---  URL: https://live.euroleague.net/api/PlaybyPlay?gamecode=1&seasoncode=E2022
* URI: Comparison       ---  URL: https://live.euroleague.net/api/Comparison?gamecode=1&seasoncode=E2022
* URI: ShootingGraphic  ---  URL: https://live.euroleague.net/api/ShootingGraphic?gamecode=1&seasoncode=E2022

The format of the URLs is the same in the Eurocup competition. The only thing that change is the starting 'E' in the season code, which is replaced by 'U', as seen in the example below:

* URI: Header           ---  URL: https://live.euroleague.net/api/Header?gamecode=1&seasoncode=U2022

### Command line arguments
* --competition, -cp
* --season_code, -sc
* --euroleague_api, -api
* --failed_extractions_limit, -fel
* --game_code_start, -gcs

More details may be found in the _utils.py_ file (class _ConfigParser_, instance method _set_arguments_).
  
### Results

The results are saved in the EuroleagueDataETL directory under a folder with name _data_ 
and they are classified in sub-directories per competition and per season.

For each season's directory there exist two sub-directories: one for successful requests (dir name: _success_) and one for failed requests (dir name: _failure_).

All the successfully loaded _json_ files from the API responses are placed in the _success_ sub-directories.

All the failed URLs are included in text files inside the _failure_ sub-directories.

### Meta-data and logs

For each season's directory a meta-data _json_ file is created that holds some useful information of the process.

A simple logfile that captures all the _print_ results is also available in the _simple_logs_ folder that is created in the current working directory.

### Program interruption

If the program is interrupted there is an option for continuing from the point of interruption.
Information for that is available in the _game_code_counter_ parameter of the meta-data _json_ file. 

The process should then be re-started manually from the season of interruption (_season_code_ argument should be updated accordingly), by adding the appropriate value in the command line argument of _game_code_start_ (should be equal to _game_code_counter_).

### Example 1 (default execution)

Fetching all the available _json_ files from the API responses of the latest Euroleague season: 

```python main.py```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/api_requests_example_1.png)

### Example 2

Fetching all the available _json_ files from the API responses of the _Header_, _PlaybyPlay_ and _BoxScore_ URIs of the Eurocup seasons _2007-08_ until _2022-23_: 

```python main.py -cp eurocup -api PlaybyPlay Boxscore -sc 2007-2022```

![default_execution_screenshot](https://github.com/bsamot10/EuroleagueDataETL/blob/main/docs/images/api_requests_example_2.png)
