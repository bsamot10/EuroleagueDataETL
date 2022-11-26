### Description
A simple Python program that fetches _json_ files from the available Euroleague APIs.
To my knowledge there exist data from the 2007-2008 Euroleague season and onwards.

### Example data: URLs for each available API from the 1st game of the 1st Round of the 2022-2023 Euroleague season
* API: Header           ---  URL: https://live.euroleague.net/api/Header?gamecode=1&seasoncode=E2022
* API: Points           ---  URL: https://live.euroleague.net/api/Points?gamecode=1&seasoncode=E2022
* API: BoxScore         ---  URL: https://live.euroleague.net/api/BoxScore?gamecode=1&seasoncode=E2022
* API: Evolution        ---  URL: https://live.euroleague.net/api/Evolution?gamecode=1&seasoncode=E2022
* API: PlaybyPlay       ---  URL: https://live.euroleague.net/api/PlaybyPlay?gamecode=1&seasoncode=E2022
* API: Comparison       ---  URL: https://live.euroleague.net/api/Comparison?gamecode=1&seasoncode=E2022
* API: ShootingGraphic  ---  URL: https://live.euroleague.net/api/ShootingGraphic?gamecode=1&seasoncode=E2022

### Example python run: extract and load _json_ files for all available APIs from the Euroleague seasons of 2021-2022 and 2022-2023
_python main.py -sc 2021-2022_