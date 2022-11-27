from argparse import ArgumentParser
from datetime import datetime, date
from itertools import chain

def get_datetime_info():

    datetime_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    month_today = date.today().month
    year_today = date.today().year
    if month_today not in (9, 10, 11, 12):
        year_today -= 1
        
    return datetime_now, year_today

class SimpleLogger:
    
    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2        
    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)        
    def flush(self):
        pass

class ConfigParser(ArgumentParser):
        
    def __init__(self, year_today):
        
        # initialze the parent class (ArgumentParser)
        super().__init__(prog='euroleague_scrapper', description='extract and load euroleague data (format of data: json files)')
        
        # set instance variables
        self.valid_euroleague_apis = ["Boxscore", "ShootingGraphic", "Points", "Comparison", "Evolution", "PlaybyPlay"]
        self.year_today = year_today
        self.season_codes = None
        self.euroleague_apis = None
        self.failed_extractions_limit = None
        self.game_code_start = None
        
    def set_arguments(self):
        
        # set the command line arguments
        self.add_argument('-sc', '--season_code', type=str, nargs='+', default=[str(self.year_today)],
                          help="A list with the season codes without the starting 'E'. "
                               "If the current month is not Sep, Oct, Nov, Dec, then today's year is decreased by 1. "
                               "If '-' separates two codes, then the complete range (inclusively) between the two seasons will be requested. "
                               "The possible seasons to request are restricted to be from the 2000-2001 Euroleague season until the current season."
                          )
        self.add_argument('-api', '--euroleague_api', type=str, nargs='+', default=self.valid_euroleague_apis,
                          help="A list with the Euroleague APIs. 'Header' API is always added."
                               "The remaining APIs are included by default, but they can also be selected manually. "
                               "Input arguments that are not valid, will result in the request of the 'Header' API only."
                          )
        self.add_argument('-fel', '--failed_extractions_limit', type=int, nargs='?', default=500,
                          help="Requesting from an API might fail. "
                               "The present argument limits the number of failed requests per season."
                               "If the limit in a season is exceeded, then the program moves onwards to the next season."
                          )
        self.add_argument('-gcs', '--game_code_start', type=int, nargs='?', default=1,
                          help="It indicates from which game code of the 1st input season the program will start. "
                               "The present argument is useful when the process is interrupted (intentionally or not)."
                          )
        args = self.parse_args()
        
        return args
        
    def parse_arguments(self, args):
        
        # parse 'season_code' argument
        season_code_args_without_dash_to_int = [int(sc) for sc in args.season_code if "-" not in sc]
        season_code_args_with_dash = [sc for sc in args.season_code if "-" in sc]
        season_code_args_with_dash_splitted_to_int = [list(map(int, sc.split("-"))) for sc in season_code_args_with_dash]
        season_code_args_with_dash_splitted_to_int_ranged = [list(range(sc[0], sc[1] + 1, 1)) for sc in season_code_args_with_dash_splitted_to_int]
        season_codes_all = [season_code_args_without_dash_to_int] + season_code_args_with_dash_splitted_to_int_ranged
        season_codes_all_flattened_sorted = sorted(set(list(chain.from_iterable(season_codes_all))))
        self.season_codes = [f"E{str(sc)}" for sc in season_codes_all_flattened_sorted if sc in range(2000, self.year_today + 1)]
        print("season_codes:", self.season_codes)

        # parse 'euroleague_api' argument
        self.euroleague_apis = ["Header"] + [api for api in args.euroleague_api if api in self.valid_euroleague_apis]
        print("euroleague_apis:", self.euroleague_apis)

        # parse 'failed_extractions_limit' argument
        self.failed_extractions_limit = args.failed_extractions_limit
        print("failed_extractions_limit:", self.failed_extractions_limit)
        if self.failed_extractions_limit <= 0:
            print("\nWrong input: 'failed_extractions_limit' should be a positive integer\n")
            exit()

        # parse 'game_code_start' argument
        self.game_code_start = args.game_code_start
        print("game_code_start:", self.game_code_start)
        if self.game_code_start > 1:
            print("")
        elif self.game_code_start <= 0:
            print("\nWrong input: 'game_code_start' should be a positive integer\n")
            exit()      
            
