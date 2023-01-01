from argparse import ArgumentParser
from datetime import datetime, date
from itertools import chain
import psycopg2
import json

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

def get_connection():

    config_conn_file = open("config_connection.json")
    config_conn_data = json.load(config_conn_file)
    config_conn_file.close()
    try:
        connection = psycopg2.connect(database=config_conn_data["database"], 
                                      user=config_conn_data["user"], 
                                      password=config_conn_data["password"], 
                                      host=config_conn_data["host"], 
                                      port=config_conn_data["port"])
        return connection

    except Exception as e:
        print(e)
        exit()
        
class ConfigParser(ArgumentParser):
        
    def __init__(self, year_today, available_season_codes):
        
        # initialze the parent class (ArgumentParser)
        super().__init__(prog='euroleague_postgreSQL', description='load the extracted euroleague data (format of data: json files) to a postgreSQL database')

        # set instance variables
        self.year_today = year_today
        self.available_season_codes = available_season_codes
        self.valid_postgres_tables = ["header", "box_score", "points", "play_by_play", "comparison"]
        self.season_codes = None
        self.postgres_tables = None

    def set_arguments(self):

        # set the command line arguments
        self.add_argument('-sc', '--season_code', type=str, nargs='+', default=[str(self.year_today)],
                          help="A list with the season codes without the starting 'E'. "
                               "If '-' separates two codes, then the complete range (inclusively) between the two seasons will be laoded. "
                               "The possible seasons to load are restricted to be from the 2000-2001 Euroleague season until the current season."
                          )
        self.add_argument('-tb', '--postgres_table', type=str, nargs='+', default=self.valid_postgres_tables,
                          help="A list with the postgreSQL tables."
                               "Available tables are 'header', 'box_score', 'points', 'play_by_play', 'comparison'."
                          )
        args = self.parse_args()

        return args

    def parse_arguments(self, args):

        # parse 'season_code' argument
        try:
            season_code_args_without_dash_to_int = [int(sc) for sc in args.season_code if "-" not in sc]
            season_code_args_with_dash = [sc for sc in args.season_code if "-" in sc]
            season_code_args_with_dash_splitted_to_int = [list(map(int, sc.split("-"))) for sc in season_code_args_with_dash]
            season_code_args_with_dash_splitted_to_int_ranged = [list(range(sc[0], sc[1] + 1, 1)) for sc in season_code_args_with_dash_splitted_to_int]
            season_codes_all = [season_code_args_without_dash_to_int] + season_code_args_with_dash_splitted_to_int_ranged
            season_codes_all_flattened_sorted = sorted(set(list(chain.from_iterable(season_codes_all))))
            self.season_codes = [f"E{str(sc)}" for sc in season_codes_all_flattened_sorted if sc in range(2000, self.year_today + 1)]
            if not self.season_codes:
                print("\nWrong input: 'season_code' is restricted to exist in the range 2000 -", self.year_today, "\n")
                exit()
            elif not set(self.season_codes).intersection(self.available_season_codes):
                print("\nRequested data does not exist: the available season codes are", sorted(self.available_season_codes))
                exit()
            else:
                self.season_codes = sorted(list(set(self.season_codes).intersection(self.available_season_codes)))
                print("season_codes:", self.season_codes)
        except Exception as e:
            print(e)
            exit()

        # parse 'postgres_table' argument
        self.postgres_tables = [table for table in args.postgres_table if table in self.valid_postgres_tables]
        if not self.postgres_tables:
            print("\nWrong input: 'postgres_table' is restricted to exist in", self.valid_postgres_tables,"\n")
        else:
            print("postgres_tables:", self.postgres_tables)



