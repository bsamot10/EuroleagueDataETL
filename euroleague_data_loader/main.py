from utils import get_datetime_info, SimpleLogger, ConfigParser
from scrapper import EuroScrapas
import sys 
import os

def main():
    
    # get datetime info
    datetime_now, year_today = get_datetime_info()     
    
    # create a simple logfile
    if not os.path.exists("simple_logs"):
        os.makedirs("simple_logs")
    sys.stdout = SimpleLogger(open(f"simple_logs/logfile_{datetime_now}", "w"), sys.stdout)
 
    # parse command line arguments
    config_parser = ConfigParser(year_today)
    args = config_parser.set_arguments()
    print("INPUT ARGUMENTS")
    print(args)
    print("\nPARSED ARGUMENTS")
    config_parser.parse_arguments(args)
    
    # get info from parsed arguments
    season_codes = config_parser.season_codes
    euroleague_apis = config_parser.euroleague_apis
    failed_extractions_limit = config_parser.failed_extractions_limit
    game_code_start = config_parser.game_code_start
    
    # initialize EuroScrapas and implement scrapping process
    euro_scrapas = EuroScrapas(season_codes, euroleague_apis, failed_extractions_limit, game_code_start, datetime_now)
    euro_scrapas.implement_scrapping_process()
      
if __name__ == '__main__':
    
    main()
