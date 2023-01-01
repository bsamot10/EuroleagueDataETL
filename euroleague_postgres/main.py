from utils import get_datetime_info, SimpleLogger, get_connection, ConfigParser
from table_loading import EuroDatabaseLoader
import sys 
import os

def main():
    
    # get datetime info and availablity of data
    datetime_now, year_today = get_datetime_info()
    available_season_codes = os.listdir("../euroleague_data")

    # create a simple logfile
    if not os.path.exists("simple_logs"):
        os.makedirs("simple_logs")
    sys.stdout = SimpleLogger(open(f"simple_logs/logfile_{datetime_now}", "w"), sys.stdout)
 
    # parse command line arguments
    config_parser = ConfigParser(year_today, available_season_codes)
    args = config_parser.set_arguments()
    print("INPUT ARGUMENTS")
    print(args)
    print("\nPARSED ARGUMENTS")
    config_parser.parse_arguments(args)
    
    # get info from parsed arguments
    season_codes = config_parser.season_codes
    postgres_tables = config_parser.postgres_tables
    
    # create a connection to the database
    connection = get_connection()
    
    # initialize EuroDatabaseLoader and implement database loading process
    euro_database_loader = EuroDatabaseLoader(connection, postgres_tables, season_codes)
    euro_database_loader.implement_table_loading()
      
if __name__ == '__main__':
    
    main()

