from utils import SimpleLogger, get_connection, table_to_csv
from datetime import datetime
import os
import sys

def main():

    # create a simple logfile
    if not os.path.exists("simple_logs"):
        os.makedirs("simple_logs")
    datetime_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    sys.stdout = SimpleLogger(open(f"simple_logs/logfile_{datetime_now}", "w"), sys.stdout)
    
    # create a connection to the database    
    connection = get_connection()

    # create a folder to save the csv data
    if not os.path.exists("../euroleague_csv_data"):
        os.makedirs("../euroleague_csv_data")

    # get the table names and the columns to order by
    tables_order_by_cols = [("header", "game_id"), 
                            ("box_score", None),
                            ("players", "player"),
                            ("teams", "season_code"),
                            ("points", "game_point_id"), 
                            ("play_by_play", "game_play_id"), 
                            ("comparison", "game_id")]

    # try to get csv data from every table
    for table, order_by_col in tables_order_by_cols:
        table_to_csv(table, order_by_col, connection) 

if __name__ == '__main__':

    main()
