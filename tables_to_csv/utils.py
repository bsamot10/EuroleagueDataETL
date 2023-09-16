from argparse import ArgumentParser
import pandas.io.sql as io_sql
import psycopg2
import json
import time
import warnings
warnings.filterwarnings("ignore")


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
    def __init__(self):
        # initialze the parent class (ArgumentParser)
        super().__init__(prog='csv_data',
                         description='load the postgreSQL database tables to csv files')

        # set instance variables
        self.competition = None

    def set_arguments(self):
        # set the command line arguments
        self.add_argument('-cp', '--competition', type=str, nargs='?', default="euroleague",
                          choices=["euroleague", "eurocup"],
                          help="The European basketball competition: 'euroleague' or 'eurocup'"
                          )
        args = self.parse_args()

        return args

    def parse_arguments(self, args):

        # parse 'competition' argument
        self.competition = args.competition
        print("competition:", self.competition)


def get_connection(competition):

    config_conn_file = open(f"../postgres_etl/config_connection.json")
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


def table_to_csv(competition, table, order_by_col, connection):

    print(f"\n{table}_to_csv")
    try:
        start = time.time()
        if table == "box_score":
            query = f"select * from {competition}_{table}"
        elif table == "players":
            query = f"select * from {competition}_{table} order by {order_by_col}, season_code"
        else:
            query = f"select * from {competition}_{table} order by {order_by_col}"
        df = io_sql.read_sql_query(query, connection)
        if table == "play_by_play":
            df["points_a"] = df["points_a"].astype(str)
            df["points_b"] = df["points_b"].astype(str)
            for i in range(len(df)):
                if (df["points_a"].iloc[i] == "0") & (df["points_b"].iloc[i] == "0"):
                    df["points_a"].values[i] = ""
                    df["points_b"].values[i] = ""
        df.to_csv(f"../data/{competition}_csv/{competition}_{table}.csv", index=False)
        print("time_counter:", round(time.time() - start, 1), "sec")
        
    except Exception as e:
        print(e)
