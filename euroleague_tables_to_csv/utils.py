import pandas.io.sql as io_sql
import psycopg2
import json
import time
import warnings
warnings.filterwarnings('ignore')

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

    config_conn_file = open("../euroleague_postgres_etl/config_connection.json")
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
        
def table_to_csv(table, order_by_col, connection):

    print(f"\n{table}_to_csv")
    try:
        start = time.time()
        if table == "box_score":
            query = f"select * from {table}"
        elif table == "players":
            query = f"select * from {table} order by {order_by_col}, season_code"
        else:
            query = f"select * from {table} order by {order_by_col}"
        df = io_sql.read_sql_query(query, connection)
        if table == "play_by_play":
            df["points_a"] = df["points_a"].astype(str)
            df["points_b"] = df["points_b"].astype(str)
            for i in range(len(df)):
                if (df["points_a"].iloc[i] == "0") & (df["points_b"].iloc[i] == "0"):
                    df["points_a"].values[i] = ""
                    df["points_b"].values[i] = ""
        df.to_csv(f"../euroleague_csv_data/{table}.csv", index=False)
        print("time_counter:", round(time.time() - start, 1), "sec")
    except Exception as e:
        print(e)
