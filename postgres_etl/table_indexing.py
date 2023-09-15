import pandas.io.sql as io_sql
from time import time
import warnings
warnings.filterwarnings("ignore")


class Indexer:
    '''
    The present class supports the indexing of each table.
    It enables the creation of a primary key, as well as, the indices of each table.
    It is imported and implemented in the 'table_loading' module.
    '''

    def __init__(self, table):

        # instance variable with the name of the table
        self.table = table

        # instance variable that maps each table to the primary key
        self.map_table_to_primary_key = {"header": "game_id",
                                         "box_score": "game_player_id",
                                         "players": "season_player_id",
                                         "teams": "season_team_id",
                                         "play_by_play": "game_play_id",
                                         "points": "game_point_id",
                                         "comparison": "game_id"}

        # instance variable that maps each table to the indices
        self.map_table_to_indices = {"header": ["game", "team_id_a", "team_id_b"],
                                     "box_score": ["game", "team_id", "player_id", "player"],
                                     "players": ["team_id", "player_id", "player"],
                                     "teams": ["team_id"],
                                     "play_by_play": ["game", "team_id", "player_id", "player"],
                                     "points": ["game", "team_id", "player_id", "player"],
                                     "comparison": ["game", "team_id_a", "team_id_b"]}

        # instance variable that facilitates the check of existing primary key and indices
        self.query = f"select count(*) from pg_indexes where schemaname = 'public' and tablename = '{self.table}'"

    def set_primary_key(self, connection, cursor, truncated_table_name):
        '''
        The present method enables the creation of a primary key for the table.
        '''
        if io_sql.read_sql_query(self.query, connection)["count"][0] == 0:
            table_primary_key = self.map_table_to_primary_key[truncated_table_name]
            cursor.execute(f"ALTER TABLE {self.table} ADD PRIMARY KEY ({table_primary_key})")
            connection.commit()

    def set_indices(self, connection, cursor, truncated_table_name, start_table):
        '''
        The present method enables the creation of indices for the table.
        '''
        if io_sql.read_sql_query(self.query, connection)["count"][0] == 1:
            start_indexing = time()
            table_indices = self.map_table_to_indices[truncated_table_name]
            print(f"\nIndexing {truncated_table_name.upper()}: {table_indices}")
            for index in table_indices:
                cursor.execute(f"CREATE INDEX ON {self.table} ({index})")
                connection.commit()
            print("TimeCounterIndexing", round(time() - start_indexing, 1), "sec  --- ",
                  "TimeCounterTable:", round(time() - start_table, 1), "sec")
