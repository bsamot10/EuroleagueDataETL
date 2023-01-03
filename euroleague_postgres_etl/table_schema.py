class SchemaLoader:
    '''
    The present class is inherited by the 'EuroDatabaseLoader' class of 'table_loading' module.
    It introduces three types of methods.
    The 1st type ('set_{table_name}_columns') sets the column names of each table.
    The 2nd type ('add_columns') adds the columns that the 1st type has set. It also determines the data types of the columns.
    The 3rd type ('create_unique_index') sets a unique index for each table.
    '''
    def __init__(self, connection):
        
        # the next two instance variables are responsible for the interaction with the database
        self.connection = connection 
        self.cursor = connection.cursor()
        
        # the next three instance variables are updated by the 'set_{table_name}_columns' functions
        self.table_column_names = {}    
        self.map_table_columns_to_json_header = {}
        self.map_table_columns_to_json_box = {} 
        
    def set_header_columns(self):
        
        self.map_table_columns_to_json_header = {"stadium": "Stadium",
                                                 "team_a": "TeamA",
                                                 "team_b": "TeamB",
                                                 "code_team_a": "CodeTeamA",
                                                 "code_team_b": "CodeTeamB",
                                                 "coach_a": "CoachA",
                                                 "coach_b": "CoachB",
                                                 "game_time": "GameTime",
                                                 "remaining_partial_time": "RemainingPartialTime",
                                                 "phase": "Phase",
                                                 "season": "Competition",
                                                 "season_code": "CompetitionReducedName",
                                                 "referee_1": "Referee1",
                                                 "referee_2": "Referee2",
                                                 "referee_3": "Referee3",
                                                 "round": "Round",
                                                 "capacity": "Capacity",
                                                 "score_a": "ScoreA",
                                                 "score_b": "ScoreB",
                                                 "w_id": "wid",
                                                 "fouls_a": "FoultsA",
                                                 "fouls_b": "FoultsB",
                                                 "timeouts_a": "TimeoutsA",
                                                 "timeouts_b": "TimeoutsB",
                                                 "score_quarter_1_a": "ScoreQuarter1A",
                                                 "score_quarter_2_a": "ScoreQuarter2A",
                                                 "score_quarter_3_a": "ScoreQuarter3A",
                                                 "score_quarter_4_a": "ScoreQuarter4A",
                                                 "score_quarter_1_b": "ScoreQuarter1B",
                                                 "score_quarter_2_b": "ScoreQuarter2B",
                                                 "score_quarter_3_b": "ScoreQuarter3B",
                                                 "score_quarter_4_b": "ScoreQuarter4B"}

        self.map_table_columns_to_json_box = {"score_extra_time_1_a": "Extra1",
                                              "score_extra_time_2_a": "Extra2",
                                              "score_extra_time_3_a": "Extra3",
                                              "score_extra_time_1_b": "Extra1",
                                              "score_extra_time_2_b": "Extra2",
                                              "score_extra_time_3_b": "Extra3"}
        
        self.table_column_names["header"] = ["game_id", "game", "date", "time"] + \
                                              list(self.map_table_columns_to_json_header.keys()) + \
                                              list(self.map_table_columns_to_json_box.keys())
            
    def set_box_score_columns(self):
    
        self.table_column_names["box_score"] = ["game_player_id",
                                                "game_id",
                                                "game",
                                                "player_id",
                                                "is_starter",
                                                "is_playing",
                                                "team",
                                                "dorsal",
                                                "player",                        
                                                "minutes",
                                                "points",
                                                "two_points_made",
                                                "two_points_attempted",
                                                "three_points_made",
                                                "three_points_attempted",
                                                "free_throws_made",
                                                "free_throws_attempted",
                                                "offensive_rebounds",
                                                "defensive_rebounds",
                                                "total_rebounds",
                                                "assistances",
                                                "steals",
                                                "turnovers",
                                                "blocks_favour",
                                                "blocks_against",
                                                "fouls_commited",
                                                "fouls_received",
                                                "valuation"]
                
    def set_play_by_play_columns(self):
    
        self.table_column_names["play_by_play"] = ["game_play_id",
                                                   "game_id",
                                                   "game",
                                                   "quarter",
                                                   "type",
                                                   "number_of_play",
                                                   "code_team",
                                                   "player_id",
                                                   "play_type",
                                                   "player",
                                                   "team",
                                                   "dorsal",
                                                   "minute",
                                                   "marker_time",
                                                   "points_a",
                                                   "points_b",
                                                   "comment",
                                                   "play_info"]
        
   
    def set_points_columns(self):
    
        self.table_column_names["points"] = ["game_point_id",
                                             "game_id",
                                             "game",
                                             "number_of_play",
                                             "team",
                                             "player_id",
                                             "player",
                                             "action_id",
                                             "action",
                                             "points",
                                             "coord_x",
                                             "coord_y",
                                             "zone",
                                             "fastbreak",
                                             "second_chance",
                                             "points_off_turnover",
                                             "minute",
                                             "console",
                                             "points_a",
                                             "points_b",
                                             "timestamp"]

    def set_comparison_columns(self):
            
        self.table_column_names["comparison"]  = ["game_id",
                                                  "game",
                                                  "fast_break_points_a",
                                                  "fast_break_points_b",
                                                  "turnover_points_a",
                                                  "turnover_points_b",
                                                  "second_chance_points_a",
                                                  "second_chance_points_b",
                                                  "defensive_rebounds_a",
                                                  "offensive_rebounds_b",
                                                  "offensive_rebounds_a",
                                                  "defensive_rebounds_b",
                                                  "turnovers_starters_a",
                                                  "turnovers_bench_a",
                                                  "turnovers_starters_b",
                                                  "turnovers_bench_b",
                                                  "steals_starters_a",
                                                  "steals_bench_a",
                                                  "steals_starters_b",
                                                  "steals_bench_b",
                                                  "assists_starters_a",
                                                  "assists_bench_a",
                                                  "assists_starters_b",
                                                  "assists_bench_b",
                                                  "points_starters_a",
                                                  "points_bench_a",
                                                  "points_starters_b",
                                                  "points_bench_b",
                                                  "max_a",
                                                  "minute_prev_a",
                                                  "prev_a",
                                                  "minute_str_a",
                                                  "str_a",
                                                  "max_b",
                                                  "minute_prev_b",
                                                  "prev_b",
                                                  "minute_str_b",
                                                  "str_b",
                                                  "max_lead_a",
                                                  "max_lead_b",
                                                  "minute_max_lead_a",
                                                  "minute_max_lead_b",
                                                  "points_max_lead_a",
                                                  "points_max_lead_b"]
        
    def add_columns(self, table):
        '''
        The present method adds the columns to the table and sets the data type of each column.
        Apart from 'date', 'time' columns of 'header' table, and 'timestamp' column of 'points' table,
        all columns have been set to be of a 'varchar' data type. This will be altered in a future version.
        '''        
        # use one of the above functions to set the column names of the table
        getattr(self, f"set_{table}_columns")()
        
        # add columns and set data types for the 'header' table
        if table == "header":

            self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS game_id varchar")
            self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS game varchar")
            self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS date date")
            self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS time time")                
            for col in self.table_column_names[table][3:]:
                self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS {col} varchar")
            self.connection.commit()   
            
        # add columns and set data types for the 'points' table
        elif table == "points":
            
            for col in self.table_column_names[table][:-1]:
                self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS {col} varchar")
            self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS timestamp timestamp")                            
            self.connection.commit()          

        # add columns and set data types for the 'play_by_play', 'comparison' or 'box_score' table
        else:

            for col in self.table_column_names[table]:
                self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS {col} varchar")
            self.connection.commit()

    def create_unique_index(self, table):
        '''
        The present method enables the indexing of a table by using the unique identifier column of the table.
        ''' 
        # map each table to its unique identifier column
        map_table_to_index = {"header": "game_id",
                              "box_score": "game_player_id",
                              "play_by_play": "game_play_id",
                              "points": "game_point_id",
                              "comparison": "game_id"}
    
        # get the unqiue indentifier of the table
        index = map_table_to_index[table]
        
        # set the indexing of the table
        self.cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS index_{table}_{index} ON {table} ({index})")
        
        self.connection.commit()
        
        return index


            
            
      
    