class SchemaLoader:
    '''
    The present class is inherited by the 'EuroDatabaseLoader' class of 'table_loading' module.
    It introduces three types of methods.
    The 1st type ('set_{table_name}_columns') sets the column names and data types of each table.
    The 2nd type ('add_columns') adds the columns that the 1st type has set.
    The 3rd type ('set_primary_key') sets a unique identifier for each table.
    '''
    def __init__(self, connection):
        
        # the next two instance variables are responsible for the interaction with the database
        self.connection = connection 
        self.cursor = connection.cursor()
        
        # the next three instance variables are updated by the 'set_{table_name}_columns' functions
        self.table_column_names = {}    
        self.map_table_columns_to_json_header = {}
        self.map_table_columns_to_json_box = {} 

        # the following instance variable maps each table to the primary key column
        self.map_table_to_primary_key = {"header": "game_id",
                                         "box_score": "game_player_id",
                                         "players": "season_player_id",
                                         "teams": "season_team_id",
                                         "play_by_play": "game_play_id",
                                         "points": "game_point_id",
                                         "comparison": "game_id"}
        
    def set_header_columns(self):
        
        self.map_table_columns_to_json_header = {"round": "Round",
                                                 "phase": "Phase",
                                                 "season_code": "CompetitionReducedName",
                                                 "score_a": "ScoreA",
                                                 "score_b": "ScoreB",
                                                 "team_a": "TeamA",
                                                 "team_b": "TeamB",
                                                 "team_id_a": "CodeTeamA",
                                                 "team_id_b": "CodeTeamB",
                                                 "coach_a": "CoachA",
                                                 "coach_b": "CoachB",
                                                 "game_time": "GameTime",
                                                 "remaining_partial_time": "RemainingPartialTime",
                                                 "referee_1": "Referee1",
                                                 "referee_2": "Referee2",
                                                 "referee_3": "Referee3",
                                                 "stadium": "Stadium",
                                                 "capacity": "Capacity",
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
        
        self.table_column_names["header"] = {"game_id": "varchar", 
                                             "game": "varchar", 
                                             "date": "date", 
                                             "time": "time",
                                             "round": "varchar",
                                             "phase": "varchar",
                                             "season_code": "varchar",
                                             "score_a": "int",
                                             "score_b": "int",
                                             "team_a": "varchar",
                                             "team_b": "varchar",
                                             "team_id_a": "varchar",
                                             "team_id_b": "varchar",
                                             "coach_a": "varchar",
                                             "coach_b": "varchar",
                                             "game_time": "varchar",
                                             "remaining_partial_time": "varchar",
                                             "referee_1": "varchar",
                                             "referee_2": "varchar",
                                             "referee_3": "varchar",
                                             "stadium": "varchar",
                                             "capacity": "int",
                                             "w_id": "varchar",
                                             "fouls_a": "int",
                                             "fouls_b": "int",
                                             "timeouts_a": "int",
                                             "timeouts_b": "int",
                                             "score_quarter_1_a": "int",
                                             "score_quarter_2_a": "int",
                                             "score_quarter_3_a": "int",
                                             "score_quarter_4_a": "int",
                                             "score_quarter_1_b": "int",
                                             "score_quarter_2_b": "int",
                                             "score_quarter_3_b": "int",
                                             "score_quarter_4_b": "int",
                                             "score_extra_time_1_a": "varchar",
                                             "score_extra_time_2_a": "varchar",
                                             "score_extra_time_3_a": "varchar",
                                             "score_extra_time_1_b": "varchar",
                                             "score_extra_time_2_b": "varchar",
                                             "score_extra_time_3_b": "varchar"}
        
    def set_box_score_columns(self):
    
        self.table_column_names["box_score"] = {"game_player_id": "varchar",
                                                "game_id": "varchar",
                                                "game": "varchar",
                                                "round": "int",
                                                "phase": "varchar",
                                                "season_code": "varchar",
                                                "player_id": "varchar",
                                                "is_starter": "float",
                                                "is_playing": "float",
                                                "team_id": "varchar",
                                                "dorsal": "varchar",
                                                "player": "varchar",                        
                                                "minutes": "varchar",
                                                "points": "int",
                                                "two_points_made": "int",
                                                "two_points_attempted": "int",
                                                "three_points_made": "int",
                                                "three_points_attempted": "int",
                                                "free_throws_made": "int",
                                                "free_throws_attempted": "int",
                                                "offensive_rebounds": "int",
                                                "defensive_rebounds": "int",
                                                "total_rebounds": "int",
                                                "assists": "int",
                                                "steals": "int",
                                                "turnovers": "int",
                                                "blocks_favour": "int",
                                                "blocks_against": "int",
                                                "fouls_committed": "int",
                                                "fouls_received": "int",
                                                "valuation": "int"}
        
    def set_players_columns(self):
    
        self.table_column_names["players"] = {"season_player_id": "varchar",
                                              "season_code": "varchar",
                                              "player_id": "varchar",
                                              "player": "varchar",
                                              "team_id": "varchar",  
                                              "games_played": "float",
                                              "games_started": "float",
                                              "minutes": "float",
                                              "points": "int",
                                              "two_points_made": "int",
                                              "two_points_attempted": "int",
                                              "three_points_made": "int",
                                              "three_points_attempted": "int",
                                              "free_throws_made": "int",
                                              "free_throws_attempted": "int",
                                              "offensive_rebounds": "int",
                                              "defensive_rebounds": "int",
                                              "total_rebounds": "int",
                                              "assists": "int",
                                              "steals": "int",
                                              "turnovers": "int",
                                              "blocks_favour": "int",
                                              "blocks_against": "int",
                                              "fouls_committed": "int",
                                              "fouls_received": "int",
                                              "valuation": "int",
                                              "minutes_per_game": "float",
                                              "points_per_game": "float",
                                              "two_points_made_per_game": "float",
                                              "two_points_attempted_per_game": "float",
                                              "two_points_percentage": "float",
                                              "three_points_made_per_game": "float",
                                              "three_points_attempted_per_game": "float",
                                              "three_points_percentage": "float",
                                              "free_throws_made_per_game": "float",
                                              "free_throws_attempted_per_game": "float",
                                              "free_throws_percentage": "float",
                                              "offensive_rebounds_per_game": "float",
                                              "defensive_rebounds_per_game": "float",
                                              "total_rebounds_per_game": "float",
                                              "assists_per_game": "float",
                                              "steals_per_game": "float",
                                              "turnovers_per_game": "float",
                                              "blocks_favour_per_game": "float",
                                              "blocks_against_per_game": "float",
                                              "fouls_committed_per_game": "float",
                                              "fouls_received_per_game": "float",
                                              "valuation_per_game": "float"}
        
    def set_teams_columns(self):
    
        self.table_column_names["teams"] = {"season_team_id": "varchar",
                                            "season_code": "varchar",
                                            "team_id": "varchar",
                                            "games_played": "float",
                                            "minutes": "float",
                                            "points": "int",
                                            "two_points_made": "int",
                                            "two_points_attempted": "int",
                                            "three_points_made": "int",
                                            "three_points_attempted": "int",
                                            "free_throws_made": "int",
                                            "free_throws_attempted": "int",
                                            "offensive_rebounds": "int",
                                            "defensive_rebounds": "int",
                                            "total_rebounds": "int",
                                            "assists": "int",
                                            "steals": "int",
                                            "turnovers": "int",
                                            "blocks_favour": "int",
                                            "blocks_against": "int",
                                            "fouls_committed": "int",
                                            "fouls_received": "int",
                                            "valuation": "int",
                                            "minutes_per_game": "float",
                                            "points_per_game": "float",
                                            "two_points_made_per_game": "float",
                                            "two_points_attempted_per_game": "float",
                                            "two_points_percentage": "float",
                                            "three_points_made_per_game": "float",
                                            "three_points_attempted_per_game": "float",
                                            "three_points_percentage": "float",
                                            "free_throws_made_per_game": "float",
                                            "free_throws_attempted_per_game": "float",
                                            "free_throws_percentage": "float",
                                            "offensive_rebounds_per_game": "float",
                                            "defensive_rebounds_per_game": "float",
                                            "total_rebounds_per_game": "float",
                                            "assists_per_game": "float",
                                            "steals_per_game": "float",
                                            "turnovers_per_game": "float",
                                            "blocks_favour_per_game": "float",
                                            "blocks_against_per_game": "float",
                                            "fouls_committed_per_game": "float",
                                            "fouls_received_per_game": "float",
                                            "valuation_per_game": "float"}
                                            
    def set_play_by_play_columns(self):
    
        self.table_column_names["play_by_play"] = {"game_play_id": "varchar",
                                                   "game_id": "varchar",
                                                   "game": "varchar",
                                                   "round": "int",
                                                   "phase": "varchar",
                                                   "season_code": "varchar",
                                                   "quarter": "varchar",
                                                   "type": "varchar",
                                                   "number_of_play": "int",
                                                   "team_id": "varchar",
                                                   "player_id": "varchar",
                                                   "play_type": "varchar",
                                                   "player": "varchar",
                                                   "team": "varchar",
                                                   "dorsal": "varchar",
                                                   "minute": "int",
                                                   "marker_time": "varchar",
                                                   "points_a": "int",
                                                   "points_b": "int",
                                                   "comment": "varchar",
                                                   "play_info": "varchar"}
        
   
    def set_points_columns(self):
    
        self.table_column_names["points"] = {"game_point_id": "varchar",
                                             "game_id": "varchar",
                                             "game": "varchar",
                                             "round": "int",
                                             "phase": "varchar",
                                             "season_code": "varchar",
                                             "number_of_play": "int",
                                             "team_id": "varchar",
                                             "player_id": "varchar",
                                             "player": "varchar",
                                             "action_id": "varchar",
                                             "action": "varchar",
                                             "points": "int",
                                             "coord_x": "int",
                                             "coord_y": "int",
                                             "zone": "varchar",
                                             "fastbreak": "varchar",
                                             "second_chance": "varchar",
                                             "points_off_turnover": "varchar",
                                             "minute": "int",
                                             "console": "varchar",
                                             "points_a": "int",
                                             "points_b": "int",
                                             "timestamp": "timestamp"}

    def set_comparison_columns(self):
            
        self.table_column_names["comparison"] = {"game_id": "varchar",
                                                 "game": "varchar",
                                                 "round": "int",
                                                 "phase": "varchar",
                                                 "season_code": "varchar",
                                                 "team_id_a": "varchar",
                                                 "team_id_b": "varchar",
                                                 "fast_break_points_a": "int",
                                                 "fast_break_points_b": "int",
                                                 "turnover_points_a": "int",
                                                 "turnover_points_b": "int",
                                                 "second_chance_points_a": "int",
                                                 "second_chance_points_b": "int",
                                                 "defensive_rebounds_a": "int",
                                                 "offensive_rebounds_b": "int",
                                                 "offensive_rebounds_a": "int",
                                                 "defensive_rebounds_b": "int",
                                                 "turnovers_starters_a": "int",
                                                 "turnovers_bench_a": "int",
                                                 "turnovers_starters_b": "int",
                                                 "turnovers_bench_b": "int",
                                                 "steals_starters_a": "int",
                                                 "steals_bench_a": "int",
                                                 "steals_starters_b": "int",
                                                 "steals_bench_b": "int",
                                                 "assists_starters_a": "int",
                                                 "assists_bench_a": "int",
                                                 "assists_starters_b": "int",
                                                 "assists_bench_b": "int",
                                                 "points_starters_a": "int",
                                                 "points_bench_a": "int",
                                                 "points_starters_b": "int",
                                                 "points_bench_b": "int",
                                                 "max_a": "int",
                                                 "minute_prev_a": "int",
                                                 "prev_a": "varchar",
                                                 "minute_str_a": "int",
                                                 "str_a": "varchar",
                                                 "max_b": "int",
                                                 "minute_prev_b": "int",
                                                 "prev_b": "varchar",
                                                 "minute_str_b": "int",
                                                 "str_b": "varchar",
                                                 "max_lead_a": "int",
                                                 "max_lead_b": "int",
                                                 "minute_max_lead_a": "int",
                                                 "minute_max_lead_b": "int",
                                                 "points_max_lead_a": "varchar",
                                                 "points_max_lead_b": "varchar"}
        
    def add_columns(self, table):
        '''
        The present method adds the columns to the table and sets the data type of each column.
        '''        
        # use one of the above functions to set the column names and data types of the table
        truncated_table_name = '_'.join(table.split('_')[1:])
        getattr(self, f"set_{truncated_table_name}_columns")()
        
        # add columns and data types
        for col_name, data_type in self.table_column_names[truncated_table_name].items():
            self.cursor.execute(f"ALTER TABLE {table} add COLUMN IF NOT EXISTS {col_name} {data_type}")
        self.connection.commit()   

    def set_primary_key(self, table, table_primary_key):
        '''
        The present method enables the creation of a primary key for each table.
        '''   
        # set the primary key of the table
        self.cursor.execute(f"ALTER TABLE {table} ADD PRIMARY KEY ({table_primary_key})")
        self.connection.commit()
    


            
            
      
    
