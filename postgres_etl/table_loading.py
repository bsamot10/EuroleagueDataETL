from table_schema import SchemaLoader
import helper_loading as h
from psycopg2 import sql
import pandas as pd
import pandas.io.sql as io_sql
from datetime import datetime
from time import time
import numpy
import json
import os
import warnings

warnings.filterwarnings('ignore')


class EuroDatabaseLoader(SchemaLoader):
    '''
    The present class inherits the 'SchemaLoader' class of 'table_schema' module.
    It uses the inherited methods and variables, as well as, the helper functions of 'helper_loading' module.
    It also introduces two instance variables and three types of methods.
    The 1st type ('implement_table_loading') implements the loading process by making use of the 2nd and 3rd type of methods.
    The 2nd type ('get_sql_insert_query') creates the table and sets the sql query that populates the table.
    The 3rd type ('extract_and_load_{table_name}') handles the extraction, transformation and loading of the data.
    '''

    def __init__(self, connection, competition, season_codes, postgres_tables, ):

        super().__init__(connection)

        # the next two instance variables correspond to the three command line arguments
        self.competition = competition
        self.season_codes = season_codes
        self.postgres_tables = postgres_tables

    def implement_table_loading(self):
        '''
        This is the main instance method of the class.
        It makes use of the remaining instance methods and provides a step by step implementation of the process.
        '''
        for table in self.postgres_tables:

            print(
                "\n-----------------------------------------------------------------------------------------------------")

            # load table
            competition_table = f"{self.competition}_{table}"
            sql_insert_table = self.get_sql_insert_query(competition_table)
            start_table = time()
            for season_code in self.season_codes:
                start_season = time()
                print(f"\nLoading {table.upper()}: SeasonCode {season_code}")
                json_success_filenames = os.listdir(fr"../data/{self.competition}_json/{season_code}/success")
                json_success_filenames_header = [filename for filename in json_success_filenames \
                                                 if "Header" in filename and filename not in h.files_to_exclude["Header"]]
                getattr(self, f"extract_and_load_{table}")(season_code, json_success_filenames,
                                                           json_success_filenames_header, sql_insert_table)
                print("TimeCounterSeason", round(time() - start_season, 1), "sec  --- ", "TimeCounterTable:",
                      round(time() - start_table, 1), "sec")

            if table == "box_score":
                # load players
                sql_insert_players = self.get_sql_insert_query(self.competition + '_players')
                start_players = time()
                print(
                    "\n-----------------------------------------------------------------------------------------------------")
                print(f"\nLoading PLAYERS: all available seasons at once")
                self.extract_and_load_players(sql_insert_players)
                print("TimeCounterTable", round(time() - start_players, 1), "sec")

                # load teams
                sql_insert_teams = self.get_sql_insert_query(self.competition + '_teams')
                start_teams = time()
                print(
                    "\n-----------------------------------------------------------------------------------------------------")
                print(f"\nLoading TEAMS: all available seasons at once")
                self.extract_and_load_teams(sql_insert_teams)
                print("TimeCounterTable", round(time() - start_teams, 1), "sec")

    def get_sql_insert_query(self, table):

        # 'players' and 'teams' tables should first be dropped, otherwise they cannot be updated properly
        if table in [f"{self.competition}_players", f"{self.competition}_teams"]:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
            self.cursor.execute(f"CREATE TABLE {table} ()")
        else:
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ()")

        # use the instance variables and methods of the inherited class
        self.add_columns(table)
        truncated_table_name = '_'.join(table.split('_')[1:])
        table_column_names = self.table_column_names[truncated_table_name]
        table_primary_key = self.map_table_to_primary_key[truncated_table_name]
        
        # set the primary key of the table, only if it does not already exist
        query = f"select count(*) from information_schema.table_constraints tc " \
                f"where tc.constraint_type = 'PRIMARY KEY' and tc.table_name = '{table}'"
        if io_sql.read_sql_query(query, self.connection)["count"][0] == 0:
            self.set_primary_key(table, table_primary_key)

        # get the sql query that populates the table
        sql_insert = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING") \
                        .format(sql.SQL(table),
                                sql.SQL(', ').join(map(sql.Identifier, list(table_column_names.keys()))),
                                sql.SQL(', ').join(sql.Placeholder() * len(table_column_names)),
                                sql.SQL(table_primary_key))

        return sql_insert

    def extract_and_load_header(self, season_code, json_success_filenames, json_success_filenames_header, sql_insert):

        ######### EXTRACT & TRANSFORM #########

        # get the json files of box_score
        json_success_filenames_box = [filename for filename in json_success_filenames \
                                      if "Box" in filename and filename not in h.files_to_exclude["Boxscore"]]

        # extract data from the json files of header
        dfs_header = []
        for json_filename in json_success_filenames_header:
            game_code = json_filename.split("_")[3]
            game_id = f"{season_code}_{game_code}"
            df_json = pd.read_json(fr"../data/{self.competition}_json/{season_code}/success/{json_filename}", orient="index").transpose()
            if game_id in ["U2017_136", "U2017_008"]:
                df_json["CompetitionReducedName"] = "U2017"
            Date, Hour = df_json.iloc[0][["Date", "Hour"]]
            date_converted = datetime.strptime(Date, "%d/%m/%Y").date()
            time_converted = datetime.strptime(Hour.replace('.', ':').strip(), "%H:%M").time()
            df = pd.DataFrame()
            for key, value in self.map_table_columns_to_json_header.items():
                df[key] = df_json[value]
            df.insert(0, "time", [time_converted])
            df.insert(0, "date", [date_converted])
            df.insert(0, "game", df[["team_id_a", "team_id_b"]].agg("-".join, axis=1))
            df.insert(0, "game_id", [game_id])
            dfs_header.append(df)
        df_header = pd.concat(dfs_header)

        # extract data from the json files of box_score
        dfs_box_score = []
        for json_filename in json_success_filenames_box:
            game_code = json_filename.split("_")[3]
            game_id = f"{season_code}_{game_code}"
            json_path = fr"../data/{self.competition}_json/{season_code}/success/{json_filename}"
            json_file = open(json_path)
            json_data = json.load(json_file)
            json_file.close()
            extra_time_columns = [("score_extra_time_1_a", "score_extra_time_2_a", "score_extra_time_3_a"),
                                  ("score_extra_time_1_b", "score_extra_time_2_b", "score_extra_time_3_b")]
            df = h.get_extra_time_df(extra_time_columns, json_data, self.map_table_columns_to_json_box)
            df.insert(0, "game_id", [game_id])
            dfs_box_score.append(df)
        df_box_score = pd.concat(dfs_box_score)

        # combine data from header and box_score
        df_merged = pd.merge(df_header, df_box_score, how="outer", on="game_id")
        df_merged["date"] = df_merged["date"].fillna(datetime(1970, 1, 1).date())
        df_merged["time"] = df_merged["time"].fillna(datetime(1970, 1, 1).time())

        # get the number of rows of the dataframe
        number_of_rows = df_merged.shape[0]

        # strip columns that might contain redundant empty spaces
        df_merged = h.strip_header(df_merged)

        # fix cases where season_code = 'EUROLEAGUE'
        df_merged["season_code"][df_merged["season_code"] == "EUROLEAGUE"] = season_code

        # secure integers
        for index, data_type in enumerate(list(self.table_column_names["header"].values())):
            if data_type == "int":
                df_merged[df_merged.columns[index]] = df_merged[df_merged.columns[index]].astype(int)

        ######### LOAD #########

        # load data on header table (populate per season)
        for i in range(number_of_rows):

            try:

                data_insert = [str(value) for value in df_merged.iloc[i]]
                self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
                self.connection.commit()

            except Exception as e:

                print(e, "\ngame_id:", df_merged.iloc[i]["game_id"], "\n")

    def extract_and_load_box_score(self, season_code, json_success_filenames, json_success_filenames_header, sql_insert):

        ######### EXTRACT & TRANSFORM #########

        # extract data from the json files of header
        df_game_header = h.get_game_header_df(self.competition, json_success_filenames_header, season_code) \
                          .drop(columns=["team_id_a", "team_id_b"])

        # get the json files of box_score
        json_success_filenames_box = [filename for filename in json_success_filenames \
                                      if "Box" in filename and filename not in h.files_to_exclude["Boxscore"]]

        # extract data from the json files of box_score
        for json_filename in json_success_filenames_box:

            game_code = json_filename.split("_")[3]
            game_id = f"{season_code}_{game_code}"

            json_path = fr"../data/{self.competition}_json/{season_code}/success/{json_filename}"
            json_file = open(json_path)
            json_data = json.load(json_file)
            json_file.close()

            data_a, data_b = json_data["Stats"]
            stats_a = h.get_team_stats(data_a)
            stats_b = h.get_team_stats(data_b)
            box_score = stats_a + stats_b

            df_box_score = pd.DataFrame(box_score)
            if "Plusminus" in df_box_score.columns:
                df_box_score = df_box_score.drop(columns=["Plusminus"])
            number_of_rows = df_box_score.shape[0]

            player_ids = df_box_score["Player_ID"].str.strip()
            game_player_ids = [f"{game_id}_{player_id}" for player_id in player_ids]
            df_box_score.insert(0, "game_id", [game_id for i in range(number_of_rows)])
            df_box_score.insert(0, "game_player_id", game_player_ids)
            df_box_score["IsPlaying"].mask(~df_box_score["Minutes"].str.contains("DNP"), 1, inplace=True)

            # combine data from header and box_score
            df_merged = pd.merge(df_box_score, df_game_header, how="left", on="game_id")

            # strip columns that might contain redundant empty spaces
            df_merged = h.strip_box_score(df_merged)

            # re-order columns and replace numpy nulls with zeros
            columns_reordered = ["game_player_id", "game_id", "game", "round", "phase", "season_code"] \
                                + list(df_merged.columns[2:-4])
            df_merged = df_merged[columns_reordered].replace(numpy.nan, 0)

            # secure integers
            for index, data_type in enumerate(list(self.table_column_names["box_score"].values())):
                if data_type == "int":
                    df_merged[df_merged.columns[index]] = df_merged[df_merged.columns[index]].astype(int)

            ######### LOAD #########

            # load data on box_score table (populate per file)
            for i in range(number_of_rows):

                try:

                    data_insert = [str(value) for value in df_merged.iloc[i]]
                    self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
                    self.connection.commit()

                except Exception as e:

                    print(e, "\ngame_player_id:", df_merged.iloc[i]["game_player_id"], "\n")

    def extract_and_load_players(self, sql_insert):

        ######### EXTRACT #########

        query = f"select * from {self.competition}_box_score"
        df_box = io_sql.read_sql_query(query, self.connection)

        ######### TRANSFORM #########

        df_box["season_player_id"] = df_box[["season_code", "player_id", "team_id"]].agg("_".join, axis=1)
        df_box[["min", "sec"]] = df_box["minutes"].fillna("00:00").str.replace("DNP", "00:00").str.split(":", expand=True)
        df_box["min"].replace(r'^\s*$', "00", regex=True, inplace=True)
        df_box["sec"].fillna("00", inplace=True)
        df_box["minutes"] = df_box["min"].astype('float') + df_box["sec"].astype('float') / 60
        df_box["is_playing"].mask(df_box["minutes"] > 0, 1.0, inplace=True)
        df_box = df_box[df_box["dorsal"] != "TOTAL"]

        columns_to_sum = ["is_playing", "is_starter"] + list(self.table_column_names["players"].keys())[7:26]

        for col in columns_to_sum:
            df_box[col] = df_box[col].astype('float')

        df_players = df_box.groupby("season_player_id") \
                           .agg(dict({col: ['first'] for col in ["season_code", "player_id", "player", "team_id"]},
                                   **{col: ['sum'] for col in columns_to_sum})).reset_index()

        for col in columns_to_sum[2:]:
            h.add_percentage_columns(df_players, col)

        df_players["minutes"] = df_players["minutes"].round(1)
        df_players = df_players.replace(numpy.nan, 0)
        number_of_rows = len(df_players)

        # secure integers
        for index, data_type in enumerate(list(self.table_column_names["players"].values())):
            if data_type == "int":
                df_players[df_players.columns[index]] = df_players[df_players.columns[index]].astype(int)

        ######### LOAD #########

        # load data on players table (populate all seasons at once)
        for i in range(number_of_rows):

            try:

                data_insert = [str(value) for value in df_players.iloc[i]]
                self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
                self.connection.commit()

            except Exception as e:

                print(e, "\nseason_player_id:", df_players.iloc[i]["season_player_id"], "\n")

    def extract_and_load_teams(self, sql_insert):

        ######### EXTRACT #########

        query = f"select * from {self.competition}_box_score"
        df_box = io_sql.read_sql_query(query, self.connection)

        ######### TRANSFORM #########

        df_box["season_team_id"] = df_box[["season_code", "team_id"]].agg("_".join, axis=1)
        df_box[["min", "sec"]] = df_box["minutes"].fillna("00:00") \
            .str.replace("DNP", "00:00") \
            .str.split(":", expand=True)
        df_box["min"].replace(r'^\s*$', "00", regex=True, inplace=True)
        df_box["sec"].fillna("00", inplace=True)
        df_box["minutes"] = (df_box["min"].astype('float') + df_box["sec"].astype('float') / 60) / 5
        df_box["is_playing"].mask(df_box["minutes"] > 0, 1.0, inplace=True)
        df_box = df_box[df_box["dorsal"] == "TOTAL"]

        columns_to_sum = ["is_playing"] + list(self.table_column_names["teams"].keys())[4:23]

        for col in columns_to_sum:
            df_box[col] = df_box[col].astype('float')

        df_teams = df_box.groupby("season_team_id") \
            .agg(dict({col: ['first'] for col in ["season_code", "team_id"]},
                      **{col: ['sum'] for col in columns_to_sum})).reset_index()

        for col in columns_to_sum[1:]:
            h.add_percentage_columns(df_teams, col)

        df_teams["minutes"] = df_teams["minutes"].round(1)
        number_of_rows = len(df_teams)

        # secure integers
        for index, data_type in enumerate(list(self.table_column_names["teams"].values())):
            if data_type == "int":
                df_teams[df_teams.columns[index]] = df_teams[df_teams.columns[index]].astype(int)

        ######### LOAD #########

        # load data on teams table (populate all seasons at once)
        for i in range(number_of_rows):

            try:

                data_insert = [str(value) for value in df_teams.iloc[i]]
                self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
                self.connection.commit()

            except Exception as e:

                print(e, "\nseason_team_id:", df_teams.iloc[i]["season_team_id"], "\n")
                exit()

    def extract_and_load_points(self, season_code, json_success_filenames, json_success_filenames_header, sql_insert):

        ######### EXTRACT & TRANSFORM #########

        # extract data from the json files of header
        df_game_header = h.get_game_header_df(self.competition, json_success_filenames_header, season_code) \
                          .drop(columns=["team_id_a", "team_id_b"])

        # get the json files of points
        json_success_filenames_points = [filename for filename in json_success_filenames \
                                         if "Points" in filename and filename not in h.files_to_exclude["Points"]]

        # extract data from the json files of points
        for json_filename in json_success_filenames_points:

            start = time()

            game_code = json_filename.split("_")[3]
            game_id = f"{season_code}_{game_code}"

            json_path = fr"../data/{self.competition}_json/{season_code}/success/{json_filename}"
            json_file = open(json_path)
            json_data = json.load(json_file)
            json_file.close()

            df_points = pd.DataFrame(json_data["Rows"])
            number_of_rows = df_points.shape[0]

            point_ids = df_points["NUM_ANOT"]
            game_point_ids = [f"{game_id}_{'{:03d}'.format(point_id)}" for point_id in point_ids]
            df_points.insert(0, "game_id", [game_id for i in range(number_of_rows)])
            df_points.insert(0, "game_point_id", game_point_ids)
            df_points["timestamp"] = pd.to_datetime(df_points["UTC"]).fillna(datetime(1970, 1, 1))

            # combine data from points and box_score
            df_merged = pd.merge(df_points, df_game_header, how="left", on="game_id").drop(["UTC"], axis=1)

            # strip columns that might contain redundant empty spaces
            df_merged = h.strip_points(df_merged)

            # re-order columns and replace numpy nulls with empty strings
            columns_reordered = ["game_point_id", "game_id", "game", "round", "phase", "season_code"] \
                                + list(df_merged.columns[2:-4])
            df_merged = df_merged[columns_reordered].replace(numpy.nan, "")

            # secure integers
            for index, data_type in enumerate(list(self.table_column_names["points"].values())):
                if data_type == "int":
                    df_merged[df_merged.columns[index]] = df_merged[df_merged.columns[index]].astype(int)

            ######### LOAD #########

            # load data on points table (populate per file)
            for i in range(number_of_rows):

                try:

                    data_insert = [str(value) for value in df_merged.iloc[i]]
                    self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
                    self.connection.commit()

                except Exception as e:

                    print(e, "\ngame_point_id:", df_merged.iloc[i]["game_point_id"], "\n")

    def extract_and_load_play_by_play(self, season_code, json_success_filenames, json_success_filenames_header,
                                      sql_insert):

        ######### EXTRACT & TRANSFORM #########

        # extract data from the json files of header
        df_game_header = h.get_game_header_df(self.competition, json_success_filenames_header, season_code) \
                          .drop(columns=["team_id_a", "team_id_b"])

        # get the json files of play_by_play
        json_success_filenames_play = [filename for filename in json_success_filenames \
                                       if "PlaybyPlay" in filename and filename not in h.files_to_exclude["PlaybyPlay"]]

        # extract data from the json files of play_by_play
        for json_filename in json_success_filenames_play:

            game_code = json_filename.split("_")[3]
            game_id = f"{season_code}_{game_code}"

            json_path = fr"../data/{self.competition}_json/{season_code}/success/{json_filename}"
            json_file = open(json_path)
            json_data = json.load(json_file)
            json_file.close()

            quarters = {"FirstQuarter": "q1", "SecondQuarter": "q2",
                        "ThirdQuarter": "q3", "ForthQuarter": "q4",
                        "ExtraTime": "extra_time"}

            df_play_by_play = h.get_quarters_df(quarters, json_data)
            number_of_rows = df_play_by_play.shape[0]

            play_ids = df_play_by_play["NUMBEROFPLAY"]
            game_play_ids = [f"{game_id}_{'{:03d}'.format(play_id)}" for play_id in play_ids]
            df_play_by_play.insert(0, "game_id", [game_id for i in range(number_of_rows)])
            df_play_by_play.insert(0, "game_play_id", game_play_ids)

            # combine data from play_by_play and box_score
            df_merged = pd.merge(df_play_by_play, df_game_header, how="left", on="game_id")

            # strip columns that might contain redundant empty spaces
            df_merged = h.strip_play_by_play(df_merged)

            # re-order columns and replace numpy nulls with zeros
            columns_reordered = ["game_play_id", "game_id", "game", "round", "phase", "season_code"] \
                                + list(df_merged.columns[2:-4])
            df_merged = df_merged[columns_reordered].replace(numpy.nan, 0)

            #  secure integers
            for index, data_type in enumerate(list(self.table_column_names["play_by_play"].values())):
                if data_type == "int":
                    df_merged[df_merged.columns[index]] = df_merged[df_merged.columns[index]].astype(int)

            ######### LOAD #########

            # load data on play_by_play table (populate per file)
            for i in range(number_of_rows):

                try:

                    data_insert = [str(value) for value in df_merged.iloc[i]]
                    self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
                    self.connection.commit()

                except Exception as e:

                    print(e, "\ngame_play_id:", df_merged.iloc[i]["game_play_id"], "\n")

    def extract_and_load_comparison(self, season_code, json_success_filenames, json_success_filenames_header, sql_insert):

        ######### EXTRACT & TRANSFORM #########

        # extract data from the json files of header
        df_game_header = h.get_game_header_df(self.competition, json_success_filenames_header, season_code)

        # get the json files of shooting
        json_success_filenames_shoot = [filename for filename in json_success_filenames \
                                        if "Shooting" in filename and filename not in h.files_to_exclude["ShootingGraphic"]]
        # get the json files of comparison
        json_success_filenames_comp = [filename for filename in json_success_filenames \
                                       if "Comparison" in filename and filename not in h.files_to_exclude["Comparison"]]

        # extract data from the json files of shooting
        dfs_shoot = []
        for json_filename in json_success_filenames_shoot:
            game_code = json_filename.split("_")[3]
            game_id = f"{season_code}_{game_code}"
            df = pd.read_json(fr"../data/{self.competition}_json/{season_code}/success/{json_filename}", orient="index").transpose()
            df.insert(0, "game_id", [game_id])
            dfs_shoot.append(df)
        df_shoot = pd.concat(dfs_shoot)

        # extract data from the json files of comparison
        dfs_comp = []
        for json_filename in json_success_filenames_comp:
            game_code = json_filename.split("_")[3]
            game_id = f"{season_code}_{game_code}"
            df = pd.read_json(fr"../data/{self.competition}_json/{season_code}/success/{json_filename}", orient="index").transpose()\
                   .drop(["minutoActual", "isLive"], axis=1)
            df.insert(0, "game_id", [game_id])
            dfs_comp.append(df)
        df_comp = pd.concat(dfs_comp)

        # combine data from shooting, comparison and header
        df_merged = pd.merge(df_shoot, df_comp, how="outer", on="game_id")
        df_merged = pd.merge(df_merged, df_game_header, how="left", on="game_id")

        # get the number of rows of the dataframe
        number_of_rows = df_merged.shape[0]

        # strip columns that might contain redundant empty spaces
        df_merged = h.strip_comparison(df_merged)

        # re-order columns and replace numpy nulls with zeros
        columns_reordered = ["game_id", "game", "round", "phase", "season_code", "team_id_a", "team_id_b"] \
                            + list(df_merged.columns[1:-6])
        df_merged = df_merged[columns_reordered].replace(numpy.nan, 0)

        #  secure integers
        for index, data_type in enumerate(list(self.table_column_names["comparison"].values())):
            if data_type == "int":
                df_merged[df_merged.columns[index]] = df_merged[df_merged.columns[index]].astype(int)

        ######### LOAD #########

        # load data on comparison table (populate per season)
        for i in range(number_of_rows):

            try:

                data_insert = [str(value) for value in df_merged.iloc[i]]
                self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
                self.connection.commit()

            except Exception as e:
                exit()
                print(e, "\ngame_id:", df_merged.iloc[i]["game_id"], "\n")
