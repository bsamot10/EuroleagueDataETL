from time import time
import pandas as pd
import json
import numpy
import warnings
warnings.filterwarnings("ignore")

files_to_exclude = {table: [f"U2008_Last16_08_112_20090203_{table}.json",
                            f"U2008_Last16_09_119_20090210_{table}.json",
                            f"U2008_RegularSeason_05_072_20090106_{table}.json",
                            f"U2008_RegularSeason_06_090_20090113_{table}.json",
                            f"U2010_RegularSeason_02_032_20101123_{table}.json",
                            f"U2013_RegularSeason_01_001_20131016_{table}.json",
                            f"U2013_RegularSeason_02_046_20131023_{table}.json",
                            f"U2013_RegularSeason_04_075_20131106_{table}.json",
                            f"U2013_RegularSeason_08_169_20131204_{table}.json",
                            f"U2015_RegularSeason_01_013_20151014_{table}.json",
                            f"E2018_RegularSeason_03_021_20181019_{table}.json"]
                    for table in ["Header", "Boxscore", "Points", "PlaybyPlay", "Comparison", "ShootingGraphic"]}


def get_extra_time_df(extra_time_columns, box_score_data, map_table_columns_to_json_box):
    '''
    A helper function for the loading of 'header' table. It returns a dataframe with information of a game's extra times.
    '''
    extra_time_df = pd.DataFrame()

    for i, team in enumerate(extra_time_columns):
        for xt in team:
            if map_table_columns_to_json_box[xt] in box_score_data["EndOfQuarter"][i]:
                extra_time_df[xt] = [box_score_data["EndOfQuarter"][i][map_table_columns_to_json_box[xt]]]
            else:
                extra_time_df[xt] = [""]

    return extra_time_df


def get_team_stats(box_score_data):
    '''
    A helper function for the loading of 'box_score' table. It returns a list with information of a team's game stats.
    '''
    players = box_score_data["PlayersStats"]
    totals = box_score_data["totr"]
    totals["Player_ID"] = players[0]["Team"].strip()
    totals["Team"] = players[0]["Team"].strip()
    totals["Dorsal"] = "TOTAL"
    totals["Player"] = box_score_data["Team"].strip()
    team_stats = players + [totals]

    return team_stats


def get_quarters_df(quarters, json_data):
    '''
    A helper function for the loading of 'play_by_play' table. It returns a dataframe with information of a game's quarters.
    '''
    dfs = []

    for q in quarters:
        if json_data[q]:
            df = pd.DataFrame(json_data[q])
            df.insert(0, "quarter", [quarters[q].strip() for i in range(len(json_data[q]))])
            dfs.append(df)

    quarters_df = pd.concat(dfs)

    return quarters_df


def get_game_header_df(competition, json_success_filenames_header, season_code):
    '''
    A helper function for the loading of every table (except 'header' table).
    It returns a dataframe with the 'game_id', 'game', 'round', 'phase', 'season_code', 'team_id_a' and 'team_id_b'.
    The dataframe is used for joining with each table on 'game_id', so that every table has the above columns.
    '''
    game_header = []

    for json_filename in json_success_filenames_header:
        _round = json_filename.split("_")[2]
        game_code = json_filename.split("_")[3]
        game_id = f"{season_code}_{game_code}"
        json_path = fr"../data/{competition}_json/{season_code}/success/{json_filename}"
        json_file = open(json_path)
        json_data = json.load(json_file)
        json_file.close()
        game = json_data["CodeTeamA"].strip() + "-" + json_data["CodeTeamB"].strip()
        team_id_a = json_data["CodeTeamA"].strip()
        team_id_b = json_data["CodeTeamB"].strip()
        phase = json_data["Phase"].strip()
        game_header.append((game_id, game, _round, phase, season_code, team_id_a, team_id_b))

    df_game_header = pd.DataFrame(game_header, columns=["game_id", "game", "round", "phase", "season_code", "team_id_a", "team_id_b"])

    return df_game_header


def add_percentage_columns(df, col):
    '''
    A helper function for the loading of 'players' and 'teams' table. It updates a dataframe by adding columns that represent percentage statistics.
    '''
    df[col + "_per_game"] = (df[col] / df["is_playing"]).round(2)
    if "attempted" in col:
        col_percentage = col.replace("_attempted", "_percentage")
        df[col_percentage] = (df[col.replace("_attempted", "_made")] / df[col]).round(3)


def strip_header(df):
    '''
    A helper function stripping the 'header' columns that might contain redundant empty spaces.
    '''
    df["team_id_a"] = df["team_id_a"].str.strip()
    df["team_id_b"] = df["team_id_b"].str.strip()
    df["score_a"] = df["score_a"].str.strip()
    df["score_b"] = df["score_b"].str.strip()
    df["capacity"] = df["capacity"].str.strip()
    df["game_time"] = df["game_time"].str.strip()
    df["w_id"] = df["w_id"].str.strip()
    df["fouls_a"] = df["fouls_a"].str.strip()
    df["fouls_b"] = df["fouls_b"].str.strip()
    df["phase"] = df["phase"].str.strip()
    df["season_code"] = df["season_code"].str.strip()

    return df


def strip_box_score(df):
    '''
    A helper function stripping the 'box_score' table columns that might contain redundant empty spaces.
    '''
    df["Player_ID"] = df["Player_ID"].str.strip()
    df["Team"] = df["Team"].str.strip()
    df["Dorsal"] = df["Dorsal"].str.strip()
    df["Minutes"] = df["Minutes"].str.strip()
    df["Player"] = df["Player"].str.strip().replace(numpy.nan, "").str.upper().str.replace("\s*,\s*", ', ', regex=True)

    return df


def strip_points(df):
    '''
    A helper function stripping the 'points' table columns that might contain redundant empty spaces.
    '''
    df["TEAM"] = df["TEAM"].str.strip()
    df["ID_PLAYER"] = df["ID_PLAYER"].str.strip()
    df["ID_ACTION"] = df["ID_ACTION"].str.strip()
    df["ACTION"] = df["ACTION"].str.strip()
    df["ZONE"] = df["ZONE"].str.strip()
    df["FASTBREAK"] = df["FASTBREAK"].str.strip()
    df["SECOND_CHANCE"] = df["SECOND_CHANCE"].str.strip()
    df["POINTS_OFF_TURNOVER"] = df["POINTS_OFF_TURNOVER"].str.strip()
    df["CONSOLE"] = df["CONSOLE"].str.strip()
    df["PLAYER"] = df["PLAYER"].str.strip().replace(numpy.nan, "").str.upper().str.replace("\s*,\s*", ', ', regex=True)

    return df


def strip_play_by_play(df):
    '''
    A helper function stripping the 'play_by_play' table columns that might contain redundant empty spaces.
    '''
    df["CODETEAM"] = df["CODETEAM"].str.strip()
    df["PLAYER_ID"] = df["PLAYER_ID"].str.strip()
    df["PLAYTYPE"] = df["PLAYTYPE"].str.strip()
    df["PLAYINFO"] = df["PLAYINFO"].str.strip()
    df["MARKERTIME"] = df["MARKERTIME"].str.strip()
    df["PLAYER"] = df["PLAYER"].str.strip().replace(numpy.nan, "").str.upper().str.replace("\s*,\s*", ', ', regex=True)
    df["TEAM"] = df["TEAM"].str.strip().replace(numpy.nan, "")
    df["DORSAL"] = df["DORSAL"].str.strip().replace(numpy.nan, "")

    return df


def strip_comparison(df):
    '''
    A helper function stripping the 'comparison' table columns that might contain redundant empty spaces.
    '''
    df["prevA"] = df["prevA"].str.strip().replace(numpy.nan, "")
    df["strA"] = df["strA"].str.strip().replace(numpy.nan, "")
    df["puntosMaxLeadA"] = df["puntosMaxLeadA"].str.strip().replace(numpy.nan, "")
    df["prevB"] = df["prevB"].str.strip().replace(numpy.nan, "")
    df["strB"] = df["strB"].str.strip().replace(numpy.nan, "")
    df["puntosMaxLeadB"] = df["puntosMaxLeadB"].str.strip().replace(numpy.nan, "")

    return df


def fix_duplicate_players(connection, cursor, competition, table, start_table):
    '''
    A helper function fixing the cases where one player_id corresponds to more than one player names and vice versa.
    '''
    if table == "box_score":
        query_temp_table = f"CREATE TEMP TABLE temp_tbl AS " \
                           f"SELECT player_id, max(player) AS player " \
                           f"FROM {competition}_{table} WHERE dorsal != 'TOTAL' " \
                           f"GROUP BY player_id HAVING count(distinct player) > 1 "
    else:
        query_temp_table = f"CREATE TEMP TABLE temp_tbl AS " \
                           f"SELECT player_id, max(player) AS player " \
                           f"FROM {competition}_{table} " \
                           f"GROUP BY player_id HAVING count(distinct player) > 1 "
		           
    query_update_table = f"UPDATE {competition}_{table} AS update_tbl " \
                         f"SET player = temp_tbl.player " \
                         f"FROM temp_tbl " \
                         f"WHERE update_tbl.player_id = temp_tbl.player_id"

    query_drop_temp_table = "DROP TABLE temp_tbl"

    start_fixing = time()
    print(f"\nFixing Duplicate Players {table.upper()}")
    
    for query in (query_temp_table, query_update_table, query_drop_temp_table):
        cursor.execute(query)
        connection.commit()
        
    if competition == "euroleague":
        for wrong_id, correct_id in {'1': 'P000668', 'MAD991': 'PKSF', '001720': 'P001720', 'PSIE374368': 'P001479',
                                     'AVD': 'PAVD', 'PTGY': 'PTHY', 'LJU1': 'P000437', 'MAL1': 'PLAC', 'A1': 'PJDQ',
                                     'BAM1': 'P000118', 'CAS GIU': 'P000273', '19': 'PLVZ', 'P000983': 'P003715'}.items():
            cursor.execute(f"UPDATE {competition}_{table} " \
                           f"SET player_id = '{correct_id}' " \
                           f"WHERE player_id = '{wrong_id}'")
            connection.commit()           
    else:
        for wrong_id, correct_id in {'P000375': 'P000378', 'WOJJAK': 'PLJI', 'P03463': 'P003463', 'PCPJ': 'P000254','KOL1': 'P000069', 
                                     'PMAL675546': 'P010442', '24': 'PKPR','P6604': 'P006604', 'P002447': 'P002457', '1977': 'PBGC', 'anwil24': 'PAGR',
                                     'EITO11': 'P000231', '4': 'PADP', '000231': 'P000231','P002445': 'P002447', '000132': 'P000132',
                                     'wlo1': 'P000587', 'AVJ': 'PAVJ','OVA1': 'P000463', 'P03384': 'P003384', '3': 'P000732',  'ven1': 'P000046',
                                     'WRO1': 'PLBP', '124': 'PKGH', '001': 'P002148', 'z02': 'P001532', '115': 'P000434', '000375': 'P000378'}.items():
            cursor.execute(f"UPDATE {competition}_{table} " \
                           f"SET player_id = '{correct_id}' " \
                           f"WHERE player_id = '{wrong_id}'")
            connection.commit() 
        for player_id, update_name in {'P002004': 'BROWN, BRANDON (A)', 'P010477': 'BROWN, BRANDON (B)',
                                       'PCSW': 'HAMILTON, JUSTIN (A)', 'P004399': 'HAMILTON, JUSTIN (B)',
                                       'P002999': 'JOVANOVIC, NIKOLA (A)', 'P003264': 'JOVANOVIC, NIKOLA (B)',
                                       'PCNV': 'POPOVIC, MARKO (A)', 'PATP': 'POPOVIC, MARKO (B)',
                                       'PJMJ': 'POPOVIC, PETAR (A)', 'P007398': 'POPOVIC, PETAR (B)',
                                       'PLCC': 'WARREN, CHRIS (A)', 'P006549': 'WARREN, CHRIS (B)',
                                       'P005261': 'WRIGHT, CHRIS (A)', 'P005968': 'WRIGHT, CHRIS (B)'}.items():
            cursor.execute(f"UPDATE {competition}_{table} " \
                           f"SET player = '{update_name}' " \
                           f"WHERE player_id = '{player_id}'")  
            
    print("TimeCounterFixing:", round(time() - start_fixing, 1), "sec  --- ",
          "TimeCounterTable:", round(time() - start_table, 1), "sec")


def fix_box_score_minutes(connection, cursor, competition, start_table):
    '''
    A helper function fixing the cases where box_score minutes are wrong.
    '''
    if competition == "euroleague":
        start_fixing = time()
        print(f"\nFixing Minutes BOX_SCORE")
        queries = ["UPDATE euroleague_box_score SET minutes = '26:04' WHERE game_player_id = 'E2009_074_PBDK'", 
                   "UPDATE euroleague_box_score SET minutes = '200:00' WHERE game_player_id = 'E2009_074_ZAL'", 
                   "UPDATE euroleague_box_score SET minutes = '13:55' WHERE game_player_id = 'E2009_074_PLXL'", 
                   "UPDATE euroleague_box_score SET minutes = '00:19' WHERE game_player_id = 'E2007_141_PLKE'", 
                   "UPDATE euroleague_box_score SET minutes = '34:48' WHERE game_player_id = 'E2010_127_PJUO'",
                   "UPDATE euroleague_box_score SET is_playing = 0 WHERE game_player_id = 'E2024_065_P012099'",
                   "UPDATE euroleague_box_score SET is_playing = 0 WHERE game_player_id = 'E2016_150_P002506'",
                   "UPDATE euroleague_box_score SET is_playing = 0 WHERE game_player_id = 'E2016_132_P007032'",
                   "UPDATE euroleague_box_score SET minutes = '01:00', is_playing = 1 WHERE game_player_id = 'E2007_163_PAWI'"]
        for query in queries:
            cursor.execute(query)
            connection.commit()
        print("TimeCounterFixing:", round(time() - start_fixing, 1), "sec  --- ",
              "TimeCounterTable:", round(time() - start_table, 1), "sec")

