import pandas as pd
import json

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
    A helper function for the loading of 'play_by_play' table. It creates a dataframe with information of a game's quarters.
    '''
    dfs = []
    
    for q in quarters:       
        if json_data[q]:          
            df = pd.DataFrame(json_data[q])
            df.insert(0, "quarter", [quarters[q].strip() for i in range(len(json_data[q]))])    
            dfs.append(df)
            
    quarters_df = pd.concat(dfs)   
    
    return quarters_df

def get_game_header_df(json_success_filenames_header, season_code):
    '''
    A helper function for the loading of every table (except 'header' table).
    It creates a dataframe with the 'game_id', 'game', 'round', 'phase' and 'season_code'.
    It is used for joining with each table on 'game_id', so that every table has the above columns.
    '''
    game_header = []
    
    for json_filename in json_success_filenames_header:
        round = json_filename.split("_")[2]
        game_code = json_filename.split("_")[3]
        game_id = f"{season_code.strip('E')}_{game_code}"
        json_path = fr"../euroleague_json_data/{season_code}/success/{json_filename}"
        json_file = open(json_path)
        json_data = json.load(json_file)
        json_file.close()            
        game = json_data["CodeTeamA"].strip() + "-" + json_data["CodeTeamB"].strip()
        phase = json_data["Phase"].strip()
        game_header.append((game_id, game, round, phase, season_code))
        
    df_game_header = pd.DataFrame(game_header, columns=["game_id", "game", "round", "phase", "season_code"])
    
    return df_game_header

def add_percentage_columns(df, col):
    '''
    A helper function for the loading of 'players' and 'teams' table. It updates a dataframe by adding columns that represent percentage statistics.
    '''
    df[col + "_per_game"] = df[col] / df["is_playing"]
    df[col + "_per_game"] = df[col + "_per_game"].round(2)
    if "attempted" in col:
        col_percentage = "_".join(col.split("_")[:2]) + "_percentage"
        df[col_percentage] = df["_".join(col.split("_")[:2]) + "_made"] / df[col]
        df[col_percentage] = df[col_percentage].round(3)

'''
A helper function for each table, stripping the columns that might contain redundant empty spaces.
'''

def strip_header(df):
    
    df["code_team_a"] = df["code_team_a"].str.strip()
    df["code_team_b"] = df["code_team_b"].str.strip()
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

    df["Player_ID"] = df["Player_ID"].str.strip()
    df["Team"] = df["Team"].str.strip()
    df["Dorsal"] = df["Dorsal"].str.strip()
    df["Minutes"] = df["Minutes"].str.strip()
    df["Player"] = df["Player"].str.strip().str.replace(",", "_")
    
    return df

def strip_points(df):

    df["TEAM"] = df["TEAM"].str.strip()
    df["ID_PLAYER"] = df["ID_PLAYER"].str.strip()
    df["ID_ACTION"] = df["ID_ACTION"].str.strip()
    df["ZONE"] = df["ZONE"].str.strip()
    df["FASTBREAK"] = df["FASTBREAK"].str.strip()
    df["SECOND_CHANCE"] = df["SECOND_CHANCE"].str.strip()
    df["POINTS_OFF_TURNOVER"] = df["POINTS_OFF_TURNOVER"].str.strip()
    df["CONSOLE"] = df["CONSOLE"].str.strip()
    df["PLAYER"] = df["PLAYER"].str.strip().str.replace(",", "_")
    
    return df
    
def strip_play_by_play(df):

    df["CODETEAM"] = df["CODETEAM"].str.strip()
    df["PLAYER_ID"] = df["PLAYER_ID"].str.strip()
    df["PLAYTYPE"] = df["PLAYTYPE"].str.strip()
    df["PLAYER"] = df["PLAYER"].str.strip().str.replace(",", "_")

    return df

def strip_comparison(df):

    df["prevA"] = df["prevA"].str.strip()
    df["strA"] = df["strA"].str.strip()
    df["puntosMaxLeadA"] = df["puntosMaxLeadA"].str.strip()
    df["prevB"] = df["prevB"].str.strip()
    df["strB"] = df["strB"].str.strip()
    df["puntosMaxLeadB"] = df["puntosMaxLeadB"].str.strip()

    return df

    
    
