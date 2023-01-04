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
    A helper function for every table loading (except 'header' table).
    It creates a dataframe with the game_id, team codes, round and season_code.
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