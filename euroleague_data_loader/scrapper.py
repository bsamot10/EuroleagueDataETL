from time import time
import requests
import json
import os

class EuroScrapas:
    
    def __init__(self, season_codes, euroleague_apis, failed_extractions_limit, game_code_start, datetime_now):
        
        self.season_codes = season_codes
        self.euroleague_apis = euroleague_apis
        self.failed_extractions_limit = failed_extractions_limit
        self.game_code_start = game_code_start
        self.datetime_now = datetime_now
        
    def implement_scrapping_process(self):
        '''
        This is the main instance method of the class. 
        It makes use of the remaining instance methods and provides a step by step implementation of the process.
        '''  
        # IMPLEMENT PROCESS PER SEASON
        for index, sc in enumerate(self.season_codes):

            # INITIALIZE BASIC VARIABLES OF PROCESS
            process_start_time, season_end, meta_data_dict, gc = self.initialize_process(index, sc)

            while not season_end:

                # UPDATE THE GAME_CODE COUNTER
                gc += 1 

                # SCRAP APIs, LOAD DATA AND UPDATE INFO
                meta_data_dict = self.request_extract_load_update(gc, sc, process_start_time, meta_data_dict)

                # SAVE META_DATA
                with open(fr"euroleague_data/{sc}/{sc}_meta_data_{self.datetime_now}.json", "w") as output_file:
                    json.dump(meta_data_dict, output_file)

                # UPDATE FLAG --- THE 1st CONDITION IS OBVIOUS --- THE 2nd CONDITION REFLECTS A POSSIBLE PROBLEM WITH THE URLs OR A PREMATURE END OF THE SEASON (e.g. COVID SEASON E2019)
                if (meta_data_dict[sc]["number_of_FinalFour_games"] == 4) or (meta_data_dict[sc]["number_of_failed_extractions"] > self.failed_extractions_limit):
                    season_end = True
        
    def initialize_process(self, index, sc):
        
        # INITIALIZE TIMING & FlAG 
        process_start_time = time()
        season_end = False

        # ONLY THE FIRST SEASON_CODE CAN START WITH GAME_CODE > 1
        if index > 0:
            self.game_code_start = 1

        # INITIALIZE THE META_DATA DICTIONARY
        meta_data_dict = {sc: {"total_number_of_games": 0,
                               "number_of_RegularSeason_games": 0,
                               "number_of_Top16_games": 0,
                               "number_of_Playoff_games": 0,
                               "number_of_FinalFour_games": 0,
                               "number_of_failed_extractions": 0,
                               "limit_of_failed_extractions": self.failed_extractions_limit,
                               "game_code_counter": None,
                               "game_code_start": self.game_code_start,
                               "unknown_Phase": [],
                               "time_counter": None}}

        # CREATE DIRECTORIES
        if not os.path.exists(fr"euroleague_data/{sc}"):
            os.makedirs(fr"euroleague_data/{sc}/success")
            os.makedirs(fr"euroleague_data/{sc}/failure")

        # INITIALIZE THE GAME_CODE COUNTER
        gc = self.game_code_start - 1
        
        return process_start_time, season_end, meta_data_dict, gc

    def request_extract_load_update(self, gc, sc, process_start_time, meta_data_dict):
                  
        for api in self.euroleague_apis:

            # HTTP REQUEST
            url = f"https://live.euroleague.net/api/{api}?gamecode={str(gc)}&seasoncode={sc}"
            response = requests.get(url)
            response_status = response.status_code

            try:
                # EXTRACT DATA
                response_dict = response.json()

                # GET USEFUL INFO FROM HEADER API
                if api == "Header":
                    Phase, Round, Date, meta_data_dict = self.get_info_from_header_api(response_dict, meta_data_dict, url, gc, sc, process_start_time)

                # SET THE FILENAME OF THE JSON FILE
                json_filename = "_".join([sc, Phase, Round, "{:02d}".format(gc), Date, api])

                # LOAD DATA
                with open(fr"euroleague_data/{sc}/success/{json_filename}.json", "w") as output_file:
                    json.dump(response_dict, output_file)

            except Exception as e:
                print(e, "--- URL:", url)
                # UPDATE THE FAILURE COUNTER OF META_DATA DICTIONARY
                meta_data_dict[sc]["number_of_failed_extractions"] += 1
                # UDPATE THE FAILURE FILE BY SAVING THE FAILED URL AND THE STATUS CODE OF THE RESPONSE)
                with open(fr"euroleague_data/{sc}/failure/{sc}_failed_extractions_{self.datetime_now}.txt", 'a') as failure_file:
                    failure_file.write(f"failed_url: {url}  ---  status_code: {str(response_status)}\n")
                    failure_file.close()
                    
            # UPDATE THE TIME AND GAME_CODE COUNTERS OF META_DATA DICTIONARY
            meta_data_dict[sc]["time_counter"] = f"{round((time() - process_start_time) / 60, 1)} minutes"
            meta_data_dict[sc]["game_code_counter"] = gc
                    
        return meta_data_dict
    
    def get_info_from_header_api(self, response_dict, meta_data_dict, url, gc, sc, process_start_time):
    
        # GET USEFUL INFO FROM HEADER API
        Round = "{:02d}".format(int(response_dict["Round"]))
        Date_split = response_dict["Date"].split("/")
        Date = "".join([Date_split[2], Date_split[1], Date_split[0]])
        Phase_split = response_dict["Phase"].lower().split(" ")
        if len(Phase_split) > 1:
            Phase = "".join([Phase_split[0].capitalize(), Phase_split[1].capitalize()])
        else:
            Phase = Phase_split[0].capitalize()

        # UPDATE THE GAME COUNTERS OF META_DATA DICTIONARY    
        meta_data_dict[sc]["total_number_of_games"] += 1
        if Phase.startswith("Reg"):
            meta_data_dict[sc]["number_of_RegularSeason_games"] += 1
        elif Phase.startswith("Top"):
            meta_data_dict[sc]["number_of_Top16_games"] += 1
        elif Phase.startswith("Play"):
            meta_data_dict[sc]["number_of_Playoff_games"] += 1
        elif Phase.startswith("Fin"):
            meta_data_dict[sc]["number_of_FinalFour_games"] += 1
        else:
            meta_data_dict[sc]["unknown_Phase"].append([Phase, url])

        # PRINT A MESSAGE FOR MONITORING PURPOSES
        if gc == 1:
            print("")
        print(f"SeasonCode: {sc}  ---  Phase: {Phase}  ---  Round: {Round}  ---  GameCode:", "{:02d}".format(gc), \
              " ---  TimeCounter:", round((time() - process_start_time) / 60, 1), "min")
            
        return Phase, Round, Date, meta_data_dict
      
