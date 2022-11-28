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
        # implement process per season 
        for index, sc in enumerate(self.season_codes):

            # initialize basic variable of process
            process_start_time, season_end, meta_data_dict, gc = self.initialize_process(index, sc)

            while not season_end:

                # update the game_code counter
                gc += 1 

                # scrap APIs, load data and update info
                meta_data_dict = self.request_extract_load_update(gc, sc, process_start_time, meta_data_dict)

                # save meta_data
                with open(fr"euroleague_data/{sc}/{sc}_meta_data_{self.datetime_now}.json", "w") as output_file:
                    json.dump(meta_data_dict, output_file)

                # update flag --- the 1st condition is obvious --- the 2nd condition reflects a possible problem with the URLs or a premature end of the season (e.g. covid season E2019)
                if (meta_data_dict[sc]["number_of_FinalFour_games"] == 4) or (meta_data_dict[sc]["number_of_failed_extractions"] > self.failed_extractions_limit):
                    season_end = True
        
    def initialize_process(self, index, sc):
        
        # initialize timer and flag 
        process_start_time = time()
        season_end = False

        # only the first season_code can start with game_code > 1 
        if index > 0:
            self.game_code_start = 1

        # initialize the meta_data dictionary
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

        # create directories
        if not os.path.exists(fr"euroleague_data/{sc}"):
            os.makedirs(fr"euroleague_data/{sc}/success")
            os.makedirs(fr"euroleague_data/{sc}/failure")

        # initialize the game_code counter
        gc = self.game_code_start - 1
        
        return process_start_time, season_end, meta_data_dict, gc

    def request_extract_load_update(self, gc, sc, process_start_time, meta_data_dict):
                  
        for api in self.euroleague_apis:

            # HTTP request
            url = f"https://live.euroleague.net/api/{api}?gamecode={str(gc)}&seasoncode={sc}"
            response = requests.get(url)
            response_status = response.status_code

            try:
                # extract data
                response_dict = response.json()

                # get useful info from Header API
                if api == "Header":
                    Phase, Round, Date, meta_data_dict = self.get_info_from_header_api(response_dict, meta_data_dict, url, gc, sc, process_start_time)

                # set the filename of the json file
                json_filename = "_".join([sc, Phase, Round, "{:02d}".format(gc), Date, api])

                # load data
                with open(fr"euroleague_data/{sc}/success/{json_filename}.json", "w") as output_file:
                    json.dump(response_dict, output_file)

            except Exception as e:
                print(e, "--- URL:", url)
                # udpate the failure counter of the meta_data dictionary
                meta_data_dict[sc]["number_of_failed_extractions"] += 1
                # update the failure file by saving the failed URL and the status_code of the responce
                with open(fr"euroleague_data/{sc}/failure/{sc}_failed_extractions_{self.datetime_now}.txt", 'a') as failure_file:
                    failure_file.write(f"failed_url: {url}  ---  status_code: {str(response_status)}\n")
                    failure_file.close()
                    
            # update the time and game_code counters of the meta_data dictionary
            meta_data_dict[sc]["time_counter"] = f"{round((time() - process_start_time) / 60, 1)} minutes"
            meta_data_dict[sc]["game_code_counter"] = gc
                    
        return meta_data_dict
    
    def get_info_from_header_api(self, response_dict, meta_data_dict, url, gc, sc, process_start_time):
    
        # get useful info from Header API
        Round = "{:02d}".format(int(response_dict["Round"]))
        Date_split = response_dict["Date"].split("/")
        Date = "".join([Date_split[2], Date_split[1], Date_split[0]])
        Phase_split = response_dict["Phase"].lower().split(" ")
        if len(Phase_split) > 1:
            Phase = "".join([Phase_split[0].capitalize(), Phase_split[1].capitalize()])
        else:
            Phase = Phase_split[0].capitalize()

        # update the game counters of the meta_data dictionary    
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

        # print a message for monitoring purposes
        if gc == 1:
            print("")
        print(f"SeasonCode: {sc}  ---  Phase: {Phase}  ---  Round: {Round}  ---  GameCode:", "{:02d}".format(gc), \
              " ---  TimeCounter:", round((time() - process_start_time) / 60, 1), "min")
            
        return Phase, Round, Date, meta_data_dict
      
