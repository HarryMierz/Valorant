from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import yaml
import psycopg2
import sys
sys.path.insert(1, './scrape')
sys.path.insert(1, './transform')
sys.path.insert(1,'./database')
import match_stats_scraper
import transform_match
import load
import time
import requests
from bs4 import BeautifulSoup
import warnings
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")
import pandas as pd

#url='https://www.vlr.gg/matches/results'
#matches_and_dates_dict = match_stats_scraper.get_matches_and_dates(url)

#print(matches_and_dates_dict)

def get_team_names(url, date):

    try:
        response = requests.get(url, timeout=5)
    except:
        print("The request timed out!")
        return None
    
    soup = BeautifulSoup(response.content, 'html.parser')

    team1_name = soup.find(class_="wf-title-med").text.strip().split("\n")[0]
    team2_name_list = soup.find(class_="match-header-link-name mod-2").text.strip().split("\n")
    team2_name_index = team2_name_list[0].rfind("\t")
    team2_name = team2_name_list[0][:team2_name_index].strip()

    event_stat_list = soup.find('a', class_='match-header-event').text.strip().split("\n")

    event_index = event_stat_list[0].find("\t")
    event = event_stat_list[0][:event_index]


    return [event, date, team1_name, team2_name]




        

def main():
    begin = time.time()
    
    url_list = ['https://www.vlr.gg/matches/results','https://www.vlr.gg/matches/results/?page=2','https://www.vlr.gg/matches/results/?page=3']
    #url_list = ['https://www.vlr.gg/matches/results/?page=2']

    with open('./config/db_config.yml','r') as f:
        db_config = yaml.safe_load(f)

    #conn = psycopg2.connect(
        #host=db_config['db_host'],
        #port=db_config['db_port'],
        #dbname=db_config['db_name'],
        #user=db_config['db_user'],
        #password=db_config['db_password']
    #)
    engine = create_engine(f'postgresql+psycopg2://{db_config["db_user"]}:{db_config["db_password"]}@{db_config["db_host"]}:{db_config["db_port"]}/{db_config["db_name"]}')
    with engine.connect() as conn:

        json_list = []
        #counter = 0

        for url in url_list:

            matches_and_dates = match_stats_scraper.get_matches_and_dates(url)

            print(f'Total Matches: {len(matches_and_dates.keys())}')

            if matches_and_dates is None:
                continue
            else:

                for key, value in matches_and_dates.items():
                    #counter += 1
                    #if counter >= 5:
                        #break
                    for match in value:
                        #counter += 1
                        #if counter >= 5:
                            #break
                        match_info = get_team_names(match, key)

                        if not load.match_exists(conn, match_info[0], match_info[1], match_info[2], match_info[3]):
                            print(f'Match {match_info[2]} vs {match_info[3]} on {match_info[1]} loading...')
                            print('-----------------------------------------------------------------------------------------------')
                            match_stats , match_name = match_stats_scraper.get_match_stats(match, key)

                            if match_stats is not None:
                                json_list.append(match_stats)
                        else:
                            print(f'Match {match_info[2]} vs {match_info[3]} on {match_info[1]} already exists. Going to next match')
                            print('-----------------------------------------------------------------------------------------------')
                        time.sleep(2)
            


        load.load_data(json_list)
        conn.close()
        engine.dispose()
        end = time.time() 

        # total time taken 
        print(f"Total runtime of the program is {end - begin}")


main()