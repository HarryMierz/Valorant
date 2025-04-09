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

warnings.filterwarnings("ignore")
import pandas as pd

#url='https://www.vlr.gg/matches/results'
#matches_and_dates_dict = match_stats_scraper.get_matches_and_dates(url)

#print(matches_and_dates_dict)

def get_team_names(url, date, conn):

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

    if load.team_exists(conn, team1_name):
        team1_id = load.get_team_id(conn, team1_name)
    else:
        load.insert_team(conn,team1_name)
        team1_id = load.get_team_id(conn, team1_name)

    if load.team_exists(conn, team2_name):
        team2_id = load.get_team_id(conn, team2_name)
    else:
        load.insert_team(conn,team2_name)
        team2_id = load.get_team_id(conn, team2_name)  

    return [team1_id, team2_id, event, date, team1_name, team2_name]




        

def main():
    begin = time.time()
    
    url_list = ['https://www.vlr.gg/matches/results']

    with open('./config/db_config.yml','r') as f:
        db_config = yaml.safe_load(f)

    conn = psycopg2.connect(
        host=db_config['db_host'],
        port=db_config['db_port'],
        dbname=db_config['db_name'],
        user=db_config['db_user'],
        password=db_config['db_password']
    )

    json_list = []

    for url in url_list:

        matches_and_dates = match_stats_scraper.get_matches_and_dates(url)

        print(f'Total Matches: {len(matches_and_dates.keys())}')

        if matches_and_dates is None:
            continue
        else:

            for key, value in matches_and_dates.items():

                for match in value:
                    match_info = get_team_names(match, key, conn)

                    if not load.match_exists(conn, match_info[2], match_info[3], match_info[0], match_info[1]):
                        print(f'Match {match_info[4]} vs {match_info[5]} on {match_info[3]} loading...')
                        print('-----------------------------------------------------------------------------------------------')
                        match_stats , match_name = match_stats_scraper.get_match_stats(match, key)

                        if match_stats is not None:
                            json_list.append(match_stats)
                    else:
                        print(f'Match {match_info[4]} vs {match_info[5]} on {match_info[3]} already exists. Going to next match')
                        print('-----------------------------------------------------------------------------------------------')
                    time.sleep(2)
            


    load.load_data(json_list)
    end = time.time() 
 
    # total time taken 
    print(f"Total runtime of the program is {end - begin}")


main()