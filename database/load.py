import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import yaml
import psycopg2
import sys
#sys.path.insert(1, '../scrape')
sys.path.insert(1, '../transform')
#import match_stats_scraper
import transform_match

def read_one_block_of_yaml_data():
    with open('../config/db_config.yml','r') as f:
        return yaml.safe_load(f)

#match_stats_scraper.scraper()


def match_exisits(conn, event, match_date, team_one_id, team_two_id):
    query = f'SELECT event, match_date, team_one_id, team_two_id FROM match_statistics.match WHERE event = \'{event}\' AND match_date = \'{match_date}\' AND team_one_id = {team_one_id} AND team_two_id = {team_two_id};'

    match = pd.read_sql_query(query, conn)

    return not match.empty

def insert_match(conn, event, match_date, stage, round, match_winner_id, team_one_id, team_two_id, map_list):
    if len(map_list) == 2:
        match_query = f'INSERT INTO match_statistics.match("match_id", "event", "match_date", "stage", "round", "match_winner", "team_one_id", "team_two_id", "map_one_id", "map_two_id", "map_three_id", "map_four_id", "map_five_id")VALUES (DEFAULT, \'{event}\', \'{match_date}\', \'{stage}\', \'{round}\', \'{match_winner_id}\', \'{team_one_id}\', \'{team_two_id}\', \'{map_list[0]}\', \'{map_list[1]}\', NULL, NULL, NULL);'
    elif len(map_list) == 3:
        match_query = f'INSERT INTO match_statistics.match("match_id", "event", "match_date", "stage", "round", "match_winner", "team_one_id", "team_two_id", "map_one_id", "map_two_id", "map_three_id", "map_four_id", "map_five_id")VALUES (DEFAULT, \'{event}\', \'{match_date}\', \'{stage}\', \'{round}\', \'{match_winner_id}\', \'{team_one_id}\', \'{team_two_id}\', \'{map_list[0]}\', \'{map_list[1]}\', \'{map_list[2]}\', NULL, NULL);'
    elif len(map_list) == 4:
        match_query = f'INSERT INTO match_statistics.match("match_id", "event", "match_date", "stage", "round", "match_winner", "team_one_id", "team_two_id", "map_one_id", "map_two_id", "map_three_id", "map_four_id", "map_five_id")VALUES (DEFAULT, \'{event}\', \'{match_date}\', \'{stage}\', \'{round}\', \'{match_winner_id}\', \'{team_one_id}\', \'{team_two_id}\', \'{map_list[0]}\', \'{map_list[1]}\', \'{map_list[2]}\', \'{map_list[3]}\', NULL);'
    else:
        match_query = f'INSERT INTO match_statistics.match("match_id", "event", "match_date", "stage", "round", "match_winner", "team_one_id", "team_two_id", "map_one_id", "map_two_id", "map_three_id", "map_four_id", "map_five_id")VALUES (DEFAULT, \'{event}\', \'{match_date}\', \'{stage}\', \'{round}\', \'{match_winner_id}\', \'{team_one_id}\', \'{team_two_id}\', \'{map_list[0]}\', \'{map_list[1]}\', \'{map_list[2]}\', \'{map_list[3]}\', \'{map_list[4]}\');'
    cursor = conn.cursor()

    cursor.execute(match_query) 
  
    conn.commit()   

def get_match_id(conn, event, match_date, team_one_id, team_two_id):
    query = f'SELECT match_id FROM match_statistics.match WHERE event = \'{event}\' AND match_date = \'{match_date}\' AND team_one_id = {team_one_id} AND team_two_id = {team_two_id};'

    match_id = pd.read_sql_query(query, conn)

    return int(match_id.loc[0, 'match_id'])


def map_exists(conn, map_name):
    map_query = f'SELECT map_id, map_name FROM match_statistics.map WHERE map_name = \'{map_name}\';'

    map_df = pd.read_sql_query(map_query, conn)

    return not map_df.empty    

def get_map_id(conn, map_name):
    map_query = f'SELECT map_id FROM match_statistics.map WHERE map_name = \'{map_name}\';'

    map_query_df = pd.read_sql_query(map_query, conn)

    return int(map_query_df.loc[0, 'map_id'])

def insert_map(conn, map_name):
    insert_sql = f'INSERT INTO match_statistics.map(map_id, map_name) VALUES (DEFAULT, \'{map_name}\');'

    cursor = conn.cursor()

    cursor.execute(insert_sql) 
  
    conn.commit()


def team_exists(conn, team_name):
    team_query = f'SELECT "team_id" FROM match_statistics.team WHERE team_name = \'{team_name}\';'

    team_pd = pd.read_sql_query(team_query, conn)

    return not team_pd.empty

def get_team_id(conn, team_name):
    team_query = f'SELECT team_id FROM match_statistics.team WHERE team_name = \'{team_name}\';'

    team_one_id_df = pd.read_sql_query(team_query, conn)

    return int(team_one_id_df.loc[0, 'team_id'])

def insert_team(conn,team_name):
    insert_sql = f'INSERT INTO match_statistics.team(team_id, team_name) VALUES (DEFAULT, \'{team_name}\');'

    cursor = conn.cursor()

    cursor.execute(insert_sql) 
  
    conn.commit() 


def player_exists(conn, player_name):
    player_query = f'SELECT "player_id" FROM match_statistics.player WHERE player_name = \'{player_name}\';'

    player_pd = pd.read_sql_query(player_query, conn)

    return not player_pd.empty

def get_player_id(conn, player_name):
    player_query = f'SELECT player_id FROM match_statistics.player WHERE player_name = \'{player_name}\';'

    player_id_df = pd.read_sql_query(player_query, conn)

    return int(player_id_df.loc[0, 'player_id'])  

def insert_player(conn,player_name, team_id):
    insert_sql = f'INSERT INTO match_statistics.player(player_id, player_name, team_id) VALUES (DEFAULT, \'{player_name}\' ,\'{team_id}\');'

    cursor = conn.cursor()

    cursor.execute(insert_sql) 
  
    conn.commit() 


def insert_match_team(conn, match_team_df, match_id):
    for row in range(match_team_df.count()):

        team_id = get_team_id(conn, match_team_df.collect()[row]['team_name'])
        final_score = match_team_df.collect()[row]['final_score']
        defense_score = match_team_df.collect()[row]['defense_score']
        attack_score = match_team_df.collect()[row]['attack_score']
        overtime_score = match_team_df.collect()[row]['overtime_score']

        insert_match_team_sql = f'INSERT INTO match_statistics.match_team(match_team_id, match_id, team_id, final_score, defense_score, attack_score, overtime_score) VALUES (DEFAULT, \'{match_id}\',\'{team_id}\',\'{final_score}\', \'{defense_score}\', \'{attack_score}\', \'{overtime_score}\');'

        cursor = conn.cursor()

        cursor.execute(insert_match_team_sql) 
  
        conn.commit() 


def insert_match_player(conn, match_player_df, match_id):
    for row in range(match_player_df.count()):

        
        player_id = get_player_id(conn, match_player_df.collect()[row]['player_name'])

        kast_overall = match_player_df.collect()[row]["kast_overall"] if match_player_df.collect()[row]["kast_overall"] is not None else 'NULL'
        kast_attack = match_player_df.collect()[row]["kast_attack"] if match_player_df.collect()[row]["kast_attack"] is not None else 'NULL'
        kast_defense = match_player_df.collect()[row]["kast_defense"] if match_player_df.collect()[row]["kast_defense"] is not None else 'NULL'
        hs_perc_overall = match_player_df.collect()[row]["headshot_percentage_overall"] if match_player_df.collect()[row]["headshot_percentage_overall"] is not None else 'NULL'
        hs_perc_attack = match_player_df.collect()[row]["headshot_percentage_attack"] if match_player_df.collect()[row]["headshot_percentage_attack"] is not None else 'NULL'
        hs_perc_defense = match_player_df.collect()[row]["headshot_percentage_defense"] if match_player_df.collect()[row]["headshot_percentage_defense"] is not None else 'NULL'

        query = f'INSERT INTO match_statistics.match_player(match_player_id, match_id, player_id, acs_overall, acs_attack, acs_defense, kills_overall, kills_attack, kills_defense, deaths_overall, deaths_attack, deaths_defense, assists_overall, assists_attack, assists_defense, kast_overall, kast_attack, kast_defense, adr_overall, adr_attack, adr_defense, headshot_percentage_overall, headshot_percentage_attack, headshot_percentage_defense, first_kills_overall, first_kills_attack, first_kills_defense, first_deaths_overall, first_deaths_attack, first_deaths_defense)VALUES (DEFAULT, {match_id}, {player_id}, {match_player_df.collect()[row]["acs_overall"]}, {match_player_df.collect()[row]["acs_attack"]}, {match_player_df.collect()[row]["acs_defense"]}, {match_player_df.collect()[row]["kills_overall"]}, {match_player_df.collect()[row]["kills_attack"]}, {match_player_df.collect()[row]["kills_defense"]}, {match_player_df.collect()[row]["deaths_overall"]}, {match_player_df.collect()[row]["deaths_attack"]}, {match_player_df.collect()[row]["deaths_defense"]}, {match_player_df.collect()[row]["assists_overall"]}, {match_player_df.collect()[row]["assists_attack"]}, {match_player_df.collect()[row]["assists_defense"]}, {kast_overall}, {kast_attack}, {kast_defense}, {match_player_df.collect()[row]["adr_overall"]}, {match_player_df.collect()[row]["adr_attack"]}, {match_player_df.collect()[row]["adr_defense"]}, {hs_perc_overall}, {hs_perc_attack}, {hs_perc_defense}, {match_player_df.collect()[row]["first_kills_overall"]}, {match_player_df.collect()[row]["first_kills_attack"]}, {match_player_df.collect()[row]["first_kills_defense"]}, {match_player_df.collect()[row]["first_deaths_overall"]}, {match_player_df.collect()[row]["first_deaths_attack"]}, {match_player_df.collect()[row]["first_deaths_defense"]});'

        cursor = conn.cursor()

        cursor.execute(query) 
  
        conn.commit()


def insert_match_map_team(conn, match_map_team_df, match_id):
    for row in range(match_map_team_df.count()):

        team_id = get_team_id(conn, match_map_team_df.collect()[row]['team_name'])
        map_id = get_map_id(conn, match_map_team_df.collect()[row]['map_name'])
        final_score = match_map_team_df.collect()[row]['final_score']
        defense_score = match_map_team_df.collect()[row]['defense_score']
        attack_score = match_map_team_df.collect()[row]['attack_score']
        overtime_score = match_map_team_df.collect()[row]['overtime_score']
        win = match_map_team_df.collect()[row]['win']

        insert_match_team_sql = f'INSERT INTO match_statistics.match_map_team(match_map_team_id, match_id, map_id, team_id, win, final_score, defense_score, attack_score, overtime_score) VALUES (DEFAULT, \'{match_id}\',\'{map_id}\', \'{team_id}\', \'{win}\', \'{final_score}\', \'{defense_score}\', \'{attack_score}\', \'{overtime_score}\');'

        cursor = conn.cursor()

        cursor.execute(insert_match_team_sql) 
  
        conn.commit()


def insert_match_map_player(conn, match_map_player_df, match_id):
    for row in range(match_map_player_df.count()):

        
        player_id = get_player_id(conn, match_map_player_df.collect()[row]['player_name'])
        map_id = get_map_id(conn, match_map_player_df.collect()[row]['map_name'])

        kast_overall = match_map_player_df.collect()[row]["kast_overall"] if match_map_player_df.collect()[row]["kast_overall"] is not None else 'NULL'
        kast_attack = match_map_player_df.collect()[row]["kast_attack"] if match_map_player_df.collect()[row]["kast_attack"] is not None else 'NULL'
        kast_defense = match_map_player_df.collect()[row]["kast_defense"] if match_map_player_df.collect()[row]["kast_defense"] is not None else 'NULL'
        hs_perc_overall = match_map_player_df.collect()[row]["headshot_percentage_overall"] if match_map_player_df.collect()[row]["headshot_percentage_overall"] is not None else 'NULL'
        hs_perc_attack = match_map_player_df.collect()[row]["headshot_percentage_attack"] if match_map_player_df.collect()[row]["headshot_percentage_attack"] is not None else 'NULL'
        hs_perc_defense = match_map_player_df.collect()[row]["headshot_percentage_defense"] if match_map_player_df.collect()[row]["headshot_percentage_defense"] is not None else 'NULL'


        query = f'INSERT INTO match_statistics.match_map_player(match_map_player_id, match_id, map_id, player_id, acs_overall, acs_attack, acs_defense, kills_overall, kills_attack, kills_defense, deaths_overall, deaths_attack, deaths_defense, assists_overall, assists_attack, assists_defense, kast_overall, kast_attack, kast_defense, adr_overall, adr_attack, adr_defense, headshot_percentage_overall, headshot_percentage_attack, headshot_percentage_defense, first_kills_overall, first_kills_attack, first_kills_defense, first_deaths_overall, first_deaths_attack, first_deaths_defense)VALUES (DEFAULT, {match_id}, {map_id}, {player_id}, {match_map_player_df.collect()[row]["acs_overall"]}, {match_map_player_df.collect()[row]["acs_attack"]}, {match_map_player_df.collect()[row]["acs_defense"]}, {match_map_player_df.collect()[row]["kills_overall"]}, {match_map_player_df.collect()[row]["kills_attack"]}, {match_map_player_df.collect()[row]["kills_defense"]}, {match_map_player_df.collect()[row]["deaths_overall"]}, {match_map_player_df.collect()[row]["deaths_attack"]}, {match_map_player_df.collect()[row]["deaths_defense"]}, {match_map_player_df.collect()[row]["assists_overall"]}, {match_map_player_df.collect()[row]["assists_attack"]}, {match_map_player_df.collect()[row]["assists_defense"]}, {kast_overall}, {kast_attack}, {kast_defense}, {match_map_player_df.collect()[row]["adr_overall"]}, {match_map_player_df.collect()[row]["adr_attack"]}, {match_map_player_df.collect()[row]["adr_defense"]}, {hs_perc_overall}, {hs_perc_attack}, {hs_perc_defense}, {match_map_player_df.collect()[row]["first_kills_overall"]}, {match_map_player_df.collect()[row]["first_kills_attack"]}, {match_map_player_df.collect()[row]["first_kills_defense"]}, {match_map_player_df.collect()[row]["first_deaths_overall"]}, {match_map_player_df.collect()[row]["first_deaths_attack"]}, {match_map_player_df.collect()[row]["first_deaths_defense"]});'

        cursor = conn.cursor()

        cursor.execute(query) 
  
        conn.commit()


def main():

    match_data_dict = transform_match.transform_match_data()
 
    db_config = read_one_block_of_yaml_data()
    postgres_config = db_config['postgres_config']



    spark = SparkSession.builder \
        .appName("pyspark_test") \
        .config("spark.jars", './postgresql-42.7.5.jar') \
        .getOrCreate()

    conn = psycopg2.connect(
        host=db_config['db_host'],
        port=db_config['db_port'],
        dbname=db_config['db_name'],
        user=db_config['db_user'],
        password=db_config['db_password']
    )


    for i in range(len(match_data_dict['map'])):
        match = match_data_dict['match'][i]

        #Get team_ids
        team_name_list = [match.collect()[0]['team_one_name'], match.collect()[0]['team_two_name']]
        team_id_list = []
        for team in team_name_list:
            if not team_exists(conn, team):
                print(f'Inserting team: {team}')
                insert_team(conn, team)
                team_id_list.append(get_team_id(conn, team))
            else:
                team_id_list.append(get_team_id(conn, team))


        match_date = match.collect()[0]['match_date']
        event = match.collect()[0]['event']
        stage = match.collect()[0]['stage']
        event_round = match.collect()[0]['round']
        match_winner_id = get_team_id(conn,match.collect()[0]['winner'])
        team_one_id = team_id_list[0]
        team_two_id = team_id_list[1]
        map_id_list = []


        
        for map in range(5):

            if map == 0:
                current_map = match.collect()[0]['map_one']
            elif map == 1:
                current_map = match.collect()[0]['map_two']
            elif map == 2:
                current_map = match.collect()[0]['map_three']
            elif map == 3:
                current_map = match.collect()[0]['map_four']
            else:
                current_map = match.collect()[0]['map_five']

            if current_map == None:
                continue
            
            if not map_exists(conn, current_map):
                print(f'Inserting Map: {current_map}')
                insert_map(conn, current_map)
                map_id_list.append(get_map_id(conn, current_map))
            else:
                map_id_list.append(get_map_id(conn, current_map))

        if not match_exisits(conn, event, match_date, team_one_id, team_two_id):
            print(f'Inserting Match: {team_name_list[0]} vs {team_name_list[1]}')
            insert_match(conn, event, match_date, stage, event_round, match_winner_id, team_one_id, team_two_id, map_id_list)

            match_id = get_match_id(conn, event, match_date, team_one_id, team_two_id)

            #Get player data

            player_df = match_data_dict['player'][i]

            for player_index in range(player_df.count()):
                player_name = player_df.collect()[player_index]['player_name']
                team_name = player_df.collect()[player_index]['team_name']
                team_id = get_team_id(conn, team_name)

                if not player_exists(conn, player_name):
                    print(f'Inserting Player: {player_name}')
                    insert_player(conn, player_name, team_id)

            #Get Match Team
            insert_match_team(conn, match_data_dict['match_team'][i], match_id)

            #Get Match Player
            insert_match_player(conn, match_data_dict['match_player'][i], match_id)

            #Get Match Map Team #Ongoing
            insert_match_map_team(conn, match_data_dict['match_map_team'][i], match_id)

            #Get Match Map Player
            insert_match_map_player(conn, match_data_dict['match_map_player'][i], match_id)

        else:
            print(f'Match: {team_name_list[0]} vs {team_name_list[1]} already exists. Going to next match.')
            print('---------------------------------------------------------------------------------------')



    conn.close()

main()










