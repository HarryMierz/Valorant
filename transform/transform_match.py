from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import json
import pandas as pd
import glob
import os

spark = spark = SparkSession.builder \
    .appName("pyspark_test") \
    .getOrCreate()

def delete_files(files_list):
    for file in files_list:
        os.remove(file)

def get_json_file_name_list():
    files = glob.glob('../data/*.json')
    return files

# Function to get json file 
def load_file(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)
    
def get_match_info(data):
    match_name = list(data.keys())[0]

    team1, team2 = match_name.split('_vs_')
    team1_name = team1.replace('_', ' ')
    team2_name = team2.replace('_', ' ')


    event_stats = data[match_name][0]
    event_stats

    return match_name, team1_name, team2_name

#Function to get list of maps in match | takes data from load_file() and match_name get_match_event_stats()
def get_map_list(data, match_name):
    map_list = list(data[match_name][1].keys())
    return map_list

# Function that gets team stats for each map in the match
def get_team_map_data(match_name, map_list, data, team1_name, team2_name):

    map_team_stats_df = pd.DataFrame(columns = ['map', 'team_name', 'win' ,'final_score', 'defense_score' ,'attack_score', 'overtime_score'])

    for i in map_list:

        team1_stats = data[match_name][1][i]['Team Stats: '][team1_name]
        team2_stats = data[match_name][1][i]['Team Stats: '][team2_name]

        team1_score = int(team1_stats['final'])
        team2_score = int(team2_stats['final'])

        winner = team1_name if int(team1_score) > int(team2_score) else team2_name

        if winner == team1_name:
            team1_win = True
            team2_win = False
        else:
            team1_win = False
            team2_win = True


        team1_dict = {'map' : [i], 'team_name' : [team1_name], 'win' : [team1_win], 'final_score' : [team1_stats['final']], 'defense_score' : [team1_stats['CT']], 'attack_score' : [team1_stats['T']], 'overtime_score' : [team1_stats['OT']]}
        team2_dict = {'map' : [i], 'team_name' : [team2_name], 'win' : [team2_win], 'final_score' : [team2_stats['final']], 'defense_score' : [team2_stats['CT']], 'attack_score' : [team2_stats['T']], 'overtime_score' : [team2_stats['OT']]}

        team1_df = pd.DataFrame(team1_dict)
        team2_df = pd.DataFrame(team2_dict)


        map_team_stats_df = pd.concat([map_team_stats_df, team1_df, team2_df], ignore_index=True)


    return map_team_stats_df

def get_team_abbreviation(data, match_name, map_list):

    match_name = list(data.keys())[0]

    team1, team2 = match_name.split('_vs_')
    team1_name = team1.replace('_', ' ')
    team2_name = team2.replace('_', ' ')

    map_name = map_list[0]

    first_player = list(data[match_name][1][map_name]['Player Stats'].keys())[0]
    last_player = list(data[match_name][1][map_name]['Player Stats'].keys())[-1]

    team1_abbr = data[match_name][1][map_name]['Player Stats'][first_player]["Team"]
    
    team2_abbr = data[match_name][1][map_name]['Player Stats'][last_player]["Team"]

    team_abbr_dict = {team1_abbr : team1_name, team2_abbr : team2_name}

    return team_abbr_dict

def get_winner(match_name, map_list, data, team1_name, team2_name):

    map_win_list = []

    for i in map_list:

        team1_stats = data[match_name][1][i]['Team Stats: '][team1_name]
        team2_stats = data[match_name][1][i]['Team Stats: '][team2_name]

        team1_score = int(team1_stats['final'])
        team2_score = int(team2_stats['final'])
    

        map_win_list.append(team1_name if int(team1_score) > int(team2_score) else team2_name)

    return max(set(map_win_list), key=map_win_list.count)

# Function that gets event and date stats for a match
def get_match_event_stats(team1_name, team2_name, winner, data, map_list):

    event_stats_df_pd = pd.DataFrame(data[list(data.keys())[0]][0], index=[0])

    event_stats_df_pd = event_stats_df_pd.rename(columns={'Event' : 'event', 'Date' : 'match_date', 'Stage' : 'stage', 'Round' : 'round'})

    event_stats_df_pd['winner'] = winner
    event_stats_df_pd['team_one_name'] = team1_name
    event_stats_df_pd['team_two_name'] = team2_name
    try:
        event_stats_df_pd['map_one'] = map_list[0]
    except IndexError:
         event_stats_df_pd['map_one'] = None
    try:
        event_stats_df_pd['map_two'] = map_list[1] 
    except IndexError:
        event_stats_df_pd['map_two'] = None
    try: 
        event_stats_df_pd['map_three'] = map_list[2] 
    except IndexError:
        event_stats_df_pd['map_three'] = None
    try: 
        event_stats_df_pd['map_four'] = map_list[3] 
    except IndexError:
        event_stats_df_pd['map_four'] = None
    try:
        event_stats_df_pd['map_five'] = map_list[4]
    except IndexError:
        event_stats_df_pd['map_five'] = None

        schema = StructType([
        StructField("event", StringType(), True),
        StructField("match_date", StringType(), True),
        StructField("stage", StringType(), True),
        StructField("round", StringType(), True),
        StructField("winner", StringType(), True),
        StructField("team_one_name", StringType(), True),
        StructField("team_two_name", StringType(), True),
        StructField("map_one", StringType(), True),
        StructField("map_two", StringType(), True),
        StructField("map_three", StringType(), True),
        StructField("map_four", StringType(), True),
        StructField("map_five", StringType(), True),
    ])
        
    event_stats_df_spark = spark.createDataFrame(event_stats_df_pd, schema=schema)

    return event_stats_df_spark

# Function that gets player stats for each map in the match
def get_map_player_data(map_list, team_name_abbr_dict, data, match_name):

    map_player_stats_df_pd = pd.DataFrame(columns=['map', 'player_name', 'agent', 'team_name', 'acs_overall','acs_attack', 'acs_defense', 'kills_overall', 'kills_attack', 'kills_defense',  'deaths_overall', 'deaths_attack', 'deaths_defense', 'assists_overall', 'assists_attack', 'assists_defense', 'kast_overall', 'kast_attack', 'kast_defense', 'adr_overall', 'adr_attack', 'adr_defense', 'headshot_percentage_overall', 'headshot_percentage_attack', 'headshot_percentage_defense', 'first_kills_overall', 'first_kills_attack', 'first_kills_defense', 'first_deaths_overall', 'first_deaths_attack', 'first_deaths_defense'])

    for i in map_list:
        map_name = i

        map_player_stats = data[match_name][1][i]['Player Stats']
    
        for player in map_player_stats:
            player_name = player
            player_stats = map_player_stats[player]

            temp_df3 = pd.DataFrame({'map' : map_name,'player_name': player_name, 'agent': player_stats['Agent'], 'team_name' : team_name_abbr_dict[player_stats['Team']], 'acs_overall': player_stats['ACS']['All'], 'acs_attack':player_stats['ACS']['T'], 'acs_defense': player_stats['ACS']['CT'], 'kills_overall' : player_stats['Elims']['All'], 'kills_attack': player_stats['Elims']['T'], 'kills_defense': player_stats['Elims']['CT'], 'deaths_overall' : player_stats['Deaths']['All'], 'deaths_attack': player_stats['Deaths']['T'], 'deaths_defense': player_stats['Deaths']['CT'], 'assists_overall' : player_stats['Assists']['All'], 'assists_attack': player_stats['Assists']['T'], 'assists_defense': player_stats['Assists']['CT'], 'kast_overall' :player_stats['KAST']['All'], 'kast_attack': player_stats['KAST']['T'], 'kast_defense': player_stats['KAST']['CT'], 'adr_overall' : player_stats['ADR']['All'], 'adr_attack': player_stats['ADR']['T'], 'adr_defense': player_stats['ADR']['CT'], 'headshot_percentage_overall' : player_stats['HS_percentage']['All'], 'headshot_percentage_attack': player_stats['HS_percentage']['T'], 'headshot_percentage_defense': player_stats['HS_percentage']['CT'], 'first_kills_overall' : player_stats['First Kills']['All'], 'first_kills_attack': player_stats['First Kills']['T'], 'first_kills_defense': player_stats['First Kills']['CT'], 'first_deaths_overall' : player_stats['First Deaths']['All'], 'first_deaths_attack': player_stats['First Deaths']['T'], 'first_deaths_defense': player_stats['First Deaths']['CT']}, index=[0])

            map_player_stats_df_pd = pd.concat([map_player_stats_df_pd, temp_df3], ignore_index=True)

    return map_player_stats_df_pd

# Function that gets team stats for each map in the match
def get_agg_team_data(match_name, map_list, data, team1_name, team2_name):

    map_team_df_pd = get_team_map_data(match_name, map_list, data, team1_name, team2_name)

    map_team_df_pd = map_team_df_pd[['team_name', 'final_score', 'defense_score', 'attack_score', 'overtime_score']]

    map_team_df_pd = map_team_df_pd.astype({'final_score' : int, 'defense_score' : int, 'attack_score' : int, 'overtime_score' : int})

    match_team_sums = map_team_df_pd.groupby('team_name').sum()

    match_team_sums.reset_index(inplace=True)

    return match_team_sums

# Function to get aggregate player data for the entire match over all maps
def get_agg_player_data(map_list, team_name_abbr_dict, data, match_name):
    map_player_stats_df_pd = get_map_player_data(map_list, team_name_abbr_dict, data, match_name)

    map_player_stats_df_pd_trim = map_player_stats_df_pd.copy()

    map_player_stats_df_pd_trim['kast_overall'] = map_player_stats_df_pd_trim['kast_overall'].str.replace('%', '').astype(float)
    map_player_stats_df_pd_trim['kast_defense'] = map_player_stats_df_pd_trim['kast_defense'].str.replace('%', '').astype(float)
    map_player_stats_df_pd_trim['kast_attack'] = map_player_stats_df_pd_trim['kast_attack'].str.replace('%', '').astype(float)
    map_player_stats_df_pd_trim['headshot_percentage_overall'] = map_player_stats_df_pd_trim['headshot_percentage_overall'].str.replace('%', '').astype(float)
    map_player_stats_df_pd_trim['headshot_percentage_defense'] = map_player_stats_df_pd_trim['headshot_percentage_defense'].str.replace('%', '').astype(float)
    try:
        map_player_stats_df_pd_trim['headshot_percentage_attack'] = map_player_stats_df_pd_trim['headshot_percentage_attack'].str.replace('%', '').astype(float)
    except:
        map_player_stats_df_pd_trim['headshot_percentage_attack'] = None
    map_player_stats_df_pd_trim = map_player_stats_df_pd_trim.astype({'acs_overall': 'int', 'acs_attack': 'int', 'acs_defense':  'int', 'kills_overall' : 'int', 'kills_attack': 'int', 'kills_defense': 'int', 'deaths_overall' : 'int', 'deaths_attack': 'int', 'deaths_defense': 'int', 'assists_overall' : 'int', 'assists_attack': 'int', 'assists_defense': 'int', 'kast_overall' : 'float', 'kast_attack':  'float', 'kast_defense':  'float', 'adr_overall' :  'int', 'adr_attack':  'int', 'adr_defense':  'int', 'headshot_percentage_overall' :  'float', 'headshot_percentage_attack':  'float', 'headshot_percentage_defense':  'float', 'first_kills_overall' : 'int', 'first_kills_attack': 'int', 'first_kills_defense': 'int', 'first_deaths_overall' : 'int', 'first_deaths_attack': 'int', 'first_deaths_defense': 'int'})

    agg_player_stats_df_pd = map_player_stats_df_pd_trim.groupby('player_name').agg({'acs_overall': 'mean', 'acs_attack': 'mean', 'acs_defense':  'mean', 'kills_overall' : 'sum', 'kills_attack': 'sum', 'kills_defense': 'sum', 'deaths_overall' : 'sum', 'deaths_attack': 'sum', 'deaths_defense': 'sum', 'assists_overall' : 'sum', 'assists_attack': 'sum', 'assists_defense': 'sum', 'kast_overall' : 'mean', 'kast_attack':  'mean', 'kast_defense':  'mean', 'adr_overall' :  'mean', 'adr_attack':  'mean', 'adr_defense':  'mean', 'headshot_percentage_overall' :  'mean', 'headshot_percentage_attack':  'mean', 'headshot_percentage_defense':  'mean', 'first_kills_overall' : 'sum', 'first_kills_attack': 'sum', 'first_kills_defense': 'sum', 'first_deaths_overall' : 'sum', 'first_deaths_attack': 'sum', 'first_deaths_defense': 'sum'}).round(2)

    agg_player_stats_df_pd.reset_index(inplace=True)

    return agg_player_stats_df_pd

#Function to get team info data
def get_team_info(match_name, map_list,data,team1_name, team2_name):

    team1_abbr, team2_abbr = get_team_abbreviation(data, match_name, map_list)

    team_info_dict = {
        'TeamName' : [team1_name, team2_name],
        'TeamAbbreviation' : [team1_abbr, team2_abbr]
    }

    team_info_df = pd.DataFrame(team_info_dict)



    return team_info_df

# Function to get player info data
def get_player_info(map_list, team_name_abbr_dict, data, match_name):
    map_player_stats_df_pd = get_map_player_data(map_list, team_name_abbr_dict, data, match_name)

    player_name_list = map_player_stats_df_pd['player_name'].unique()

    team1_name = map_player_stats_df_pd['team_name'].unique()[0]
    team2_name = map_player_stats_df_pd['team_name'].unique()[1]



    player_info_df = pd.DataFrame({'player_name' : player_name_list, 'team_name' : [i for i in range(10)]})

    player_info_df = player_info_df.astype({'team_name' : 'object'})

    player_info_df.loc[0:4, 'team_name'] = team1_name
    player_info_df.loc[5:9, 'team_name'] = team2_name

    return player_info_df

# Function to get map info data
def get_map_info(map_list):

    map_df = pd.DataFrame({'MapName' : map_list})

    return map_df


# Function to get map info data
def get_map_info(map_list):

    map_df = pd.DataFrame({'MapName' : map_list})

    return map_df

def transform_match_data():

    files_list = get_json_file_name_list()

    map_team_stats_df_list = []
    match_event_info_df_list = []
    map_player_stats_df_list = []
    team_match_agg_stats_df_list = []
    agg_player_stats_df_list = []
    team_info_df_list = []
    player_info_df_list = []
    map_df_list = []

    for file in files_list:
        data = load_file(file)

        match_name, team1_name, team2_name = get_match_info(data)

        map_list = list(data[match_name][1].keys())

        map_team_stats_df_list.append(spark.createDataFrame(get_team_map_data(match_name, map_list, data, team1_name, team2_name)))

        team_name_abbr_dict = get_team_abbreviation(data, match_name, map_list)

        winner = get_winner(match_name, map_list, data, team1_name, team2_name)

        match_event_info_df_list.append(get_match_event_stats(team1_name, team2_name, winner, data, map_list))

        map_player_stats_df_list.append(spark.createDataFrame(get_map_player_data(map_list, team_name_abbr_dict, data, match_name)))

        team_match_agg_stats_df_list.append(spark.createDataFrame(get_agg_team_data(match_name, map_list, data, team1_name, team2_name)))

        agg_player_stats_df_list.append(spark.createDataFrame(get_agg_player_data(map_list, team_name_abbr_dict, data, match_name)))

        team_info_df_list.append(spark.createDataFrame(get_team_info(match_name, map_list, data, team1_name, team2_name)))

        player_info_df_list.append(spark.createDataFrame(get_player_info(map_list, team_name_abbr_dict, data, match_name)))

        map_df_list.append(spark.createDataFrame(get_map_info(map_list)))

    match_data_dict = {
        'map' : map_df_list, #map
        'team' : team_info_df_list, #team
        'player' : player_info_df_list, #player
        'match' : match_event_info_df_list, #match
        'match_team' : team_match_agg_stats_df_list, #match_team_stats
        'match_player' : agg_player_stats_df_list, #match_player_stats
        'match_map_team' : map_team_stats_df_list, #match_map_team_stats
        'match_map_player' : map_player_stats_df_list #match_map_player_stats
    }

    #delete_files(files_list)

    return match_data_dict

if __name__ == 'main':
    transform_match_data()
     

#match_data_dict = transform_match_data()
#print('map')
#match_data_dict['map'][0].show(2)
#print('team')
#match_data_dict['team'][0].show(2)
#print('player')
#match_data_dict['player'][0].show(2)
#print('match')
#match_data_dict['match'][0].show(2)
#print('match_team')
#match_data_dict['match_team'][0].show(2)
#print('match_player')
#match_data_dict['match_player'][0].show(1)
#print('match_map_team')
#match_data_dict['match_map_team'][0].show(2)
#print('match_map_player')
#match_data_dict['match_map_player'][0].show(1)