import requests
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.safari.options import Options
import json


def get_matches_url():

    url = 'https://www.vlr.gg/matches/results'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    matches_url = []

    for match in soup.find_all('div', class_='wf-card'):
        for a in match.find_all('a', href=True):
            matches_url.append('https://www.vlr.gg' + a['href'])

    print('Obtained matches urls')
    print('Total Matches: ', len(matches_url[2:]))


    return matches_url[2:]

def get_stats(cols):
    pass

def get_player_stats1(table, player_stats={}):

    #for i in tables:
        rows = table.find_elements(By.TAG_NAME, 'tr')
        for row in rows:
            agent_span = row.find_element(By.TAG_NAME, 'span')
            agent_name = agent_span.find_elements(By.TAG_NAME, 'img')
            if len(agent_name) == 0:
                agent_name = 'Unknown'
            else:
                agent_name = agent_name[0].get_attribute('title')

            cols = row.find_elements(By.TAG_NAME, 'td')
            cols = [ele.text.strip() for ele in cols]

            if len(cols) == 0:
                continue
        
            name_space_index = cols[0].find(" ")
            player_name = cols[0][:name_space_index]

            team_name_index = cols[0].rfind("\t")
            team_name = cols[0][team_name_index + 1:]

            acs = cols[3].split("\n")
            acs_all = acs[0]
            acs_t_index = acs[1].rfind("\t")
            acs_t = acs[1][acs_t_index + 1:]
            acs_ct_index = acs[2].rfind("\t")
            acs_ct = acs[2][acs_ct_index + 1:]

            elims = cols[4].split("\n")
            elims_all = elims[0]
            elims_t_index = elims[1].rfind("\t")
            elims_t = elims[1][elims_t_index + 1:]
            elims_ct_index = elims[2].rfind("\t")
            elims_ct = elims[2][elims_ct_index + 1:]

            deaths = cols[5].split("\n")
            deaths_all = deaths[0]
            deaths_t_index = deaths[1].rfind("\t")
            deaths_t = deaths[1][deaths_t_index + 1:]
            deaths_ct_index = deaths[2].rfind("\t")
            deaths_ct = deaths[2][deaths_ct_index + 1:]

            assists = cols[6].split("\n")
            assists_all = assists[0]
            assists_t_index = assists[1].rfind("\t")
            assists_t = assists[1][assists_t_index + 1:]
            assists_ct_index = assists[2].rfind("\t")
            assists_ct = assists[2][assists_ct_index + 1:]

            kast = cols[8].split("\n")
            kast_all = kast[0]
            kast_t_index = kast[1].rfind("\t")
            kast_t = kast[1][kast_t_index + 1:]
            kast_ct_index = kast[2].rfind("\t")
            kast_ct = kast[2][kast_ct_index + 1:]

            adr = cols[9].split("\n")
            adr_all = adr[0]
            adr_t_index = adr[1].rfind("\t")
            adr_t = adr[1][adr_t_index + 1:]
            adr_ct_index = adr[2].rfind("\t")
            adr_ct = adr[2][adr_ct_index + 1:]

            hs_per = cols[10].split("\n")
            hs_per_all = hs_per[0]
            hs_per_t_index = hs_per[1].rfind("\t")
            hs_per_t = hs_per[1][hs_per_t_index + 1:]
            hs_per_ct_index = hs_per[2].rfind("\t")
            hs_per_ct = hs_per[2][hs_per_ct_index + 1:]

            first_kills = cols[11].split("\n")
            first_kills_all = first_kills[0]
            first_kills_t_index = first_kills[1].rfind("\t")
            first_kills_t = first_kills[1][first_kills_t_index + 1:]
            first_kills_ct_index = first_kills[2].rfind("\t")
            first_kills_ct = first_kills[2][first_kills_ct_index + 1:]

            first_deaths = cols[12].split("\n")
            first_deaths_all = first_deaths[0]
            first_deaths_t_index = first_deaths[1].rfind("\t")
            first_deaths_t = first_deaths[1][first_deaths_t_index + 1:]
            first_deaths_ct_index = first_deaths[2].rfind("\t")
            first_deaths_ct = first_deaths[2][first_deaths_ct_index + 1:]

            player_stats[player_name] = {'Team' : team_name,
                                        'Agent': agent_name,
                                        'ACS': {'All': acs_all, 'T': acs_t, 'CT': acs_ct}, 
                                        'Elims': {'All': elims_all, 'T': elims_t, 'CT': elims_ct}, 
                                        'Deaths': {'All': deaths_all, 'T': deaths_t, 'CT': deaths_ct}, 
                                        'Assists': {'All': assists_all, 'T': assists_t, 'CT': assists_ct}, 
                                        'KAST': {'All': kast_all, 'T': kast_t, 'CT': kast_ct}, 
                                        'ADR': {'All': adr_all, 'T': adr_t, 'CT': adr_ct}, 
                                        'HS_percentage': {'All': hs_per_all, 'T': hs_per_t, 'CT': hs_per_ct}, 
                                        'First Kills': {'All': first_kills_all, 'T': first_kills_t, 'CT': first_kills_ct}, 
                                        'First Deaths': {'All': first_deaths_all, 'T': first_deaths_t, 'CT': first_deaths_ct}}
            
        return player_stats





def get_match_stats(match_url):

    match_stats = {}

    match_stats_final = {}

    event_stats = {}
    
    time.sleep(1)
    response = requests.get(match_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    event_stat_list = soup.find('a', class_='match-header-event').text.strip().split("\n")

    event_index = event_stat_list[0].find("\t")
    event = event_stat_list[0][:event_index]

    stage_index = event_stat_list[2].rfind("\t")
    stage = event_stat_list[2][stage_index:][1:-2]

    round_index = event_stat_list[3].rfind("\t")
    round = event_stat_list[3][round_index:][1:]

    event_stats['Event'] = event
    event_stats['Stage'] = stage
    event_stats['Round'] = round

    maps = []

    map_list = soup.find_all('div', class_='vm-stats-gamesnav-item js-map-switch')

    for map in map_list:

        team_stats = {}

        game_id = map['data-game-id']

        map_url = 'https://www.vlr.gg' + map['data-href'][:-6] + f'?game={game_id}&tab=overview'

        time.sleep(1)
        safari_options = Options()
        safari_options.add_argument("--headless")
        driver = webdriver.Safari(options=safari_options)

        driver.get(map_url)

        team_active = driver.find_element(By.CLASS_NAME,'vm-stats-game.mod-active')

        team1 = team_active.find_element(By.CLASS_NAME,'team')
        team1_name = team1.find_element(By.CLASS_NAME,'team-name').text.strip()
        team1_ct_sc = team1.find_element(By.CLASS_NAME,'mod-ct').text.strip()
        team1_t_sc = team1.find_element(By.CLASS_NAME,'mod-t').text.strip()

        try:
            team1_ot_sc = team1.find_element(By.CLASS_NAME,'mod-ot').text.strip()
            team1_final_sc = str(int(team1_ct_sc) + int(team1_t_sc) + int(team1_ot_sc))
        except:
            team1_final_sc = str(int(team1_ct_sc) + int(team1_t_sc))
            team1_ot_sc = 0

        team2 = team_active.find_element(By.CLASS_NAME,'team.mod-right')
        team2_name = team2.find_element(By.CLASS_NAME,'team-name').text.strip()
        team2_ct_sc = team2.find_element(By.CLASS_NAME,'mod-ct').text.strip()
        team2_t_sc = team2.find_element(By.CLASS_NAME,'mod-t').text.strip()

        try:
            team2_ot_sc = team2.find_element(By.CLASS_NAME,'mod-ot').text.strip()
            team2_final_sc = str(int(team2_ct_sc) + int(team2_t_sc) + int(team2_ot_sc))
        except:
            team2_final_sc = str(int(team2_ct_sc) + int(team2_t_sc))
            team2_ot_sc = 0

        team_stats[team1_name] = {'final': team1_final_sc,'CT': team1_ct_sc, 'T': team1_t_sc, 'OT': team1_ot_sc}
        team_stats[team2_name] = {'final': team2_final_sc,'CT': team2_ct_sc, 'T': team2_t_sc, 'OT': team2_ot_sc}

        map_raw = team_active.find_element(By.CLASS_NAME,'map').text.strip()
        map_index = map_raw.find("\t")
        map_name = map_raw[:map_index]

        tables = team_active.find_elements(By.CLASS_NAME,'wf-table-inset.mod-overview')
        
        player_stats = get_player_stats1(tables[0])

        match_stats[map_name] = {'Team Stats: ' : team_stats.copy(),'Player Stats' : player_stats.copy()}

        driver.quit()

    match_stats_final[f'{team1_name} vs {team2_name}'] = [event_stats, match_stats]

    match = f'{team1_name}_vs_{team2_name}'

    print(match)

    return match_stats_final, match


def main():
    matches_url = get_matches_url()
    print(matches_url[:1])

    for match_url in matches_url[:3]:
        match_stats = get_match_stats(match_url)
        with open('match_stats.json', 'w') as f:
            json.dump(match_stats, f)
    



match_stats, match = get_match_stats('https://www.vlr.gg/448600/drx-vs-sentinels-champions-tour-2025-masters-bangkok-r1')

with open(f'{match}.json', 'w') as f:
    json.dump(match_stats, f)