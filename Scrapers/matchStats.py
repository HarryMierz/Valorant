import requests
from bs4 import BeautifulSoup
import time

def get_matches_url():

    url = 'https://www.vlr.gg/matches/results'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    matches_url = []

    for match in soup.find_all('div', class_='wf-card'):
        matches_url.append('https://www.vlr.gg/' + match['href'])

    print('Obtained matches urls')
    print('Total Matches: ', len(matches_url))

    return matches_url

def get_player_stats(table, player_stats):
    rows = table.find_all('tr')

    for row in rows:
        agent = row.find('span', class_='stats-sq mod-agent small')
        agent_name = agent.find('img')['title'] if agent else 'Unknown'
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]

        
        if len(cols) == 0:
            continue
        
        name_space_index = cols[0].find(" ")
        player_name = cols[0][:name_space_index]

        team_name_index = cols[0].rfind("\t")
        team_name = cols[0][team_name_index + 1:]

        acs = cols[3].split("\n")
        acs_all = acs[0]
        acs_t = acs[1]
        acs_ct = acs[2]

        elims = cols[4].split("\n")
        elims_all = elims[0]
        elims_t = elims[1]
        elims_ct = elims[2]

        deaths = cols[5].split("\n")
        deaths_all = deaths[0]
        deaths_t = deaths[1]
        deaths_ct = deaths[2]

        assists = cols[6].split("\n")
        assists_all = assists[0]
        assists_t = assists[1]
        assists_ct = assists[2]

        kast = cols[8].split("\n")
        kast_all = kast[0]
        kast_t = kast[1]
        kast_ct = kast[2]

        adr = cols[9].split("\n")
        adr_all = adr[0]
        adr_t = adr[1]
        adr_ct = adr[2]

        hs_per = cols[10].split("\n")
        hs_per_all = hs_per[0]
        hs_per_t = hs_per[1]
        hs_per_ct = hs_per[2]

        first_kills = cols[11].split("\n")
        first_kills_all = first_kills[0]
        first_kills_t = first_kills[1]
        first_kills_ct = first_kills[2]

        first_deaths = cols[12].split("\n")
        first_deaths_all = first_deaths[0]
        first_deaths_t = first_deaths[1]
        first_deaths_ct = first_deaths[2]

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

def get_match_stats(match_url,map_stats={}):
    

    time.sleep(1)
    response = requests.get(match_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    maps_urls = []

    team_stats = {}

    for map in soup.find_all('div', class_='vm-stats-gamesnav-item js-map-switch'):
        maps_urls.append('https://www.vlr.gg/' + map['data-href'])

    for map_url in maps_urls[1:]:
        print(map_url)
        time.sleep(1)
        response = requests.get(map_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        team1 = soup.find('div', class_='team')
        team1_name = team1.find('div', class_='team-name').text.strip()
        team1_ct_sc = team1.find('span', class_='mod-ct').text.strip()
        team1_t_sc = team1.find('span', class_='mod-t').text.strip()

        try:
            team1_ot_sc = team1.find('span', class_='mod-ot').text.strip()
            team1_final_sc = str(int(team1_ct_sc) + int(team1_t_sc) + int(team1_ot_sc))
        except:
            team1_final_sc = str(int(team1_ct_sc) + int(team1_t_sc))
            team1_ot_sc = 0


        

        team2 = soup.find('div', class_='team mod-right')
        team2_name = team2.find('div', class_='team-name').text.strip()
        team2_ct_sc = team2.find('span', class_='mod-ct').text.strip()
        team2_t_sc = team2.find('span', class_='mod-t').text.strip()

        try:
            team2_ot_sc = team2.find('span', class_='mod-ot').text.strip()
            team2_final_sc = str(int(team2_ct_sc) + int(team2_t_sc) + int(team2_ot_sc))
        except:
            team2_final_sc = str(int(team2_ct_sc) + int(team2_t_sc))
            team2_ot_sc = 0

        team_stats[team1_name] = {'final': team1_final_sc,'CT': team1_ct_sc, 'T': team1_t_sc, 'OT': team1_ot_sc}
        team_stats[team2_name] = {'final': team2_final_sc,'CT': team2_ct_sc, 'T': team2_t_sc, 'OT': team2_ot_sc}

        map_raw = soup.find('div', class_='map').find_next('span').text.strip()
        map_index = map_raw.find("\t")
        map_name = map_raw[:map_index]

        player_stats = {}

        table = soup.find('table')
    
        player_stats = get_player_stats(table, player_stats)

        next_table = table.find_next('table')

        player_stats = get_player_stats(next_table, player_stats)

        match_date = soup.find('div', class_='moment-tz-convert').text.strip()

        map_stats = {'Map': map_name, 'Team Stats': team_stats, 'Player Stats': player_stats}

    return map_stats


def main():
    matches_url = get_matches_url()

    for match_url in matches_url[:1]:
        map_stats = get_match_stats(match_url)



main()


#Need match data object: date, maps, teams, scores