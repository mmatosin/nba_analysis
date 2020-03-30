#### imports
import os, string
import pandas as pd
import numpy as np

from urllib.request import urlopen
from bs4 import BeautifulSoup


#### data directories
currDir = os.getcwd()
rootDir = os.path.abspath(os.path.join(currDir,'..'))

dataDir = os.path.abspath(os.path.join(rootDir,'data'))
rawDataDir = os.path.abspath(os.path.join(dataDir,'raw'))
interimDataDir = os.path.abspath(os.path.join(dataDir,'interim'))
finalDataDir = os.path.abspath(os.path.join(dataDir,'final'))

#### scrape player bio data
"""
scrapes basic bio data of *all* NBA & ABA players from bball reference
link: https://www.basketball-reference.com/

Used to collect all the names and unique Ids of all NBA & ABA players
Ids are then used to scrape game logs
"""
def get_player_names(save=False):

    player_dict = {}

    # iterate through each letter of the alphabet
    for letter in string.ascii_lowercase:

        # open + read the html for each letter
        url = f"https://www.basketball-reference.com/players/{letter}/"
        webpage = urlopen(url)
        html = BeautifulSoup(webpage)

        # get column names
        if letter == 'a':
            columns = [col.getText() for col in html.findAll('tr', limit=1)[0].findAll('th')]

        # get all player name and bio html
        name_rows = html.findAll('th')[len(columns):]
        bio_rows = html.findAll('tr')[1:]

        # extract the data we're looking for the html
        names = [name.getText() for name in name_rows]
        idx = [str(row).split("data-stat=")[0].split("data-append-csv=")[-1][1:-2].strip() for row in name_rows]
        bios  = [[bio.getText() for bio in row.findAll('td')] for row in bio_rows]

        # ensure the shapes match
        assert(len(names) == len(idx))
        assert(len(names) == len(bios))

        # save scraped data in a dict
        for key, name, bio in zip(idx,names,bios):
            player_dict[key] = [name] + bio

    # save dict to dataframe
    df = pd.DataFrame.from_dict(player_dict,orient="index").reset_index()
    df.columns = ['index'] + columns

    # save dataframe to csv in raw data directory
    if save:
        filename = f"{rawDataDir}//all_NBA_ABA_players.csv"
        df.to_csv(filename,index=False)

    return df


#### scrape player game logs (basic stats only)
"""
scrapes the regular season AND playoff data for each NBA + ABA player
on bball reference. Data is saved locally on a .csv

errors are logged in the data/error_log directory

Only BASIC metrics are scraped. Functionality to scrape advanced
metrics will be added

examples of bball reference url structure:
#### BASIC STATS
# https://www.basketball-reference.com/players/a/abdelal01.html
# https://www.basketball-reference.com/players/a/abdelal01/gamelog/1992
# https://www.basketball-reference.com/players/a/abdelal01/gamelog-playoffs/

#### ADVANCED STATS
# https://www.basketball-reference.com/players/a/abdelal01/gamelog-advanced/1992/
# https://www.basketball-reference.com/players/a/abdelal01/gamelog-playoffs-advanced/
"""



def get_reg_season_game_logs(player_idx,letter,from_year,to_year):
    reg_season_game_logs = None
    yearly_dfs = []

    # SCRAPING REGULAR SEASON GAMES

    for year in range(from_year, to_year+1):
        url = f"https://www.basketball-reference.com/players/{letter}/{player_idx}/gamelog/{year}"
        webpage = urlopen(url)
        html = BeautifulSoup(webpage)
        tables = html.findAll('table')

        if len(tables)==0:
            if year > 1976: continue
            else:
                aba_url = url + "/aba/"
                webpage = urlopen(aba_url)
                html = BeautifulSoup(webpage)
                aba_tables = html.findAll('table')

                if len(aba_tables)==0: continue
                else:
                    table = str(aba_tables[-1])
                    yearly_game_log = pd.read_html(table)[0]
                    yearly_game_log["PLAYOFF"] = 'N'
                    yearly_game_log["LEAGUE"] = 'ABA'

        else:
            table = str(tables[-1])
            yearly_game_log = pd.read_html(table)[0]
            yearly_game_log["PLAYOFF"] = 'N'
            yearly_game_log["LEAGUE"] = 'NBA'

        yearly_dfs.append(yearly_game_log)

    reg_season_game_logs = pd.concat([x for x in yearly_dfs],ignore_index=True,sort=False)
    reg_season_game_logs["PLAYOFF"] = 'N'
    reg_season_game_logs['SERIES'] = np.nan

    reg_season_game_logs.rename(columns = {'Date': 'DATE', 'Age': 'AGE', 'Tm': 'TEAM',
                                      'Unnamed: 5': 'HOME/AWAY', 'Opp': 'OPPONENT',
                                      'Unnamed: 7': 'RESULT', 'GmSc': 'GAME_SCORE'}, inplace=True)

    return reg_season_game_logs



def get_playoff_game_logs(player_idx,letter):

    playoff_game_logs = None

    playoff_url = f"https://www.basketball-reference.com/players/{letter}/{player_idx}/gamelog-playoffs/"
    webpage = urlopen(playoff_url)
    html = BeautifulSoup(webpage)
    tables = html.findAll('table')

    if len(tables) > 0:
        table = str(html.findAll('table')[7])
        playoff_game_logs = pd.read_html(table)[0]
        playoff_game_logs["PLAYOFF"] = 'Y'
        playoff_game_logs["AGE"] = np.nan

        playoff_game_logs.rename(columns = {'Date': 'DATE', 'Age': 'AGE', 'Tm': 'TEAM', 'Series':'SERIES',
                                          'Unnamed: 5': 'HOME/AWAY', 'Opp': 'OPPONENT',playoff_game_logs.columns[2]:'DATE',
                                          'Unnamed: 8': 'RESULT', 'GmSc': 'GAME_SCORE'}, inplace=True)

    return playoff_game_logs




def get_career_game_logs(player_data,verbose=True):

    logger = f"{errorLog}/log.txt"

    # PLAYER DATA
    player_idx = player_data['index']
    letter = player_idx[0]
    player_name = player_data['Player']
    from_yr, to_yr = row[["From","To"]].values

    if verbose:
        print(player_idx,player_name)

    # GET REGULAR SEASON GAME LOGS
    try:
        reg_season_game_logs = get_reg_season_game_logs(player_idx,letter,from_yr,to_yr)
    except:
        reg_season_game_logs = None
        with open(logger,"a") as logs:
            logs.write(f"{player_idx} , {player_name}, error in regular season game logs \n")

    # GET PLAYOFF GAME LOGS
    try:
        playoff_game_logs = get_playoff_game_logs(player_idx,letter)
    except:
        playoff_game_logs = None
        with open(logger,"a") as logs:
            logs.write(f"{player_idx} , {player_name}, error in playoff game logs \n")


    try:
        # CONCATENATING REGULAR SEASON AND PLAYOFF GAMES
        career_game_logs = pd.concat([reg_season_game_logs,playoff_game_logs],ignore_index=True,sort=False)
        career_game_logs['HOME/AWAY'] = career_game_logs['HOME/AWAY'].apply(lambda x: 'AWAY' if x=='@' else 'HOME')

        # FORMAT CAREER GAME LOGS, SORT BY DATE
        career_game_logs['Rk'] = career_game_logs['Rk'].astype(str)
        career_game_logs = career_game_logs[career_game_logs['Rk'].str.isnumeric().values]

        career_game_logs.rename(columns = {'Series':'SERIES'},inplace=True)
        career_game_logs['INDEX'] = player_idx
        career_game_logs['NAME']  = player_name

        career_game_logs.sort_values('DATE',inplace=True)
        career_game_logs.reset_index(drop=True,inplace=True)

    except:
        career_game_logs = None
        with open(logger,"a") as logs:
            logs.write(f"{player_idx} , {player_name}, no playoff OR regular season data \n")

    return career_game_logs
