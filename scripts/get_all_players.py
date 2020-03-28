"""
scrapes basic bio data of *all* NBA & ABA players from bball reference
link: https://www.basketball-reference.com/
"""

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

#### scrape player data
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
