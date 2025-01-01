from datetime import datetime
import re
import json
import pickle
import urllib
import requests
from requests.exceptions import SSLError
import bs4
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import gc

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
############################################################################################
## Parameters
TOTO_DRAW_LIST_URL = 'http://www.singaporepools.com.sg/DataFileArchive/Lottery/Output/toto_result_draw_list_en.html'
TOTO_RESULT_URL = 'http://www.singaporepools.com.sg/en/product/sr/Pages/toto_results.aspx?sppl='

PARSER_NAME = 'html.parser'

SPPL_ATTR = 'querystring'
SPPL_TAG = 'option'

DT_FORMAT = '%d %b %Y'
DRAW_DATE_CLASS = 'drawDate'

FD_FIRST_PRIZE_CLASS = 'tdFirstPrize'
FD_SECOND_PRIZE_CLASS = 'tdSecondPrize'
FD_THIRD_PRIZE_CLASS = 'tdThirdPrize'
FD_STARTER_PRIZE_CLASS = 'tbodyStarterPrizes'
FD_CONSOLATION_PRIZE_CLASS = 'tbodyConsolationPrizes'

FD_STARTER_PRIZE_CSS_SEL = ' '.join(['.' + FD_STARTER_PRIZE_CLASS, 'td'])
FD_CONSOLAION_PRIZE_CSS_SEL = ' '.join(['.' + FD_CONSOLATION_PRIZE_CLASS, 'td'])

TOTO_WIN_CLASS = 'win'
TOTO_ADDITIONAL_CLASS = 'additional'

TOTO_WIN_CSS_SEL = TOTO_WIN_CLASS
TOTO_ADDITIONAL_CSS_SEL = '.' + TOTO_ADDITIONAL_CLASS

TOTO_LAST_N_DRAWS = 50

TOTO_OUTLETS = 'divWinningOutlets'

G1_WINNER_SEARCH_TEXT = 'Group 1 winning tickets sold at'
G2_WINNER_SEARCH_TEXT = 'Group 2 winning tickets sold at'

NON_PHYSICAL_WIN_LOCS = ['Singapore Pools Account Betting Service - -', 'iTOTO - System 1']
############################################################################################

### Get Toto Draw List ###
toto_draw_list_page = requests.get(TOTO_DRAW_LIST_URL)
toto_draw_list_soup = BeautifulSoup(toto_draw_list_page.content, PARSER_NAME)
toto_sppl_ids = [draw.get(SPPL_ATTR).rpartition('=')[2] for draw in toto_draw_list_soup.find_all(SPPL_TAG)]

# Set up retry strategy
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("http://", adapter)
http.mount("https://", adapter)

# Load existing data
toto_result_df = pd.read_csv('results/toto_results.csv')
toto_win_loc_df = pd.read_csv('results/toto_win_locations.csv')

# Get the latest date in the dataframe
toto_result_df['Date'] = pd.to_datetime(toto_result_df['Date'])
latest_saved_date = toto_result_df['Date'].max().date()

# Iterate through dates, stop at the latest date saved in the file
toto_result_list = []
toto_win_loc_list = []
current_loop_date = datetime.now().date()
i = 0
for toto_sppl_id in toto_sppl_ids:
    if i > TOTO_LAST_N_DRAWS:
        break

    try:
        with http.get(TOTO_RESULT_URL + toto_sppl_id, stream=True) as toto_result_page:
            toto_result_page.raise_for_status()  # Raise an error for bad status codes
            toto_result_soup = BeautifulSoup(toto_result_page.content, PARSER_NAME)
            toto_result_dt = datetime.strptime(
                toto_result_soup.find_all(class_=DRAW_DATE_CLASS)[0].get_text().rpartition(', ')[2], 
                DT_FORMAT
            )
            # Check if the date is already in the dataframe, break if it is
            current_loop_date = toto_result_dt.date()
            if current_loop_date == latest_saved_date:
                print("Breaking")
                break

            ## Prize numbers
            print("Adding results for ", toto_result_dt)
            toto_prize_numbers = [
                int(toto_prize_num.get_text())
                for toto_prize_num 
                in toto_result_soup.find_all('td', {'class': lambda x: x and x.startswith(TOTO_WIN_CSS_SEL)})
            ]
            toto_additional_number = int(toto_result_soup.select(TOTO_ADDITIONAL_CSS_SEL)[0].get_text())
            gc.collect()

            # Append to result list
            toto_result_list.append([toto_result_dt, toto_additional_number, TOTO_ADDITIONAL_CLASS])
            for toto_prize_num in toto_prize_numbers:
                toto_result_list.append([toto_result_dt, toto_prize_num, TOTO_WIN_CLASS])

            ## Winning outlets
            check_win_list = toto_result_soup.select('.divWinningOutlets')
            has_g1_winner = True if len(check_win_list[0].find_all(string=re.compile(G1_WINNER_SEARCH_TEXT))) > 0 else False
            has_g2_winner = True if len(check_win_list[0].find_all(string=re.compile(G2_WINNER_SEARCH_TEXT))) > 0 else False

            g1_win_loc_list = []
            g2_win_loc_list = []

            if has_g1_winner and has_g2_winner:
                g1_win_loc_list = [loc.contents[0].strip() for loc in check_win_list[0].select('ul')[0].select('li')]
                g2_win_loc_list = [loc.contents[0].strip() for loc in check_win_list[0].select('ul')[1].select('li')]
            elif has_g1_winner and not has_g2_winner:
                g1_win_loc_list = [loc.contents[0].strip() for loc in check_win_list[0].select('ul')[0].select('li')]
            elif not has_g1_winner and has_g2_winner:
                g2_win_loc_list = [loc.contents[0].strip() for loc in check_win_list[0].select('ul')[0].select('li')]
            else:
                pass

            for g1_win_loc in g1_win_loc_list:
                location = g1_win_loc[:g1_win_loc.rfind(' (')].strip()
                ticket_type = g1_win_loc[g1_win_loc.rfind(' (')+1:].replace("(","").replace(")", "").strip()
                toto_win_loc_list.append([toto_result_dt, location, ticket_type, 1])

            for g2_win_loc in g2_win_loc_list:
                location = g2_win_loc[:g2_win_loc.rfind(' (')].strip()
                ticket_type = g2_win_loc[g2_win_loc.rfind(' (')+1:].replace("(","").replace(")", "").strip()
                toto_win_loc_list.append([toto_result_dt, location, ticket_type, 2])

    except requests.exceptions.RequestException as e:
        print(f"Error fetching the page: {e}")

# Update the dataframes with the new data
new_toto_result_df = pd.DataFrame(np.array(toto_result_list), columns=['Date', 'Win Number', 'Win Type'])
new_toto_win_loc_df = pd.DataFrame(np.array(toto_win_loc_list), columns=['Date', 'Location', 'Ticket Type', 'Group'])

# new_toto_result_df['Date'] = pd.to_datetime(new_toto_result_df['Date']).dt.date
new_toto_win_loc_df['Date'] = pd.to_datetime(new_toto_win_loc_df['Date']).dt.date

print(new_toto_result_df)
print(new_toto_win_loc_df)

# Append new data to existing dataframes
toto_result_df = pd.concat([new_toto_result_df, toto_result_df], axis=0)
toto_win_loc_df = pd.concat([new_toto_win_loc_df, toto_win_loc_df], axis=0)

print(toto_result_df.head(10))
print(toto_win_loc_df.head(5))

# Save updated dataframes to CSV
toto_result_df.to_csv('results/toto_results.csv')
toto_win_loc_df.to_csv('results/toto_win_locations.csv')