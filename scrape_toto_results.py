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

# FD_LAST_N_DRAWS = 50
TOTO_LAST_N_DRAWS = 350

TOTO_OUTLETS = 'divWinningOutlets'

# LOSE = 'Lose'

G1_WINNER_SEARCH_TEXT = 'Group 1 winning tickets sold at'
G2_WINNER_SEARCH_TEXT = 'Group 2 winning tickets sold at'


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

### Iterate through draw list and consolidate results ###
toto_result_list = []
toto_win_loc_list = []
i = 0
for toto_sppl_id in toto_sppl_ids:
    i += 1
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
            toto_prize_numbers = [
                int(toto_prize_num.get_text())
                for toto_prize_num 
                in toto_result_soup.find_all('td', {'class': lambda x: x and x.startswith(TOTO_WIN_CSS_SEL)})
            ]
            toto_additional_number = int(toto_result_soup.select(TOTO_ADDITIONAL_CSS_SEL)[0].get_text())
            gc.collect()
            toto_result_list.append([toto_result_dt, toto_additional_number, TOTO_ADDITIONAL_CLASS])
            for toto_prize_num in toto_prize_numbers:
                toto_result_list.append([toto_result_dt, toto_prize_num, TOTO_WIN_CLASS])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the page: {e}")
    
### Present Findings in pd DataFrame ###
toto_result_df = pd.DataFrame(np.array(toto_result_list), columns=['Date', 'Win Number', 'Win Type'])
toto_result_df.set_index('Date', inplace=True)
# toto_result_df['Win'] = (toto_result_df['Win Type'] != LOSE).replace(True, 1)

# Save DataFrames to CSV
toto_result_df.to_csv('results/toto_results.csv')