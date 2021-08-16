
# Libraries
import urllib
from urllib.request import urlopen
from bs4 import BeautifulSoup
from translate import Translator
from datetime import datetime
import cx_Oracle as cx
import pandas as pd
import urllib3.exceptions
from db.db_config.db_config_var import *
import requests as req  # library to handle api requests
import time


# Parameters
url = 'https://www.finstat.sk/'
translator = Translator(to_lang='en', from_lang='sk')
# Date parameters
input_format = '%d. %B %Y'  # day, full month name and year
output_format = '%d/%m/%Y'
translation_dict = {
'január': 'january', 'januára': 'january',
'február': 'february', 'februára': 'february',
'marec': 'march', 'marca': 'march',
'april': 'april', 'apríla': 'april',
'máj': 'may', 'mája': 'may',
'jún': 'june', 'júna': 'june',
'júl': 'july', 'júla': 'july',
'august': 'august', 'augusta': 'august',
'september': 'september', 'septembra': 'september',
'oktober': 'october', 'októbra': 'october',
'november': 'november', 'novembra': 'november',
'december': 'december', 'decembra': 'december'}


def translate_words(initial_string, dictionary):
    return " ".join(dictionary.get(ele, ele) for ele in initial_string.split())


# Parameters for Oracle client
cx.init_oracle_client(lib_dir=lib_direc,
                      config_dir=config_direc)

# Initialising connection to DB
con = cx.connect(user=user_db,
                 password=pass_db,
                 dsn=dsn_db,
                 encoding=encoding_db)

# Initialising Cursor
cur = con.cursor()

# Retrieving Firms from DB
query = 'SELECT * FROM FIRMS ORDER BY ICO'

firms = pd.read_sql(query, con=con)
ico_firms = firms['ICO']

# TABLE to save scrapped dates
query = 'CREATE TABLE company_and_relevant_dates' \
        '(ico                   NUMBER      NOT NULL,' \
        ' date_loaded           TIMESTAMP   WITH TIME ZONE,' \
        ' date_of_formation     VARCHAR2    (20)  ,' \
        ' date_of_closing       VARCHAR2    (20))'

cur.execute(query)

# query to add rows
sql_json = ('insert into company_and_relevant_dates (ico,'
            '                               date_loaded, '
            '                               date_of_formation,'
            '                               date_of_closing)'
            'values(:ico,:date_and_time,:date_of_formation,:date_of_closing)')
ico_i = 3447
n_firms = len(ico_firms)
while ico_i <= n_firms:
    try:
        ico = ico_firms[ico_i]
        # Loading the webpage
        webpage = urlopen(f'{url}{ico}')
        html_bytes = webpage.read()
        html = html_bytes.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        # Retrieving the relevant data
        rel_text = soup.find('div', {'class': 'col-sm-6 p-r-lg'})
        rel_dates = rel_text.find_all_next('li', {'class': 'hidden-xs'})
        # Iterating over elements in the hidden-xs class
        for element in range(len(rel_dates)):
            date_created = None
            date_closed = None
            line_i = rel_dates[element].get_text()
            # date of creation of the company
            if (line_i.find('Dátum vzniku utorok') == 0) or (line_i.find('vzniku') >= 0):
                # 0th index means that the string is found at the beginning
                date_created_i = line_i.split(', ', 1)[1]
                # Translating the dates to english to convert to date-time
                date_created_i_eng = translate_words(date_created_i, translation_dict)
                date_created = datetime.strptime(date_created_i_eng, input_format).strftime(output_format)
            # date of closing of the company
            if (line_i.find('Dátum zániku štvrtok') == 0) or (line_i.find('zániku') >= 0):
                # 0th index means that the string is found at the beginning
                date_closed_i = line_i.split(', ', 1)[1]
                # Translating the dates to english to convert to date-time
                date_closed_i_eng = translate_words(date_closed_i, translation_dict)
                date_closed = datetime.strptime(date_closed_i_eng, input_format).strftime(output_format)
            date_now = datetime.now()
        cur.execute(sql_json,
                    [int(ico),
                     date_now,
                     date_created,
                     date_closed])
        con.commit()
        print(f'Uploading info for company number: {ico_i}/{n_firms} - ico: {ico}')
        ico_i += 1
    except KeyError:
        print(KeyError)
        print("Skipping ICO, issue with the web/data ...")
        ico_i += 1
        continue
    except cx.Error:
        print(cx.Error)
        print('Issue with Oracle ...')
        continue
    except (TimeoutError, req.exceptions.ConnectionError, urllib3.exceptions.NewConnectionError,
            urllib3.exceptions.MaxRetryError, urllib.error.URLError) as Error:
        print(Error)
        print('Ooops, there was an connection error. Let us wait 5 minutes ...')
        time.sleep(300)  # Wait for 5 minutes
        continue

con.close()




