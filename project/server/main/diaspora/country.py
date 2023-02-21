import requests
import pickle
import os
from project.server.main.logger import get_logger

logger = get_logger(__name__)

countries = {}
try:
    countries = pickle.load(open('/upw_data/countries.pkl', 'rb'))
    logger.debug(f'{len(countries)} affiliations - countries loaded')
except:
    pass

def get_country(c):
    if c in countries:
        res = countries[c]
    else:
        url = 'https://affiliation-matcher.staging.dataesr.ovh/match'
        param = {'type': 'country', 'query': c, 'verbose': False}
        r = requests.post(url, json=param)
        res = r.json()
        countries[c] = res
    if 'highlights' in res:
        del res['highlights']
    if 'results' in res:
        return res['results']
    return None

def save_countries():
    pickle.dump(countries, open('/upw_data/countries.pkl', 'wb'))
    logger.debug(f'{len(countries)} affiliations - countries saved')
