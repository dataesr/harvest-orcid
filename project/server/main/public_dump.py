import requests
import pandas as pd
from bs4 import BeautifulSoup
from project.server.main.utils import download_file

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_figshare_url(dump_year):
    df = pd.read_csv('urls.csv', sep=';')
    url = df[df.year==dump_year].url.values[0]
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    for b in soup.find_all('div'):
        try:
            name = b.find('button').find_all('span')[1].attrs['title']
            if 'summaries' not in name:
                continue
            logger.debug(name)
            return (b.find_all('a', {'tooltip': 'Download file'})[0]['href'])
        except:
            continue

def download_dump(dump_year):
    url = get_figshare_url(dump_year)
    if url:
        return download_file(url)
