import requests
from urllib.parse import quote_plus
import pickle
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_aurehal(aurehal_type):
    logger.debug(f'start {aurehal_type} aurehal')
    nb_rows = 10000
    cursor='*'
    data = []
    while True:
        url = f'https://api.archives-ouvertes.fr/ref/{aurehal_type}/?q=orcidId_s:*&wt=json&fl=orcidId_s,idrefId_s,idHal_s&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
        r = requests.get(url)
        res = r.json()
        new_cursor = quote_plus(res['nextCursorMark'])
        data += res['response']['docs']
        if new_cursor == cursor:
            break
        cursor = new_cursor
    logger.debug(f'end {aurehal_type} aurehal')
    return data

def get_data_from_hal():
    data = get_aurehal('author')
    orcid_dict = {}
    for e in data:
        for orcid in e.get('orcidId_s', []):
            orcid = orcid.split('/')[-1]
            if orcid[0:2] != '00':
                continue
            new_elt = {}
            if e.get('idHal_s'):
                new_elt['id_hal_s'] = e['idHal_s']
            if e.get('idrefId_s'):
                new_elt['idref'] = 'idref'+e['idrefId_s'][0].split('/')[-1]
            if new_elt.get('id_hal_s'):
                orcid_dict[orcid] = new_elt
    pickle.dump(orcid_dict, open('/upw_data/orcid_hal_dict.pkl', 'wb'))
