import time
import datetime
import os
import requests
import json
import pandas as pd
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object

logger = get_logger(__name__)

headers_token = {'Accept': 'application/json'}
payload_token = {'client_id': os.getenv('ORCID_CLIENT_ID'),
 'client_secret': os.getenv('ORCID_CLIENT_TOKEN'),
 'scope': '/read-public',
 'grant_type': 'client_credentials'}

os.system('mkdir -p /upw_data/links_publications_persons')

def clean_doi(doi):
    res = doi.lower().replace(' ', '').strip()
    res = res.replace('%2f', '/')
    res = res.replace('doi:', '')
    for f in [',', ';', ' ']:
        res = res.replace(f, '')
    if 'doi.org' in doi:
        res = re.sub('(.*)?doi.org/', '', res)
    return res.strip()

def get_header():
    post_request_token = requests.post("https://orcid.org/oauth/token", data = payload_token, headers=headers_token)

    headers_search = {'Content-type': 'application/vnd.orcid+json',
      'Authorization type': 'Bearer',
      'Access token': json.loads(post_request_token.text)['access_token'] }
    return headers_search

def get_orcid(orcid):
    try:
        r_orcid = requests.get(f"https://pub.orcid.org/v2.1/{orcid}", headers=get_header())
        orcid_json = r_orcid.json()
        return orcid_json
    except:
        logger.debug(f'error with get_orcid {orcid}')
        return None

def get_publications(orcid, record, person_id):
    elt = {'person_id': person_id}
    try:
        last_name = record['person']['name']['family-name']['value'].strip()
    except:
        logger.debug(f'missing lastName for orcid {orcid}')
        return []
    try:
        first_name = record['person']['name']['given-names']['value'].strip()
    except:
        logger.debug(f'missing lastName for orcid {orcid}')
        return []
    full_name = f'{first_name} {last_name}'.strip()
    elt['full_name'] = full_name
    elt['last_name'] = last_name
    elt['first_name'] = first_name

    publi_author_link = []
    for w in record['activities-summary']['works']['group']:
        publication_id = None
        ext_map = { e['external-id-type']: e['external-id-value'].lower().strip() for e in w['external-ids']['external-id']}
        for id_type in ['doi', 'hal']:
            if id_type in ext_map:
                if id_type == 'doi':
                    publication_id = 'doi' + clean_doi(ext_map[id_type])
                else:
                    publication_id = f'{id_type}{ext_map[id_type]}'
                break
        if publication_id is None:
            if len(w['external-ids']['external-id']) == 0: # no identifier
                publication_id = 'orcid'+w['work-summary'][0]['path']
            else:
                id_type = w['external-ids']['external-id'][0]['external-id-type']
                id_value = w['external-ids']['external-id'][0]['external-id-value'].lower().strip()
                publication_id = f'{id_type}{ext_map[id_type]}'
        new_elt = elt.copy()
        new_elt['publi_id'] = publication_id
        publi_author_link.append(new_elt)
    return publi_author_link

def get_links_from_orcid(record, person_id, orcid):
    record = get_orcid(orcid)
    if record is None:
        return []
    links = get_publications(orcid, record, person_id)
    logger.debug(f'{len(links)} links for orcid {orcid} ({person_id})')
    pd.DataFrame(links).to_json(f'/upw_data/links_publications_persons/links_orcid{orcid}.jsonl', lines=True, orient='records')
    #upload_object('misc', f'links_orcid{orcid}.jsonl', f'links_publications_persons/links_orcid{orcid}.jsonl')
