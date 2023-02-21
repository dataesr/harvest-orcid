import pandas as pd
import ast
import os
import pickle
import numpy as np

from project.server.main.utils_swift import download_container
from project.server.main.logger import get_logger

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data'


df_dewey = pd.read_csv('project/server/main/dewey.csv',sep=';')
dewey_code_dict = {}
for i, row in df_dewey.iterrows():
    codes = ast.literal_eval(row.codes)
    for c in codes:
        dewey_code_dict[c] = {"discipline": row.discipline, "macro":row.macro}

def get_classification_dewey(publi_codes):
    thesis_classification = {"discipline": "unknown", "macro":"unknown"}
    for c in publi_codes:
        if c['reference'] != 'dewey':
            continue
        if c['code'] in dewey_code_dict:
            thesis_classification = dewey_code_dict[c['code']]
            break
        if c['code'][0:1] in dewey_code_dict:
            thesis_classification = dewey_code_dict[c['code'][0:1]]
            break
    return thesis_classification

def get_these_data(snapshot_date):
    french_doctors_dict = {}
    #download_container(container = 'theses', skip_download=False, download_prefix=f'{snapshot_date}/parsed')
    for f in os.listdir(f'{MOUNTED_VOLUME}/theses/{snapshot_date}/parsed/'):
        logger.debug(f'reading {f}')
        df = pd.read_json(f'{MOUNTED_VOLUME}/theses/{snapshot_date}/parsed/{f}')
        for e in df.itertuples():
            if not isinstance(e.authors, list):
                continue
            for a in e.authors:
                if a['role'] == 'author' and a.get('idref'):
                    idref = 'idref'+a['idref']
                    if idref not in french_doctors_dict:
                        french_doctors_dict[idref] = {
                                'idref_abes': idref,
                                'has_these': True,
                                'theses_id': [], 'first_these_year': 9999, 'first_these_discipline': 'other', 'first_these_affiliations': [], 
                                'nb_theses': 0
                                }
                        for f in ['first_name', 'last_name']:
                            if isinstance(a.get(f), str):
                                french_doctors_dict[idref][f] = a[f]
                    if isinstance(e.nnt_id, str) and e.nnt_id not in french_doctors_dict[idref]['theses_id']:
                        french_doctors_dict[idref]['theses_id'].append(e.nnt_id)
                    if e.year and e.year == e.year:
                        french_doctors_dict[idref]['first_these_year'] = min(french_doctors_dict[idref]['first_these_year'], e.year)
                    if isinstance(e.classifications, list) and e.year == french_doctors_dict[idref]['first_these_year']:
                        french_doctors_dict[idref]['first_these_discipline'] = get_classification_dewey(e.classifications)
                    if isinstance(e.affiliations, list) and e.year == french_doctors_dict[idref]['first_these_year']:
                        french_doctors_dict[idref]['first_these_affiliations'] = e.affiliations
                    french_doctors_dict[idref]['nb_theses'] = len(french_doctors_dict[idref]['theses_id'])
    nb_doctors = len(french_doctors_dict)
    logger.debug(f'{nb_doctors} personnes avec un idref et un doctorat soutenu ont été identifées.')
    df_data_idref = pd.read_json('/upw_data/data_idref.jsonl', lines=True)
    df_doctors = pd.DataFrame(list(french_doctors_dict.values()))
    df_doctors_enriched = df_doctors.merge(df_data_idref, on='idref_abes', how='outer')
    df_doctors_enriched.has_these.fillna(False, inplace=True)
    df_doctors_enriched.has_id_hal_abes.fillna(False, inplace=True)
    df_doctors_enriched.has_idref_abes.fillna(True, inplace=True)
    for f in ['orcid', 'id_hal_abes', 'last_name', 'first_name']:
        df_doctors_enriched[f].replace({np.nan: None}, inplace=True)
    df_doctors_enriched.to_json('/upw_data/data_abes.jsonl', lines=True, orient='records')
    return
