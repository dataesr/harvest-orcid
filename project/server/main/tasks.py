import time
import string
import datetime
import os
import requests
import json
import pandas as pd
from urllib import parse
from project.server.main.utils_swift import upload_object
from project.server.main.utils import get_orcid_prefix
from project.server.main.public_dump import download_dump
from project.server.main.elastic import reset_index
from project.server.main.logger import get_logger

logger = get_logger(__name__)

ES_LOGIN_BSO_BACK = os.getenv('ES_LOGIN_BSO_BACK', '')
ES_PASSWORD_BSO_BACK = os.getenv('ES_PASSWORD_BSO_BACK', '')
ES_URL = os.getenv('ES_URL', 'http://localhost:9200')

def to_json(input_list, output_file, ix):
    if ix == 0:
        mode = 'w'
    else:
        mode = 'a'
    with open(output_file, mode) as outfile:
        if ix == 0:
            outfile.write('[')
        for jx, entry in enumerate(input_list):
            if ix + jx != 0:
                outfile.write(',\n')
            json.dump(entry, outfile)

def merge_field(a, b):
    if isinstance(a, str) and not isinstance(b, str):
        return a
    if isinstance(b, str) and not isinstance(a, str):
        return b
    # priority to the first value
    return a

def concat_results(dump_year):
    logger.debug('concat all files into one')
    filename = f'/upw_data/ORCID_{dump_year}_10_summaries/results.jsonl'
    orcid_data = []
    for prefix in get_orcid_prefix():
        tmp = pd.read_json(f'/upw_data/ORCID_{dump_year}_10_summaries/{prefix}/{prefix}.jsonl', lines=True)
        orcid_data.append(tmp)
    df_orcid = pd.concat(orcid_data)
    df_abes = pd.read_json('/upw_data/data_abes.jsonl', lines=True)
    for f in ['has_idref_abes', 'has_id_hal_abes', 'id_hal_abes']:
        del df_orcid[f]
    df_final = df_orcid.merge(df_abes, on='idref_abes', how='outer')
    ix = 0
    for r in df_final.itertuples():
        df_final.at[ix, 'orcid'] = merge_field(r.orcid_y, r.orcid_x) 
        df_final.at[ix, 'first_name'] = merge_field(r.first_name_y, r.first_name_x) 
        df_final.at[ix, 'last_name'] = merge_field(r.last_name_y, r.last_name_x)
        ix += 1
    for f in ['orcid', 'first_name', 'last_name']:
        del df_final[f'{f}_x']
        del df_final[f'{f}_y']
    df_final['has_orcid'] = ~df_final.orcid.isna()
    df_final['has_these'] = df_final.has_these.fillna(0).astype(bool)
    for f in ['has_these', 'has_idref_abes', 'has_id_hal_abes', 'has_idref_aurehal', 'has_id_hal_aurehal', 
            'is_fr', 'is_fr_present', 'has_work', 'has_work_from_hal', 'active', 'same_id_hal', 'same_idref']:
        df_final[f] = df_final[f].fillna(0).astype(bool)
    df_final.to_json(filename, lines=True, orient='records')

def import_es(dump_year, index_name):
    input_file = f'/upw_data/ORCID_{dump_year}_10_summaries/results.jsonl'
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}'
    logger.debug('loading bso-orcid index')
    reset_index(index=index_name)
    elasticimport = f"elasticdump --input={input_file} --output={es_host}{index_name} --type=data --limit 1000 " + "--transform='doc._source=Object.assign({},doc)'"
    # logger.debug(f'{elasticimport}')
    logger.debug('starting import in elastic')
    os.system(elasticimport)

def create_task_load(arg):
    dump_year = arg.get('dump_year')
    index_name = arg.get('index_name')
    if arg.get('concat'):
        concat_results(dump_year)
    import_es(dump_year, index_name)

def create_task_dois(arg):
    cmd2 = 'rm -rf /upw_data/orcid_tmp.jsonl'
    os.system(cmd2)

    for i1 in range(0, 10):
        for i2 in range(0, 10):
            for i3 in range(0, 10):
                #cmd1 = f"cat /upw_data/links_publications_persons/links_orcid0000-00{i1}{i2}-{i3}* | jq -r .publi_id | grep '^doi10.' | grep -v ',' | cut -c 4- | sort -u >> /upw_data/dois_from_orcid.csv"
                #os.system(cmd1)
                cmd2 = f"cat /upw_data/links_publications_persons/links_orcid0000-00{i1}{i2}-{i3}* >> /upw_data/orcid_tmp.jsonl"
                os.system(cmd2)
    cmd4 = "sed -e 's/}{/}\\n{/g' /upw_data/orcid_tmp.jsonl > /upw_data/orcid_idref.jsonl"
    os.system(cmd4)
    #logger.debug('getting dois from orcid links')
    #output_file = '/upw_data/dois_from_orcid.json'
    #df = pd.read_csv('/upw_data/dois_from_orcid.csv', chunksize = 20000)
    #ix = 0
    #for c in df:
    #    dois = c.doi.tolist()
    #    to_json(dois, output_file, ix)
    #    ix += 1
    #with open(output_file, 'a') as outfile:
    #    outfile.write(']')
    #upload_object('publications-related', output_file, f'dois_from_orcid.json')
    upload_object('misc', '/upw_data/orcid_idref.jsonl', f'orcid_idref.jsonl')

def create_task_public_dump(arg):
    dump_year = arg.get('dump_year')
    filename=None
    if arg.get('download'):
        filename = download_dump(dump_year)
        filename = filename.split('/')[-1]
    if arg.get('uncompress'):
        if filename is None:
            filename = arg.get('filename') + '.tar.gz'
        os.system(f'cd /upw_data && tar -xvf {filename}')
