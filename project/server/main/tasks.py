import time
import datetime
import os
import requests
import json
import pandas as pd
from project.server.main.utils_swift import upload_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

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
