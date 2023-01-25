import datetime
import os
import pandas as pd
import re
import requests
import shutil
import json
import string

from typing import Union
from dateutil import parser

from project.server.main.logger import get_logger

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data/'

def get_date(x):
    try:
        d = parser.parse(x)
    except:
        try:
            d = parser.parse(x[0:8]+'01')
        except:
            return None
    return d.isoformat()[0:10]

def get_orcid_prefix():
    prefixes = []
    for a in list(string.digits):
        for b in list(string.digits):
            for c in list(string.digits)+['X']:
                prefix = f'{a}{b}{c}'
                prefixes.append(prefix)
    return prefixes

def get_filename_from_cd(cd: str) -> Union[str, None]:
    """ Get filename from content-disposition """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]


def download_file(url: str, upload_to_object_storage: bool = True, destination: str = None) -> str:
    os.makedirs(MOUNTED_VOLUME, exist_ok=True)
    start = datetime.datetime.now()
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        try:
            local_filename = get_filename_from_cd(r.headers.get('content-disposition')).replace('"', '')
        except:
            local_filename = url.split('/')[-1]
        logger.debug(f'Start downloading {local_filename} at {start}')
        local_filename = f'{MOUNTED_VOLUME}{local_filename}'
        if destination:
            local_filename = destination
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'End download in {delta}')
    return local_filename
