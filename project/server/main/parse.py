import pandas as pd
import bs4
from bs4 import BeautifulSoup
import os
import lxml

from project.server.main.utils import get_date
from project.server.main.logger import get_logger

logger = get_logger(__name__)

FRENCH_ALPHA2 = ['fr', 'gp', 'gf', 'mq', 're', 'yt', 'pm', 'mf', 'bl', 'wf', 'tf', 'nc', 'pf']

def parse_all(base_path, dump_year, filter_fr = True):
    logger.debug(f'parsing {base_path} files')
    orcids_info = []
    ix = 0
    for notice in os.listdir(base_path):
        try:
            parsed = parse_notice(notice, base_path, dump_year)
            is_fr =  parsed['is_fr']
            if filter_fr and not is_fr:
                continue
            if is_fr:
                logger.debug(parsed)
            orcids_info.append(parsed)
            ix += 1
        except:
            pass
            #logger.debug("error reading "+notice)
    df = pd.DataFrame(orcids_info)
    filename = base_path.split('/')[-2]
    logger.debug(f'{len(orcids_info)} french orcids in {filename}')
    df.to_json(f'{base_path}/{filename}.jsonl', lines=True, orient='records')
    return df


def get_soup(notice_path, base_path):
    soup = BeautifulSoup(open(base_path+notice_path, 'r'), 'lxml')
    return soup
    
def parse_notice(notice_path, base_path, dump_year, verbose = False):
    res = {} 
    soup = get_soup(notice_path, base_path)
    orcid = notice_path.split('.')[0]
    res['orcid'] = orcid 
    creation_date = soup.find("history:submission-date").text[0:10]
    res['creation_date'] = creation_date
    res['creation_year'] = creation_date[0:4]
    first_name, last_name, current_address = None, None, None
    
    name1 = soup.find("personal-details:given-names")
    if name1:
        first_name = name1.text
        res['first_name'] = first_name
    
    name2 = soup.find("personal-details:family-name")
    if name2:
        last_name = name2.text
        res['last_name'] = last_name
    
    country = soup.find("address:country")
    if country:
        current_address = country.text
        res['current_address_country'] = current_address.lower()
       
    employments_elt = soup.find("activities:employments")
    if employments_elt:
        employments = parse_activities(employments_elt, 'employment', dump_year)
        res.update(employments)
    educations_elt = soup.find("activities:educations")
    if educations_elt:
        educations = parse_activities(educations_elt, 'education', dump_year)
        res.update(educations)
    
    is_fr = False
    is_fr_present = False
    fr_reasons_present = []
    fr_reasons = []
    if isinstance(current_address, str) and current_address.lower() in FRENCH_ALPHA2:
        is_fr = True
        is_fr_present = True
        fr_reasons_present.append("address")
        fr_reasons.append("address")

    for a in res.get('employment_present', []):
        if a.get('country') in FRENCH_ALPHA2:
            is_fr = True
            is_fr_present = True
            fr_reasons_present.append('employment')
            fr_reasons.append('employment')
    
    for a in res.get('education_present', []):
        if a.get('country') in FRENCH_ALPHA2:
            is_fr = True
            is_fr_present = True
            fr_reasons_present.append('education')
            fr_reasons.append('education')
    
    for a in res.get('employment_other', []):
        if a.get('country') in FRENCH_ALPHA2:
            is_fr = True
            fr_reasons.append('employment')
    
    for a in res.get('education_other', []):
        if a.get('country') in FRENCH_ALPHA2:
            is_fr = True
            fr_reasons.append('education')
    
    fr_reasons = list(set(fr_reasons)) 
    fr_reasons.sort()

    res['is_fr'] = is_fr
    res['fr_reasons'] = fr_reasons
    res['fr_reasons_concat'] = ';'.join(fr_reasons)
    res['fr_reasons_main'] = get_main_reason(res['fr_reasons_concat'])
    
    fr_reasons_present = list(set(fr_reasons)) 
    fr_reasons_present.sort()

    res['is_fr_present'] = is_fr_present
    res['fr_reasons_present'] = fr_reasons_present
    res['fr_reasons_present_concat'] = ';'.join(fr_reasons_present)
    res['fr_reasons_present_main'] = get_main_reason(res['fr_reasons_present_concat'])
    return res

def get_main_reason(x):
    for k in ['employment', 'education', 'address']:
        if k in x:
            return k

def parse_date(x):
    year, month, day = None, None, None
    try:
        year = x.find('common:year').get_text()
    except:
        return None
    try:
        month = x.find('common:month').get_text()
    except:
        month = '01'
    try:
        day = x.find('common:day').get_text()
    except:
        day = '01'
    return get_date(f'{year}-{month}-{day}')

def parse_organization(x):
    res = {}
    for f in ['name', 'city', 'country']:
        elt = x.find(f'common:{f}')
        if elt:
            res[f] = elt.get_text()
            if f in ['country'] and res[f]:
                res[f] = res[f].lower()
    for f in x.find_all('common:disambiguated-organization'):
        source = f.find('common:disambiguation-source').get_text().lower()
        value = f.find('common:disambiguated-organization-identifier').get_text()
        res[source] = value
    return res

def parse_activities(x, activity, dump_year):
    dump_date = f'{dump_year}-11-25'
    present, other = [], []
    first_year, last_year = '9999', '0000'
    for r in x.find_all(f'{activity}:{activity}-summary'):
        start_date_elt = r.find(f'common:start-date')
        end_date_elt = r.find(f'common:end-date')
        start_date, end_date = None, None
        if start_date_elt:
            start_date = parse_date(start_date_elt)
        if end_date_elt:
            end_date = parse_date(end_date_elt)
        is_current=False
        if (end_date is None or end_date > dump_date) and (start_date is None or start_date < dump_date):
            is_current = True
        for org_elt in r.find_all('common:organization'):
            org = parse_organization(org_elt)
            org['is_current'] = is_current
            if start_date:
                org['start_date'] = start_date
                org['start_year'] = start_date[0:4]
                if org['start_year']<first_year:
                    first_year = org['start_year']
            if end_date:
                org['end_date'] = end_date
                org['end_year'] = end_date[0:4]
                if org['end_year'] > last_year:
                    last_year = org['end_year']
        if is_current:
            if org not in present:
                present.append(org)
        else:
            if org not in other:
                other.append(org)
    ans = {f'{activity}_present': present, f'{activity}_other': other}
    if first_year != '9999':
        ans[f'first_{activity}_year'] = first_year
    if last_year != '0000':
        ans[f'last_{activity}_year'] = last_year
    return ans
