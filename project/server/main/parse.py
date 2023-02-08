import pandas as pd
import pickle
import bs4
from bs4 import BeautifulSoup
import os
import lxml

from project.server.main.utils import get_date
from project.server.main.logger import get_logger

logger = get_logger(__name__)

#debug
def test():
    dump_year=2022
    base_path='/upw_data//ORCID_2022_10_summaries/295/'
    notice_path='0000-0002-9361-5295.xml'
    return parse_notice(notice_path, base_path, dump_year)

FRENCH_ALPHA2 = ['fr', 'gp', 'gf', 'mq', 're', 'yt', 'pm', 'mf', 'bl', 'wf', 'tf', 'nc', 'pf']

orcid_idref = pickle.load(open('/upw_data/orcid_idref_dict.pkl', 'rb'))
orcid_hal = pickle.load(open('/upw_data/orcid_hal_dict.pkl', 'rb'))

def parse_all(base_path, dump_year, filter_fr = True):
    logger.debug(f'parsing {base_path} files')
    orcids_info = []
    ix = 0
    for notice in os.listdir(base_path):
        parsed = parse_notice(notice, base_path, dump_year)
        is_fr =  parsed['is_fr']
        if filter_fr and not is_fr:
            continue
        #if is_fr:
        #    logger.debug(parsed)
        orcids_info.append(parsed)
        ix += 1
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

    # data from ABES
    if orcid in orcid_idref:
        res['idref_abes'] = orcid_idref[orcid]['idref']
        res['has_idref_abes'] = True
        if 'id_hal_s' in orcid_idref[orcid] and isinstance(orcid_idref[orcid]['id_hal_s'], str):
            res['id_hal_abes'] = orcid_idref[orcid]['id_hal_s']
            res['has_id_hal_abes'] = True
        else:
            res['has_id_hal_abes'] = False
    else:
        res['has_idref_abes'] = False
        res['has_id_hal_abes'] = False
    # data from HAL
    if orcid in orcid_hal:
        res['id_hal_aurehal'] = orcid_hal[orcid]['id_hal_s']
        res['has_id_hal_aurehal'] = True
        if 'idref' in orcid_hal[orcid] and isinstance(orcid_hal[orcid]['idref'], str):
            res['idref_aurehal'] = orcid_hal[orcid]['idref']
            res['has_idref_aurehal'] = True
        else:
            res['has_idref_aurehal'] = False
    else:
        res['has_id_hal_aurehal'] = False
        res['has_idref_aurehal'] = False
    if res['has_id_hal_aurehal'] and res['has_id_hal_abes']:
        res['same_id_hal'] = (res['id_hal_abes'] == res['id_hal_aurehal'])
    else:
        res['same_id_hal'] = None
    if res['has_idref_abes'] and res['has_idref_aurehal']:
        res['same_idref'] = (res['idref_aurehal'] == res['idref_abes'])
    else:
        res['same_idref'] = None

    try:
        creation_date = soup.find("history:submission-date").text[0:10]
        res['creation_date'] = creation_date
        res['creation_year'] = creation_date[0:4]
    except:
        #logger.debug(f'no history:submission-date for {orcid}')
        pass

    try:
        last_modified_date = soup.find("common:last-modified-date").text[0:10]
        res['last_modified_date'] = last_modified_date
        res['active'] = int(dump_year) - int(last_modified_date[0:4]) <= 1 # active if last modified current year or year before
    except:
        #logger.debug(f'no common:last-modified-date for {orcid}')
        pass
    
    try:
        for email_elt in soup.find_all('email:email'):
            if '@' in email_elt.get_text():
                email = email_elt.get_text()
                email_domain = email.split('@')[1]
                res['email_domain'] = email_domain
    except:
        pass

    first_name, last_name, current_address = None, None, None
    
    name1 = soup.find("personal-details:given-names")
    if name1:
        first_name = name1.text
        res['first_name'] = first_name
    
    name2 = soup.find("personal-details:family-name")
    if name2:
        last_name = name2.text
        res['last_name'] = last_name

    full_name = f'{first_name} {last_name}'
    
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

    current_employment_fr_has_id_types = None
    current_employment_fr_has_id = None
    current_employment_fr_id = []
    current_employment_fr_id_types = []
    for a in res.get('employment_present', []):
        if a.get('country') in FRENCH_ALPHA2:
            current_employment_fr_has_id = False
            is_fr = True
            is_fr_present = True
            fr_reasons_present.append('employment')
            fr_reasons.append('employment')
            current_employment_fr_id_types = [f.lower().strip() for f in a.get('disambiguation_sources', [])]
            current_employment_fr_id = a.get('disambiguation_ids', [])
            for f in ['ror', 'grid']:
                if f in current_employment_fr_id_types:
                    current_employment_fr_has_id = True
    res['current_employment_fr_id_types'] = current_employment_fr_id_types
    res['current_employment_fr_id'] = current_employment_fr_id
    res['current_employment_fr_has_id'] = current_employment_fr_has_id
    
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

    if res.get('email_domain', '').lower()[-3:] == '.fr':
        is_fr = True
        is_fr_present = True
        fr_reasons_present.append("email")
        fr_reasons.append("email")
    
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

    res['has_work_from_hal'] = False
    res['has_work'] = False
    works = soup.find_all('work:work-summary')
    res['nb_works'] = len(works)
    if works:
        res['has_work'] = True
    res['works'] = []
    for work in works:
        current_work = {'ids': []}
        ext_ids = work.find_all('common:external-id')
        for ext_id in ext_ids:
            id_type = ext_id.find('common:external-id-type')
            id_value = ext_id.find('common:external-id-value')
            if id_type and id_value:
                current_work['ids'].append({'id_type': id_type.get_text(), 'id_value': id_value.get_text()})
        source = work.find('common:source-name')
        if source:
            current_work['source'] = source.get_text()
            if current_work['source'].lower().strip() == 'hal':
                res['has_work_from_hal'] = True
            elif current_work['source'].lower().strip() == full_name.lower().strip():
                current_work['source'] = 'author'
            elif len(current_work['source'].strip()) < 1:
                current_work['source'] = 'no source'
        else:
            current_work['source'] = 'no source'
        res['works'].append(current_work)
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
    res['disambiguation_sources'] = []
    res['disambiguation_ids'] = []
    for f in x.find_all('common:disambiguated-organization'):
        source = f.find('common:disambiguation-source').get_text().lower()
        value = f.find('common:disambiguated-organization-identifier').get_text()
        if source not in res['disambiguation_sources']:
            res['disambiguation_sources'].append(source)
        res[source] = value
        if value not in res['disambiguation_ids']:
            res['disambiguation_ids'].append(value)
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
