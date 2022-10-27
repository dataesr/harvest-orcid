import pandas as pd
import bs4
from bs4 import BeautifulSoup
import os
import lxml

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def parse_all(base_path, filter_fr = True):
    logger.debug(f'parsing {base_path} files')
    orcids_info = []
    ix = 0
    for notice in os.listdir(base_path):
        try:
            parsed = parse_notice(notice, base_path)
            if filter_fr and "FR" not in (parsed['previous_countries'] + parsed['current_countries']):
                continue
            orcids_info.append(parsed)
            ix += 1
        except:
            logger.debug("error reading "+notice)
    df = pd.DataFrame(orcids_info)
    filename = base_path.split('/')[-1]
    df.to_json(f'{base_path}/{filename}.jsonl', lines=True, orient='records')
    return df


def get_soup(notice_path, base_path):
    soup = BeautifulSoup(open(base_path+notice_path, 'r'), 'lxml')
    return soup
    
def parse_notice(notice_path, base_path, verbose = False):
    
    soup = get_soup(notice_path, base_path)
    creation_date = soup.find("history:submission-date").text[0:10]
    orcid = notice_path.split('.')[0]
    
    first_name, last_name, current_address = None, None, None
    
    name1 = soup.find("personal-details:given-names")
    if name1:
        first_name = name1.text
    
    name2 = soup.find("personal-details:family-name")
    if name2:
        last_name = name2.text
    
    country = soup.find("address:country")
    if country:
        current_address = country.text
        
    employments = get_employment_countries(soup)
    educations = get_education_countries(soup)
    
    current_fr = False
    fr_reasons = []
    if current_address == "FR":
        current_fr = True
        fr_reasons.append("address")
        
    if "FR" in employments["current_countries"]:
        current_fr = True
        fr_reasons.append("employments")
        
    if "FR" in educations["current_countries"] :
        current_fr = True
        fr_reasons.append("educations")
        
    fr_reasons.sort()
    current_countries = employments["current_countries"] + educations["current_countries"]
    if current_address:
        current_countries.append(current_address)
    return {
        "orcid": orcid,
        "creation_date": creation_date,
        "first_name": first_name,
        "last_name": last_name,
        "current_address_country": current_address,
        "previous_countries_employment": employments["previous_countries"],
        "current_countries_employment": employments["current_countries"],
        "previous_countries_education": educations["previous_countries"],
        "current_countries_education": educations["current_countries"],
        "current_countries": list(set(current_countries)),
        "previous_countries": list(set(employments["previous_countries"] + educations["previous_countries"])),
        "fr_reason": fr_reasons
    }

def get_employment_countries(soup):
    employments = soup.find("activities:employments")
    current_countries = []
    previous_countries = []
    for c in employments.children:
        if not isinstance(c, bs4.element.Tag):
            continue

        country = c.find('common:country')
        if country is None:
            continue

        end_date = c.find("common:end-date")
        if end_date:
            end_str = end_date.text.strip()
            previous_countries.append(country.text)
        else:
            current_countries.append(country.text)

    return {"previous_countries": list(set(previous_countries)), 
            "current_countries": list(set(current_countries))
           }

def get_education_countries(soup):
    educations = soup.find("activities:educations")
    current_educations = []
    previous_educations = []
    for c in educations.children:
        if not isinstance(c, bs4.element.Tag):
            continue

        country = c.find('common:country')
        if country is None:
            continue

        end_date = c.find("common:end-date")
        if end_date:
            end_str = end_date.text.strip()
            previous_educations.append(country.text)
        else:
            current_educations.append(country.text)

    return {"previous_countries": list(set(previous_educations)), 
            "current_countries": list(set(current_educations))
           }
