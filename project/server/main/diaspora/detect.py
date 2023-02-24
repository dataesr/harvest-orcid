import pandas as pd
import os
import requests
import json
from retry import retry
from project.server.main.strings import normalize
from project.server.main.concepts import get_concepts
from project.server.main.diaspora.country import *

from project.server.main.logger import get_logger

logger = get_logger(__name__)

#EXCLUDE_NAME = ['Nguyen', 'Wang', 'Zhang', 'Li', 'Liu', 'Chen', 'Luo',
#                'Kim', 'Tran', 'Lee', 'Yang', 'Wu', 'Zhao', 'Sun', 'Peng']
RECENT_YEAR = 2020

os.system('mkdir -p /upw_data/openalex')
os.system('mkdir -p /upw_data/openalex/author_name')
os.system('mkdir -p /upw_data/openalex/author_works')

def fetch_author_name(full_name):
    full_name_normalized = normalize(full_name).replace(' ', '')
    filename = f'/upw_data/openalex/author_name/{full_name_normalized}.json'
    try:
        return json.load(open(filename, 'r'))
    except:
        results = fetch_author_name_openalex(full_name)
        if results:
            json.dump(results, open(filename, 'w'))
        return results

@retry(delay=2, tries=5, backoff=2)
def fetch_author_name_openalex(full_name):
    #r = requests.get(f'https://api.openalex.org/autocomplete/authors?q={full_name}&author_hint=institution')
    r = requests.get(f'https://api.openalex.org/authors?filter=display_name.search:{full_name}')
    results = []
    try:
        results = r.json()['results']
    except:
        logger.debug(full_name)
        logger.debug(r.text)
    return results


def fetch_author_works(author_id):
    filename = f'/upw_data/openalex/author_works/{author_id}.json'
    try:
        return json.load(open(filename, 'r'))
    except:
        results = fetch_author_works_openalex(author_id)
        if results:
            json.dump(results, open(filename, 'w'))
        return results

@retry(delay=2, tries=5, backoff=2)
def fetch_author_works_openalex(author_id):
    r = requests.get(f'https://api.openalex.org/works?filter=author.id:{author_id}')
    results = []
    try:
        results = r.json()['results']
    except:
        logger.debug(author_id)
        logger.debug(r.text)
    return results


def get_authors_openalex(full_name):
    results = fetch_author_name(full_name)
    french, foreign, unknown = [], [], []
    for e in results:
        if normalize(e.get('display_name')) != normalize(full_name):
            continue
        if e.get('last_known_institution') and e.get('last_known_institution').get('country_code'):
            if e['last_known_institution']['country_code'] == 'FR':
                french.append(e)
            else:
                foreign.append(e)
        else:
            unknown.append(e)
    return {'french': french, 'foreign': foreign, 'unknown': unknown}


def get_author_works(author_id):
    results = fetch_author_works(author_id)
    data = []
    for e in results:
        elt = {}
        for f in ['id', 'doi', 'authorships', 'publication_year', 'title', 'concepts']:
            elt[f] = e[f]
        data.append(elt)
    return data

def check_first_publication_year_credible(works, min_publication_year_credible):
    if len(works) == 0:
        return False
    first_publication_year = min([w['publication_year'] for w in works])
    elt['first_publication_year'] = first_publication_year
    # on vérifie si pas de publi avant une date crédible (par ex 4 ans avant la soutenance)
    if first_publication_year < min_publication_year_credible:
        return False
    return True

def analyze_works(works, author_name, author_concepts = []):
    affiliations, recent_affiliations = [], []
    countries, recent_countries, recent_foreign_countries = [], [], []
    all_works, recent_works = [], []
    has_fr = False
    has_foreign = False
    has_fr_recent = False
    has_foreign_recent = False
    has_concept = False
    matching_concepts = []
    openalex_country_diagnosis, seen_raw_affiliation_string = [], []
    for w in works:
        current_work = {
            'id': w['id'],
            'doi': w.get('doi'),
            'publication_year': w.get('publication_year'),
            'title': w.get('title')
        }
        authorships = [a for a in w['authorships'] if normalize(a.get('author', {}).get('display_name')) == normalize(author_name)]
        for aut in authorships:
            if isinstance(aut.get('raw_affiliation_string'), str) and len(aut['raw_affiliation_string']) > 2:
                current_work['raw_affiliation_string'] = aut['raw_affiliation_string']
                affiliations.append(aut['raw_affiliation_string'])
                current_country = get_country(aut['raw_affiliation_string'])
                if isinstance(aut.get('institutions'), list) and (aut['raw_affiliation_string'] not in seen_raw_affiliation_string):
                    openalex_country_diagnosis.append({
                        'raw_affiliation_string': aut['raw_affiliation_string'],
                        'openalex_work_id': w['id'],
                        'openalex_institutions': aut.get('institutions'),
                        'matched_countries': current_country
                        })
                    seen_raw_affiliation_string.append(aut['raw_affiliation_string'])
                current_foreign = [k for k in current_country if k != 'fr']
                if current_country:
                    countries += current_country
                    if 'fr' in current_country:
                        has_fr = True
                    if current_foreign:
                        has_foreign = True

                if w['publication_year'] > RECENT_YEAR:
                    recent_affiliations.append(aut['raw_affiliation_string'])
                    if current_country:
                        recent_countries += current_country
                        if 'fr' in current_country:
                            has_fr_recent = True
                        if current_foreign:
                            has_foreign_recent = True

        
        if isinstance(w.get('concepts'), list):
            for concept in w.get('concepts', []):
                if concept.get('display_name') in author_concepts:
                    matching_concepts.append(concept['display_name'])

        if current_work.get('raw_affiliation_string'):
            all_works.append(current_work)
            if w['publication_year'] > RECENT_YEAR:
                recent_works.append(current_work)

    has_concept = (len(matching_concepts) > 0)

    trust = 'homonym ?'
    if has_fr or has_concept:
        trust = 'no recent paper'
        if has_fr_recent:
            trust = 'academic in france'
        elif has_foreign_recent:
            trust = 'academic abroad'
        if has_fr_recent and has_foreign_recent:
            trust = 'academic mix france/abroad'

    return {'affiliations': list(set(affiliations)), 'recent_affiliations': list(set(recent_affiliations)),
            'countries': list(set(countries)), 'recent_countries': list(set(recent_countries)),
            'has_fr': has_fr, 'has_foreign': has_foreign, 'has_fr_recent': has_fr_recent, 'has_foreign_recent': has_foreign_recent,
            'trust': trust, 'openalex_country_diagnosis': openalex_country_diagnosis,
            'has_concept': has_concept, 'matching_concepts': matching_concepts,
           'all_works': all_works, 'recent_works': recent_works}

def get_potential(full_name, input_data={}, min_publication_year_credible=1980):
    potentials = []
    # noms trop communs
    #for e in EXCLUDE_NAME:
    #    if e.lower() in full_name.lower().split(' '):
    #        return potentials
    # auteurs avec ce full name dans OpenAlex
    concepts = []
    #logger.debug(input_data)
    if 'first_these_discipline' in input_data:
        concepts = get_concepts(input_data['first_these_discipline']['discipline'])
    #logger.debug(concepts)
    data_openalex = get_authors_openalex(full_name)
    has_one_potential = False
    # pour chacun
    openalex_authors = data_openalex['foreign'] + data_openalex['french'] + data_openalex['unknown']
    for d in openalex_authors:
        elt = input_data.copy()
        elt.update(d)
        if d['works_count'] < 2:
            continue
        openalex_id = d['id']
        works = get_author_works(openalex_id.split('/')[-1])
        if works:
            first_publication_year = min([w['publication_year'] for w in works])
            elt['first_publication_year'] = first_publication_year
            # on vérifie si pas de publi avant une date crédible (par ex 4 ans avant la soutenance)
            if first_publication_year < min_publication_year_credible:
                continue
        analyzed = analyze_works(works, d['display_name'], concepts)
        elt.update(analyzed)
        potentials.append(elt)
        has_one_potential = True
    if has_one_potential is False:
        elt = input_data.copy()
        potentials.append(elt)
    return potentials


def analyze_diaspora(df, filename):
    logger.debug(f'{len(df)} lines read')
    potentials = []
    ix = 0
    for row in df.itertuples():
        if row.last_name == row.last_name and len(row.last_name) > 3:
            full_name = f'{row.first_name} {row.last_name}'.replace(',', ' ').replace('  ', ' ')
            input_data = row._asdict()
            potential = get_potential(full_name, input_data = input_data, min_publication_year_credible = row.min_publication_year_credible)
            if potential:
                potentials += potential
        if ix % 1000 == 0:
            logger.debug(f'{ix} treated')
        ix += 1
    pd.DataFrame(potentials).to_json(f'/upw_data/{filename}.jsonl', lines=True, orient='records')


def diaspora_these(year):
    df_theses = pd.read_json('/upw_data/data_abes.jsonl', lines=True)
    df_theses['min_publication_year_credible'] = df_theses.first_these_year.apply(lambda y: y-4) # 4 ans avant la soutenance
    logger.debug(f'{len(df_theses)} theses read')
    df = df_theses[df_theses.first_these_year == year]
    analyze_diaspora(df, f'diaspora_theses_{year}')
    save_countries()

def diaspora_x():
    df_x = pd.read_csv('/upw_data/admis_x.csv', sep=';')
    df_x['last_name'] = df_x['Nom']
    df_x['first_name'] = df_x['Prenom']
    df_x['min_publication_year_credible'] = df_x['Année'].apply(lambda y: y+3) # 3 ans après l'admission à l'X
    logger.debug(f'{len(df_x)} admis x read')
    analyze_diaspora(df_x, f'diaspora_x')
    save_countries()
