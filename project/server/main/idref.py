import pandas as pd
import pickle
from SPARQLWrapper import SPARQLWrapper, JSON
from project.server.main.logger import get_logger

logger = get_logger(__name__)

sparql = SPARQLWrapper("https://data.idref.fr/sparql")

QUERY_START = """
SELECT ?idref ?ext_id
WHERE {?idref owl:sameAs ?ext_id.
?idref a foaf:Person.
FILTER (STRSTARTS(STR(?ext_id),
"""

def get_matches(uri_prefix):
    QUERY_END = f"'{uri_prefix}'))" + "}"
    query = QUERY_START+QUERY_END
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    result = sparql.query().convert()
    return result['results']['bindings']

def get_data_from_idref():
    data_orcid = []
    orcid_matches = get_matches('https://orcid.org')
    for r in orcid_matches:
        idref = 'idref'+r['idref']['value'].split('/')[3]
        orcid = r['ext_id']['value'].split('/')[3].split('#')[0]
        if orcid[0:2] != '00':
            continue
        data_orcid.append({'idref': idref, 'orcid': orcid})
    logger.debug(f'correspondance idref - orcid : {len(data_orcid)}')

    data_id_hal = []
    id_hal_matches = get_matches('https://data.archives-ouvertes.fr')
    for r in id_hal_matches:
        idref = 'idref'+r['idref']['value'].split('/')[3]
        id_hal_s = r['ext_id']['value'].split('/')[4].split('#')[0]
        data_id_hal.append({'idref': idref, 'id_hal_s': id_hal_s})
    logger.debug(f'correspondance idref - id_hal : {len(data_id_hal)}')
    df = pd.merge(pd.DataFrame(data_orcid), pd.DataFrame(data_id_hal), on='idref', how='outer')
    df['idref_abes'] = df['idref']
    df['has_idref_abes'] = True
    df['id_hal_abes'] = df['id_hal_s']
    df['has_id_hal_abes'] = df['id_hal_s'].apply(lambda x: isinstance(x, str))
    orcid_dict = {}
    for row in df.itertuples():
        if row.orcid==row.orcid:
            orcid_dict[str(row.orcid)] = {'idref': row.idref, 'id_hal_s': row.id_hal_s, 'orcid': str(row.orcid)}
    pickle.dump(orcid_dict, open('/upw_data/orcid_idref_dict.pkl', 'wb'))
    for f in ['idref', 'id_hal_s']:
        del df[f]
    df.to_json('/upw_data/data_idref.jsonl', lines=True, orient='records')
