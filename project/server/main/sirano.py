import pandas as pd
import os
import hashlib
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, to_int, to_float
from project.server.main.logger import get_logger

logger = get_logger(__name__)

URL_SIRANO = 'https://www.data.gouv.fr/api/1/datasets/r/56589f33-b66b-4b00-ae5c-fe9dcdc9a6e3'
# check URL on a regular basis from https://www.data.gouv.fr/datasets/projets-de-recherche-appliquee-en-sante-finances-dans-le-cadre-des-appels-a-projets-du-ministere-charge-de-la-sante/

def update_sirano(args, cache_participant):
    reset_db('SIRANo', 'projects')
    new_data_sirano = harvest_sirano_projects()
    post_data(new_data_sirano)

def harvest_sirano_projects(cache_participant):
    projects, partners = [], []
    df1 = pd.read_excel(URL_SIRANO)
    df1.numero_tranche = df1.numero_tranche.apply(lambda x:to_int(x))
    df1.financement_total = df1.financement_total.apply(lambda x:to_float(x))
    df_sirano = pd.concat([df1]).drop_duplicates()
    for e in df_sirano.to_dict(orient='records'):
        new_elt = {}
        if e['appel_a_projets'] != e['appel_a_projets']:
            continue
        if e['annee_de_selection'] != e['annee_de_selection']:
            continue
        year = int(e['annee_de_selection'])
        title_hash = hashlib.md5(e['titre'].encode()).hexdigest()
        project_id = (str(e['appel_a_projets'])+'-'+str(year)+'-'+e['acronyme']+'-'+title_hash[0:3]).upper()
        new_elt['id'] = project_id
        project_type = 'SIRANo'
        new_elt['type'] = project_type
        new_elt['year'] = year

        new_elt['name'] = {}
        new_elt['name']['fr'] = e['titre']
        #new_elt['action'] = [{'level': '1', 'code': e.get('Nom AAP & millésime'), 'name': e.get('Nom AAP & millésime')}]
        description = {}
        if isinstance(e.get('numero_registre_essais'), str):
            description['fr'] = e['numero_registre_essais']
        if description:
            new_elt['description'] = description
        new_elt['budget_financed'] = e.get('financement_total')
        person = {}
        if isinstance(e.get('nom_porteur'), str):
            person['last_name'] = e.get('nom_porteur')
            person['role'] = 'coordinator'
        if isinstance(e.get('prenom_porteur'), str):
            person['first_name'] = e.get('prenom_porteur')
        if person:
            new_elt['persons'] = [person]
        projects.append(new_elt)

        new_part = {}
        new_part['id'] = project_id+'-01'
        new_part['project_id'] = project_id
        new_part['project_type'] = project_type
        part_id = None
        if isinstance(e.get("nom_etablissement"), str):
            new_part['name'] = e["nom_etablissement"]
            new_part['role'] = 'coordinator'
        if new_part.get('name'):
            part_id = identify_participant(new_part['name'], cache_participant)
        if part_id:
            new_part['participant_id'] = part_id
            new_part['organizations_id'] = part_id
            new_part['identified'] = True
        else:
            new_part['identified'] = False
        address = {}
        partners.append(new_part)
    return {'projects': projects, 'partners': partners}


