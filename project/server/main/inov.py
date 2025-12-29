import pandas as pd
import os
import hashlib
import requests
from retry import retry
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, to_int, to_float, transform_scanr
from project.server.main.logger import get_logger

logger = get_logger(__name__)

URL_INOV = 'https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-laureats-concours-i-nov/exports/csv/?delimiters=%3B&lang=fr&timezone=Europe%2FParis&use_labels=true'

def update_inov_v2(args, cache_participant):
    new_data = harvest_inov_projects(cache_participant)
    transform_scanr(new_data)

project_type = 'i-NOV'
def update_inov(args, cache_participant):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')
    new_data = harvest_inov_projects(cache_participant)
    post_data(new_data)

@retry(delay=20, tries=3)
def harvest_inov_projects(cache_participant):
    projects, partners = [], []
    df_inov = pd.read_csv(URL_INOV, sep=';')
    projects, partners = [], []
    for e in df_inov.to_dict(orient='records'):
        new_elt = {}
        new_elt['type'] = project_type
        title = None
        acronym = None
        
        if isinstance(e.get('Dénomination du projet lauréat'), str):
            acronym = e['Dénomination du projet lauréat']
            new_elt['acronym'] = acronym
            title=acronym
        else:
            continue
        if isinstance(e.get('Slogan du projet lauréat'), str):
            title = e.get('Slogan du projet lauréat')
        new_elt['name'] =  {'fr': title, 'default': title}
        
        title_hash = hashlib.md5(title.encode()).hexdigest()
        project_id = None
        project_id = 'iNOV-' + title_hash[-10:]
        if project_id:
            new_elt['id'] = project_id
        else:
            continue

        if isinstance(e.get('Date de publication des résultats'), str):
            new_elt['year'] = int(e.get('Date de publication des résultats')[0:4])
        if isinstance(e.get('Résumé du projet lauréat'), str):
            new_elt['description'] = {'fr': e['Résumé du projet lauréat'], 'default': e['Résumé du projet lauréat']}
            
        if isinstance(e.get('Montant de la subvention accordée (en €)'), float):
            if e.get('Montant de la subvention accordée (en €)') == e.get('Montant de la subvention accordée (en €)'):
                new_elt['budget_financed'] = e.get('Montant de la subvention accordée (en €)')
        if isinstance(e.get('Montant total du projet lauréat (en €)'), float):
            if e.get('Montant total du projet lauréat (en €)') == e.get('Montant total du projet lauréat (en €)'):
                new_elt['budget_total'] = e.get('Montant total du projet lauréat (en €)')

        if new_elt['id'] not in [k['id'] for k in projects]:
            projects.append(new_elt)
        else:
            continue
        
        if isinstance(e.get('Entreprise'), str):
            new_elt_partner = {}
            new_elt_partner['id'] = project_id + '-01'
            new_elt_partner['project_id'] = project_id
            new_elt_partner['project_type'] = project_type
            siren = str(e["Identifiant SIREN de l'entreprise lauréate"]).replace('.0', '')
            part_id = None
            new_elt_partner['name'] = e.get('Entreprise')
            if len(siren)==9:
                part_id = siren
            else:
                part_id = identify_participant(new_elt['name'], cache_participant)
            if part_id:
                new_elt_partner['participant_id'] = part_id
                new_elt_partner['organizations_id'] = part_id
                new_elt_partner['identified'] = True
            else:
                new_elt_partner['identified'] = False
            if new_elt_partner not in partners:
                partners.append(new_elt_partner)

    return {'projects': projects, 'partners': partners}


