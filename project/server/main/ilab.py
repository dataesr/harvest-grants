import pandas as pd
import os
import hashlib
import requests
from retry import retry
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, to_int, to_float, transform_scanr
from project.server.main.logger import get_logger

logger = get_logger(__name__)

URL_ILAB = 'https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-laureats-concours-national-i-lab/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'

def update_ilab_v2(args, cache_participant):
    new_data = harvest_ilab_projects(cache_participant)
    transform_scanr(new_data)

project_type = 'i-LAB'
def update_ilab(args, cache_participant):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')
    new_data = harvest_ilab_projects(cache_participant)
    post_data(new_data)

@retry(delay=20, tries=3)
def harvest_ilab_projects(cache_participant):
    projects, partners = [], []
    df_ilab = pd.read_csv(URL_ILAB, sep=';')
    projects, partners = [], []
    for e in df_ilab.to_dict(orient='records'):
        new_elt = {}
        new_elt['type'] = project_type
        title = None
        acronym = None
        if isinstance(e.get('Projet'), str):
            acronym = e['Projet']
            new_elt['acronym'] = acronym
        if isinstance(e.get('Moto'), str) :
            title=e['Moto']
        elif isinstance(e.get('Libellé entreprise'), str) :
            title = e['Libellé entreprise']
        else:
            continue
        new_elt['name'] =  {'fr': title, 'default': title}
        title_hash = hashlib.md5(title.encode()).hexdigest()
        project_id = None
        if isinstance(e.get('Identifiant'), str):
            project_id = 'iLAB-' + e['Identifiant'].replace(' ', '')
        else:
            project_id = 'iLAB-' + title_hash[-10:]
        if project_id:
            new_elt['id'] = project_id
        else:
            continue
        if isinstance(e.get('Année de concours'), int):
            new_elt['year'] = e['Année de concours']
        if isinstance(e.get('Résumé'), str):
            new_elt['description'] = {'fr': e['Résumé'], 'default': e['Résumé']}

        person = {}
        if e.get('Nom du lauréat'):
            person['last_name'] = e.get('Nom du lauréat')
            person['role'] = 'coordinator'
        if e.get('Prénom du candidat'):
            person['first_name'] = e.get('Prénom du candidat')
        if person:
            new_elt['persons'] = [person]

        projects.append(new_elt)

        if isinstance(e.get('Libellé entreprise'), str):
            new_elt_partner = {}
            new_elt_partner['id'] = project_id + '-01'
            new_elt_partner['project_id'] = project_id
            new_elt_partner['project_type'] = project_type
            siren = str(e['N° SIREN']).replace('.0', '')
            part_id = None
            new_elt_partner['name'] = e.get('Libellé entreprise')
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

        if isinstance(e.get('Unité de recherche liée au projet'), str):
            new_elt_partner = {}
            new_elt_partner['id'] = project_id + '-02'
            new_elt_partner['project_id'] = project_id
            new_elt_partner['project_type'] = project_type
            rnsr = str(e["Id de l'unité de recherche liée au projet"]).replace('.0', '')
            part_id = None
            new_elt_partner['name'] = e.get('Unité de recherche liée au projet')
            if len(rnsr)==10:
                part_id = rnsr
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
