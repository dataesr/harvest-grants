import pandas as pd
import os
import hashlib
import requests
from retry import retry
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, to_int, to_float, transform_scanr
from project.server.main.logger import get_logger

logger = get_logger(__name__)

URL_IPHD = 'https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-laureats-concours-i-phd/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'

def update_iphd_v2(args, cache_participant):
    new_data = harvest_iphd_projects(cache_participant)
    transform_scanr(new_data)

project_type = 'i-PHD'
def update_iphd(args, cache_participant):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')
    new_data = harvest_iphd_projects(cache_participant)
    post_data(new_data)

@retry(delay=20, tries=3)
def harvest_iphd_projects(cache_participant):
    projects, partners = [], []
    df_iphd = pd.read_csv(URL_IPHD, sep=';')
    projects, partners = [], []
    for e in df_iphd.to_dict(orient='records'):
        new_elt = {}
        new_elt['type'] = project_type
        title = None
        acronym = None
        if isinstance(e.get('Acronyme du projet'), str):
            acronym = e['Acronyme du projet']
            new_elt['acronym'] = acronym
            title=acronym
        else:
            continue
        new_elt['name'] =  {'fr': title, 'default': title}
        title_hash = hashlib.md5(title.encode()).hexdigest()
        project_id = None
        identifiant = e.get('Numero national de thèse')
        if isinstance(identifiant, str):
            project_id = 'iPHD-' + identifiant.replace(' ', '')
        else:
            project_id = 'iPHD-' + title_hash[-10:]
        if project_id:
            new_elt['id'] = project_id
        else:
            continue
        if isinstance(e.get('Millesime'), int):
            new_elt['year'] = e['Millesime']
        if isinstance(e.get('Résumé du projet'), str):
            new_elt['description'] = {'fr': e['Résumé du projet'], 'default': e['Résumé du projet']}

        person = {}
        if e.get('Nom'):
            person['last_name'] = e.get('Nom')
            person['role'] = 'coordinator'
        if e.get('Prenom'):
            person['first_name'] = e.get('Prenom')
        if person:
            new_elt['persons'] = [person]

        projects.append(new_elt)

        if isinstance(e.get('Structure de transfert de technologie'), str):
            new_elt_partner = {}
            new_elt_partner['id'] = project_id + '-01'
            new_elt_partner['project_id'] = project_id
            new_elt_partner['project_type'] = project_type
            siren = str(e['Code Structure de transfert de technologie']).replace('.0', '')
            part_id = None
            new_elt_partner['name'] = e.get('Structure de transfert de technologie')
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

        if isinstance(e.get('Laboratoire'), str):
            new_elt_partner = {}
            new_elt_partner['id'] = project_id + '-02'
            new_elt_partner['project_id'] = project_id
            new_elt_partner['project_type'] = project_type
            rnsr = str(e["Code Laboratoire"]).replace('.0', '')
            part_id = None
            new_elt_partner['name'] = e.get('Laboratoire')
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
