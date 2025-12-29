import pandas as pd
import os
import hashlib
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, pays_map, transform_scanr
from project.server.main.logger import get_logger

logger = get_logger(__name__)

URL_DIM_MAP_PROJETS_1 = 'https://www.data.gouv.fr/api/1/datasets/r/db9ab6db-d468-4c34-997b-b38842bfd94c'
URL_DIM_MAP_PARTNERS_1 = 'https://www.data.gouv.fr/api/1/datasets/r/36fe1438-9ad2-4497-9f60-aaf4c8682c49'
URL_DIM_MAP_PARTNERS_DESC_1 = 'https://www.data.gouv.fr/api/1/datasets/r/41b7c234-6433-4b32-884d-5c8eae1c3bb9'

URL_DIM_PAMIR_PROJETS_1 = 'https://www.data.gouv.fr/api/1/datasets/r/3f0893d9-2593-4b06-b10f-4697a4f17c18'
URL_DIM_PAMIR_PARTNERS_1 ='https://www.data.gouv.fr/api/1/datasets/r/6810f427-9e60-46b8-865a-105c068bcb4e'
URL_DIM_PAMIR_PARTNERS_DESC_1 = 'https://www.data.gouv.fr/api/1/datasets/r/53bbbe72-54fc-442c-a28a-ccf684ec0be7'

def update_dim_v2(args, cache_participant):
    new_data = harvest_dim_projects(cache_participant)
    transform_scanr(new_data)

project_type = 'DIM Ile-de-France'
def update_dim(args, cache_participant):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')
    new_data_dim = harvest_dim_projects(cache_participant)
    post_data(new_data_dim)

def build_desc_map(df_partners):
    partners_desc_map = {}
    for e in df_partners.to_dict(orient='records'):
        acronym = e['Acronyme de l’entité']
        if acronym in partners_desc_map:
            print('doublon?')
            print(e)
        partners_desc_map[acronym] = e
    return partners_desc_map


def harvest_dim_projects(cache_participant):
    projects, partners = [], []
    df_projets_map_1 = pd.read_csv(URL_DIM_MAP_PROJETS_1, sep=';')
    df_partners_map_1 = pd.read_csv(URL_DIM_MAP_PARTNERS_1, sep=';')
    df_partners_desc_map_1 = pd.read_csv(URL_DIM_MAP_PARTNERS_DESC_1, sep=';')

    df_projets_pamir_1 = pd.read_csv(URL_DIM_PAMIR_PROJETS_1, sep=';')
    df_partners_pamir_1 = pd.read_csv(URL_DIM_PAMIR_PARTNERS_1, sep=';')
    df_partners_desc_pamir_1 = pd.read_csv(URL_DIM_PAMIR_PARTNERS_DESC_1, sep=';')

    partners_desc_map_1 = build_desc_map(df_partners_desc_map_1)
    partners_desc_pamir_1 = build_desc_map(df_partners_desc_pamir_1)

    df_projets_pamir_1.rename(columns={'Année du dépôt du projet': 'Année du dépôt',
                                   'Mode de sélection du projet': 'Mode de sélection',
                                  'Axe(s) méthodologique(s)': 'Axes méthodologiques',
                                  'Champ(s) thématique(s)': 'Champs thématiques',
                                  'Secteur(s) disciplinaire(s)': 'Secteurs disciplinaires'}, inplace=True)


    cols = ['Identifiant du projet', 'Acronyme du projet', 'Titre du projet (fr)',
       'Titre du projet (en)', 'Résumé du projet (fr)',
       'Résumé du projet (en)', 'Secteurs disciplinaires',
       'Axes méthodologiques', 'Champs thématiques', 'Catégorie transverse',
       'Subvention allouée', 'Année du dépôt', 'Mode de sélection',  'Type du projet']

    d1 = df_projets_map_1[cols]
    d2 = df_projets_pamir_1[cols]
    df_dim = pd.concat([d1, d2]).drop_duplicates()

    for e in df_dim.to_dict(orient='records'):
        new_elt = {}
        if not e['Identifiant du projet']:
            continue
        new_elt['id'] = e['Identifiant du projet']
        new_elt['type'] = project_type
        if '-PAMIR-' in new_elt['id']:
            new_elt['action'] = [{'level': '1', 'code': 'DIM-PAMIR', 'name': 'Patrimoines matériels – innovation, expérimentation, résilience'}]
        elif 'DIM-MAP-' in new_elt['id']:
            new_elt['action'] = [{'level': '1', 'code': 'DIM-MAP', 'name': 'Matériaux Anciens et Patrimoniaux'}]
        
        if isinstance(e['Année du dépôt'], int):
            new_elt['year'] = e['Année du dépôt']

        if isinstance(e.get('Acronyme du projet'), str):
            new_elt['acronym'] = e['Acronyme du projet']
        new_elt['name'] = {}
        if isinstance(e.get('Titre du projet (fr)'), str):
            new_elt['name']['fr'] = e['Titre du projet (fr)'].replace("\xa0", " ")
        if isinstance(e.get('Titre du projet (en)'), str):
            new_elt['name']['en'] = e['Titre du projet (en)'].replace("\xa0", " ")

        new_elt['description'] = {}
        if isinstance(e.get('Résumé du projet (en)'), str):
            new_elt['description']['en'] = e['Résumé du projet (en)'].replace("\xa0", " ")
            new_elt['description']['default'] = e['Résumé du projet (en)'].replace("\xa0", " ")
        if isinstance(e.get('Résumé du projet (fr)'), str):
            new_elt['description']['fr'] = e['Résumé du projet (fr)'].replace("\xa0", " ")
            new_elt['description']['default'] = e['Résumé du projet (fr)'].replace("\xa0", " ")
        if isinstance( e.get('Subvention allouée'), float):
            if e.get('Subvention allouée') == e.get('Subvention allouée'):
                new_elt['budget_financed'] = e.get('Subvention allouée')
        projects.append(new_elt)

    cols_partners = ['Identifiant du projet', 'Acronyme de l’entité', 'Rôle de l’entité dans le partenariat']
    df_partners = pd.concat([df_partners_pamir_1[cols_partners], df_partners_map_1[cols_partners]])
    partner_counter = {}
    for e in df_partners.to_dict(orient='records'):
        new_part = {}
        project_id = e['Identifiant du projet']
        if project_id not in partner_counter:
            partner_counter[project_id] = 0
        partner_counter[project_id] += 1
        new_part['id'] = project_id+'-'+str(partner_counter[project_id]).zfill(2)
        new_part['project_id'] = project_id
        new_part['project_type'] = project_type
        acronym_part = e['Acronyme de l’entité']
        new_part['name'] = acronym_part
        if 'DIM-MAP' in project_id and acronym_part in partners_desc_map_1:
            current_part = partners_desc_map_1[acronym_part]
        if 'DIM-PAMIR' in project_id and acronym_part in partners_desc_pamir_1:
            current_part = partners_desc_pamir_1[acronym_part]
        if isinstance(current_part.get('Nom complet de l’entité'), str):
            new_part['name'] = current_part.get('Nom complet de l’entité')
        if isinstance(current_part.get('Identifiant RNSR'), str):
            part_id = current_part['Identifiant RNSR']
        if e.get('Rôle de l’entité dans le partenariat') == 'porteur':
            new_part['role'] = 'coordinator'
        if part_id:
            new_part['participant_id'] = part_id
            new_part['organizations_id'] = part_id
            new_part['identified'] = True
        else:
            new_part['identified'] = False
        address = {}
        if isinstance(current_part.get('Ville'), str):
            address['city'] = current_part.get('Ville')
        if isinstance(current_part.get('Pays'), str):
            if current_part.get('Pays') in pays_map:
                address['country'] = pays_map[current_part.get('Pays')]
        partners.append(new_part)
    return {'projects': projects, 'partners': partners}


