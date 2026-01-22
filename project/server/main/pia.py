import pandas as pd
import re
import os
import requests
from retry import retry
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, get_ods_data, get_all_struct, build_correspondance_structures, transform_scanr
from project.server.main.anr import URL_ANR_PROJECTS_DGPIE
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def update_pia_v2(args):
    new_data = harvest_pia_projects()
    transform_scanr(new_data)

project_type = 'PIA hors ANR'
def update_pia(args):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')
    new_data_pia = harvest_pia_projects()
    post_data(new_data_pia)

def get_pid(x, df_paysage, corresp):
    try:
        siret = df_paysage[(df_paysage.index==x) & (df_paysage.id_type=='siret')].id_value.values[0]
        siren = siret[0:9]
        return corresp[siren]
    except:
        return None

def clean_project_id(x):
    return x.strip().replace(' ', '-').lower().replace('--', '-').replace('é', 'e').replace('è', 'e')

@retry(delay=20, tries=3)
def harvest_pia_projects():
    df_pia_anr = pd.read_json(URL_ANR_PROJECTS_DGPIE, orient='split')
    code_decision = 'Projet.Code_Decision'
    # get code from DGPIE ANR open data, removing prefix ANR-
    pia_anr_code = set(df_pia_anr[code_decision].apply(lambda x:clean_project_id(x[4:])).to_list())
    logger.debug(f'{len(pia_anr_code)} PIA ANR code from DGPIE open data')
    df_pia = get_ods_data('fr-esr-piaweb')
    df_pia['code_projet'] = df_pia['code_projet'].apply(lambda x:clean_project_id(x))
    logger.debug(f'Total data from piaweb = {len(df_pia)} lines')
    df_pia = df_pia[df_pia['code_projet'].apply(lambda x: x not in pia_anr_code)]
    logger.debug(f'Data from piaweb after removing known PIA ANR codes = {len(df_pia)} lines')
    # for ids
    #df_paysage = get_ods_data('fr-esr-paysage_structures_identifiants')
    #df_paysage = df_paysage.set_index('id_paysage')
    #all_struct = get_all_struct()
    #corresp = build_correspondance_structures(all_struct)
    df_projects = df_pia[['code_projet', 'acronyme',  'domaine_thematique', 'dotation',
        'strategie_nationale', 'action', 'libelle', 'resumes', 'debut_du_projet']].drop_duplicates()
    # trouver la dotation globale et la remettre sur chaque participant
    dotation_map = {}
    for e in df_projects.to_dict(orient='records'):
        project_id = e['code_projet']
        if isinstance(e.get('dotation'), float) and e['dotation'] == e['dotation']:
            if project_id in dotation_map:
                logger.debug(f'already in dotation map for {e}')
            dotation_map[project_id] = e['dotation']
    projects = []
    known_ids = []
    for e in df_projects.to_dict(orient='records'):
        new_elt = {}
        project_id = e['code_projet']
        if project_id in known_ids:
            continue
        new_elt['id'] = project_id
        new_elt['type'] = project_type
        if isinstance(e.get('debut_du_projet'), str):
            try:
                year = int(e['debut_du_projet'][0:4])
                new_elt['year'] = year
            except:
                pass
        acronym, title = None, None
        if isinstance(e.get('acronyme'), str):
            acronym = e['acronyme']
            new_elt['acronym'] = acronym
        if isinstance(e.get('libelle'), str):
            title = e['libelle']
            new_elt['name'] = {'en': title}
        elif acronym:
            new_elt['name'] = {'en': acronym}
        if isinstance(e.get('action'), str):
            new_elt['action'] = [{'level': '1', 'code': e.get('action'), 'name': e.get('action')}]
        if isinstance(e.get('resumes'), str):
            new_elt['description'] = {'en': e['resumes'].replace('_x000D_', '')}
        if project_id in dotation_map:
            dotation = dotation_map[project_id]
            new_elt['budget_total'] = dotation
            new_elt['budget_financed'] = dotation
        projects.append(new_elt)
        known_ids.append(project_id)
    
    partners = []
    nb_partners_map = {}
    for e in df_pia.to_dict(orient='records'):
        new_part = {}
        project_id = e['code_projet']
        new_part['project_id'] = project_id
        if project_id not in nb_partners_map:
            nb_partners_map[project_id] = 0
        nb_partners_map[project_id] += 1
        new_part['project_type'] = project_type
        new_part['id'] = project_id + '-' + str(nb_partners_map[project_id]).zfill(2)
        if isinstance(e.get("etablissement"), str):
            new_part['name'] = e["etablissement"]
        new_part['role'] = 'participant'
        if isinstance(e.get('coordinateur_oui_non'), str):
            if e["coordinateur_oui_non"] == "Oui":
                new_part['role'] = 'coordinator'
        paysage_ids = [None]
        if isinstance(e.get('id_paysage'), str):
            paysage_ids = re.split(r"[;,]", e['id_paysage'])
            if len(paysage_ids)==0:
                paysage_ids = [None]
        for paysage_id in paysage_ids:
            new_part_to_add = new_part.copy()
            part_id = paysage_id #get_pid(paysage_id, df_paysage, corresp)
            if part_id:
                part_id = part_id.strip()
                new_part_to_add['participant_id'] = part_id
                new_part_to_add['organizations_id'] = part_id
                new_part_to_add['identified'] = True
            else:
                new_part_to_add['identified'] = False
            if new_part_to_add not in partners:
                partners.append(new_part_to_add)
    return {'projects': projects, 'partners': partners}
    
