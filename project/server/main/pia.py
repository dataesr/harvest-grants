import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, get_ods_data, get_all_struct, build_correspondance_structures
from project.server.main.anr import URL_ANR_PROJECTS_DGPIE
from project.server.main.logger import get_logger

logger = get_logger(__name__)

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

def harvest_pia_projects():
    df_pia_anr = pd.read_json(URL_ANR_PROJECTS_DGPIE, orient='split')
    code_decision = 'Projet.Code_Decision'
    pia_anr_code = set(df_pia_anr[code_decision].apply(lambda x:x[4:]).to_list())
    df_pia = get_ods_data('fr-esr-piaweb')
    df_pia = df_pia[df_pia['code_projet'].apply(lambda x: x not in pia_anr_code)]
    # for ids
    df_paysage = get_ods_data('fr-esr-paysage_structures_identifiants')
    df_paysage = df_paysage.set_index('id_paysage')
    all_struct = get_all_struct()
    corresp = build_correspondance_structures(all_struct)

    df_projects = df_pia[['code_projet', 'acronyme',  'domaine_thematique',
        'strategie_nationale', 'action', 'libelle', 'resumes', 'debut_du_projet']].drop_duplicates()
    projects = []
    for e in df_projects.to_dict(orient='records'):
        new_elt = {}
        project_id = e['code_projet'].replace(' ', '_').lower().replace('é', 'e').replace('è', 'e')
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
            new_elt['description'] = {'en': e['resumes']}
        projects.append(new_elt)
    partners = []
    nb_partners_map = {}
    for e in df_pia.to_dict(orient='records'):
        new_part = {}
        new_part['project_id'] = project_id
        if project_id not in nb_partners_map:
            nb_partners_map[project_id] = 0
        nb_partners_map[project_id] += 1
        new_part['project_type'] = project_type
        new_part['id'] = project_id + '-' + str(nb_partners_map[project_id]).zfill(2)
        part_id = None
        if isinstance(e.get("etablissement"), str):
            new_part['name'] = e["etablissement"]
        new_part['role'] = 'participant'
        if isinstance(e.get('coordinateur_oui_non'), str):
            if e["coordinateur_oui_non"] == "Oui":
                new_part['role'] = 'coordinator'
        if isinstance(e.get('id_paysage'), str):
            paysage_id = e['id_paysage'].split(',')[0]
            part_id = get_pid(paysage_id, df_paysage, corresp)
        if part_id:
            new_part['participant_id'] = part_id
            new_part['organizations_id'] = part_id
            new_part['identified'] = True
        else:
            new_part['identified'] = False
        partners.append(new_part)
    return {'projects': projects, 'partners': partners}
    
