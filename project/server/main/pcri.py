import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, get_ods_data, get_all_struct, build_correspondance_structures, to_jsonl
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def update_pcri(args, cache_participant):
    #reset_db(project_type, 'projects')
    #reset_db(project_type, 'participations')
    new_data_pcri = harvest_pcri_projects()
    to_jsonl(new_data_pcri, 'projects.jsonl')
    #post_data(new_data_pia)

def get_part_dict():
    df_horizon_part = get_ods_data('fr-esr-horizon-projects-entities')
    df_h2020_part = get_ods_data('fr-esr-h2020-projects-entities')
    columns_part = ['project_id', 'fund_eur', 'entities_id', 'role', 'entities_name', 'participates_as', 'participation_linked', 'country_code', 'country_name_fr', 'numero_national_de_structure']
    df_eu_part = pd.concat([df_horizon_part, df_h2020_part])[columns_part]
    part_dict = {}
    for e in df_eu_part.to_dict(orient='records'):
        if str(e['project_id']) not in part_dict:
            part_dict[str(e['project_id'])] = []
        current_elt = {}
        for f in ['project_id', 'fund_eur', 'entities_id', 'role', 'entities_name', 'participates_as', 'participation_linked', 'country_code', 'country_name_fr']:
            if e.get(f):
                current_elt[f] = e[f]
        part_dict[str(e['project_id'])].append(current_elt)
        if isinstance(e.get('numero_national_de_structure'), str):
            for rnsr in e['numero_national_de_structure'].strip().split(';'):
                new_elt = current_elt.copy()
                new_elt['entities_id'] = rnsr
                new_elt['entities_name'] = f'labo {rnsr} from '+current_elt.get('entities_name', '')
                part_dict[str(e['project_id'])].append(new_elt)
    return part_dict

def get_participants(project_id, part_dict):
    participants = []
    if project_id not in part_dict:
        return []
    for part in part_dict[project_id]:
        participation = {}
        try:
            participation['funding'] = float(part['fund_eur'])
        except:
            pass
        participation['structure'] = part['entities_id']
        participation['role'] = part['role'].lower()
        participation['label'] = {'default': part['entities_name'] + '__-__' + str(part['entities_id'])}
        participation['participates_as'] = part['participates_as']
        address = {}
        if isinstance(part.get('country_code'), str):
            address['country_code'] = part['country_code']
        if isinstance(part.get('country_name_fr'), str):
            address['country'] = part['country_name_fr']
        if address:
            participation['address'] = address
        participants.append(participation)
    return participants

def harvest_pcri_projects():
    df_projects = get_ods_data('fr-esr-all-projects-signed-informations')
    projects = []
    part_dict = get_part_dict()
    for e in df_projects.to_dict(orient='records'):
        project = {}
        project['type'] = e['framework']
        project['year'] = e['call_year']
        project['id'] = str(e['project_id'])
        project['participants'] = get_participants(project['id'], part_dict)
        if isinstance(e.get('start_date'), str):
            project['startDate'] = e['start_date']+'T00:00:00'
        if isinstance(e.get('end_date'), str):
            project['endDate'] = e['end_date']+'T00:00:00'
        if isinstance(e.get('signature_date'), str):
            project['signatureDate'] = e['signature_date']+'T00:00:00'
        if isinstance(e.get('duration'), int):
            project['duration'] = e['duration']
        if isinstance(e.get('abstract'), str):
            project['description'] = {'default': e['abstract']}
        if isinstance(e.get('acronym'), str):
            project['acronym'] = {'default': e['acronym']}
        if isinstance(e.get('title'), str):
            project['label'] = {'default': e['title']}
        if isinstance(e.get('project_numberofparticipants'), int):
            project['participantCount'] = e['project_numberofparticipants']
        if isinstance(e.get('project_totalcost'), float):
            project['budgetTotal'] = e['project_totalcost']
        if isinstance(e.get('project_eucontribution'), float):
            project['budgetFinanced'] = e['project_eucontribution']
        if isinstance(e.get('free_keywords'), str):
            project['keywords'] = {'en': e['free_keywords'].split('|')}
        if e.get('action_code'):
            project["action"] = {
                "id": e['action_code'],
                "label": {
                  "default": e['action_name']
                },
                "level": "1"
              }
            project['instrument'] = e['action_name']
        if isinstance(e.get('pilier_global_name'), str):
            project['pilier_global_name'] = e['pilier_global_name']
        if isinstance(e.get('call_id'), str):
            project['call'] = {'id':e['call_id']}
        priorities = []
        for f in ['pilier_name_en', 'programme_name_en', 'thema_name_en', 'destination_name_en']:
            if isinstance(e.get(f), str):
                new_prio = {
                  "type": "priorities",
                  "label": {
                    "default": e[f]
                  }
                }
            if new_prio not in priorities:
                priorities.append(new_prio)
        if isinstance(e.get('topic_name'), str):
            new_prio = {
                  "type": "topic",
                  "label": {
                    "default": e['topic_name']
                  }
                }
            if new_prio not in priorities:
                priorities.append(new_prio)
        project['priorities'] = priorities
        if isinstance(e.get('cordis_project_webpage'), str):
            project['url'] = e['cordis_project_webpage']
        projects.append(project)
    logger.debug(f'{len(projects)} PCRI projects prepared')
    return projects
