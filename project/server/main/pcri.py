import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, get_ods_data, get_all_struct, build_correspondance_structures
from project.server.main.anr import URL_ANR_PROJECTS_DGPIE
from project.server.main.logger import get_logger

logger = get_logger(__name__)

project_type = 'PIA hors ANR'
def update_pcri(args):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')
    new_data_pia = harvest_pia_projects()
    post_data(new_data_pia)

def harvest_pcri_projects():
    df_projects = get_ods_data('fr-esr-all-projects-signed-informations')
    
    df_horizon_part = get_ods_data('fr-esr-horizon-projects-entities')
    part_dict = {}
    for e in df_horizon_part.to_dict(orient='records'):
        if str(e['project_id']) not in part_dict:
            part_dict[str(e['project_id'])] = []
        part_dict[str(e['project_id'])].append(e)
    
    projects = []
    for e in df_projects.to_dict(orient='records'):
        project = {}
        project['type'] = e['framework']
        project['year'] = e['call_year']
        project['id'] = str(e['project_id'])
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
            project['title'] = {'default': e['title']}
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


    
