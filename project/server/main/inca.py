import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data
from project.server.main.logger import get_logger

logger = get_logger(__name__)

#URL_INCA_2020_2021 = 'https://www.data.gouv.fr/api/1/datasets/r/14df9170-a0f9-4d52-8f91-ebecb8fcfc30'
#URL_INCA_2008_2019 = 'https://www.data.gouv.fr/api/1/datasets/r/9f5ab856-9b65-4446-a014-474e76fcd4db'
#URL_INCA_2022 = 'https://www.data.gouv.fr/api/1/datasets/r/9411c01a-5c91-467f-846c-70c9f2631c0c'
URL_INCA = 'https://www.data.gouv.fr/api/1/datasets/r/478b1659-1b3c-4cd6-8f47-190a5bf542a9'

def update_inca(args, cache_participant):
    reset_db('INCa', 'projects')
    reset_db('INCa', 'participations')
    new_data_inca = harvest_inca_projects(cache_participant)
    post_data(new_data_inca)

def harvest_inca_projects(cache_participant):
    projects, partners = [], []
    #df1 = pd.read_excel(URL_INCA_2020_2021)
    #df2 = pd.read_excel(URL_INCA_2008_2019)
    #df3 = pd.read_excel(URL_INCA_2022)
    #df_inca = pd.concat([df1, df2, df3]).drop_duplicates()
    df_inca = pd.read_excel(URL_INCA).drop_duplicates()
    for e in df_inca.to_dict(orient='records'):
        new_elt = {}
        project_id = str(e['N° subvention']).replace('\xa0', '')
        if 'INCa' not in project_id:
            project_id = 'INCa-'+project_id
        new_elt['id'] = project_id
        project_type = 'INCa'
        new_elt['type'] = project_type
        year = None
        try:
            tmp = int(e.get('Nom AAP & millésime')[-2:])
            if tmp < 30:
                year = int('20'+e.get('Nom AAP & millésime')[-2:])
        except:
            try:
                for y in range(0, 30):
                    candidate = str(y).zfill(2)
                    if candidate in e.get('Nom AAP & millésime'):
                        year = int('20'+candidate)
            except:
                print(e.get('Nom AAP & millésime'))

        if year:
            new_elt['year'] = year

        new_elt['name'] = {}
        if isinstance(e.get('Titre du projet'), str):
            new_elt['name']['en'] = e.get('Titre du projet')
        description = {}
        if isinstance(e.get('Résumé en anglais ou en français'), str):
            description['fr'] = e.get('Résumé en anglais ou en français')
        if isinstance(e.get('Résumé en anglais'), str):
            description['en'] = e.get('Résumé en anglais')
        if isinstance(e.get('Résumé en français'), str):
            description['fr'] = e.get('Résumé en français')
        if description:
            new_elt['description'] = description
        if isinstance(e.get('Nom AAP & millésime'), str):
            new_elt['action'] = [{'level': '1', 'code': e.get('Nom AAP & millésime'), 'name': e.get('Nom AAP & millésime')}]

        if isinstance(e.get('Montant attribué'), float) or isinstance(e.get('Montant attribué'), int):
            if e.get('Montant attribué') == e.get('Montant attribué'):
                new_elt['budget_financed'] = float(e.get('Montant attribué'))
        elif isinstance(e.get('Montant attribué'), str):
            new_elt['budget_financed'] = float(e.get('Montant attribué').replace('€', '')\
                                               .replace('\xa0', '').replace(',', '.')\
                                               .replace(' ', '').strip())
        person = {}
        if isinstance(e.get('Nom du porteur'), str):
            person['last_name'] = e.get('Nom du porteur')
            person['role'] = 'coordinator'
        if isinstance(e.get('Prénom du porteur'), str):
            person['first_name'] = e.get('Prénom du porteur')
        if person:
            new_elt['persons'] = [person]
        projects.append(new_elt)

        new_part = {}
        new_part['id'] = project_id+'-01'
        new_part['project_id'] = project_id
        new_part['project_type'] = project_type
        part_id = None
        if isinstance(e.get("Laboratoire d'appartenance"), str):
            new_part['name'] = e["Laboratoire d'appartenance"]
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
        if isinstance(e['Ville'], str):
            address['city'] = e['Ville']
        if address:
            new_part['address'] = address
        partners.append(new_part)
    return {'projects': projects, 'partners': partners}


