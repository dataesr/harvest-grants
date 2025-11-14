import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, reset_db_projects_and_partners
from project.server.main.logger import get_logger

URL_ANSES_PROJECTS = 'https://www.data.gouv.fr/api/1/datasets/r/ea1a1cc1-911f-4b0d-84ba-3d6447c255d7'
URL_ANSES_PARTNERS= 'https://www.data.gouv.fr/api/1/datasets/r/0c4252ad-b1dc-4e1d-84da-fd8afc4094fb'

logger = get_logger(__name__)

project_type = 'ANSES'
def update_anses(args, cache_participant):
    reset_db_projects_and_partners(project_type)
    new_data_anses = harvest_anses_projects(args, cache_participant)
    post_data(data = new_data_anses)

def get_person_map(df_partners):
    person_map = {}
    for e in df_partners.to_dict(orient='records'):
        code_decision = e['code convention homogénéisé']
        if code_decision not in person_map:
            person_map[code_decision] = []

        suffixes = ['']
        for k in range(1, 14):
            suffixes.append(f'.{k}')
        for suf in suffixes:
            person = {}
            name_field = f'Nom Responsable Scientifique{suf}'
            firstname_field = f'Prénom Responsable Scientifique{suf}'
            if isinstance(e[name_field], str):
                person['last_name'] = e[name_field]
            if isinstance(e[firstname_field], str):
                person['first_name'] = e[firstname_field]
            if person and suf=='':
                person['role'] = 'coordinator'
            elif person:
                person['role'] = 'participant'
            if person and person not in person_map[code_decision]:
                person_map[code_decision].append(person)
    return person_map

def harvest_anses_projects(project_type, cache_participant):
    df_projects = pd.read_csv(URL_ANSES_PROJECTS, sep=';', encoding='iso-8859-1')
    df_partners = pd.read_csv(URL_ANSES_PARTNERS, sep=';', encoding='iso-8859-1', skiprows=1)

    person_map = get_person_map(df_partners)
    projects, partners = [], []
    for e in df_projects.to_dict(orient='records'):
        new_elt = {}
        code_decision = e['Projet.Code'].replace('"', '')
        new_elt['id'] = code_decision
        new_elt['type'] = project_type
        new_elt['name'] = {}
        if isinstance(e.get('Projet.Titre_Francais'), str):
            new_elt['name']['fr'] = e.get('Projet.Titre_Francais').replace('\x92', "'")
        if isinstance(e.get('Projet.Titre_Anglais'), str):
            new_elt['name']['en'] = e.get('Projet.Titre_Anglais')
        if isinstance(e.get('Projet.Acronyme'), str):
            acronym= e.get('Projet.Acronyme').replace('"', '')
            if acronym:
                new_elt['acronym'] = acronym
        description = {}
        if isinstance(e.get('Projet.Resume_Francais'), str):
            description['fr'] = e.get('Projet.Resume_Francais').replace('\x92', "'")
        if isinstance(e.get('Projet.Resume_Anglais'), str):
            description['en'] = e.get('Projet.Resume_Anglais')
        if description:
            new_elt['description'] = description
        if isinstance(e.get('Programme.Acronyme'), str):
            prgm_acronym = e.get('Programme.Acronyme').replace('"', '')
            new_elt['action'] = [{'level': '1', 'code': prgm_acronym, 'name': prgm_acronym}]
        year = e.get('Programme.Millesime').replace('"', '')
        try:
            year = int(year)
            new_elt['year'] = year
        except:
            pass
        if e.get('Projet.Montant_Aide'):
            montant = e.get('Projet.Montant_Aide').replace('\x80', '').replace(' ', '')
            new_elt['budget_financed'] = float(montant)
        if code_decision in person_map:
            new_elt['persons'] = person_map[code_decision]
        projects.append(new_elt)
    
    for e in df_partners.to_dict(orient='records'):
        new_elt = {}
        code_decision = e['code convention homogénéisé']
        suffixes = ['']
        for k in range(1, 14):
            suffixes.append(f'.{k}')
        for sx, suf in enumerate(suffixes):
            organism_key = f'Organisme{suf}'
            if isinstance(e.get(organism_key), str):
                code_decision_partenaire = code_decision+'-'+str(sx).zfill(2)
                new_elt['id'] = code_decision_partenaire
                new_elt['project_id'] = code_decision
                new_elt['project_type'] = project_type
                part_id = None
                if isinstance(e.get(organism_key), str):
                    new_elt['name'] = e[organism_key]
                    part_id = identify_participant(new_elt['name'], cache_participant)
                    if part_id:
                        new_elt['participant_id'] = part_id
                        new_elt['organizations_id'] = part_id
                        new_elt['identified'] = True
                    else:
                        new_elt['identified'] = False
                    if suf=='':
                        new_elt['role'] = 'coordinator'
                    else:
                        new_elt['role'] = 'participant'
                address = {}
                if isinstance(e.get(f'Ville{suf}'), str):
                    address['city'] = e.get(f'Ville{suf}')
                if isinstance(e.get(f'Pays{suf}'), str):
                    address['country'] = e.get(f'Pays{suf}')
                if address:
                    new_elt['address'] = address
        partners.append(new_elt)
    return {'projects': projects, 'partners': partners}
