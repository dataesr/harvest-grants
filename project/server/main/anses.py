import pandas as pd
import os
import requests
from retry import retry
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, reset_db_projects_and_partners, transform_scanr
from project.server.main.logger import get_logger

URL_ANSES_PROJECTS = 'https://www.data.gouv.fr/api/1/datasets/r/ea1a1cc1-911f-4b0d-84ba-3d6447c255d7'
#URL_ANSES_PARTNERS= 'https://www.data.gouv.fr/api/1/datasets/r/0c4252ad-b1dc-4e1d-84da-fd8afc4094fb'
URL_ANSES_PARTNERS= 'https://www.data.gouv.fr/api/1/datasets/r/e46c7e03-332d-4cc5-a7c6-57afc328f4e2'

logger = get_logger(__name__)

def update_anses_v2(args, cache_participant):
    new_data = harvest_anses_projects(cache_participant)
    transform_scanr(new_data)

project_type = 'ANSES'
def update_anses(args, cache_participant):
    reset_db_projects_and_partners(project_type)
    new_data_anses = harvest_anses_projects(cache_participant)
    post_data(data = new_data_anses)

def get_person_map(df_partners):
    person_map = {}
    for e in df_partners.to_dict(orient='records'):
        code_decision = e['Projet.Code']
        if code_decision not in person_map:
            person_map[code_decision] = []

        person = {}
        name_field = f'Projet.Responsable.Nom'
        firstname_field = df_partners.columns[4]
        if isinstance(e[name_field], str):
            person['last_name'] = e[name_field]
        if isinstance(e[firstname_field], str):
            person['first_name'] = e[firstname_field]
        if person and e['Projet.Partenaire.numero'] == "1":
            person['role'] = 'coordinator'
        elif person:
            person['role'] = 'participant'
        if person and person not in person_map[code_decision]:
            person_map[code_decision].append(person)
    return person_map

@retry(delay=20, tries=3)
def harvest_anses_projects(cache_participant):
    df_projects = pd.read_csv(URL_ANSES_PROJECTS, sep=';', encoding='cp850')
    df_projects = df_projects.loc[:, ~df_projects.columns.str.startswith("Unnamed")]
    df_projects = df_projects.apply(lambda s: s.str.strip('"').replace("", pd.NA))


    #df_partners = pd.read_csv(URL_ANSES_PARTNERS, sep=';', encoding='iso-8859-1', skiprows=1)
    df_partners = pd.read_csv(URL_ANSES_PARTNERS, sep=';', encoding="cp850", dtype=str)
    # supprimer les colonnes vides dues aux ";;" finaux
    df_partners = df_partners.loc[:, ~df_partners.columns.str.startswith("Unnamed")]
    # retirer les guillemets résiduels et transformer les vides en NaN
    df_partners = df_partners.apply(lambda s: s.str.strip('"').replace("", pd.NA))

    person_map = get_person_map(df_partners)
    projects, partners = [], []
    for e in df_projects.to_dict(orient='records'):
        new_elt = {}
        if not isinstance(e['Projet.Code'], str):
            continue
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
        if isinstance(e.get('Projet.Montant_Aide'), str):
            montant = e.get('Projet.Montant_Aide').replace('\x80', '').replace('?', '').replace(' ', '')
            new_elt['budget_financed'] = float(montant)
        if code_decision in person_map:
            new_elt['persons'] = person_map[code_decision]
        projects.append(new_elt)
    
    for e in df_partners.to_dict(orient='records'):
        new_elt = {}
        #code_decision = e['code convention homogénéisé']
        code_decision = e['Projet.Code']
        sx = e['Projet.Partenaire.numero']
        code_decision_partenaire = code_decision+'-'+str(sx).zfill(2)
        new_elt['id'] = code_decision_partenaire
        new_elt['project_id'] = code_decision
        new_elt['project_type'] = project_type
        part_id = None
        new_elt['name'] = e['Projet.Partenaire.Organisme']
        part_id = identify_participant(new_elt['name'], cache_participant)
        if part_id:
            new_elt['participant_id'] = part_id
            new_elt['organizations_id'] = part_id
            new_elt['identified'] = True
        else:
            new_elt['identified'] = False
        if sx == "1" :
            new_elt['role'] = 'coordinator'
        else:
            new_elt['role'] = 'participant'
        address = {}
        if isinstance(e.get(f'Projet.Partenaire.Ville'), str):
            address['city'] = e.get('Projet.Partenaire.Ville')
        if isinstance(e.get(f'Projet.Partenaire.Pays'), str):
            address['country'] = e.get(f'Projet.Partenaire.Pays')
        if address:
            new_elt['address'] = address
        if new_elt not in partners:
            partners.append(new_elt)
    return {'projects': projects, 'partners': partners}
