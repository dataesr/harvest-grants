import pandas as pd
import os
import requests
from retry import retry
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, reset_db_projects_and_partners, transform_scanr
from project.server.main.logger import get_logger

URL_AAPG_ANR = 'https://data.anr.fr/api/explore/v2.1/catalog/datasets/comites_aapg0/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'
URL_ANR_PROJECTS_05_09 = 'https://www.data.gouv.fr/api/1/datasets/r/a16e0fd7-a008-499b-bbd3-b640f8651bd9'
URL_ANR_PARTNERS_05_09 = 'https://www.data.gouv.fr/api/1/datasets/r/18e345ee-7a16-4727-8ac5-b237db974e24'
URL_ANR_PROJECTS_10 = 'https://www.data.gouv.fr/api/1/datasets/r/afe3d11b-9ea2-48b0-9789-2816d5785466'
URL_ANR_PARTNERS_10 = 'https://www.data.gouv.fr/api/1/datasets/r/9b08ee21-7372-47a4-9831-4c56a8099ee8'
URL_ANR_PROJECTS_DGPIE = 'https://www.data.gouv.fr/api/1/datasets/r/d9b2d1e7-0c85-43fc-a47a-5b57fc6b505a'
URL_ANR_PARTNERS_DGPIE = 'https://www.data.gouv.fr/api/1/datasets/r/559459fb-b947-44b7-849c-9287b1ef1134'

logger = get_logger(__name__)

def update_anr_v2(args, cache_participant):
    new_data_anr = harvest_anr_projects('ANR', cache_participant)
    transform_scanr(new_data_anr)

    new_data_pia = harvest_anr_projects('PIA ANR', cache_participant)
    transform_scanr(new_data_pia)


def update_anr_dos(args, cache_participant):
    reset_db_projects_and_partners('ANR')
    new_data_anr = harvest_anr_projects('ANR', cache_participant)
    post_data(data = new_data_anr)
   
def update_anr_dgpie(args, cache_participant):
    reset_db_projects_and_partners('PIA ANR')
    new_data_pia = harvest_anr_projects('PIA ANR', cache_participant)
    post_data(data = new_data_pia)

def get_person_map(df_partners):
    person_map = {}
    for e in df_partners.to_dict(orient='records'):
        code_decision = e['Projet.Code_Decision']
        if code_decision not in person_map:
            person_map[code_decision] = []
        person = {}
        if isinstance(e['Projet.Partenaire.Responsable_scientifique.Nom'], str):
            person['last_name'] = e['Projet.Partenaire.Responsable_scientifique.Nom']
        if isinstance(e['Projet.Partenaire.Responsable_scientifique.Prenom'], str):
            person['first_name'] = e['Projet.Partenaire.Responsable_scientifique.Prenom']
        if e['Projet.Partenaire.Est_coordinateur']:
            person['role'] = 'coordinator'
        else:
            person['role'] = 'participant'
        if person and person not in person_map[code_decision]:
            person_map[code_decision].append(person)
        #TODO idref?
        #if isinstance(e['Projet.Partenaire.Responsable_scientifique.ORCID'], str):
        #    person['orcid'] = e['Projet.Partenaire.Responsable_scientifique.ORCID']
    return person_map


@retry(delay=20, tries=3)
def harvest_anr_projects(project_type, cache_participant):
    enrichment_dict = {}
    if project_type=='ANR':
        df_projects1 = pd.read_json(URL_ANR_PROJECTS_05_09, orient='split')
        df_projects1.rename(columns={"Projet.Code_Decision_ANR": "Projet.Code_Decision"}, inplace=True)
        df_projects2 = pd.read_json(URL_ANR_PROJECTS_10, orient='split')
        df_projects = pd.concat([df_projects1, df_projects2])
        df_partners1 = pd.read_json(URL_ANR_PARTNERS_05_09, orient='split')
        df_partners1.rename(columns={"Projet.Code_Decision_ANR": "Projet.Code_Decision"}, inplace=True)
        df_partners1.rename(columns={"Projet.Partenaire.Code_Decision_ANR": "Projet.Partenaire.Code_Decision"}, inplace=True)
        df_partners2 = pd.read_json(URL_ANR_PARTNERS_10, orient='split')
        df_partners = pd.concat([df_partners1, df_partners2])
        # enrichment with data from ANR portal
        df_aapg = pd.read_csv(URL_AAPG_ANR, sep=';')
        for k in df_aapg.to_dict(orient='records'):
            current_id = k['Code projet ANR']
            enrichment_dict[current_id] = k
    elif project_type == 'PIA ANR':
        df_projects = pd.read_json(URL_ANR_PROJECTS_DGPIE, orient='split')
        df_partners = pd.read_json(URL_ANR_PARTNERS_DGPIE, orient='split')
    person_map = get_person_map(df_partners)
    projects, partners = [], []
    for e in df_projects.to_dict(orient='records'):
        new_elt = {}
        code_decision = e['Projet.Code_Decision']
        new_elt['id'] = code_decision
        if code_decision in enrichment_dict:
            if 'Instrument financement' in enrichment_dict[code_decision]:
                new_elt['instrument'] = enrichment_dict[code_decision]['Instrument financement']
            if 'Intitulé du comité' in enrichment_dict[code_decision]:
                new_elt['comite'] = enrichment_dict[code_decision]['Intitulé du comité']
        new_elt['type'] = project_type
        new_elt['name'] = {}
        if isinstance(e.get('Projet.Titre.Francais'), str):
            new_elt['name']['fr'] = e.get('Projet.Titre.Francais')
        if isinstance(e.get('Projet.Titre.Anglais'), str):
            new_elt['name']['en'] = e.get('Projet.Titre.Anglais')
        if isinstance(e.get('Projet.Acronyme'), str):
            new_elt['acronym'] = e.get('Projet.Acronyme')
        description = {}
        if isinstance(e.get('Projet.Resume.Francais'), str):
            description['fr'] = e.get('Projet.Resume.Francais')
        if isinstance(e.get('Projet.Resume.Anglais'), str):
            description['en'] = e.get('Projet.Resume.Anglais')
        if description:
            new_elt['description'] = description
        if isinstance(e.get('Programme.Acronyme'), str):
            new_elt['action'] = [{'level': '1', 'code': e.get('Programme.Acronyme'), 'name': e.get('Programme.Acronyme')}]
        if isinstance(e.get('Action.Titre.Francais'), str):
            new_elt['action'] = [{'level': '1', 'code': e.get('Action.Titre.Francais'), 'name': e.get('Action.Titre.Francais')}]
        if isinstance(e.get('AAP.Edition'), int):
            new_elt['year'] = e.get('AAP.Edition')
        if isinstance(e.get('Action.Edition'), int):
            new_elt['year'] = e.get('Action.Edition')
        if isinstance(e.get('Projet.Montant.AF.Aide_allouee.ANR'), float):
            if e.get('Projet.Montant.AF.Aide_allouee.ANR') ==  e.get('Projet.Montant.AF.Aide_allouee.ANR'):
                new_elt['budget_financed'] = e.get('Projet.Montant.AF.Aide_allouee.ANR')
        if isinstance(e.get('Projet.Aide_allouee'), float):
            if e.get('Projet.Aide_allouee') == e.get('Projet.Aide_allouee'):
                new_elt['budget_total'] = e.get('Projet.Aide_allouee')
        if code_decision in person_map:
            new_elt['persons'] = person_map[code_decision]
        projects.append(new_elt)
    for e in df_partners.to_dict(orient='records'):
        new_elt = {}
        code_decision = e['Projet.Code_Decision']
        code_decision_partenaire = e['Projet.Partenaire.Code_Decision']
        new_elt['id'] = code_decision_partenaire
        new_elt['project_id'] = code_decision
        new_elt['project_type'] = project_type
        part_id = None
        if isinstance(e.get('Projet.Partenaire.Nom_organisme'), str):
            new_elt['name'] = e['Projet.Partenaire.Nom_organisme']
        if isinstance(e.get('Projet.Partenaire.Code_RNSR'), str):
            part_id = e['Projet.Partenaire.Code_RNSR']
        elif new_elt.get('name'):
            part_id = identify_participant(new_elt['name'], cache_participant)
        if part_id:
            new_elt['participant_id'] = part_id
            new_elt['organizations_id'] = part_id
            new_elt['identified'] = True
        else:
            new_elt['identified'] = False
        if e['Projet.Partenaire.Est_coordinateur']:
            new_elt['role'] = 'coordinator'
        else:
            new_elt['role'] = 'participant'
        if isinstance(e.get('Projet.Partenaire.Aide_allouee.ANR'), float):
            if e.get('Projet.Partenaire.Aide_allouee.ANR') == e.get('Projet.Partenaire.Aide_allouee.ANR'):
                new_elt['funding'] = e.get('Projet.Partenaire.Aide_allouee.ANR')
        address = {}
        if isinstance(e['Projet.Partenaire.Adresse.Ville'], str):
            address['city'] = e['Projet.Partenaire.Adresse.Ville']
        if isinstance(e['Projet.Partenaire.Adresse.Pays'], str):
            address['country'] = e['Projet.Partenaire.Adresse.Pays']
        if address:
            new_elt['address'] = address
        partners.append(new_elt)
    return {'projects': projects, 'partners': partners}
