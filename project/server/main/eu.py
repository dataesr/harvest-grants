import json
import requests
from retry import retry
import pandas as pd
from project.server.main.participants import identify_participant
from project.server.main.utils import to_jsonl
from project.server.main.logger import get_logger

logger = get_logger(__name__)

eu_url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
eu_headers = {"Accept": "application/json, text/plain, */*"}

MAPPING = {
        'h2020': '31045243',
        'he': '43108390',
        'erasmus': '43353764',
        'crea': '43251814',
        'cerv': '43251589',
        'smp': '43252476',
        'cef': '43251567',
        'life': '43252405',
        'comp': '31059643',
        'dig': '43152860'
        }

def get_eu_query(case):
    assert(case in list(MAPPING.keys()) + ['rest'] )
    eu_query = {
        "bool": {
            "must": [
                {"terms": {"DATASOURCE": ["SEDIA_NONH2020_PROD"]}},
                {"terms": {"status": ["Ongoing", "Ended"]}},
                {"terms": {"language": ["en"]}},
            ],
            "must_not": [
                {"terms": {"programId": [MAPPING['he'], MAPPING['h2020']]}},  # H2020, Horizon
            ],
        }
    }
    if case == 'base':
        return eu_query
    elif case in MAPPING:
        eu_query['bool']['must'].append( {"terms": {"programId": [MAPPING[case]]}})
    elif case == 'rest':
        eu_query['bool']['must_not'] =  [{"terms": {"programId": list(MAPPING.values())}}]
    return eu_query

eu_sort = {"order": "DESC", "field": "es_SortDate"}
eu_fields = [
    "title",
    "acronym",
    "objective",
    "projectId",
    "programId",
    "callIdentifier",
    "programAbbreviation",
    "programmes",
    "status",
    "participants",
    "numberOfContributors",
    "topicAbbreviation",
    "topicDescription",
    "overallBudget",
    "euContributionRate",
    "euContributionAmount",
    "freeKeywords",
    "startDate",
    "endDate",
    "ecSignatureDate",
    "typeOfAction",
    "typeOfActions",
]
eu_languages = ["en"]
def get_eu_files(case):
    eu_files = {
      "query": ("blob", json.dumps(get_eu_query(case)), "application/json"),
        "sort": ("blob", json.dumps(eu_sort), "application/json"),
        "displayFields": ("blob", json.dumps(eu_fields), "application/json"),
        "languages": ("blob", json.dumps(eu_languages), "application/json"),
    }
    return eu_files

eu_params = {"apiKey": "SEDIA_NONH2020_PROD", "text": "***", "pageSize": 1, "pageNumber": 1}


@retry(delay=20, tries=3)
def fetch_one_page(case: str, page_number: int, page_size: int) -> dict:
    params = {**eu_params, "pageSize": page_size, "pageNumber": page_number}
    response = requests.post(
        url=eu_url,
        headers=eu_headers,
        files=get_eu_files(case),
        params=params,
    )
    data = response.json()
    return data


def fetch_all(case: str, page_size: int = 50) -> list:
    next_page = 1
    results = []
    logger.info(f"Start fetching EU API for {case} ...")

    while next_page > 0:
        logger.debug(f"Fetching page {next_page}...")
        data = fetch_one_page(case, next_page, page_size)
        results.extend(data.get("results", []))

        next_page = next_page + 1 if data["totalResults"] > (page_size * next_page) else 0

    logger.info(f"Successfully fetched {len(results)} projects")
    return results


def extract_participants(project_id: str, raw_text: str, cache_participant: dict, pic_map) -> list:
    participants = []

    if not len(raw_text):
        logger.error(f"[{project_id}] Json participants raw text empty")
        return participants

    try:
        data = json.loads(raw_text)
    except Exception as error:
        logger.error(f"[{project_id}] Error while parsing json particiants: {error}")
        return participants

    for index, d in enumerate(data):
        participant = {}
        participant["role"] = d["role"]
        participant["pic"] = d["pic"]
        if d['pic'] in pic_map:
            part_id = pic_map[d['pic']]
            participant['participant_id'] = part_id
            participant['organizations_id'] = part_id
            participant['identified'] = True
        participant["funding"] = d["eucontribution"]
        participant["id"] = f"{project_id}-{index+1:02d}"
        participant["label"] = {"default": d["legalName"]}

        participant_id = identify_participant(d["legalName"], cache_participant)
        if participant_id:
            participant["participant_id"] = participant_id
        # TODO: identify other participants

        address = {}
        postal_address = d.get("postalAddress", {})
        country = postal_address.get("countryCode", {})
        if postal_address.get("city"):
            address["city"] = postal_address["city"]
        if country.get("abbreviation"):
            address["country_code"] = country["abbreviation"]
        if country.get("description"):
            address["country"] = country["description"]
        if address:
            participant["address"] = address

        participants.append(participant)

    return participants


def extract_projects(data: list, cache_participant: dict) -> list:
    projects = []
    project_ids = set()

    if not len(data):
        logger.warning("No data to extract")
        return projects

    logger.info(f"Start extracting {len(data)} EU projects")

    for d in data:
        project = {}
        reference = d["reference"]

        if "metadata" not in d:
            logger.debug(f"No metadata for project {reference=}")
            continue

        metadata = d["metadata"]
        project_id = metadata["projectId"][0]

        if project_id in project_ids:
            logger.debug(f"Skipping duplicate EU project {project_id}")
            continue
        project_ids.add(project_id)

        project["id"] = project_id
        if metadata.get("url") and len(metadata.get("url")):
            project["url"] = metadata["url"][0]
        #project["type"] = metadata["programAbbreviation"][0]  # TODO mapping ?
        project["type"] = 'Autres financements européens'

        if metadata.get("startDate") and len(metadata.get("startDate")):
            project["startDate"] = metadata["startDate"][0]
            project["year"] = project["startDate"][0:4]
        if len(metadata.get("endDate")):
            project["endDate"] = metadata["endDate"][0]
        if metadata.get("ecSignatureDate") and len(metadata.get("ecSignatureDate")):
            project["signatureDate"] = metadata["ecSignatureDate"][0]

        if len(metadata.get("acronym")):
            project["acronym"] = {"default": metadata["acronym"][0]}
        if len(metadata.get("title")):
            project["label"] = {"default": metadata["title"][0]}
        if len(metadata.get("objective")):
            project["description"] = {"default": metadata["objective"][0]}

        if metadata.get("numberOfContributors") and len(metadata.get("numberOfContributors")):
            project["participantCount"] = metadata["numberOfContributors"][0]

        project["participants"] = extract_participants(project_id, metadata["participants"][0], cache_participant)

        if metadata.get("overallBudget") and len(metadata.get("overallBudget")):
            project["budgetTotal"] = metadata["overallBudget"][0]
        if metadata.get("euContributionAmount") and len(metadata.get("euContributionAmount")):
            project["budgetFinanced"] = metadata["euContributionAmount"][0]

        if metadata.get("callIdentifier") and len(metadata.get("callIdentifier")):
            project["callIdentifier"] = {"id": metadata["callIdentifier"][0]}

        if metadata.get("typeOfActions") and len(metadata.get("typeOfActions")):
            action_code = metadata["typeOfActions"][0]
        if metadata.get("typeOfAction") and len(metadata.get("typeOfAction")):
            action_label = metadata["typeOfAction"][0]
        if metadata.get("programmes") and len(metadata.get("programmes")):
            program = metadata['programmes'][0]
        if metadata.get("programId") and len(metadata.get("programId")):
            program_id = metadata['programId'][0]
        if metadata.get("programAbbreviation") and len(metadata.get("programAbbreviation")):
            program_acronym = metadata['programAbbreviation'][0]
        project["instrument"] = action_label
        project["action"] = {
            "code": program_id,
            "label": {"default": program},
            "level": 1
        }

        if len(metadata.get("freeKeywords", [])):
            project["keywords"] = {"en": metadata["freeKeywords"]}

        priorities = []
        if isinstance(metadata.get('topicAbbreviation'), list):
            for topic in metadata.get('topicAbbreviation'):
                new_prio = {
                      "type": "topic",
                    "label": {
                            "default": topic
                        }
                    }
                if new_prio not in priorities:
                    priorities.append(new_prio)
        project['priorities'] = priorities

        projects.append(project)

    logger.debug(f"{len(projects)} EU projects extracted")
    return projects


def harvest_eu_projects(cache_participant: dict, pic_map) -> list:
    # Il Faut découper car il y a plus de 10000 projets à récupérer
    results = []
    for c in MAPPING:
        if c in ['he', 'h2020']:
            continue
        results += fetch_all(c)
    results += fetch_all('rest')
    projects = extract_projects(results, cache_participant, pic_map)

    if len(projects):
        logger.debug("projects sample:")
        logger.debug(f"{projects[0]}")

    return projects


def update_eu(args, cache_participant: dict):
    pic_map = get_pic_correspondance()
    new_data_eu = harvest_eu_projects(cache_participant, pic_map)
    to_jsonl(new_data_eu, "projects.jsonl")

def get_pic_correspondance():
    GRIST_TOKEN = os.getenv('GRIST_TOKEN')
    URL = "https://grist.numerique.gouv.fr/api/docs/s8u4VDyE9RoTp38S22gQE8/download/xlsx"
    HEADERS = {"Authorization": f"Bearer {GRIST_TOKEN}"}
    df = pd.read_excel(URL, storage_options=HEADERS, sheet_name='from_pic_to_id')
    pic_map = {}
    for e in df.to_dict(orient='records'):
        if isinstance(e['generalPic'], str) and isinstance(e['from_id_to_ref'], str):
            pic_map[e['generalPic']] = e['from_id_to_ref'].strip()
    return pic_map

