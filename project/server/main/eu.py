import json
import requests
from retry import retry
from project.server.main.participants import identify_participant
from project.server.main.utils import to_jsonl
from project.server.main.logger import get_logger

logger = get_logger(__name__)

eu_url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
eu_headers = {"Accept": "application/json, text/plain, */*"}
eu_query = {
    "bool": {
        "must": [
            {"terms": {"DATASOURCE": ["SEDIA_NONH2020_PROD"]}},
            {"terms": {"status": ["Ongoing", "Ended"]}},
            {"terms": {"language": ["en"]}},
        ],
        "must_not": [
            {"terms": {"programId": ["43108390", "31045243"]}},  # H2020, Horizon
        ],
    }
}
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
eu_files = {
    "query": ("blob", json.dumps(eu_query), "application/json"),
    "sort": ("blob", json.dumps(eu_sort), "application/json"),
    "displayFields": ("blob", json.dumps(eu_fields), "application/json"),
    "languages": ("blob", json.dumps(eu_languages), "application/json"),
}
eu_params = {"apiKey": "SEDIA_NONH2020_PROD", "text": "***", "pageSize": 1, "pageNumber": 1}


@retry(delay=20, tries=3)
def fetch_one_page(page_number: int, page_size: int) -> dict:
    params = {**eu_params, "pageSize": page_size, "pageNumber": page_number}
    response = requests.post(
        url=eu_url,
        headers=eu_headers,
        files=eu_files,
        params=params,
    )
    data = response.json()
    return data


def fetch_all(page_size: int = 50) -> list:
    next_page = 1
    results = []
    logger.info("Start fetching EU API...")

    while next_page > 0:
        logger.debug(f"Fetching page {next_page}...")
        data = fetch_one_page(next_page, page_size)
        results.extend(data.get("results", []))

        next_page = next_page + 1 if data["totalResults"] > (page_size * next_page) else 0

    logger.info(f"Successfully fetched {len(results)} projects")
    return results


def extract_participants(project_id: str, raw_text: str, cache_participant: dict) -> list:
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
        project["url"] = metadata["url"][0]
        #project["type"] = metadata["programAbbreviation"][0]  # TODO mapping ?
        project["type"] = 'Autres financements européens'

        project["startDate"] = metadata["startDate"][0]
        project["endDate"] = metadata["endDate"][0]
        project["signatureDate"] = metadata["ecSignatureDate"][0]
        project["year"] = project["startDate"][0:4]

        project["acronym"] = {"default": metadata["acronym"][0]}
        project["label"] = {"default": metadata["title"][0]}
        if len(metadata["objective"]):
            project["description"] = {"default": metadata["objective"][0]}

        project["participantCount"] = metadata["numberOfContributors"][0]
        project["participants"] = extract_participants(project_id, metadata["participants"][0], cache_participant)

        if len(metadata["overallBudget"]):
            project["budgetTotal"] = metadata["overallBudget"][0]
        if len(metadata["euContributionAmount"]):
            project["budgetFinanced"] = metadata["euContributionAmount"][0]

        project["callIdentifier"] = {"id": metadata["callIdentifier"][0]}

        action_code = metadata["typeOfActions"][0]
        action_label = metadata["typeOfAction"][0]
        program = metadata['programmes'][0]
        program_id = metadata['programId'][0]
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


def harvest_eu_projects(cache_participant: dict) -> list:
    results = fetch_all()
    projects = extract_projects(results, cache_participant)

    if len(projects):
        logger.debug("projects sample:")
        logger.debug(f"{projects[0]}")

    return projects


def update_eu(args, cache_participant: dict):
    new_data_eu = harvest_eu_projects(cache_participant)
    to_jsonl(new_data_eu, "projects.jsonl")
