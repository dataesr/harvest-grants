import requests
import pandas as pd

from project.server.main.logger import get_logger

logger = get_logger(__name__)

# correspondance calculée et poussée sur s3 par scanr-backend (meta_orga)
PIC_ROR_URL = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/pic_ror.json'


def get_pic_ror_correspondance() -> list:
    """Correspondance PIC -> ROR, récupérée depuis scanr-data."""
    r = requests.get(PIC_ROR_URL, timeout=60)
    r.raise_for_status()
    pic_ror = r.json()
    logger.info(f"PIC -> ROR : {len(pic_ror)} PIC récupérés depuis {PIC_ROR_URL}")
    data = []
    for pic_k in pic_ror:
        data.append({'pic': pic_k, 'ror': pic_ror[pic_k]})
    pd.DataFrame(data).to_csv('pic_ror.csv', index=False)
    return data
    #return pic_ror
