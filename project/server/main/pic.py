import re
from collections import defaultdict
from typing import Optional
import pandas as pd

from project.server.main.eu import get_pic_correspondance
from project.server.main.utils import get_all_struct
from project.server.main.logger import get_logger

logger = get_logger(__name__)

ROR_REGEX = re.compile(r"^0[a-z0-9]{6}[0-9]{2}$")
# identifiants scanR utilisables comme clé pour retrouver une structure
ID_TYPES = ["ror", "paysage", "rnsr", "siren", "siret", "grid"]


def normalize_ror(ror) -> Optional[str]:
    if not isinstance(ror, str):
        return None
    ror = ror.strip().lower().replace("https://ror.org/", "").replace("ror.org/", "")
    return ror if ROR_REGEX.match(ror) else None


def get_rors(struct: dict) -> set:
    """ROR(s) d'une structure scanR : son id s'il s'agit d'un ROR + les externalIds de type ror."""
    rors = set()
    own = normalize_ror(struct.get("id"))
    if own:
        rors.add(own)
    for ext in struct.get("externalIds") or []:
        if ext.get("type") == "ror":
            ror = normalize_ror(ext.get("id"))
            if ror:
                rors.add(ror)
    return rors


def build_id_to_ror(all_struct: list) -> tuple:
    """
    Construit deux index vers les ROR à partir des structures scanR :
    - by_scanr_id : id scanR (siren / rnsr / paysage / ror) -> set(ror)
    - by_external_id : identifiant externe (paysage, rnsr, siren, siret, grid, ror) -> set(ror)
    L'id scanR est prioritaire ; les externalIds servent de repli
    (un même siren / ror peut être porté par plusieurs structures).
    """
    by_scanr_id = {}
    by_external_id = defaultdict(set)
    for struct in all_struct:
        rors = get_rors(struct)
        if not rors:
            continue
        by_scanr_id[struct["id"]] = rors
        for ext in struct.get("externalIds") or []:
            if ext.get("type") in ID_TYPES and isinstance(ext.get("id"), str):
                by_external_id[ext["id"].strip()] |= rors
    return by_scanr_id, dict(by_external_id)


def resolve_ror(identifier: str, by_scanr_id: dict, by_external_id: dict) -> Optional[str]:
    identifier = identifier.strip()
    direct = normalize_ror(identifier)
    if direct:
        return direct
    rors = by_scanr_id.get(identifier) or by_external_id.get(identifier) or set()
    if len(rors) == 1:
        return next(iter(rors))
    if len(rors) > 1:
        logger.debug(f"Ambiguous ROR for {identifier}: {sorted(rors)}")
    return None


def get_pic_ror_correspondance(pic_map: Optional[dict] = None, all_struct: Optional[list] = None) -> dict:
    """
    Génère la correspondance PIC -> ROR.
    - pic_map : PIC -> id scanR (paysage / rnsr / siren / ror), par défaut la table Grist utilisée par eu.py
    - all_struct : structures scanR, par défaut organizations-v2.jsonl.gz
    """
    if pic_map is None:
        pic_map = get_pic_correspondance()
    if all_struct is None:
        all_struct = get_all_struct()

    by_scanr_id, by_external_id = build_id_to_ror(all_struct)

    pic_ror = {}
    unresolved = []
    for pic, identifier in pic_map.items():
        ror = resolve_ror(identifier, by_scanr_id, by_external_id)
        if ror:
            pic_ror[str(pic)] = ror
        else:
            unresolved.append((pic, identifier))

    logger.info(f"PIC -> ROR : {len(pic_ror)} / {len(pic_map)} PIC résolus ({len(unresolved)} sans ROR)")
    data = []
    for pic_k in pic_ror:
        data.append({'pic': pic_k, 'ror': pic_ror[pic_k]})
    pd.DataFrame(data).to_csv('pic_ror.csv', index=False)
    return data
    #return pic_ror
