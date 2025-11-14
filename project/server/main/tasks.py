import time
import datetime
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.anr import update_anr_dos, update_anr_dgpie
from project.server.main.inca import update_inca
from project.server.main.pia import update_pia
from project.server.main.sirano import update_sirano
from project.server.main.dim import update_dim
from project.server.main.anses import update_anses
from project.server.main.ilab import update_ilab
from project.server.main.iphd import update_iphd
from project.server.main.inov import update_inov

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def test():
    cache_participant = enrich_cache()
    update_inov({}, cache_participant)

def create_task_update(arg):
    cache_participant = enrich_cache()
    if arg.get('anr_dos'):
        update_anr_dos(arg, cache_participant)
    if arg.get('anr_dgpie'):
        update_anr_dgpie(arg, cache_participant)
    if arg.get('inca'):
        update_inca(arg, cache_participant)
    if arg.get('sirano'):
        update_sirano(arg, cache_participant)
    if arg.get('dim'):
        update_dim(arg, cache_participant)
    if arg.get('anses'):
        update_anses(arg, cache_participant)
    if arg.get('ilab'):
        update_ilab(arg, cache_participant)
    if arg.get('iphd'):
        update_iphd(arg, cache_participant)
    if arg.get('inov'):
        update_inov(arg, cache_participant)
    if arg.get('pia'):
        update_pia(arg)
    if arg.get('task'):
        url = 'http://185.161.45.213/projects/tasks'
        r = requests.post(url, json={"task_name": "identify_participants"},  headers={"Authorization":os.getenv('AUTHORIZATION')})
        time.sleep(60*60*3)
        r = requests.post(url, json={"task_name": "etl_scanr"}, headers={"Authorization":os.getenv('AUTHORIZATION')})
        time.sleep(60*20)
