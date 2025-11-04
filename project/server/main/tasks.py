import time
import datetime
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.anr import update_anr_dos, update_anr_dgpie
from project.server.main.inca import update_inca
from project.server.main.pia import update_pia

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_update(arg):
    cache_participant = enrich_cache()
    if arg.get('anr_dos'):
        update_anr_dos(arg, cache_participant)
    if arg.get('anr_dgpie'):
        update_anr_dgpie(arg, cache_participant)
    if arg.get('inca'):
        update_inca(arg, cache_participant)
    if arg.get('pia'):
        update_pia(arg)
    if arg.get('task'):
        url = 'http://185.161.45.213/projects/tasks'
        r = requests.post(url, json={"task_name": "identify_participants"},  headers={"Authorization":os.getenv('AUTHORIZATION')})
        time.sleep(60*60*3)
        r = requests.post(url, json={"task_name": "etl_scanr"}, headers={"Authorization":os.getenv('AUTHORIZATION')})
        time.sleep(60*20)
