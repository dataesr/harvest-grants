import time
import datetime
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.anr import update_anr_dos, update_anr_dgpie, update_anr_v2
from project.server.main.inca import update_inca, update_inca_v2
from project.server.main.pia import update_pia, update_pia_v2
from project.server.main.sirano import update_sirano, update_sirano_v2
from project.server.main.dim import update_dim, update_dim_v2
from project.server.main.anses import update_anses, update_anses_v2
from project.server.main.ilab import update_ilab, update_ilab_v2
from project.server.main.iphd import update_iphd, update_iphd_v2
from project.server.main.inov import update_inov, update_inov_v2
from project.server.main.pcri import update_pcri
from project.server.main.s3 import upload_object

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def test():
    cache_participant = enrich_cache()
    update_inov({}, cache_participant)

def create_task_update_v2(arg):
    cache_participant = enrich_cache()
    os.system('rm -rf projects.jsonl')
    os.system('rm -rf projects.jsonl.gz')
    if arg.get('pcri') or arg.get('all'):
        update_pcri(arg, cache_participant)
    if arg.get('anr') or arg.get('all'):
        update_anr_v2(arg, cache_participant)
    if arg.get('inca') or arg.get('all'):
        update_inca_v2(arg, cache_participant)
    if arg.get('sirano') or arg.get('all'):
        update_sirano_v2(arg, cache_participant)
    if arg.get('dim') or arg.get('all'):
        update_dim_v2(arg, cache_participant)
    if arg.get('anses') or arg.get('all'):
        update_anses_v2(arg, cache_participant)
    if arg.get('ilab') or arg.get('all'):
        update_ilab_v2(arg, cache_participant)
    if arg.get('iphd') or arg.get('all'):
        update_iphd_v2(arg, cache_participant)
    if arg.get('inov') or arg.get('all'):
        update_inov_v2(arg, cache_participant)
    if arg.get('pia') or arg.get('all'):
        update_pia_v2(arg, cache_participant)
    os.system(f'gzip projects.jsonl')
    upload_object(container='scanr-data', source = f'projects.jsonl.gz', destination=f'production/projects-v2.jsonl.gz')

def create_task_update(arg):
    cache_participant = enrich_cache()
    if arg.get('anr_dos') or arg.get('all'):
        update_anr_dos(arg, cache_participant)
    if arg.get('anr_dgpie') or arg.get('all'):
        update_anr_dgpie(arg, cache_participant)
    if arg.get('inca') or arg.get('all'):
        update_inca(arg, cache_participant)
    if arg.get('sirano') or arg.get('all'):
        update_sirano(arg, cache_participant)
    if arg.get('dim') or arg.get('all'):
        update_dim(arg, cache_participant)
    if arg.get('anses') or arg.get('all'):
        update_anses(arg, cache_participant)
    if arg.get('ilab') or arg.get('all'):
        update_ilab(arg, cache_participant)
    if arg.get('iphd') or arg.get('all'):
        update_iphd(arg, cache_participant)
    if arg.get('inov') or arg.get('all'):
        update_inov(arg, cache_participant)
    if arg.get('pia') or arg.get('all'):
        update_pia(arg)
    if arg.get('task'):
        url = 'http://185.161.45.213/projects/tasks'
        r = requests.post(url, json={"task_name": "identify_participants"},  headers={"Authorization":os.getenv('AUTHORIZATION')})
        time.sleep(60*60*3)
        r = requests.post(url, json={"task_name": "etl_scanr"}, headers={"Authorization":os.getenv('AUTHORIZATION')})
        time.sleep(60*20)
