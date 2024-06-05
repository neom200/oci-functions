import numpy as np
import pandas as pd
from oci.config import from_file
from oci.object_storage import ObjectStorageClient
import oci
import ads
from requests.auth import HTTPBasicAuth
import requests
from ads.common.auth import default_signer
# from extra_funcs import *

ads.set_auth(auth="api_key", oci_config_location="~/.oci/config", profile="DEFAULT")

def set_initial_config():
    url = "https://brado.topdesk.net/services/reporting/v2/odata"
    username = "qlikview.admin"
    password = "itom6-zshnx-olq56-cchrb-pgfqg"
    config = from_file()
    obj_client = ObjectStorageClient(config)
    
    # Configura autenticação básica
    topdesk_auth = HTTPBasicAuth(username, password)
     
    # Headers para aceitar resposta em JSON
    headers = {
        "Accept": "application/json"
    }
    return url, topdesk_auth, headers, obj_client

def load_topdesk_data(url, topdesk_auth, headers):
    response = requests.get(url, auth=topdesk_auth, headers=headers)
    # Verifica se a conexão foi bem-sucedida
    if response.status_code == 200:
        print("Conexão bem-sucedida!")
        #print("Dados recebidos: ", response.json())
    else:
        print("Falha na conexão. Status code:", response.status_code)
        print("Mensagem de erro:", response.text)
    return response.json()['value']

def load_tables(topdesk_auth, url, headers, dados_topdesk):
    topdesk_dfs = {}    
    for item in dados_topdesk:
        res = requests.get(url+"/"+item['url'], auth=topdesk_auth, headers=headers)
        df = pd.DataFrame(res.json()['value'])
        topdesk_dfs[item['name']] = df
    return topdesk_dfs

def first_transformation(topdesk_dfs):
    novos_tds = {}
    for item in topdesk_dfs.keys():
        if len(topdesk_dfs[item]) > 0:
            novos_tds[item] = topdesk_dfs[item].copy()
    return novos_tds

def second_transformation(novos_tds):
    limpos_tds = {}
    for it in novos_tds.keys():
        df = novos_tds[it].copy()
        df = df.dropna(how='all', axis='columns')
        df = df.dropna(how='all')
        limpos_tds[it] = df
    return limpos_tds

def third_transformation(limpos_tds):
    parquets_tds = {}
    for it in limpos_tds.keys():
        df = limpos_tds[it].copy()
        df = df.to_parquet()
        parquets_tds[it] = df
    return parquets_tds

def save_to_bucket(obj_client, parquets_tds):
    object_storage_namespace = "grqn05sriwg6"
    object_storage_bucket = "STAGE"
    for bin in parquets_tds.keys():
        obj_client.put_object(
            namespace_name=object_storage_namespace,
            bucket_name=object_storage_bucket,
            object_name=f"TOPDESK FILES/{bin}.parquet",
            put_object_body=parquets_tds[bin]
        )
        print("> Sucessfully loaded {} into bucket STAGE.".format(bin))