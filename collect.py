# Databricks notebook source
# https://dadosabertos.tse.jus.br/dataset/candidatos-2022/resource/435145fd-bc9d-446a-ac9d-273f585a0bb9

import requests
import zipfile
import json
import os
from multiprocessing import Pool

datasource = "votacao_uf"
anos = list(range(2000,2021,4))
#anos = [2022]
with open("datasource.json", "r") as open_file:
    datasources = json.load(open_file)

url = datasources[datasource]['url']

base_path = datasources[datasource]['path']
if not os.path.exists(base_path):
    os.mkdir(base_path)
download_path = os.path.join(base_path, "download.zip")

sufix_not_remove = datasources[datasource]['sufix_not_remove']

ufs = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']

# COMMAND ----------

def get_data(url, download_path):
    response = requests.get(url)
    if response.status_code == 404:
        return response
    with open(download_path, "wb") as open_file:
        open_file.write(response.content)
    
    return response

def unzip_download(download_path, unzip_path):
    with zipfile.ZipFile(download_path, "r") as zip_ref:
        zip_ref.extractall(unzip_path)

def remove_files(path, sufix_not_remove="BRASIL.csv"):
    all_files = dbutils.fs.ls(path.replace("/dbfs", ""))

    to_remove = [i.path for i in all_files if not i.name.endswith(sufix_not_remove)]

    for i in to_remove:
        dbutils.fs.rm(i)

def get_year(url, anos,uf, download_path, base_path):
    for i in anos:
        print(uf)
        resp = get_data(url.format(ano=i, uf=uf), download_path)
        if resp.status_code == 404:
            continue
        unzip_download(download_path, base_path)
        

# COMMAND ----------

if datasource.endswith("uf"):
    data = [(url, anos, uf, download_path, base_path) for uf in ufs]
    with Pool(5) as p:
        p.starmap(get_year,data)
else:
    get_year(url, anos, "BR", download_path, base_path)

remove_files(base_path, sufix_not_remove)

# COMMAND ----------

dbutils.fs.ls("/mnt/tse/votacao")

# COMMAND ----------

df = (spark.read
      .format("csv")
      .options(**{
          "sep":";", 
          "header":"true",
          "encoding":"latin1"})
      .load("/mnt/tse/votacao/"))
      
df.count()

# COMMAND ----------

df.display()
