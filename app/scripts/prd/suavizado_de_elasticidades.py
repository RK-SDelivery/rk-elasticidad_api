#!/usr/bin/env python
# coding: utf-8
# El objetivo de este notebook es implementar suavizado de elastividades y revisar los efectos en elasticidades positivas promedio
# In[2]:


import pandas as pd
import numpy as np
import time

# from pulp import *
from google.cloud import bigquery
from scipy.optimize import minimize


# In[3]:


pd.options.display.max_columns = None
pd.options.display.max_rows = None

# # Suavizado semanal

# In[7]:


# Carga de tabla
client = bigquery.Client()

#  Consulta a BigQuery
query = """
SELECT 
    *
FROM `staging.test_elasticidad_historica_kgv_semanal`
"""
df_backup = client.query(query).to_dataframe()


# In[8]:


df = df_backup.copy()


# ## Filtros

# In[9]:


# Por material/zona/canal/fecha
## TODO: Insertar lista de material a modificar con filtros historicos
# df = df[df['id_material'] == '6033']

# Por rangos
lim_elast_neg = -3  # Limite de elasticidad negativa Prueba: -10 Real: -3
# df = df[(df['elasticidad_promedio_historico_prev'] > 0) | (df['elasticidad_promedio_historico_prev'] < lim_elast_neg)] #Prueba
# df = df[(df['elasticidad_promedio_historico'] > 0) | (df['elasticidad_promedio_historico'] < lim_elast_neg)] #Para pipeline
df = df[
    (df["elasticidad_promedio_historico"] > 0)
    & (df["elasticidad_promedio_historico"] <= 0.5)
]  # Para evaluacion


# In[10]:


list_mat = (
    df.id_material.unique().tolist()
)  # [0:5] #Sample de 5 materiales en exceso, numero real aprox = 1571
list_zon = df.id_zona.unique().tolist()
list_can = df.id_canal_venta.unique().tolist()
total = len(list_mat) * len(list_zon) * len(list_can)


# In[11]:


list_suav = []
for m in list_mat:
    for z in list_zon:
        for c in list_can:
            df_ev = df[
                (df["id_material"] == m)
                & (df["id_zona"] == z)
                & (df["id_canal_venta"] == c)
            ][["fecha_semana", "elasticidad_semana"]]
            if len(df_ev) > 0:
                df_ev = df_ev.sort_values(by="fecha_semana")
                s = df_ev["elasticidad_semana"]
                # Promedio
                elas_prom = s.mean()
                prom_rango = (
                    "True"
                    if (elas_prom <= 0 and elas_prom >= lim_elast_neg)
                    else "False"
                )
                if prom_rango == "False":  # Se hace suavizado solo si es necesario
                    # Media movil
                    media_movil = s.rolling(window=3).mean().dropna()
                    elas_media_movil = media_movil.mean()
                    media_rango = (
                        "True"
                        if (elas_media_movil <= 0 and elas_media_movil >= lim_elast_neg)
                        else "False"
                    )
                    # Exponencial
                    suavizado_exp = s.ewm(alpha=0.3, adjust=False).mean()
                    elas_suav_exp = suavizado_exp.mean()
                    suav_rango = (
                        "True"
                        if (elas_suav_exp <= 0 and elas_suav_exp >= lim_elast_neg)
                        else "False"
                    )
                    list_suav.append(
                        [
                            m,
                            z,
                            c,
                            elas_prom,
                            prom_rango,
                            elas_media_movil,
                            media_rango,
                            elas_suav_exp,
                            suav_rango,
                        ]
                    )
                else:
                    list_suav.append(
                        [
                            m,
                            z,
                            c,
                            elas_prom,
                            prom_rango,
                            elas_prom,
                            "False",
                            elas_prom,
                            "False",
                        ]
                    )
                # print(m,z,c,len(df_ev))
df_suav_test = pd.DataFrame(
    list_suav,
    columns=[
        "id_material",
        "id_zona",
        "id_canal_venta",
        "elasticidad_promedio_historico",
        "elasticidad_promedio_historico_rango",
        "elasticidad_media_movil",
        "elasticidad_media_movil_rango",
        "elasticidad_exp",
        "elasticidad_exp_rango",
    ],
)

# In[12]:


df_suav_test = pd.DataFrame(
    list_suav,
    columns=[
        "id_material",
        "id_zona",
        "id_canal_venta",
        "elasticidad_promedio_historico",
        "elasticidad_promedio_historico_rango",
        "elasticidad_media_movil",
        "elasticidad_media_movil_rango",
        "elasticidad_exp",
        "elasticidad_exp_rango",
    ],
)


# In[13]:


falso_prev = df_suav_test[
    df_suav_test["elasticidad_promedio_historico_rango"] == "False"
].shape[0]
media_fix = df_suav_test[df_suav_test["elasticidad_media_movil_rango"] == "True"].shape[
    0
]
exp_fix = df_suav_test[df_suav_test["elasticidad_exp_rango"] == "True"].shape[0]
total_fix = df_suav_test[
    (df_suav_test["elasticidad_media_movil_rango"] == "True")
    | (df_suav_test["elasticidad_exp_rango"] == "True")
].shape[0]


# In[16]:


df_suav_test = df_suav_test.drop(columns=["elasticidad_promedio_historico"])
df_g = df_backup.merge(
    df_suav_test, on=["id_material", "id_zona", "id_canal_venta"], how="left"
)

df_g["elasticidad_fix"] = np.select(
    [
        df_g["elasticidad_promedio_historico_rango"] == "True",
        df_g["elasticidad_media_movil_rango"] == "True",
        df_g["elasticidad_exp_rango"] == "True",
    ],
    [
        df_g["elasticidad_promedio_historico"],
        df_g["elasticidad_media_movil"],
        df_g["elasticidad_exp"],
    ],
    default=df_g["elasticidad_promedio_historico"],
)

df_g["suavizado_aplicado"] = np.select(
    [
        df_g["elasticidad_media_movil_rango"] == "True",
        df_g["elasticidad_exp_rango"] == "True",
    ],
    [
        "suavizado",
        "suavizado",
    ],
    default="no_suavizado",
)

df_g["elasticidad_promedio_historico_presuavizado"] = df_g[
    "elasticidad_promedio_historico"
]
df_g["elasticidad_promedio_historico"] = df_g["elasticidad_fix"]
df_g = df_g.drop(
    columns=[
        "elasticidad_fix",
        "elasticidad_promedio_historico_rango",
        "elasticidad_media_movil",
        "elasticidad_media_movil_rango",
        "elasticidad_exp",
        "elasticidad_exp_rango",
    ]
)
# df_g = df_g.drop(columns = ['elasticidad_fix','elasticidad_media_movil','elasticidad_exp'])


# In[22]:


table_id = "onus-prd-proy-retail-elastici.staging.test_suavizado_semanal_prev"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
client.load_table_from_dataframe(df_g, table_id, job_config=job_config).result()
