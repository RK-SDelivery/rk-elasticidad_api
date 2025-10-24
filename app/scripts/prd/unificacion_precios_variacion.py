#!/usr/bin/env python
# coding: utf-8

# ### Este codigo busca propagar valores de precio_unitario_promedio de la tabla staging.test_variaciones_precios_unidad_15dias_externos (quincenal) y staging.test_variaciones_precios_unidad_semanal_externos (semanal) agrupados por id_material, id_zona e id_canal_venta ordenado por fecha_quincena si es la diferencia de los precios actual y anterior es menor de 1%, esto es para evitar potenciales elasticidades elevadas

# In[1]:


import pandas as pd
import numpy as np
import time
from google.cloud import bigquery


# In[2]:


pd.options.display.max_columns = None
pd.options.display.max_rows = None


# # Unificacion de precio y fecha

# ## Por semana

# In[3]:

client = bigquery.Client()

#  Consulta a BigQuery
query = """
SELECT 
    *,
    TRUNC(LAG(precio_unitario_promedio, 1, NULL) OVER (PARTITION BY id_material, id_zona, id_canal_venta ORDER BY fecha_semana),2) AS precio_unitario_promedio_anterior,
    LAG(fecha_semana, 1, NULL) OVER (PARTITION BY id_material, id_zona, id_canal_venta ORDER BY fecha_semana) AS fecha_semana_anterior,
FROM `staging.test_variaciones_precios_unidad_semanal_externos`
"""
df = client.query(query).to_dataframe()


# In[4]:


df_p = df.copy()  # backup


# In[6]:


df_p = df_p.sort_values(by=["id_material", "id_zona", "id_canal_venta", "fecha_semana"])


# In[7]:


mat = ""
zona = ""
can = ""
fecha = ""
precio_actual = 0
precios = []
fechas = []

print("Starting iteration over DataFrame")
for index, row in df_p.iterrows():
    if mat == "":  # Solo primera fila
        mat = row["id_material"]
        zona = row["id_zona"]
        can = row["id_canal_venta"]
        precio_anterior = row[
            "precio_unitario_promedio_anterior"
        ]  # Se toma el valor de lag para la comparativa
        fecha_anterior = row[
            "fecha_semana_anterior"
        ]  # Se toma el valor de lag para la comparativa y agrupamiento
    if (
        mat == row["id_material"]
        and zona == row["id_zona"]
        and can == row["id_canal_venta"]
    ):  # Si es mismo material/zona/canal
        if (
            precio_anterior is not None
        ):  # Revision del valor nulo, debe ser el primer valor del conjunto
            if (
                abs(row["precio_unitario_promedio"] - precio_anterior) / precio_anterior
                < 0.01
            ):  # Si la diferencia es menor de 1%
                precios.append(precio_anterior)
                fechas.append(fecha_anterior)
            else:
                precio_anterior = row[
                    "precio_unitario_promedio"
                ]  # Nuevo valor para la propagacion ya que si hay diferencia mayor de 1%
                fecha_anterior = row[
                    "fecha_semana"
                ]  # Nuevo valor para la propagacion ya que si hay diferencia mayor de 1%
                precios.append(row["precio_unitario_promedio"])
                fechas.append(row["fecha_semana"])
        else:
            precio_anterior = row[
                "precio_unitario_promedio"
            ]  # Es el primer precio del conjunto
            fecha_anterior = row["fecha_semana"]  # Es el primer precio del conjunto
            precios.append(row["precio_unitario_promedio"])
            fechas.append(row["fecha_semana"])
    else:  # Si no es el mismo material/zona/canal, sobreescribir punteros
        mat = row["id_material"]
        zona = row["id_zona"]
        can = row["id_canal_venta"]
        precio_anterior = row[
            "precio_unitario_promedio"
        ]  # Va a saltar a siguiente fila, asi que este es el precio_unitario_promedio_anterior
        fecha_anterior = row["fecha_semana"]  # Usar la fecha del registro actual
        precios.append(row["precio_unitario_promedio"])
        fechas.append(row["fecha_semana"])

# In[8]:


df_p["precio_promedio_adaptado"] = precios
df_p["fecha_semana_adaptado"] = fechas
df_p["precio_unitario_promedio"] = df_p["precio_promedio_adaptado"]
df_p = df_p.drop(
    columns=[
        "precio_unitario_promedio_anterior",
        "precio_promedio_adaptado",
        "fecha_semana_anterior",
        "semana",
    ]
)

# In[11]:


table_id = "onus-prd-proy-retail-elastici.staging.test_variaciones_precios_unidad_semanal_externos_v2"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
client.load_table_from_dataframe(df_p, table_id, job_config=job_config).result()


# # Unificacion de variables externas

# ## Tasa de ocupacion

# In[12]:


import pandas as pd
import numpy as np
import time
from google.cloud import bigquery


# In[13]:


client = bigquery.Client()

#  Consulta a BigQuery
query = """
SELECT
      Periodo,
      Trimestre,
      Entidad_Federativa,
      tasa_ocupacion,
      LAG(tasa_ocupacion) OVER(PARTITION BY Entidad_Federativa ORDER BY Periodo, Trimestre) AS tasa_ocupacion_anterior,
      SAFE_DIVIDE(tasa_ocupacion-LAG(tasa_ocupacion) OVER(PARTITION BY Entidad_Federativa ORDER BY Periodo, Trimestre),LAG(tasa_ocupacion) OVER(PARTITION BY Entidad_Federativa ORDER BY Periodo, Trimestre)) AS variacion_ocupacion
    FROM
    (
      SELECT 
        Periodo,
        Trimestre,
        Entidad_Federativa,
        SUM(`Población_ocupada`) AS pob_ocup,
        SUM(`Población_en_edad_de_trabajar`) AS `pob_econ_act`,
        100*(SUM(`Población_ocupada`)/SUM(`Población_en_edad_de_trabajar`)) AS tasa_ocupacion
      FROM `fuentes_externas.tasa_neta_ocupacion`
      WHERE
        Entidad_Federativa != 'Nacional'
      GROUP BY
        Periodo,
        Trimestre,
        Entidad_Federativa
    )
"""
df = client.query(query).to_dataframe()


# In[14]:


df_t = df.copy()


# In[16]:


df_t = df_t[
    [
        "Entidad_Federativa",
        "Periodo",
        "Trimestre",
        "tasa_ocupacion",
        "tasa_ocupacion_anterior",
    ]
]
df_t = df_t.sort_values(by=["Entidad_Federativa", "Periodo", "Trimestre"])


# In[18]:


ent = ""
tasa_anterior = 0
tasas = []
print("Starting iteration over DataFrame")
for index, row in df_t.iterrows():
    if ent == "":  # Solo primera fila
        ent = row["Entidad_Federativa"]
        tasa_anterior = row[
            "tasa_ocupacion_anterior"
        ]  # Se toma el valor de lag para la comparativa
    if ent == row["Entidad_Federativa"]:  # Si es mismo entidad
        if (
            tasa_anterior is not None
        ):  # Revision del valor nulo, debe ser el primer valor del conjunto
            if (
                abs(row["tasa_ocupacion"] - tasa_anterior) / tasa_anterior < 0.01
            ):  # Si la diferencia es menor de 1%
                tasas.append(tasa_anterior)
            else:
                tasa_anterior = row[
                    "tasa_ocupacion"
                ]  # Nuevo valor para la propagacion ya que si hay diferencia mayor de 1%
                tasas.append(row["tasa_ocupacion"])
        else:
            tasa_anterior = row["tasa_ocupacion"]  # Es el primer precio del conjunto
            tasas.append(row["tasa_ocupacion"])
    else:  # Si no es el mismo entidad, sobreescribir punteros
        ent = row["Entidad_Federativa"]
        tasa_anterior = row[
            "tasa_ocupacion"
        ]  # Va a saltar a siguiente fila, asi que este es el tasa_ocupacion_anterior
        tasas.append(row["tasa_ocupacion"])


# In[19]:


df_t["tasa_ocupacion_adaptada"] = tasas


# In[20]:


df_t["tasa_ocupacion"] = df_t["tasa_ocupacion_adaptada"]
df_t = df_t.drop(columns=["tasa_ocupacion_anterior", "tasa_ocupacion_adaptada"])
df_t = df_t.drop_duplicates(
    subset=["Entidad_Federativa", "tasa_ocupacion"], keep="first"
)


# In[22]:


table_id = "onus-prd-proy-retail-elastici.staging.variaciones_tasa_ocupacion_filtrado"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
client.load_table_from_dataframe(df_t, table_id, job_config=job_config).result()


# ## Tipo de cambio

# In[23]:


client = bigquery.Client()

#  Consulta a BigQuery
query = """
    SELECT
      fecha,
      AVG(tipo_cambio) AS tipo_cambio_dia,
      LAG(AVG(tipo_cambio)) OVER(ORDER BY fecha) AS tipo_cambio_dia_anerior,
    FROM `fuentes_externas.tipo_cambio`
    GROUP BY fecha
"""
df = client.query(query).to_dataframe()


# In[24]:


df_tc = df.copy()


# In[26]:


df_tc = df_tc.sort_values(by=["fecha"])


# In[28]:


cambio_anterior = -100
cambio = []
print("Starting iteration over DataFrame")
for index, row in df_tc.iterrows():
    if cambio_anterior < 0:  # Primer valor
        cambio_anterior = row[
            "tipo_cambio_dia_anterior"
        ]  # Se toma el valor de lag para la comparativa
    if (
        cambio_anterior is not None
    ):  # Revision del valor nulo, debe ser el primer valor del conjunto
        if (
            abs(row["tipo_cambio_dia"] - cambio_anterior) / cambio_anterior < 0.01
        ):  # Si la diferencia es menor de 1%
            cambio.append(cambio_anterior)
        else:
            cambio_anterior = row[
                "tipo_cambio_dia"
            ]  # Nuevo valor para la propagacion ya que si hay diferencia mayor de 1%
            cambio.append(row["tipo_cambio_dia"])
    else:
        cambio_anterior = row["tipo_cambio_dia"]  # Es el primer precio del conjunto
        cambio.append(row["tipo_cambio_dia"])

# In[29]:


df_tc["tipo_cambio_dia_adaptada"] = cambio


# In[30]:


df_tc["tipo_cambio_dia"] = df_tc["tipo_cambio_dia_adaptada"]
df_tc = df_tc.drop(columns=["tipo_cambio_dia_adaptada", "tipo_cambio_dia_anerior"])
df_tc = df_tc.drop_duplicates(subset=["tipo_cambio_dia"], keep="first")


# In[32]:


table_id = "onus-prd-proy-retail-elastici.staging.variaciones_tipo_cambio_filtrado"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
client.load_table_from_dataframe(df_tc, table_id, job_config=job_config).result()


# ## INPC

# In[33]:


client = bigquery.Client()

#  Consulta a BigQuery
query = """
    SELECT
      fecha,
      AVG(inpc_nacional) AS inpc_avg,
      LAG(AVG(inpc_nacional)) OVER(ORDER BY fecha) AS inpc_avg_anterior,
    FROM `fuentes_externas.ipc_mensual`
    GROUP BY fecha
"""
df = client.query(query).to_dataframe()


# In[34]:


df_i = df.copy()


# In[36]:


df_i = df_i.sort_values(by=["fecha"])


# In[38]:


inpc_anterior = -100
cambio = []

for index, row in df_i.iterrows():
    if inpc_anterior < 0:  # Primer valor
        inpc_anterior = row[
            "inpc_avg_anterior"
        ]  # Se toma el valor de lag para la comparativa
    if (
        inpc_anterior is not None
    ):  # Revision del valor nulo, debe ser el primer valor del conjunto
        if (
            abs(row["inpc_avg"] - inpc_anterior) / inpc_anterior < 0.01
        ):  # Si la diferencia es menor de 1%
            cambio.append(inpc_anterior)
        else:
            inpc_anterior = row[
                "inpc_avg"
            ]  # Nuevo valor para la propagacion ya que si hay diferencia mayor de 1%
            cambio.append(row["inpc_avg"])
    else:
        inpc_anterior = row["inpc_avg"]  # Es el primer precio del conjunto
        cambio.append(row["inpc_avg"])

# In[39]:

df_i["inpc_avg_adaptada"] = cambio


# In[40]:


df_i["inpc_avg"] = df_i["inpc_avg_adaptada"]
df_i = df_i.drop(columns=["inpc_avg_adaptada", "inpc_avg_anterior"])
df_i = df_i.drop_duplicates(subset=["inpc_avg"], keep="first")


# In[42]:


table_id = "onus-prd-proy-retail-elastici.staging.variaciones_inpc_filtrado"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
client.load_table_from_dataframe(df_i, table_id, job_config=job_config).result()


# In[ ]:
