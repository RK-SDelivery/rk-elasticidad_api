#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import time
from google.cloud import bigquery
from scipy.optimize import minimize


# In[2]:


pd.options.display.max_columns = None
pd.options.display.max_rows = None


# In[3]:


list_rangos = []
list_rangos.append(['ABARROTES COMESTIBLES',0.05,0.04,0.03,0.02])
list_rangos.append(['LÁCTEOS',0.04,0.03,0.02,0.01])
list_rangos.append(['ABARROTES INSTITUCIONAL',0.06,0.05,0.04,0.03])
list_rangos.append(['COMIDAS PREPARADAS',0.05,0.04,0.03,0.02])
list_rangos.append(['CONGELADOS',0.05,0.04,0.03,0.02])
list_rangos.append(['RES',0.06,0.05,0.04,0.03])
list_rangos.append(['BEBIDAS NO ALCOHÓLICAS',0.04,0.03,0.02,0.01])
list_rangos.append(['ABARROTES NO COMESTIBLES',0.06,0.05,0.04,0.03])
list_rangos.append(['VÍSCERAS Y OTROS',0.08,0.06,0.05,0.04])
list_rangos.append(['CERDO',0.06,0.05,0.04,0.03])
list_rangos.append(['FRUTAS Y VERDURAS',0.05,0.04,0.03,0.02])
list_rangos.append(['CARNES FRÍAS',0.05,0.04,0.03,0.02])
list_rangos.append(['MADURADOS',0.06,0.05,0.04,0.03])
list_rangos.append(['CREMAS Y YOGHURTS',0.04,0.03,0.02,0.01])
list_rangos.append(['PESCADOS Y MARISCOS',0.06,0.05,0.04,0.03])
list_rangos.append(['AVES',0.05,0.04,0.03,0.02])
df_rangos = pd.DataFrame(list_rangos, columns=['grupo_articulo','PU','MM','MA','DI'])


# # Optimizacion con ventas agrupadas por semana

# In[4]:


# Carga de tabla
client = bigquery.Client()

#  Consulta a BigQuery
query = """
SELECT 
    *
FROM `staging.tabla_prep_optimizacion_semanal`
WHERE 
    1=1
    AND fecha_semana >= '2023-01-01'
    AND id_canal_venta != 'CO'
"""
df_backup = client.query(query).to_dataframe()


# ## Funcion aproximada

# In[5]:


# Copia de respaldo para evitar llamar query nuevamente
df = df_backup.copy()



# In[7]:


# Cambio de tipos
df['precio_unitario_promedio'] = df['precio_unitario_promedio'].astype(float)
df['coste_unitario'] = df['coste_unitario'].astype(float)

# Evaluacion de cuefieciente k para elasticidad variable
df['coef_k'] = np.select(
    [
        df['elasticidad_promedio_historico'] <= -1.5,
        df['elasticidad_promedio_historico'] <= -1.0,
        df['elasticidad_promedio_historico'] <= -0.2
    ],
    [
        8,
        5,
        3,
    ],
    default = 3
)


# In[8]:


# Calculo de constantes (variables externas)
#df['demanda_tasa_desocupacion'] = df['elasticidad_tasa_ocupacion'] * df['porc_var_tasa_desocupacion'] # beta * valor

# Original
df['demanda_tasa_ocupacion'] = df['elasticidad_tasa_ocupacion'] * df['porc_var_tasa_ocupacion'] # beta * valor
df['demanda_tipo_cambio'] = df['elasticidad_tipo_cambio'] * df['porc_var_tipo_cambio'] # beta * valor
df['demanda_inpc'] = df['elasticidad_inpc'] * df['porc_var_inpc_nacional'] # beta * valor
df['demanda_pib'] = df['elasticidad_pib'] * df['porc_var_pib'] # beta * valor

# Reducción a 0 si la elasticidad esta en rango [-0.2, 0.2]
df['demanda_tasa_ocupacion'] = np.where(abs(df['elasticidad_tasa_ocupacion']) > 0.2, df['elasticidad_tasa_ocupacion'] * df['porc_var_tasa_ocupacion'], 0.0) # beta * valor
df['demanda_tipo_cambio'] = np.where(abs(df['elasticidad_tipo_cambio']) > 0.2, df['elasticidad_tipo_cambio'] * df['porc_var_tipo_cambio'], 0.0) # beta * valor
df['demanda_inpc'] = np.where(abs(df['elasticidad_inpc']) > 0.2, df['elasticidad_inpc'] * df['porc_var_inpc_nacional'], 0.0) # beta * valor
df['demanda_pib'] = np.where(abs(df['elasticidad_pib']) > 0.2, df['elasticidad_pib'] * df['porc_var_pib'], 0.0) # beta * valor


# In[10]:


opt = []
precio = 0
for index, row in df.iterrows():
    valores_externos = row['demanda_tasa_ocupacion'] + row['demanda_tipo_cambio'] + row['demanda_inpc'] + row['demanda_pib']
    def objective(p): # Funcion objetivo
        # El objetivo es maximizar, asi que debe hacerse negativo
        # Cambio v3. La elasticidad aumenta cada vez que la diferencia entre el precio original y propuesto aumenta/disminuye en espacios de 5%
        # aumento_elast = (abs(p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']) / 0.05
        # nuevo_elast = row['elasticidad_promedio_historico'] + (int(aumento_elast) * row['elasticidad_promedio_historico'])

        # Cambio v3 - usando coeficiente k y elasticidad variable
        nuevo_elast = row['elasticidad_promedio_historico'] * (1 + row['coef_k'] * abs((p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']))
        
        delta_q = (nuevo_elast*(100*((p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']))+valores_externos) / 100 #con valores externos
        #delta_q = (nuevo_elast*(100*((p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']))) / 100 #Sin valores externos
        return -1.0*((row['unidades_sum_kgv'] * (1+delta_q)) * (p[0]-row['coste_unitario']))
    
    def constraint1(p): # Restriccion precio >= coste
        return (p[0]-0.01)-row['coste_unitario'] #Debe ser mayor el precio al coste, aunque sea 1 centavo

    def get_nueva_elast(p): # Obtiene la nueva elasticidad para guardarlo en el dataset
        aumento_elast = (abs(p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']) / 0.05
        nuevo_elast = row['elasticidad_promedio_historico'] + (int(aumento_elast) * row['elasticidad_promedio_historico'])
        return nuevo_elast

    # Variable y valor inicial
    p = [row['precio_unitario_promedio']]
    #print('Precio original = ' + str(p[0]))

    # Valor inicial
    #print('Revenue original: ' + str(-1.0*objective(p)))
    ganancia_anterior = -1.0*objective(p)
    if(row['precio_unitario_promedio'] != row['coste_unitario']):
        unidades_anterior = (-1.0*objective(p))/(row['precio_unitario_promedio']-row['coste_unitario'])
    else:
        unidades_anterior = 0

    # Optimizacion
    # Rango fijo
    # b = [(row['precio_unitario_promedio'] * 0.70, row['precio_unitario_promedio'] * 1.30)]

    # Rango dependiendo de elasticidad
    '''
    if(row['elasticidad_promedio_historico'] <= -1.5): # 30%
        b = [(row['precio_unitario_promedio'] * 0.70, row['precio_unitario_promedio'] * 1.30)]
    elif(row['elasticidad_promedio_historico'] <= -1.0): # 20%
        b = [(row['precio_unitario_promedio'] * 0.80, row['precio_unitario_promedio'] * 1.20)]
    elif(row['elasticidad_promedio_historico'] <= -0.2): # 10%
        b = [(row['precio_unitario_promedio'] * 0.90, row['precio_unitario_promedio'] * 1.10)]
    else: #Si la elasticidad es muy pequeña o cercana a 0 (-0.2 a 0), el optimizador no puede hacer una propuesta adecuada
        b = [(row['precio_unitario_promedio'], row['precio_unitario_promedio'])]
    '''

    # Rango dependiendo de grupo de articulo
    '''
    match row['grupo_articulo']:
        case "ABARROTES COMESTIBLES":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "LÁCTEOS":
            b = [(row['precio_unitario_promedio'] * 0.96, row['precio_unitario_promedio'] * 1.04)] # 4%
        case "ABARROTES INSTITUCIONAL":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "COMIDAS PREPARADAS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "CONGELADOS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "RES":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "BEBIDAS NO ALCOHÓLICAS":
            b = [(row['precio_unitario_promedio'] * 0.96, row['precio_unitario_promedio'] * 1.04)] # 4%
        case "ABARROTES NO COMESTIBLES":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "VÍSCERAS Y OTROS":
            b = [(row['precio_unitario_promedio'] * 0.92, row['precio_unitario_promedio'] * 1.08)] # 8%
        case "CERDO":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "FRUTAS Y VERDURAS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "CARNES FRÍAS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "MADURADOS":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "CREMAS Y YOGHURTS":
            b = [(row['precio_unitario_promedio'] * 0.96, row['precio_unitario_promedio'] * 1.04)] # 4%
        case "PESCADOS Y MARISCOS":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "AVES":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case _:
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5% por default
    '''

    # Rango dependiendo de grupo de articulo + canal de venta
    precio_min = 0.0
    precio_max = 0.0
    match row['id_canal_venta']:
        case 'PU':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
        case 'MM':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,2]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,2]) * row['precio_unitario_promedio']
        case 'MA':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,3]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,3]) * row['precio_unitario_promedio']
        case 'DI':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,4]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,4]) * row['precio_unitario_promedio']
        case _:
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
    b = [(precio_min, precio_max)]

    bounds = b
    con1 = {'type': 'ineq', 'fun': constraint1}
    cons = ([con1])
    solution = minimize(objective,p,method='SLSQP',bounds=bounds,constraints=cons)
    p = solution.x

    # Revenue final
    #print('Revenue final: ' + str(-1.0*objective(p)))
    
    # Precio sugerido
    #print('Precio sugerido = ' + str(p[0]))

    # Unidades predichas
    unidades_predichas = (-1.0*objective(p))/(p[0]-row['coste_unitario'])

    # Nueva elasticidad
    # nueva_elast = get_nueva_elast(p)

    # TODO - Agregar todos los datos a un array
    opt.append([row['id_material'],row['id_zona'],row['id_canal_venta'],row['precio_unitario_promedio'],row['coste_unitario'],unidades_anterior,ganancia_anterior,p[0],unidades_predichas,-1.0*objective(p)])

df_opt = pd.DataFrame(opt,columns=['id_material','id_zona','id_canal_venta','precio_actual','coste_unitario','demanda_actual','ganancia_actual','precio_sugerido','prediccion_ventas','ganancia_nueva'])


# In[11]:


df_opt['test_porc_dif_precio'] = 100*(df_opt['precio_sugerido']-df_opt['precio_actual'])/df_opt['precio_actual']
df_opt['test_porc_dif_ganancia'] = 100*(df_opt['ganancia_nueva']-df_opt['ganancia_actual'])/df_opt['ganancia_actual']


# ## Funcion exacta

# In[12]:


# Copia de respaldo para evitar llamar query nuevamente
df = df_backup.copy()


# In[13]:


# Cambio de tipos
df['precio_unitario_promedio'] = df['precio_unitario_promedio'].astype(float)
df['coste_unitario'] = df['coste_unitario'].astype(float)

# Evaluacion de cuefieciente k para elasticidad variable
df['coef_k'] = np.select(
    [
        df['elasticidad_promedio_historico'] <= -1.5,
        df['elasticidad_promedio_historico'] <= -1.0,
        df['elasticidad_promedio_historico'] <= -0.2
    ],
    [
        8,
        5,
        3,
    ],
    default = 3
)


# In[14]:


# Calculo de constantes (variables externas)
# Original
df['isoelastica_tasa_desocupacion'] = (df['tasa_desocupacion_avg_actual'] / df['tasa_desocupacion_avg']) ** df['elasticidad_tasa_ocupacion']
df['isoelastica_tasa_ocupacion'] = (df['tasa_ocupacion_avg_actual'] / df['tasa_ocupacion_avg']) ** df['elasticidad_tasa_ocupacion']
df['isoelastica_tipo_cambio'] = (df['tipo_cambio_avg_actual'] / df['tipo_cambio_avg']) ** df['elasticidad_tipo_cambio']
df['isoelastica_inpc'] = (df['inpc_nacional_actual'] / df['inpc_nacional']) ** df['elasticidad_inpc']
df['isoelastica_pib'] = (df['pib_millones_actual'] / df['pib_millones']) ** df['elasticidad_pib']

# Reducción a 1 (sin cambio en multiplicacion) si la elasticidad esta en rango [-0.2, 0.2]
df['isoelastica_tasa_desocupacion'] = np.where(abs(df['elasticidad_tasa_ocupacion']) > 0.2, (df['tasa_desocupacion_avg_actual'] / df['tasa_desocupacion_avg']) ** df['elasticidad_tasa_ocupacion'], 1.0)
df['isoelastica_tasa_ocupacion'] = np.where(abs(df['elasticidad_tasa_ocupacion']) > 0.2, (df['tasa_ocupacion_avg_actual'] / df['tasa_ocupacion_avg']) ** df['elasticidad_tasa_ocupacion'], 1.0)
df['isoelastica_tipo_cambio'] = np.where(abs(df['elasticidad_tipo_cambio']) > 0.2, (df['tipo_cambio_avg_actual'] / df['tipo_cambio_avg']) ** df['elasticidad_tipo_cambio'], 1.0)
df['isoelastica_inpc'] = np.where(abs(df['elasticidad_inpc']) > 0.2, (df['inpc_nacional_actual'] / df['inpc_nacional']) ** df['elasticidad_inpc'], 1.0)
df['isoelastica_pib'] = np.where(abs(df['elasticidad_pib']) > 0.2, (df['pib_millones_actual'] / df['pib_millones']) ** df['elasticidad_pib'], 1.0)


# In[16]:


opt = []
precio = 0
for index, row in df.iterrows():
    def objective(p): # Funcion objetivo
        # El objetivo es maximizar, asi que debe hacerse negativo
        # Cambio v3. La elasticidad aumenta cada vez que la diferencia entre el precio original y propuesto aumenta/disminuye en espacios de 5%
        # aumento_elast = (abs(p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']) / 0.05
        # nuevo_elast = row['elasticidad_promedio_historico'] + (int(aumento_elast) * row['elasticidad_promedio_historico'])

        # Cambio v3 - usando coeficiente k y elasticidad variable
        nuevo_elast = row['elasticidad_promedio_historico'] * (1 + row['coef_k'] * abs((p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']))

        # El objetivo es maximizar, asi que debe hacerse negativo
        isoelastica_precio = (p[0] / row['precio_unitario_promedio']) ** nuevo_elast
        nueva_demanda = row['unidades_sum_kgv'] * isoelastica_precio * row['isoelastica_tasa_ocupacion'] * row['isoelastica_tipo_cambio'] * row['isoelastica_inpc'] * row['isoelastica_pib'] #Con externas
        #nueva_demanda = row['unidades_sum_kgv'] * isoelastica_precio #Sin externas
        return -1.0*(nueva_demanda * (p[0]-row['coste_unitario']))
    
    def constraint1(p): # Restriccion precio >= coste
        return (p[0]-0.01)-row['coste_unitario'] #Debe ser mayor el precio al coste, aunque sea 1 centavo

    def get_nueva_elast(p): # Obtiene la nueva elasticidad para guardarlo en el dataset
        aumento_elast = (abs(p[0]-row['precio_unitario_promedio'])/row['precio_unitario_promedio']) / 0.05
        nuevo_elast = row['elasticidad_promedio_historico'] + (int(aumento_elast) * row['elasticidad_promedio_historico'])
        return nuevo_elast

    # Variable y valor inicial
    p = [row['precio_unitario_promedio']]
    #print('Precio original = ' + str(p[0]))

    # Valor inicial
    ganancia_anterior = -1.0*objective(p)
    if(row['precio_unitario_promedio'] != row['coste_unitario']):
        unidades_anterior = (-1.0*objective(p))/(row['precio_unitario_promedio']-row['coste_unitario'])
    else:
        unidades_anterior = 0

    # Optimizacion
    # Rango fijo
    # b = [(row['precio_unitario_promedio'] * 0.70, row['precio_unitario_promedio'] * 1.30)]

    # Rango dependiendo de elasticidad
    '''
    if(row['elasticidad_promedio_historico'] <= -1.5): # 30%
        b = [(row['precio_unitario_promedio'] * 0.70, row['precio_unitario_promedio'] * 1.30)]
    elif(row['elasticidad_promedio_historico'] <= -1.0): # 20%
        b = [(row['precio_unitario_promedio'] * 0.80, row['precio_unitario_promedio'] * 1.20)]
    elif(row['elasticidad_promedio_historico'] <= -0.2): # 10%
        b = [(row['precio_unitario_promedio'] * 0.90, row['precio_unitario_promedio'] * 1.10)]
    else: #Si la elasticidad es muy pequeña o cercana a 0 (-0.2 a 0), el optimizador no puede hacer una propuesta adecuada
        b = [(row['precio_unitario_promedio'], row['precio_unitario_promedio'])]
    '''

    # Rango dependiendo de grupo de articulo
    '''
    match row['grupo_articulo']:
        case "ABARROTES COMESTIBLES":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "LÁCTEOS":
            b = [(row['precio_unitario_promedio'] * 0.96, row['precio_unitario_promedio'] * 1.04)] # 4%
        case "ABARROTES INSTITUCIONAL":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "COMIDAS PREPARADAS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "CONGELADOS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "RES":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "BEBIDAS NO ALCOHÓLICAS":
            b = [(row['precio_unitario_promedio'] * 0.96, row['precio_unitario_promedio'] * 1.04)] # 4%
        case "ABARROTES NO COMESTIBLES":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "VÍSCERAS Y OTROS":
            b = [(row['precio_unitario_promedio'] * 0.92, row['precio_unitario_promedio'] * 1.08)] # 8%
        case "CERDO":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "FRUTAS Y VERDURAS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "CARNES FRÍAS":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case "MADURADOS":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "CREMAS Y YOGHURTS":
            b = [(row['precio_unitario_promedio'] * 0.96, row['precio_unitario_promedio'] * 1.04)] # 4%
        case "PESCADOS Y MARISCOS":
            b = [(row['precio_unitario_promedio'] * 0.94, row['precio_unitario_promedio'] * 1.06)] # 6%
        case "AVES":
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5%
        case _:
            b = [(row['precio_unitario_promedio'] * 0.95, row['precio_unitario_promedio'] * 1.05)] # 5% por default
    '''

    # Rango dependiendo de grupo de articulo + canal de venta
    precio_min = 0.0
    precio_max = 0.0
    match row['id_canal_venta']:
        case 'PU':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
        case 'MM':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,2]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,2]) * row['precio_unitario_promedio']
        case 'MA':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,3]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,3]) * row['precio_unitario_promedio']
        case 'DI':
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,4]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,4]) * row['precio_unitario_promedio']
        case _:
            precio_min = (1.0 - df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
            precio_max = (1.0 + df_rangos[df_rangos['grupo_articulo'] == row['grupo_articulo']].iat[0,1]) * row['precio_unitario_promedio']
    b = [(precio_min, precio_max)]
    
    bounds = b
    con1 = {'type': 'ineq', 'fun': constraint1}
    cons = ([con1])
    solution = minimize(objective,p,method='SLSQP',bounds=bounds,constraints=cons)
    p = solution.x

    # Revenue final
    #print('Revenue final: ' + str(-1.0*objective(p)))
    
    # Precio sugerido
    #print('Precio sugerido = ' + str(p[0]))

    # Unidades predichas
    unidades_predichas = (-1.0*objective(p))/(p[0]-row['coste_unitario'])

    # Nueva elasticidad
    # nueva_elast = get_nueva_elast(p)

    # TODO - Agregar todos los datos a un array
    opt.append([row['id_material'],row['id_zona'],row['id_canal_venta'],row['precio_unitario_promedio'],row['coste_unitario'],unidades_anterior,ganancia_anterior,p[0],unidades_predichas,-1.0*objective(p)])
df_ex = pd.DataFrame(opt,columns=['id_material','id_zona','id_canal_venta','precio_actual','coste_unitario','demanda_actual','ganancia_actual','precio_sugerido','prediccion_ventas','ganancia_nueva'])



# In[17]:


df_ex['test_porc_dif_precio'] = 100*(df_ex['precio_sugerido']-df_ex['precio_actual'])/df_ex['precio_actual']
df_ex['test_porc_dif_ganancia'] = 100*(df_ex['ganancia_nueva']-df_ex['ganancia_actual'])/df_ex['ganancia_actual']


# ## Unificacion de ambas predicciones

# In[23]:


df_opt['modelo'] = 'aproximada'
df_ex['modelo'] = 'exacta'
df_unificado = pd.concat([df_opt, df_ex], ignore_index = True)


# ### Revision de los datos optimizados para elegir modelo adecuado

# In[25]:


#  Consulta a BigQuery
query = """
SELECT
    DISTINCT *
FROM
(
    SELECT 
        id_material, id_zona, id_canal_venta, elasticidad_promedio_historico_count, elasticidad_promedio_historico
    FROM `staging.test_elasticidad_historica_kgv_semanal`
    WHERE 
        1=1
        AND fecha_semana >= '2023-01-01'
        AND id_canal_venta != 'CO'
        AND elasticidad_promedio_historico IS NOT NULL
)
"""
df_check = client.query(query).to_dataframe()


# In[26]:


# Relleno de nulos para evitar errores
df_check['elasticidad_promedio_historico_count'] = df_check['elasticidad_promedio_historico_count'].fillna(0)

# Seleccion de metodo por condiciones
df_check['modelo_elast'] = np.select(
    [
        df_check['elasticidad_promedio_historico_count'] >= 12,
        np.abs(df_check['elasticidad_promedio_historico']) > 2,
    ],
    [
        "exacta",
        "exacta"
    ],
    default = 'aproximada'
)


# ### Revision de cambio de precio para seleccion de modelo

# In[27]:


#  Consulta a BigQuery
query = """
SELECT 
    *,
    TRUNC(LAG(precio_unitario_promedio, 1, NULL) OVER (PARTITION BY id_material, id_zona, id_canal_venta ORDER BY fecha_semana),2) AS precio_unitario_promedio_anterior,
    LAG(fecha_semana, 1, NULL) OVER (PARTITION BY id_material, id_zona, id_canal_venta ORDER BY fecha_semana) AS fecha_semana_anterior,
FROM `staging.test_variaciones_precios_unidad_semanal_externos_v2`
"""
df_cambio = client.query(query).to_dataframe()

# In[28]:


df_cambio['var_precio'] = np.abs((df_cambio['precio_unitario_promedio'] - df_cambio['precio_unitario_promedio_anterior']) / df_cambio['precio_unitario_promedio_anterior'])


# In[29]:

df_var_precio = df_cambio[['id_material','id_zona','id_canal_venta','var_precio']].groupby(by=['id_material','id_zona','id_canal_venta']).max().reset_index()
df_var_precio['modelo_precio'] = np.where(df_var_precio['var_precio'] > 0.05,"exacta","aproximada")

# In[31]:


# Union de dos formas de seleccion - Revision de elasticidad y cambio de precio
# Modelo usado: Si ambos concluyen el mismo modelo, usarlo. Si son diferentes, usar exacta
df_metodo = df_check.merge(df_var_precio, on = ['id_material','id_zona','id_canal_venta'], how='left')
df_metodo['modelo'] = np.where(df_metodo['modelo_elast'] == df_metodo['modelo_precio'],df_metodo['modelo_elast'],"exacta")

# Filtrar salida con respecto a metodo seleccionado
df_salida = df_unificado.merge(df_metodo[['id_material','id_zona','id_canal_venta','modelo']], on = ['id_material','id_zona','id_canal_venta','modelo'], how = 'inner')


# In[36]:


# Salida a BigQuery
table_id = "onus-prd-proy-retail-elastici.staging.tabla_optimizacion_semanal"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
client.load_table_from_dataframe(df_salida, table_id, job_config=job_config).result()


# In[ ]:




