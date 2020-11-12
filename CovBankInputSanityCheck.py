# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import os
import numpy as np
import re
import sys
import logging
import gc


_debug = False

base_dir_out = "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/" 

liste_envois_to_tsp_geo_mismatch = os.path.join(base_dir_out,"liste_envois_to_tsp_geo_mismatch.xlsx")

stat_tsp_geo = os.path.join(base_dir_out,"stat_tsp_geo.xlsx")
stat_liste_envois = os.path.join(base_dir_out,"stat_liste_envois.xlsx")
stat_sgil = os.path.join(base_dir_out,"stat_sgil.xlsx")


if _debug:
    tsp_geo = "/data/Databases/CovBanQ_Epi/TSP_GEO/TSP_geo_20201014_small.xlsx"
    liste_envois = "/data/Databases/CovBanQ_Epi/LISTE_ENVOIS_GENOME_QUEBEC/EnvoiSmall.xlsx"
    sgil = "/data/Databases/CovBanQ_Epi/SGIL_EXTRACT/extract_with_Covid19_extraction_v2_20200923_CovidPos_small.txt"
else:
    tsp_geo = "/data/Databases/CovBanQ_Epi/TSP_GEO/TSP_geo_20201028.xlsx"
    liste_envois = "/data/Databases/CovBanQ_Epi/LISTE_ENVOIS_GENOME_QUEBEC/ListeEnvoisGenomeQuebec_2020-10-29.xlsx"
    sgil = "/data/Databases/CovBanQ_Epi/SGIL_EXTRACT/extract_with_Covid19_extraction_v2_20200923_CovidPos.txt"


pd_df_tsp_geo = pd.read_excel(tsp_geo,sheet_name=0)
pd_df_liste_envois = pd.read_excel(liste_envois,sheet_name=0)
pd_df_sgil = pd.read_table(sgil)


################### TEST LIST ENVOIS ###########################
print("Check List Envois")


pd_df_liste_envois['Date de naissance'] = pd.to_datetime(pd_df_liste_envois['Date de naissance'],format='%Y-%m-%d',errors='coerce')
pd_df_liste_envois['Date de prélèvement'] = pd.to_datetime(pd_df_liste_envois['Date de prélèvement'],format='%Y-%m-%d',errors='coerce')
pd_df_liste_envois['DateEnvoiGenomeQuebec'] = pd.to_datetime(pd_df_liste_envois['DateEnvoiGenomeQuebec'],format='%Y-%m-%d',errors='coerce')

df_test = pd_df_liste_envois.loc[pd.isna(pd_df_liste_envois['Date de naissance'])]
nb_dt_naiss_prob = df_test.shape[0]

df_test = pd_df_liste_envois.loc[pd.isna(pd_df_liste_envois['Date de prélèvement'])]
nb_dt_prel_prob = df_test.shape[0]

df_test = pd_df_liste_envois.loc[pd.isna(pd_df_liste_envois['DateEnvoiGenomeQuebec'])]
nb_dt_envois_prob = df_test.shape[0]

df_test = pd_df_liste_envois.loc[pd.isna(pd_df_liste_envois['NAM'])]
nb_nam_prob = df_test.shape[0]

nb_total = pd_df_liste_envois.shape[0]

pd_serie_stat_liste_envois = pd.Series([nb_total,nb_dt_naiss_prob,nb_dt_prel_prob,nb_dt_envois_prob,nb_nam_prob],index=['Total records','Date naiss missing', 'Date prelev missing', 'Date envois missing','NAM missing'])
#print(pd_serie_stat_liste_envois)

pd_serie_stat_liste_envois.to_excel(stat_liste_envois,sheet_name='Sheet1')

################### TEST TSP GEO ###########################
print("Check TSP GEO")

pd_df_tsp_geo['date_nais'] = pd.to_datetime(pd_df_tsp_geo['date_nais'],format='%Y%m%d',errors='coerce')
pd_df_tsp_geo['date_prel'] = pd.to_datetime(pd_df_tsp_geo['date_prel'],format='%Y%m%d',errors='coerce')

df_test = pd_df_tsp_geo.loc[pd.isna(pd_df_tsp_geo['date_nais'])]
nb_dt_naiss_prob = df_test.shape[0]

df_test = pd_df_tsp_geo.loc[pd.isna(pd_df_tsp_geo['date_prel'])]
nb_dt_prel_prob = df_test.shape[0]

df_test = pd_df_tsp_geo.loc[pd.isna(pd_df_tsp_geo['nam'])]
nb_nam_prob = df_test.shape[0]

df_test = pd_df_tsp_geo.loc[pd.isna(pd_df_tsp_geo['nom_rss'])]
nb_rss_prob = df_test.shape[0]

df_test = pd_df_tsp_geo.loc[pd.isna(pd_df_tsp_geo['code_pos'])]
nb_rta_prob = df_test.shape[0]

nb_total = pd_df_tsp_geo.shape[0]

pd_serie_stat_tsp_geo = pd.Series([nb_total,nb_dt_naiss_prob,nb_dt_prel_prob,nb_nam_prob,nb_rss_prob,nb_rta_prob],index=['Total records','Date naiss missing', 'Date prelev missing', 'NAM  missing','RSS missing','RTA missing'])

pd_serie_stat_tsp_geo.to_excel(stat_tsp_geo,sheet_name='Sheet1')


################### TEST SGIL ###########################
print("Check SGIL")

pd_df_sgil['DATE_NAISS'] = pd.to_datetime(pd_df_sgil['DATE_NAISS'],format='%Y-%m-%d',errors='coerce')
pd_df_sgil['SAMPLED_DATE'] = pd.to_datetime(pd_df_sgil['SAMPLED_DATE'],format='%Y-%m-%d',errors='coerce')

df_test = pd_df_sgil.loc[pd.isna(pd_df_sgil['DATE_NAISS'])]
nb_dt_naiss_prob = df_test.shape[0]

df_test = pd_df_sgil.loc[pd.isna(pd_df_sgil['SAMPLED_DATE'])]
nb_dt_prel_prob = df_test.shape[0]

df_test = pd_df_sgil.loc[pd.isna(pd_df_sgil['NAM'])]
nb_nam_prob = df_test.shape[0]

nb_total = pd_df_sgil.shape[0]

pd_serie_stat_sgil = pd.Series([nb_total,nb_dt_naiss_prob,nb_dt_prel_prob,nb_nam_prob],index=['Total records','Date naiss missing', 'Date prelev missing', 'NAM  missing'])

pd_serie_stat_sgil.to_excel(stat_sgil,sheet_name='Sheet1')












