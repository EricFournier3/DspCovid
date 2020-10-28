# -*- coding: utf-8 -*-

"""
Eric Fournier 2020-10-28

"""

import mysql.connector
import datetime
import pandas as pd
import os
import numpy as np
import re
import sys
import logging
import gc
import yaml # install de yaml avec la commande  sudo /data/Applications/Miniconda/miniconda3/bin/python -m pip install pyyaml  le 2020-10-28

global _debug_
_debug_ = True

class CovBankDB:
    def __init__(self,tsp_geo_obj,envois_genome_qc_obj):
        self.tsp_geo_obj = tsp_geo_obj
        self.envois_genome_qc_obj = envois_genome_qc_obj
        self.yaml_conn_param = open('CovBankParam.yaml')
        self.ReadConnParam()
        self.connection = self.SetConnection()

    def SetConnection(self):
        return mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database)

    def GetConnection(self):
        return self.connection

    def GetCursor(self):
        return self.GetConnection().cursor()

    def Commit(self):
        self.connection.commit()

    def GetPatientsColumns(self):
        col_list = ['PRENOM','NOM','SEXE','DTNAISS','RSS','RTA','NAM']
        return ','.join(col_list)

    def Insert(self):
        logging.info("Begin insert")

        for index, row in self.envois_genome_qc_obj.pd_df.loc[:,].iterrows():
            #print(row)
            nom = row['Nom']
            prenom = row['Prénom']
            date_naiss = row['Date de naissance']
            date_prelev = row['Date de prélèvement']
            nam = row['NAM']
            #print("************************")
            tsp_geo_match_df = self.GetTspGeoMatch(nom,prenom,date_naiss,nam,date_prelev)
            #print(tsp_geo_match_df)

            if tsp_geo_match_df.shape[0] != 0:
                patient_record = self.GetPatientValToInsert(tsp_geo_match_df,row,'HOSPITAL_NAME','HOSPITAL_ADDRESS' )
                self.InsertPatient(patient_record)

    def InsertPatient(self,patient_record):
        #self.CheckIfPatientExist()

        try:
            ncols = len(patient_record)
            print(self.GetPatientsColumns())

        except:
            pass


    def GetPatientValToInsert(self,tsp_geo_match_df,envois_genome_qc_row,hospital_name,hospital_address):
        def GetVal(x):
            return x

        nom = tsp_geo_match_df['nom'].values[0]
        prenom = tsp_geo_match_df['prenom'].values[0]

        sexe =  tsp_geo_match_df['sexe'].values[0]
        if str(sexe) == "1":
            sexe = "M"
        elif str(sexe) == "2":
            sexe = "F"
        else:
            sexe = "Inconnu" 

        date_naiss = tsp_geo_match_df['date_nais'].values[0]        
        rss = tsp_geo_match_df['RSS'].values[0]
        rta = tsp_geo_match_df['RTA'].values[0]

        return(tuple(map(GetVal,(nom,prenom,sexe,date_naiss,rss,rta,hospital_name,hospital_address))))

    def GetTspGeoMatch(self,nom,prenom,date_naiss,nam,date_prelev):
        match_df = pd.DataFrame()
        tsp_geo_obj_pd_df = self.tsp_geo_obj.pd_df

        if(len(str(nam)) > 9):
            match_df = tsp_geo_obj_pd_df.loc[tsp_geo_obj_pd_df['nam'] == nam,:].copy()
            #print(match_df)

        if match_df.shape[0] == 0 :
            match_df = tsp_geo_obj_pd_df.loc[(tsp_geo_obj_pd_df['nom'] == nom) & (tsp_geo_obj_pd_df['prenom'] == prenom) & (tsp_geo_obj_pd_df['date_nais'] == date_naiss) & (tsp_geo_obj_pd_df['date_prel'] == date_prelev),:].copy()
            #print(match_df)

        if match_df.shape[0] == 0 :
            return match_df

        self.ComputeDatePrelevDiff(match_df,date_prelev)

        return(match_df[match_df.DATE_DIFF == match_df.DATE_DIFF.min()])

    def ComputeDatePrelevDiff(self,match_df,date_prelev):
        match_df['DATE_DIFF'] = match_df['date_prel'] - date_prelev
        match_df['DATE_DIFF'] = match_df['DATE_DIFF'].abs()


    def ReadConnParam(self):
        param = yaml.load(self.yaml_conn_param,Loader=yaml.FullLoader)
        self.host = param['host']
        self.user = param['user']
        self.password = param['password']
        self.database = param['database']


class TspGeoData:
    def __init__(self):
        self.base_dir = "/data/Databases/CovBanQ_Epi/TSP_GEO"
        
        if _debug_:
            excel_data = "TSP_geo_20201014_small.xlsx"
        else:
            excel_data = "TSP_geo_20201014.xlsx"
        
        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)
        self.Format()

    def Format(self):
        self.pd_df['nom'] = self.pd_df['nom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c')
        self.pd_df['prenom'] = self.pd_df['prenom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c')

        self.pd_df['nom'] = self.pd_df['nom'].str.strip(' ')
        self.pd_df['prenom'] = self.pd_df['prenom'].str.strip(' ')
        self.pd_df['nam'] = self.pd_df['nam'].str.strip(' ')

        self.pd_df['nom'] = self.pd_df['nom'].str.upper()
        self.pd_df['prenom'] = self.pd_df['prenom'].str.upper()
        self.pd_df['nam'] = self.pd_df['nam'].str.upper()

        #print(self.pd_df[['date_nais','date_prel']])
        self.pd_df['date_nais'] = pd.to_datetime(self.pd_df['date_nais'],format='%Y%m%d',errors='coerce')
        self.pd_df['date_prel'] = pd.to_datetime(self.pd_df['date_prel'],format='%Y%m%d',errors='coerce')
        #print(self.pd_df[['date_nais','date_prel']])

        self.pd_df['RSS'] =  self.pd_df['RSS_code'].astype(str) + "-" + self.pd_df['RSS_nom']  # ATTENTION ici ca enleve le leading 0
        self.pd_df['RTA'] = self.pd_df['code_pos'].str.slice(0,3)

class EnvoisGenomeQuebecData:
    def __init__(self):
        self.base_dir = "/data/Databases/CovBanQ_Epi/LISTE_ENVOIS_GENOME_QUEBEC"

        if _debug_:
            excel_data = "EnvoiSmall.xlsx"
        else:
            excel_data = "ListeEnvoisGenomeQuebec_2020-09-28_corrCG.xlsx"

        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)
        self.Format()

    def Format(self):
        self.pd_df['Nom'] = self.pd_df['Nom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c')
        self.pd_df['Prénom'] = self.pd_df['Prénom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c')

        self.pd_df['Nom'] = self.pd_df['Nom'].str.strip(' ')
        self.pd_df['Prénom'] = self.pd_df['Prénom'].str.strip(' ')
        self.pd_df['NAM'] = self.pd_df['NAM'].str.strip(' ')

        self.pd_df['Nom'] = self.pd_df['Nom'].str.upper()
        self.pd_df['Prénom'] = self.pd_df['Prénom'].str.upper()
        self.pd_df['NAM'] = self.pd_df['NAM'].str.upper()

        #print(self.pd_df[['Date de naissance','Date de prélèvement','DateEnvoiGenomeQuebec']])
        self.pd_df['Date de naissance'] = pd.to_datetime(self.pd_df['Date de naissance'],format='%Y-%m-%d',errors='coerce')
        self.pd_df['Date de prélèvement'] = pd.to_datetime(self.pd_df['Date de prélèvement'],format='%Y-%m-%d',errors='coerce')
        self.pd_df['DateEnvoiGenomeQuebec'] = pd.to_datetime(self.pd_df['DateEnvoiGenomeQuebec'],format='%Y-%m-%d',errors='coerce')
        #print(self.pd_df[['Date de naissance','Date de prélèvement','DateEnvoiGenomeQuebec']])


class SgilData:
    def __init__(self):
        pass


def Main():
    logging.info("Begin update")
    tsp_geo_obj = TspGeoData()
    envois_genome_qc_obj = EnvoisGenomeQuebecData() 
    cov_bank_db = CovBankDB(tsp_geo_obj,envois_genome_qc_obj)

    cov_bank_db.Insert()

if __name__ == '__main__':
    Main()


