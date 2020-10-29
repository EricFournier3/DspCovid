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

        self.patient_col_list = ['PRENOM','NOM','SEXE','DTNAISS','RSS','RTA','NAM']
        self.prelevement_col_list = ['ID_PATIENT','CODE_HOPITAL','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV','GENOME_QUEBEC_REQUETE','DATE_ENVOI_GENOME_QUEBEC','TRAVEL_HISTORY','CT']

    def CloseConnection(self):
        self.GetConnection().close()

    def SetConnection(self):
        return mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database)

    def GetConnection(self):
        return self.connection

    def GetCursor(self):
        return self.GetConnection().cursor()

    def Commit(self):
        self.connection.commit()

    def GetPatientsColumns(self):
        return ','.join(self.patient_col_list)

    def Insert(self):
        logging.info("Begin insert")

        self.nb_patients_inserted = 0

        for index, row in self.envois_genome_qc_obj.pd_df.loc[:,].iterrows():
            nom = row['Nom']
            prenom = row['Prénom']
            date_naiss = row['Date de naissance']
            date_prelev = row['Date de prélèvement']
            nam = row['NAM']
            tsp_geo_match_df = self.GetTspGeoMatch(nom,prenom,date_naiss,nam,date_prelev)

            if tsp_geo_match_df.shape[0] != 0:
                patient_record = self.GetPatientValToInsert(tsp_geo_match_df)
                patient_id = self.InsertPatient(patient_record)
                if patient_id is not None:
                    prelevement_record = self.GetPrelevementToInsert(tsp_geo_match_df,row,patient_id,"NOM_HOPITAL","ADDRESS_HOPITAL")
                    print(prelevement_record)
                else:
                    logging.error("Impossible d inserer ce prelevement " + str(row))

    def CheckIfPatientExist(self,patient_record,cursor):
        rec = dict(list(zip(self.patient_col_list,patient_record)))

        sql = "SELECT ID_PATIENT from Patients where PRENOM = '{0}' and NOM = '{1}' and SEXE = '{2}' and DTNAISS = '{3}' and RSS = '{4}' and RTA = '{5}' and NAM = '{6}'".format(rec['PRENOM'],rec['NOM'],rec['SEXE'],rec['DTNAISS'],rec['RSS'],rec['RTA'],rec['NAM'])

        cursor.execute(sql)
        id_patient_tuple_list = cursor.fetchall()
        nb_patient = len(id_patient_tuple_list)

        if nb_patient < 1:
            return [False,None]
        elif nb_patient == 1:
            id_patient = id_patient_tuple_list[0][0] 
            return [True,id_patient]
        elif nb_patient > 1:
            logging.error("Probleme : plus de un seul match Patients " + str(id_patient_tuple_list))
            return [True,None]
        
    def InsertPatient(self,patient_record):
        cursor = self.GetCursor()
        exist_list = self.CheckIfPatientExist(patient_record,cursor)
        exist = exist_list[0]
        patient_id = exist_list[1]

        if not exist:
 
            try:
                ncols = len(patient_record)
                sql_insert = "INSERT INTO Patients ({0}) values ({1})".format(self.GetPatientsColumns(),str("%s,"*ncols)[:-1])
                cursor.execute(sql_insert,patient_record)
                cursor.execute("SELECT LAST_INSERT_ID()")
                patient_id = cursor.fetchone()[0]
                
                cursor.close()
                self.Commit()
                self.nb_patients_inserted += 1
                sys.stdout.write("Insert in Patient >>> %d\r"%self.nb_patients_inserted)
                sys.stdout.flush()
                return patient_id
            except mysql.connector.Error as err:
                logging.error("Erreur d'insertion dans la table Patients avec le record " + str(patient_record))
                print(err)
                return None
        elif exist and (patient_id is not None):
            return patient_id
        else:
            return None      

    def GetPrelevementToInsert(self,tsp_geo_match_df,envois_genome_qc,patient_id,nom_hopital,adresse_hopital):
        #self.prelevement_col_list = ['ID_PATIENT','CODE_HOPITAL','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV','GENOME_QUEBEC_REQUETE','DATE_ENVOI_GENOME_QUEBEC','TRAVEL_HISTORY','CT']

        def GetVal(x):
            return x

        patient_id = patient_id
        code_hopital = envois_genome_qc['Hopital']
        nom_hopital = nom_hopital
        adresse_hopital = adresse_hopital
        date_prelev = tsp_geo_match_df['date_prel'].values[0]
        genome_quebec_requete = envois_genome_qc['# Requête']
        date_envois_genome_quebec = envois_genome_qc['DateEnvoiGenomeQuebec']
        travel_history = 'TRAVEL_HISTORY' # prendre de tsp_geo mais temporairement de sgil
        ct = 'CT' # prendre de tsp_geo mais temporaire de sgil

        return(tuple(map(GetVal,(patient_id,code_hopital,nom_hopital,adresse_hopital,date_prelev,genome_quebec_requete,date_envois_genome_quebec,travel_history,ct))))
        
        
    def GetPatientValToInsert(self,tsp_geo_match_df):
        def GetVal(x):
            return x

        prenom = tsp_geo_match_df['prenom'].values[0]
        nom = tsp_geo_match_df['nom'].values[0]

        sexe =  tsp_geo_match_df['sexe'].values[0]
        if str(sexe) == "1":
            sexe = "M"
        elif str(sexe) == "2":
            sexe = "F"
        else:
            sexe = "Inconnu" 

        date_naiss = tsp_geo_match_df['date_nais'].values[0]        
        #print(tsp_geo_match_df['date_nais'].dtype, " --  ", type(date_naiss))
        date_naiss = str(date_naiss)
        rss = tsp_geo_match_df['RSS'].values[0]
        rta = tsp_geo_match_df['RTA'].values[0]
        nam = tsp_geo_match_df['nam'].values[0]
        nam = str(nam)

        return(tuple(map(GetVal,(prenom,nom,sexe,date_naiss,rss,rta,nam))))

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
    cov_bank_db.CloseConnection()

if __name__ == '__main__':
    Main()


