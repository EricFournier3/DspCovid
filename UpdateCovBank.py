# -*- coding: utf-8 -*-

"""
Eric Fournier 2020-10-28


TODO
- ajouter les 0 pour les rss a un chiffre OK FAIT
- differencier les 2 HDS    OK FAIT
- date de naissance a partir du NAM pour les envois sans date de naissance
- faire match avec NAM en premier
- ajouter travel history
- les noms de rss doivent matcher nextstrain OK FAIT
- class Outbreak manager => champ eclosion et match entre id sgil et id genome center OK FAIT
- option pour ajouter seulement SGIL DATA => plus rapide lorsque l on veut analyser des eclosion 
- ajouter champ eclosion dans BD OK FAIT
"""

"""
Usage example

python UpdateCovBank_Dev.py --debug --onlysgil

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
import argparse


parser = argparse.ArgumentParser(description="Update CovBank database")
parser.add_argument('--debug',help="run in debug mode",action='store_true')
parser.add_argument('--onlysgil',help="Use only sgil data",action='store_true')

args = parser.parse_args()

global _debug_
_debug_ = args.debug


global _only_sgil_
_only_sgil_ = args.onlysgil


class CovBankDB:
    def __init__(self,tsp_geo_obj,envois_genome_qc_obj,hopital_list_obj,sgil_obj):
        self.tsp_geo_obj = tsp_geo_obj
        self.envois_genome_qc_obj = envois_genome_qc_obj
        self.hopital_list_obj = hopital_list_obj
        self.sgil_obj = sgil_obj

        self.nomatch_tspGeo_envoisGenomeQc_df = pd.DataFrame(columns=['Nom','Prénom','# Requête','Date de naissance','Date de prélèvement','NAM'])
        self.nb_nomatch_tspGeo_envoisGenomeQc = 0
        self.nomatch_tspGeo_envoisGenomeQc_out = "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/nomatch_tspGeo_envoisGenomeQc.xlsx"        

        self.multiplematch_tspGeo_envoisGenomeQc_df = pd.DataFrame(columns=['Nom','Prénom','# Requête','Date de naissance','Date de prélèvement','NAM'])
        self.nb_multiplematch_tspGeo_envoisGenomeQc = 0
        self.multiplematch_tspGeo_envoisGenomeQc_out = "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/multiplematch_tspGeo_envoisGenomeQc.xlsx"


        self.req_no_ch_code = set()
        self.req_no_ch_code_out = "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/rec_no_ch_code.xlsx"

        self.yaml_conn_param = open('CovBankParam.yaml')
        self.ReadConnParam()
        self.connection = self.SetConnection()

        self.patient_col_list = ['PRENOM','NOM','SEXE','DTNAISS','RSS','RTA','NAM']
        self.prelevement_col_list = ['ID_PATIENT','CODE_HOPITAL','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV','GENOME_QUEBEC_REQUETE','DATE_ENVOI_GENOME_QUEBEC','TRAVEL_HISTORY','CT']
        self.prelevement_col_list_sgil = ['ID_PATIENT','CODE_HOPITAL','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV','GENOME_QUEBEC_REQUETE','TRAVEL_HISTORY','CT','OUTBREAK','NUMERO_SGIL']

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

    def GetPrelevementColumns(self,is_sgil):
        if is_sgil:
            return ','.join(self.prelevement_col_list_sgil)
        else:
            return ','.join(self.prelevement_col_list)

    def Insert(self):
        logging.info("Begin insert")

        self.nb_patients_inserted = 0
        self.nb_prelevements_inserted = 0


        if not _only_sgil_ :

            for index, row in self.envois_genome_qc_obj.pd_df.loc[:,].iterrows():
                nom = row['Nom']
                prenom = row['Prénom']
                date_naiss = row['Date de naissance']
                date_prelev = row['Date de prélèvement']
                req = row['# Requête']
                nam = row['NAM']
                tsp_geo_match_df = self.GetTspGeoMatch(nom,prenom,date_naiss,nam,date_prelev,req)

                if tsp_geo_match_df.shape[0] != 0:
                    patient_record = self.GetPatientValToInsert(tsp_geo_match_df)
                    patient_id = self.InsertPatient(patient_record)
                    if patient_id is not None:
                        prelevement_record = self.GetPrelevementToInsert(tsp_geo_match_df,row,patient_id)
                        self.InsertPrelevement(prelevement_record,False)
                    else:
                        logging.error("Impossible d inserer ce prelevement " + str(row))

        for index, row in self.sgil_obj.pd_df.loc[:,].iterrows():
            nom = row['NOM']
            prenom = row['PRENOM']
            date_naiss = row['DATE_NAISS']
            date_prelev = row['SAMPLED_DATE']
            nam = row['NAM']
            tsp_geo_match_df = self.GetTspGeoMatchWithSgilData(nom,prenom,date_naiss,nam,date_prelev) 

            patient_record = self.GetPatientValToInsertFromSgilData(tsp_geo_match_df,row)
            patient_id = self.InsertPatient(patient_record)
            #print("SGIL PATIENT ID ", patient_id, "\n")

            if patient_id is not None:
                prelevement_record = self.GetPrelevementToInsertFromSgilData(row,patient_id) 
                self.InsertPrelevement(prelevement_record,True)
            else:
                logging.error("Impossible d inserer ce prelevement sgil")
            

    def CheckIfPrelevementExist(self,prelevement_record,cursor,is_sgil):
        if is_sgil:
            rec = dict(list(zip(self.prelevement_col_list_sgil,prelevement_record)))
        else:
            rec = dict(list(zip(self.prelevement_col_list,prelevement_record)))
        
        sql = "SELECT ID_PRELEV from Prelevements where GENOME_QUEBEC_REQUETE = '{0}'".format(rec['GENOME_QUEBEC_REQUETE'])
        cursor.execute(sql)
        id_prelev_tuple_list = cursor.fetchall()
        nb_prelev = len(id_prelev_tuple_list)

        if nb_prelev < 1:
            return False
        else:
           return True

    def CheckIfPatientExist(self,patient_record,cursor):
        rec = dict(list(zip(self.patient_col_list,patient_record)))

        sql = "SELECT ID_PATIENT from Patients where PRENOM = '{0}' and NOM = '{1}' and SEXE = '{2}' and DTNAISS = '{3}' and RSS = '{4}' and RTA = '{5}' and NAM = '{6}'".format(rec['PRENOM'],rec['NOM'],rec['SEXE'],rec['DTNAISS'],rec['RSS'],rec['RTA'],rec['NAM'])
        try:
            cursor.execute(sql)
        except:
            print("SQL IS ",sql)
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
      
 
    def InsertPrelevement(self,prelevement_record,is_sgil):
        cursor = self.GetCursor()
        exist = self.CheckIfPrelevementExist(prelevement_record,cursor,is_sgil)

        if not exist:
            try:
                pass
                ncols = len(prelevement_record)
                sql_insert = "INSERT INTO Prelevements ({0}) values ({1})".format(self.GetPrelevementColumns(is_sgil),str("%s,"*ncols)[:-1])
                cursor.execute(sql_insert,prelevement_record)
                cursor.close()
                self.Commit()
                self.nb_prelevements_inserted += 1
                sys.stdout.write("Insert in Prelevement >>> %d\r"%self.nb_prelevements_inserted)
                sys.stdout.flush()
            except mysql.connector.Error as err:
                logging.error("Erreur d'insertion dans la table Prelevements avec le record " + str(prelevement_record))
                print(err)


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

    def GetPrelevementToInsertFromSgilData(self,sgil_record,patient_id):
        def GetVal(x):
            return x

        patient_id = patient_id
        code_hopital = 'LSPQ-' 
        nom_hopital = self.hopital_list_obj.GetHospitalName(code_hopital)
        adresse_hopital = self.hopital_list_obj.GetHospitalAddress(code_hopital)
        date_prelev = sgil_record['SAMPLED_DATE']
        sgil_folderno = sgil_record['NUMERO_SGIL']
        travel_history = sgil_record['TRAVEL_HISTORY']
        ct = sgil_record['CT']

        if str(sgil_record['NOM_ECLOSION']) == 'nan':
            outbreak = 'NA'
        else:
            outbreak = sgil_record['NOM_ECLOSION']

        if str(sgil_record['ID_SGIL']) == 'nan':
            id_sgil = 'NA'
        else:
            id_sgil = sgil_record['ID_SGIL']
        return(tuple(map(GetVal,(patient_id,code_hopital,nom_hopital,adresse_hopital,date_prelev,sgil_folderno,travel_history,ct,outbreak,id_sgil))))

    def WriteReqNoChCodeToFile(self):
        self.req_no_ch_code = list(self.req_no_ch_code)
        req_no_ch_code_series =  pd.Series(self.req_no_ch_code,name="Req missing CH code", dtype='object')
        req_no_ch_code_df = req_no_ch_code_series.to_frame()
        req_no_ch_code_df.to_excel(self.req_no_ch_code_out,sheet_name='Sheet1') 


    def GetPrelevementToInsert(self,tsp_geo_match_df,envois_genome_qc,patient_id):

        def GetVal(x):
            return x

        patient_id = patient_id
        #code_hopital = str(envois_genome_qc['Hopital']) + "-"
        #date_prelev = str(tsp_geo_match_df['date_prel'].values[0])
        date_prelev = envois_genome_qc['Date de prélèvement']
        genome_quebec_requete = envois_genome_qc['# Requête']

        if not re.search(r'-',genome_quebec_requete):
            self.req_no_ch_code.add(genome_quebec_requete)
            code_hopital = "NAN"
        else:
            try:
                try:
                    code_hopital = re.search(r'(\S+)-\S+-\S+',genome_quebec_requete).group(1)
                except:
                    code_hopital = re.search(r'(\S+)-\S+',genome_quebec_requete).group(1)

                code_hopital = code_hopital + "-"
            except:
                logging.error("PROBLEM WITH THIS CH CODE " + str(genome_quebec_requete))
                code_hopital = "NAN"


        if str(genome_quebec_requete[0:5]) == 'HDS-S':
            nom_hopital = self.hopital_list_obj.GetChSorelName()
            adresse_hopital = self.hopital_list_obj.GetChSorelAdress()
        else:
            nom_hopital = self.hopital_list_obj.GetHospitalName(code_hopital)
            adresse_hopital = self.hopital_list_obj.GetHospitalAddress(code_hopital)

        date_envois_genome_quebec = envois_genome_qc['DateEnvoiGenomeQuebec']
        travel_history = 'TRAVEL_HISTORY' # prendre de tsp_geo mais temporairement de sgil
        ct = 'CT' # prendre de tsp_geo mais temporaire de sgil

        return(tuple(map(GetVal,(patient_id,code_hopital,nom_hopital,adresse_hopital,date_prelev,genome_quebec_requete,date_envois_genome_quebec,travel_history,ct))))
        

    def GetSexLetterFromNumber(self,number):
        if str(number) == "1":
            sexe = "M"
        elif str(number) == "2":
            sexe = "F"
        else:
            sexe = "Inconnu" 

        return sexe


    def GetPatientValToInsertFromSgilData(self,tsp_geo_match_df,sgil_record):
        def GetVal(x):
            return x

        if tsp_geo_match_df.shape[0] != 0:
            prenom = tsp_geo_match_df['prenom'].values[0]
            nom = tsp_geo_match_df['nom'].values[0]
            sexe =  tsp_geo_match_df['sexe'].values[0]
            sexe = self.GetSexLetterFromNumber(sexe)
            date_naiss = tsp_geo_match_df['date_nais'].values[0]        
            date_naiss = str(date_naiss)
            rss = str(tsp_geo_match_df['RSS'].values[0])
            rta = str(tsp_geo_match_df['RTA'].values[0])
            nam = str(tsp_geo_match_df['nam'].values[0])
            nam = str(nam)

        else:
            prenom = sgil_record['PRENOM']
            nom = sgil_record['NOM']
            sexe = str(sgil_record['SEX'])
            date_naiss = sgil_record['DATE_NAISS']
            date_naiss = str(date_naiss)
            rss = str(sgil_record['RSS_PATIENT'])
            rta = str(sgil_record['POSTAL_CODE'][0:3])
            nam = str(sgil_record['NAM'])
            

        return(tuple(map(GetVal,(prenom,nom,sexe,date_naiss,rss,rta,nam))))

    def GetPatientValToInsert(self,tsp_geo_match_df):
        def GetVal(x):
            return x

        prenom = tsp_geo_match_df['prenom'].values[0]
        nom = tsp_geo_match_df['nom'].values[0]
        sexe =  tsp_geo_match_df['sexe'].values[0]
        sexe = self.GetSexLetterFromNumber(sexe)

        date_naiss = tsp_geo_match_df['date_nais'].values[0]        
        #print(tsp_geo_match_df['date_nais'].dtype, " --  ", type(date_naiss))
        date_naiss = str(date_naiss)
        rss = str(tsp_geo_match_df['RSS'].values[0])
        rta = str(tsp_geo_match_df['RTA'].values[0])
        
        nam = str(tsp_geo_match_df['nam'].values[0])
        
        return(tuple(map(GetVal,(prenom,nom,sexe,date_naiss,rss,rta,nam))))

    def GetTspGeoMatchWithSgilData(self,nom,prenom,date_naiss,nam,date_prelev):
        match_df = pd.DataFrame()
        return match_df
        tsp_geo_obj_pd_df = self.tsp_geo_obj.pd_df

        if(len(str(nam)) > 9):
            match_df = tsp_geo_obj_pd_df.loc[tsp_geo_obj_pd_df['nam'] == nam,:].copy()

        if match_df.shape[0] != 0:
            self.ComputeDatePrelevDiff(match_df,date_prelev)
            return(match_df[match_df.DATE_DIFF == match_df.DATE_DIFF.min()])

        return match_df
        

    def GetTspGeoMatch(self,nom,prenom,date_naiss,nam,date_prelev,req):
        match_df = pd.DataFrame()
        tsp_geo_obj_pd_df = self.tsp_geo_obj.pd_df

        if(len(str(nam)) > 9):
            match_df = tsp_geo_obj_pd_df.loc[tsp_geo_obj_pd_df['nam'] == nam,:].copy()

        if match_df.shape[0] == 0 :
            match_df = tsp_geo_obj_pd_df.loc[(tsp_geo_obj_pd_df['nom'] == nom) & (tsp_geo_obj_pd_df['prenom'] == prenom) & (tsp_geo_obj_pd_df['date_nais'] == date_naiss),:].copy()

        if match_df.shape[0] == 0 :
            self.nomatch_tspGeo_envoisGenomeQc_df.loc[self.nb_nomatch_tspGeo_envoisGenomeQc] = {'Nom':nom,'Prénom':prenom,'NAM':nam,'Date de naissance':date_naiss,'Date de prélèvement':date_prelev,'# Requête':req} # ['Nom','Prénom','# Requête','Date de naissance','Date de prélèvement','NAM']
            self.nb_nomatch_tspGeo_envoisGenomeQc += 1 
            return match_df
        elif  match_df.shape[0] > 1 :
            self.multiplematch_tspGeo_envoisGenomeQc_df.loc[self.nb_multiplematch_tspGeo_envoisGenomeQc] = {'Nom':nom,'Prénom':prenom,'NAM':nam,'Date de naissance':date_naiss,'Date de prélèvement':date_prelev,'# Requête':req}
            self.nb_multiplematch_tspGeo_envoisGenomeQc += 1
            match_df = match_df[0:0]
            return match_df

        return match_df
        #self.ComputeDatePrelevDiff(match_df,date_prelev)

        #return(match_df[match_df.DATE_DIFF == match_df.DATE_DIFF.min()])

    def ComputeDatePrelevDiff(self,match_df,date_prelev):
        match_df['DATE_DIFF'] = match_df['date_prel'] - date_prelev
        match_df['DATE_DIFF'] = match_df['DATE_DIFF'].abs()

    def WriteNoMatchTspGeoToEnvoisGenomeQcToFile(self):
        self.nomatch_tspGeo_envoisGenomeQc_df.to_excel(self.nomatch_tspGeo_envoisGenomeQc_out,sheet_name='Sheet1')
        self.multiplematch_tspGeo_envoisGenomeQc_df.to_excel(self.multiplematch_tspGeo_envoisGenomeQc_out,sheet_name='Sheet1')


    def ReadConnParam(self):
        param = yaml.load(self.yaml_conn_param,Loader=yaml.FullLoader)
        self.host = param['host']
        self.user = param['user']
        self.password = param['password']
        self.database = param['database']

class HopitalList:
    def __init__(self):
        self.base_dir = "/data/Databases/CovBanQ_Epi/HOPITAUX"
        excel_data = "ListeHopitaux.xlsx"
        self.missing_ch_code = set()
        self.missing_ch_code_out = "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/missing_ch_code.xlsx"
        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)

        self.BuildAddressDict()

    def BuildAddressDict(self):
        
        self.ch_address_dict = dict()

        for index, row in self.pd_df.loc[:,].iterrows():
            ch_code = row['CODE']
            ch_name = row['ETABLISSEMENTS'] 
            ch_address = row['ADRESSE'] 
            self.ch_address_dict[ch_code] = [ch_name,ch_address]

    def GetChSorelName(self):
        return "Hôtel-Dieu de Sorel"

    def GetChSorelAdress(self):
        return "400, avenue de l'Hôtel-Dieu , Sorel-Tracy, QC, Canada"

    def GetHospitalName(self,ch_code):
        if ch_code in self.ch_address_dict:
            return self.ch_address_dict[ch_code][0]
        else:
            self.missing_ch_code.add(ch_code)
            #logging.error("No Hospital name for "+ ch_code)
            return("na")

    def GetHospitalAddress(self,ch_code):
        if ch_code in self.ch_address_dict:
            return self.ch_address_dict[ch_code][1]
        else:
            #logging.error("No Hospital address for "+ ch_code)
            return("na")
        
    def WriteMissingChCodeToFile(self):
        self.missing_ch_code = list(self.missing_ch_code)
        missing_ch_code_series = pd.Series(self.missing_ch_code,name="Missing CH code", dtype='object')
        missing_ch_code_df = missing_ch_code_series.to_frame()
        missing_ch_code_df.to_excel(self.missing_ch_code_out,sheet_name='Sheet1')


class OutbreakData:
    def __init__(self):
        logging.info("In Outbreakdata")

        self.base_dir = "/data/Databases/CovBanQ_Epi/SGIL_EXTRACT"
        excel_data =  "AllOutbreaks_20201123.xlsx"

        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)

    def GetPdDf(self):
        return self.pd_df

class SGILdata:
    def __init__(self,outbreak_obj):
        logging.info("In SGILdata")

        self.base_dir = "/data/Databases/CovBanQ_Epi/SGIL_EXTRACT/"

        self.outbreak_obj = outbreak_obj


        if _debug_:
            table_data = "extract_with_Covid19_extraction_v2_20201123_CovidPos_test.txt"
        else:
            table_data = "extract_with_Covid19_extraction_v2_20201123_CovidPos.txt"

        self.pd_df = pd.read_table(os.path.join(self.base_dir,table_data))
        self.Format()
        self.MergeOutbreakData()

    def MergeOutbreakData(self):
        #self.pd_df_test = pd.merge(self.pd_df,self.outbreak_obj.GetPdDf(),left_on='NUMERO_SGIL',right_on='ID_SGIL',how='left')
        #self.pd_df_test.loc[self.pd_df_test['ID_SGIL'].notnull(),['NUMERO_SGIL']] = self.pd_df_test['ID_GENOME_CENTER']
        #print(self.pd_df_test.loc[self.pd_df_test['ID_SGIL'].notnull(),:])

        #print(self.pd_df.loc[self.pd_df['NUMERO_SGIL'] == 'L00306413',:])
        self.pd_df = pd.merge(self.pd_df,self.outbreak_obj.GetPdDf(),left_on='NUMERO_SGIL',right_on='ID_SGIL',how='left')
        self.pd_df.loc[self.pd_df['ID_SGIL'].notnull(),['NUMERO_SGIL']] = self.pd_df['ID_GENOME_CENTER']
        #print(self.pd_df.loc[self.pd_df['NUMERO_SGIL'] == 'S7033690',:])
        #print(self.pd_df.loc[self.pd_df['ID_SGIL'].notnull(),:])



    def Format(self):
        self.pd_df['NOM'] = self.pd_df['NOM'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","")
        self.pd_df['PRENOM'] = self.pd_df['PRENOM'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","")
        self.pd_df['NAM'] = self.pd_df['NAM'].str.strip(' ').str.replace("'","")
        
        self.pd_df['NOM'] = self.pd_df['NOM'].str.strip(' ')
        self.pd_df['PRENOM'] = self.pd_df['PRENOM'].str.strip(' ')
        self.pd_df['NAM'] = self.pd_df['NAM'].str.upper()

        self.pd_df['NOM'] = self.pd_df['NOM'].str.upper()
        self.pd_df['PRENOM'] = self.pd_df['PRENOM'].str.upper()

        self.pd_df['DATE_NAISS'] = pd.to_datetime(self.pd_df['DATE_NAISS'],format='%Y-%m-%d',errors='coerce')
        self.pd_df['SAMPLED_DATE'] = pd.to_datetime(self.pd_df['SAMPLED_DATE'],format='%Y-%m-%d',errors='coerce')

        self.pd_df = self.pd_df.dropna(subset = ['SAMPLED_DATE','DATE_NAISS'])
        self.pd_df['RSS_PATIENT'] = self.pd_df['RSS_PATIENT'].str.replace(r' – ',r'-').str.replace('é','e').str.replace('è','e').str.replace('ô','o').str.replace('î','i')


        self.pd_df = self.pd_df.sort_values(by=['SAMPLED_DATE'],ascending=True)

class TspGeoData:
    def __init__(self):
        logging.info("In TspGeoData")

        self.base_dir = "/data/Databases/CovBanQ_Epi/TSP_GEO"
        
        if _debug_:
            excel_data = "TSP_geo_20201014_small.xlsx"
        else:
            excel_data = "TSP_geo_20201111.xlsx"
        
        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)
        self.Format()

    def Format(self):
        #TODO SI MANQUE DATE NAISS ON PEUT L OBTENIR A PARTIR DU NAM
        self.pd_df['nom'] = self.pd_df['nom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace('-','').str.replace(' ','')
        
        self.pd_df['prenom'] = self.pd_df['prenom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace('-','').str.replace(' ','')
        self.pd_df['nam'] = self.pd_df['nam'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","")

        self.pd_df['nom'] = self.pd_df['nom'].str.strip(' ')
        self.pd_df['prenom'] = self.pd_df['prenom'].str.strip(' ')
        self.pd_df['nam'] = self.pd_df['nam'].str.strip(' ')

        self.pd_df['nom'] = self.pd_df['nom'].str.upper()
        self.pd_df['prenom'] = self.pd_df['prenom'].str.upper()
        self.pd_df['nam'] = self.pd_df['nam'].str.upper()

        self.pd_df['date_nais'] = pd.to_datetime(self.pd_df['date_nais'],format='%Y%m%d',errors='coerce')
        self.pd_df['date_prel'] = pd.to_datetime(self.pd_df['date_prel'],format='%Y%m%d',errors='coerce')
        

        self.pd_df['RSS_code'] = self.pd_df['RSS_code'].astype(str)
        self.pd_df['RSS_code'] = self.pd_df['RSS_code'].str.replace(r'(^\d+)\.0',r'\1',regex=True)
        self.pd_df['RSS_code'] = self.pd_df['RSS_code'].str.replace(r'(^\d$)',r'0\1',regex=True)  # – -
        self.pd_df['RSS'] =  self.pd_df['RSS_code'] + "-" + self.pd_df['RSS_nom']  #TODO ATTENTION ici ca enleve le leading 0
        self.pd_df['RSS'] = self.pd_df['RSS'].str.replace(r' – ',r'-').str.replace('é','e').str.replace('è','e').str.replace('ô','o').str.replace('î','i')
        self.pd_df['RTA'] = self.pd_df['code_pos'].str.slice(0,3)

        #self.pd_df = self.pd_df.dropna(subset = ['date_prel'])

class EnvoisGenomeQuebecData:
    def __init__(self):
        logging.info("In EnvoisGenomeQuebecData")

        self.base_dir = "/data/Databases/CovBanQ_Epi/LISTE_ENVOIS_GENOME_QUEBEC"

        if _debug_:
            excel_data = "EnvoiSmall.xlsx"
        else:
            excel_data = "ListeEnvoisGenomeQuebec_2020-11-06.xlsx"

        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)
        self.Format()

    def Format(self):
        self.pd_df['Nom'] = self.pd_df['Nom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace('-','').str.replace(' ','')
        self.pd_df['Prénom'] = self.pd_df['Prénom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace('-','').str.replace(' ','')
        self.pd_df['NAM'] = self.pd_df['NAM'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","")
        self.pd_df['# Requête'] = self.pd_df['# Requête'].str.replace(' ','')

        self.pd_df['Nom'] = self.pd_df['Nom'].str.strip(' ')
        self.pd_df['Prénom'] = self.pd_df['Prénom'].str.strip(' ')
        self.pd_df['NAM'] = self.pd_df['NAM'].str.strip(' ')

        self.pd_df['Nom'] = self.pd_df['Nom'].str.upper()
        self.pd_df['Prénom'] = self.pd_df['Prénom'].str.upper()
        self.pd_df['NAM'] = self.pd_df['NAM'].str.upper()

        self.pd_df['Date de naissance'] = pd.to_datetime(self.pd_df['Date de naissance'],format='%Y-%m-%d',errors='coerce')
        self.pd_df['Date de prélèvement'] = pd.to_datetime(self.pd_df['Date de prélèvement'],format='%Y-%m-%d',errors='coerce')
        self.pd_df['DateEnvoiGenomeQuebec'] = pd.to_datetime(self.pd_df['DateEnvoiGenomeQuebec'],format='%Y-%m-%d',errors='coerce')

        self.pd_df = self.pd_df.dropna(subset = ['Date de prélèvement'])


def Main():
    logging.info("Begin update")

    tsp_geo_obj = TspGeoData()
    envois_genome_qc_obj = EnvoisGenomeQuebecData() 
    hopital_list_obj = HopitalList()
    outbreak_obj = OutbreakData()
    sgil_obj = SGILdata(outbreak_obj)

    cov_bank_db = CovBankDB(tsp_geo_obj,envois_genome_qc_obj,hopital_list_obj,sgil_obj)

    cov_bank_db.Insert()
    cov_bank_db.WriteReqNoChCodeToFile()
    cov_bank_db.WriteNoMatchTspGeoToEnvoisGenomeQcToFile()
    cov_bank_db.CloseConnection()

    hopital_list_obj.WriteMissingChCodeToFile()

if __name__ == '__main__':
    Main()


