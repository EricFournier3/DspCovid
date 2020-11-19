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
import yaml 

global _debug_
_debug_ = True



class CovBankDB:
    def __init__(self,outbreak_obj,hopital_list_obj):
        self.outbreak_obj = outbreak_obj
        self.hopital_list_obj = hopital_list_obj

        self.yaml_conn_param = open('CovBankParam.yaml')
        self.ReadConnParam()
        self.connection = self.SetConnection()

        self.patient_col_list = ['ID_PATIENT','PRENOMINFO','NOMINFO','SEXEINFO','DTNAISSINFO','RSS_LSPQ_CAS']
        self.prelevement_col_list = ['ID_PATIENT','STATUT','CODE_HOPITAL_DSP','CODE_HOPITAL_LSPQ','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV_1_DSP','DATE_CONF_LSPQ_1',
        'DATE_PRELEV_2_DSP','DATE_CONF_LSPQ_2','DATE_PRELEV_HOPITAL','GENOME_QUEBEC_REQUETE','DATE_ENVOI_GENOME_QUEBEC','ID_PHYLO','TRAVEL_HISTORY','CT']

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

    def GetPrelevementColumns(self):
        return ','.join(self.prelevement_col_list)

    def ReadConnParam(self):
        param = yaml.load(self.yaml_conn_param,Loader=yaml.FullLoader)
        self.host = param['host']
        self.user = param['user']
        self.password = param['password']
        self.database = param['database']

    def Insert(self):
        logging.info("Begin insert")

        self.nb_patients_inserted = 0
        self.nb_prelevements_inserted = 0

        for index, row in self.outbreak_obj.pd_df.loc[:,].iterrows():
            nom = row['NOM']
            prenom = row['PRENOM']
            date_naiss = row['DATE_NAISS']
            date_prelev = row['SAMPLED_DATE']

            patient_record = self.GetPatientValToInsert(row)
            #print(patient_record)
            patient_id = self.InsertPatient(patient_record)
            #print("PATIENT ID ", patient_id)
            if patient_id is not None:
                prelevement_record = self.GetPrelevementToInsert(row,patient_id)

    def GetPrelevementToInsert(self,record,patient_id):
        def GetVal(x):
            return x

        patient_id = patient_id
        code_hopital = 'LSPQ-'
        nom_hopital = self.hopital_list_obj.GetHospitalName(code_hopital)
        adresse_hopital = self.hopital_list_obj.GetHospitalAddress(code_hopital)
        date_prelev = record['SAMPLED_DATE']
        req = record['GENOME_QUEBEC_REQUETE']
        travel_history = record['TRAVEL_HISTORY']
        ct = record['CT']
        #['ID_PATIENT','STATUT','CODE_HOPITAL_DSP','CODE_HOPITAL_LSPQ','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV_1_DSP','DATE_CONF_LSPQ_1',
        #'DATE_PRELEV_2_DSP','DATE_CONF_LSPQ_2','DATE_PRELEV_HOPITAL','GENOME_QUEBEC_REQUETE','DATE_ENVOI_GENOME_QUEBEC','ID_PHYLO','TRAVEL_HISTORY','CT']
        return(tuple(map(GetVal,(patient_id,code_hopital,nom_hopital,adresse_hopital,date_prelev,sgil_folderno,travel_history,ct))))


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

    def CheckIfPatientExist(self,patient_record,cursor):
        rec = dict(list(zip(self.patient_col_list,patient_record)))

        sql = "SELECT ID_PATIENT from Patients where PRENOMINFO = '{0}' and NOMINFO = '{1}' and SEXEINFO = '{2}' and DTNAISSINFO = '{3}' and RSS_LSPQ_CAS = '{4}'".format(rec['PRENOMINFO'],rec['NOMINFO'],rec['SEXEINFO'],rec['DTNAISSINFO'],rec['RSS_LSPQ_CAS'])


        try:
            cursor.execute(sql)
        except:
            print("BUG : SQL IS ",sql)

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

    def GetPatientValToInsert(self,record):
        def GetVal(x):
            return x
        
        prenom = record['PRENOM']
        nom = record['NOM']
        sexe = str(record['SEX'])
        date_naiss = record['DATE_NAISS']
        date_naiss = str(date_naiss)
        rss = str(record['RSS_PATIENT'])
        id_patient = nom + "-" + prenom + "-"  + str(date_naiss) 
        
        return(tuple(map(GetVal,(id_patient,prenom,nom,sexe,date_naiss,rss))))


class OutbreakData:
    def __init__(self):
        logging.info("In OurbreakData")
        self.basedir = "/data/Databases/CovBanQ_Epi/OUTBREAK"

        excel_data = "sgil_export_20201119_for_Outbreak_Lanaudiere_StEusebe.xlsx"
        self.pd_df = pd.read_excel(os.path.join(self.basedir,excel_data),sheet_name=0)

        self.Format()

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

        self.pd_df = self.pd_df.dropna(subset = ['SAMPLED_DATE'])

        self.pd_df = self.pd_df.sort_values(by=['SAMPLED_DATE'],ascending=True)


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
        missing_ch_code_series = pd.Series(self.missing_ch_code,name="Missing CH code")
        missing_ch_code_df = missing_ch_code_series.to_frame()
        missing_ch_code_df.to_excel(self.missing_ch_code_out,sheet_name='Sheet1')


def Main():
    logging.info("Begin update")

    outbreak_obj =  OutbreakData()

    hopital_list_obj = HopitalList()

    cov_bank_db = CovBankDB(outbreak_obj,hopital_list_obj)

    cov_bank_db.Insert()
    #cov_bank_db.WriteReqNoChCodeToFile()
    #cov_bank_db.WriteNoMatchTspGeoToEnvoisGenomeQcToFile()
    #cov_bank_db.CloseConnection()

    #hopital_list_obj.WriteMissingChCodeToFile()

if __name__ == '__main__':
    Main()
