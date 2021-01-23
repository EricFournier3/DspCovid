# -*- coding: utf-8 -*-

"""
Eric Fournier 2020-12-01


TODO
- date de naissance a partir du NAM pour les envois sans date de naissance
- faire match avec NAM en premier
- ajouter travel history
- TRAVEL HISTORY
- POUR LES NUMERO SGIL METTRE L HOPITAL DE PRELEVEMENT. C est important pour GISAID SUBMISSION
- plus besoin de outbreak_obj
- utiliser le fichier no match fait par Marianne pour trouver les no match
- si pas de match pour numero eclosion faire contains
- traiter les caractere speciaux de marianne nomatch
- match avec nomatch
"""

"""
Usage example

python UpdateCovBank_v2_Dev.py  --debug --mode init --gq ListeEnvoisGenomeQuebec_debug.xlsx --tsp TSP_geo_debug.xlsx --mm nomatch_tspGeo_envoisGenomeQc_debug.xlsx --sgil extract_with_Covid19_extraction_v2_CovidPos_debug.txt

python UpdateCovBank_v2_Dev.py  --debug --mode outbreak --gq ListeEnvoisGenomeQuebec_outbreak_debug.xlsx --tsp TSP_geo_outbreak_debug.xlsx --mm nomatch_tspGeo_envoisGenomeQc_outbreak_debug.xlsx --sgil extract_with_Covid19_extraction_v2_CovidPos_debug.txt


Ne pas oublier de choisir la bonne db dans /data/Applications/GitScript/Dsp_Covid_MySql/CovBankParam.yaml
En mode init, ne pas oublier de mettre a jour la liste de fichier eclosion dans /data/Databases/CovBanQ_Epi/OUTBREAK_OLD
En mode outbreak, ne pas oublier de mettre a jour la liste de fichier eclosion dans /data/Databases/CovBanQ_Epi/OUTBREAK_NEW
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
import glob

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Update CovBank database")
parser.add_argument('--debug',help="run in debug mode",action='store_true')
parser.add_argument('--mode',help='Execution mode',choices=['init','outbreak'],required=True)
parser.add_argument('--gq',help='Nom du fichier liste des envois genome quebec qui se trouve dans /data/Databases/CovBanQ_Epi/LISTE_ENVOIS_GENOME_QUEBEC',required=True)
parser.add_argument('--tsp',help='Nom du fichier tsp geo qui se trouve dans /data/Databases/CovBanQ_Epi/TSP_GEO',required=True)
parser.add_argument('--mm',help='Nom du fichier mismatch qui se trouve dans /data/Databases/CovBanQ_Epi/NOMATCH_ENVOISGQ_TSPGEO/IN',required=True)
parser.add_argument('--sgil',help='Nom du fichier sgil extract qui se trouve dans /data/Databases/CovBanQ_Epi/SGIL_EXTRACT',required=True)
parser.add_argument('--bb',help='Nom du BB_C19B',required=True)

args = parser.parse_args()

global _debug_
_debug_ = args.debug

global mode
_mode_ = args.mode

global bbc19_file
bbc19_file = args.bb

global gq_in_file
gq_in_file = args.gq

global tsp_in_file
tsp_in_file = args.tsp

global mm_in_file
mm_in_file = args.mm

global sgil_in_file
sgil_in_file = args.sgil 

#print("GQ ",gq_in_file)
#print("TSP ",tsp_in_file)
#print("MM ",mm_in_file)
#print("SGIL ",sgil_in_file)

#exit(0)


class CovBankDB:
    def __init__(self,tsp_geo_obj,envois_genome_qc_obj,hopital_list_obj,sgil_obj,outbreak_obj_v2,nomatch_envoisgq_tspgeo):
        self.tsp_geo_obj = tsp_geo_obj
        self.envois_genome_qc_obj = envois_genome_qc_obj
        self.hopital_list_obj = hopital_list_obj
        self.sgil_obj = sgil_obj
        self.outbreak_obj_v2 = outbreak_obj_v2

        self.nomatch_envoisgq_tspgeo = nomatch_envoisgq_tspgeo

        self.nb_nomatch_tspGeo_envoisGenomeQc = 0

        self.multiplematch_tspGeo_envoisGenomeQc_df = pd.DataFrame(columns=['Nom','Prénom','# Requête','Date de naissance','Date de prélèvement','NAM'])
        self.nb_multiplematch_tspGeo_envoisGenomeQc = 0
        
        if not  _debug_:
            self.base_dir_scriptout = "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/"
        else:
            self.base_dir_scriptout = "/data/Databases/CovBanQ_Epi/DEBUG/"

        self.multiplematch_tspGeo_envoisGenomeQc_out = os.path.join(self.base_dir_scriptout,"multiplematch_tspGeo_envoisGenomeQc.xlsx")
        self.req_no_ch_code_out = os.path.join(self.base_dir_scriptout,"rec_no_ch_code.xlsx")
        self.nomatch_tspGeo_envoisGenomeQc_out = os.path.join(self.base_dir_scriptout,"nomatch_tspGeo_envoisGenomeQc.xlsx")        
        self.missing_old_outbreak_sample_id_out = os.path.join(self.base_dir_scriptout,"missing_old_outbreak_sample_id.xlsx")
        self.missing_new_outbreak_sample_id_out = os.path.join(self.base_dir_scriptout,"missing_new_outbreak_sample_id.xlsx")

        self.req_no_ch_code = set()

        self.yaml_conn_param = open('CovBankParam.yaml')
        self.ReadConnParam()
        self.connection = self.SetConnection()

        self.patient_col_list = ['PRENOM','NOM','SEXE','DTNAISS','RSS','RTA','NAM']
        self.prelevement_col_list = ['ID_PATIENT','CODE_HOPITAL','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV','GENOME_QUEBEC_REQUETE','DATE_ENVOI_GENOME_QUEBEC','TRAVEL_HISTORY','CT','OUTBREAK','DEB_VOY1','FIN_VOY1','DEST_VOY1','DEB_VOY2','FIN_VOY2','DEST_VOY2','DEB_VOY3','FIN_VOY3','DEST_VOY3','DEB_VOY4','FIN_VOY4','DEST_VOY4','DEB_VOY5','FIN_VOY5','DEST_VOY5','DEB_VOY6','FIN_VOY6','DEST_VOY6','DEB_VOY7','FIN_VOY7','DEST_VOY7','DEB_VOY8','FIN_VOY8','DEST_VOY8','DEB_VOY9','FIN_VOY9','DEST_VOY9','DEB_VOY10','FIN_VOY10','DEST_VOY10','DEB_VOY11','FIN_VOY11','DEST_VOY11','DEB_VOY12','FIN_VOY12','DEST_VOY12','DEB_VOY13','FIN_VOY13','DEST_VOY13','COVBANK_ID','BIOBANK_ID']
        self.prelevement_col_list_sgil = ['ID_PATIENT','CODE_HOPITAL','NOM_HOPITAL','ADRESSE_HOPITAL','DATE_PRELEV','GENOME_QUEBEC_REQUETE','TRAVEL_HISTORY','CT','OUTBREAK','NUMERO_SGIL','COVBANK_ID','BIOBANK_ID']

    def CloseConnection(self):
        self.GetConnection().close()

    def GetDatabaseName(self):
        return(self.database)

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

    def UpdateGenomeQuebecRequete(self):
        for index,row in self.outbreak_obj_v2.GetNewOutbreakDf().loc[:,].iterrows():
            cursor = self.GetCursor()
            outbreak = row['Outbreak']
            covbank_id = row['COVBANK']
            biobank_id = row['BIOBANK']
            sql = "UPDATE Prelevements SET GENOME_QUEBEC_REQUETE  = '{0}', BIOBANK_ID = '{0}', COVBANK_ID = '{1}' WHERE GENOME_QUEBEC_REQUETE = '{1}'".format(biobank_id,covbank_id)
            cursor.execute(sql)
            nb_update = cursor.rowcount
            cursor.close()
            self.Commit()
                
    def UpdateWithOldOutbreak(self):
        sample_id_not_found = []
        for index,row in self.outbreak_obj_v2.GetOldOutbreakDf().loc[:,].iterrows():
            cursor = self.GetCursor()
            outbreak = row['Outbreak']
            sample_id = row['COVBANK']
            #sql = "UPDATE Prelevements SET OUTBREAK = '{0}'  WHERE GENOME_QUEBEC_REQUETE = '{1}'".format(outbreak,sample_id)
            sql = "UPDATE Prelevements SET OUTBREAK = '{0}'  WHERE COVBANK_ID  = '{1}'".format(outbreak,sample_id)
            #print(sql)
            cursor.execute(sql)
            nb_update = cursor.rowcount
            #print(nb_update, " ",type(nb_update))
            if nb_update < 1:
                sample_id_not_found.append(sample_id)
            else:
                pass
                #biobank_id = row['BIOBANK']
                #sql2 = "UPDATE Prelevements SET GENOME_QUEBEC_REQUETE  = '{0}', BIOBANK_ID = '{0}', COVBANK_ID = '{1}' WHERE GENOME_QUEBEC_REQUETE = '{1}'".format(biobank_id,sample_id)
                #cursor.execute(sql2)

            cursor.close()
            self.Commit()
         
        self.missing_old_outbreak_sample_id_df = self.outbreak_obj_v2.GetOldOutbreakDf().loc[self.outbreak_obj_v2.GetOldOutbreakDf()['COVBANK'].isin(sample_id_not_found),:]
            
    def Insert(self):
        logging.info("Begin insert")

        self.nb_patients_inserted = 0
        self.nb_prelevements_inserted = 0


        for index, row in self.envois_genome_qc_obj.pd_df.loc[:,].iterrows():
            nom = row['Nom']
            prenom = row['Prénom']
            date_naiss = row['Date de naissance']
            date_prelev = row['Date de prélèvement']
            req = row['# Requête']
            Biobank_Id = row['GENOME_CENTER_ID']    
            #print(Biobank_Id)
        
            nam = row['NAM']
            tsp_geo_match_df = self.GetTspGeoMatch(nom,prenom,date_naiss,nam,date_prelev,req)

            if tsp_geo_match_df.shape[0] == 0:
                nomatch_df = self.nomatch_envoisgq_tspgeo.GetNoMatchInDf()
                matchdf_in_nomatchdf = nomatch_df.loc[nomatch_df['# Requête'] == req,['Nom TSP_GEO','Prenom TSP_GEO','Date de naissance TSP_GEO','NAM TSP_GEO']]
                if matchdf_in_nomatchdf.shape[0] == 1:
                    try:
                        nom_tsp_geo = str(matchdf_in_nomatchdf['Nom TSP_GEO'].iloc[0])
                        prenom_tsp_geo = str(matchdf_in_nomatchdf['Prenom TSP_GEO'].iloc[0])
                        datenaiss_tsp_geo = matchdf_in_nomatchdf['Date de naissance TSP_GEO'].iloc[0]
                        nam_tsp_geo = str(matchdf_in_nomatchdf['NAM TSP_GEO'].iloc[0])
                        #print("TSP GEO ",nom_tsp_geo, " ", prenom_tsp_geo, " ",datenaiss_tsp_geo, " ",nam_tsp_geo)
                        tsp_geo_match_df = self.GetTspGeoMatch(nom_tsp_geo,prenom_tsp_geo,datenaiss_tsp_geo,nam_tsp_geo,date_prelev,req)
                    except:
                        print("PROBLEM SEARCH IN NOMATCH ")
                        tsp_geo_match_df = pd.DataFrame()
                else:
                    pass
                    #print("NO MATCH FOR ",req)

            if tsp_geo_match_df.shape[0] != 0:
                patient_record = self.GetPatientValToInsert(tsp_geo_match_df)
                patient_id = self.InsertPatient(patient_record)
                if patient_id is not None:
                    prelevement_record = self.GetPrelevementToInsert(tsp_geo_match_df,row,patient_id)
                    self.InsertPrelevement(prelevement_record,False)
                else:
                    logging.error("Impossible d inserer ce prelevement " + str(row))


        for index, row in self.sgil_obj.pd_df.loc[:,].iterrows():
            if _mode_ == 'outbreak':
                pass
            nom = row['NOM']
            prenom = row['PRENOM']
            date_naiss = row['DATE_NAISS']
            date_prelev = row['SAMPLED_DATE']
            nam = row['NAM']
            tsp_geo_match_df = self.GetTspGeoMatchWithSgilData(nom,prenom,date_naiss,nam,date_prelev) 

            patient_record = self.GetPatientValToInsertFromSgilData(tsp_geo_match_df,row)
            patient_id = self.InsertPatient(patient_record)

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
       
        #NOTE le GENOME_QUEBEC_REQUETE est en fait le BIOBANK_ID 
        sql = "SELECT ID_PRELEV from Prelevements where GENOME_QUEBEC_REQUETE = '{0}'".format(rec['GENOME_QUEBEC_REQUETE'])
        #sql = "SELECT ID_PRELEV from Prelevements where COVBANK_ID = '{0}'".format(rec['GENOME_QUEBEC_REQUETE'])
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

    def SetMissingNewOutbreakSamples(self):
        new_outbreak_df = self.outbreak_obj_v2.GetNewOutbreakDf()
        new_outbreak_list = list(new_outbreak_df['BIOBANK'])
        sql = "SELECT GENOME_QUEBEC_REQUETE from Prelevements where GENOME_QUEBEC_REQUETE in " + " %s" % str(tuple(new_outbreak_list))
        outbreak_inserted_df = pd.read_sql(sql,con=self.GetConnection())
        outbreak_inserted_df = outbreak_inserted_df.rename(columns={'GENOME_QUEBEC_REQUETE':'BIOBANK'})
        self.missing_new_outbreak_sample_id_df = new_outbreak_df.loc[~new_outbreak_df['BIOBANK'].isin(list(outbreak_inserted_df['BIOBANK'])),:]
      
    def InsertPrelevement(self,prelevement_record,is_sgil):
        cursor = self.GetCursor()
        exist = self.CheckIfPrelevementExist(prelevement_record,cursor,is_sgil)

        if not exist:
            try:
                pass
                ncols = len(prelevement_record)
                sql_insert = "INSERT INTO Prelevements ({0}) values ({1})".format(self.GetPrelevementColumns(is_sgil),str("%s,"*ncols)[:-1])
                #print(prelevement_record)
                cursor.execute(sql_insert,prelevement_record)
                cursor.close()
                self.Commit()
                self.nb_prelevements_inserted += 1
                sys.stdout.write("Insert in Prelevement >>> %d\r"%self.nb_prelevements_inserted)
                sys.stdout.flush()
            except mysql.connector.Error as err:
                logging.error("Erreur d'insertion dans la table Prelevements avec le record " + str(prelevement_record))
                print(err)
        elif _mode_ == 'outbreak':
            if is_sgil:
                rec = dict(list(zip(self.prelevement_col_list_sgil,prelevement_record)))
            else:
                rec = dict(list(zip(self.prelevement_col_list,prelevement_record)))
            
            sql_update = "UPDATE Prelevements SET OUTBREAK = '{0}' WHERE GENOME_QUEBEC_REQUETE = '{1}'".format(rec['OUTBREAK'],rec['GENOME_QUEBEC_REQUETE'])
            cursor.execute(sql_update)
            cursor.close()
            self.Commit()
             
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

        if _mode_ == 'outbreak':
            outbreak = sgil_record['Outbreak']
        else:
            outbreak = 'NA'

        id_sgil = sgil_folderno

        return(tuple(map(GetVal,(patient_id,code_hopital,nom_hopital,adresse_hopital,date_prelev,sgil_folderno,travel_history,ct,outbreak,id_sgil,id_sgil,id_sgil))))


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
        CovbankId = envois_genome_qc['# Requête']
        BiobankId = envois_genome_qc['GENOME_CENTER_ID']

        if _mode_ == 'outbreak':
            outbreak = envois_genome_qc['Outbreak']
        else:
            outbreak = 'NA'

        if not re.search(r'-',CovbankId):
            self.req_no_ch_code.add(CovbankId)
            code_hopital = "NAN"
        else:
            try:
                try:
                    code_hopital = re.search(r'(\S+)-\S+-\S+',CovbankId).group(1)
                except:
                    code_hopital = re.search(r'(\S+)-\S+',CovbankId).group(1)

                code_hopital = code_hopital + "-"
            except:
                logging.error("PROBLEM WITH THIS CH CODE " + str(CovbankId))
                code_hopital = "NAN"


        if str(CovbankId[0:5]) == 'HDS-S':
            nom_hopital = self.hopital_list_obj.GetChSorelName()
            adresse_hopital = self.hopital_list_obj.GetChSorelAdress()
        else:
            nom_hopital = self.hopital_list_obj.GetHospitalName(code_hopital)
            adresse_hopital = self.hopital_list_obj.GetHospitalAddress(code_hopital)

        date_envois_genome_quebec = envois_genome_qc['DateEnvoiGenomeQuebec']
        travel_history = 'INDETERMINE' # prendre de tsp_geo mais temporairement de sgil
        ct = '99' # prendre de tsp_geo mais temporaire de sgil
        deb_voy1 = str(list(tsp_geo_match_df['deb_voy1'])[0])
        fin_voy1 = str(list(tsp_geo_match_df['fin_voy1'])[0])
        dest_voy1 = str(list(tsp_geo_match_df['dest_voy1'])[0])
        deb_voy2 = str(list(tsp_geo_match_df['deb_voy2'])[0])
        fin_voy2 = str(list(tsp_geo_match_df['fin_voy2'])[0])
        dest_voy2 = str(list(tsp_geo_match_df['dest_voy2'])[0])
        deb_voy3 = str(list(tsp_geo_match_df['deb_voy3'])[0])
        fin_voy3 = str(list(tsp_geo_match_df['fin_voy3'])[0])
        dest_voy3 = str(list(tsp_geo_match_df['dest_voy3'])[0])
        deb_voy4 = str(list(tsp_geo_match_df['deb_voy4'])[0])
        fin_voy4 = str(list(tsp_geo_match_df['fin_voy4'])[0])
        dest_voy4 = str(list(tsp_geo_match_df['dest_voy4'])[0])
        deb_voy5 = str(list(tsp_geo_match_df['deb_voy5'])[0])
        fin_voy5 = str(list(tsp_geo_match_df['fin_voy5'])[0])
        dest_voy5 = str(list(tsp_geo_match_df['dest_voy5'])[0])
        deb_voy6 = str(list(tsp_geo_match_df['deb_voy6'])[0])
        fin_voy6 = str(list(tsp_geo_match_df['fin_voy6'])[0])
        dest_voy6 = str(list(tsp_geo_match_df['dest_voy6'])[0])
        deb_voy7 = str(list(tsp_geo_match_df['deb_voy7'])[0])
        fin_voy7 = str(list(tsp_geo_match_df['fin_voy7'])[0])
        dest_voy7 = str(list(tsp_geo_match_df['dest_voy7'])[0])
        deb_voy8 = str(list(tsp_geo_match_df['deb_voy8'])[0])
        fin_voy8 = str(list(tsp_geo_match_df['fin_voy8'])[0])
        dest_voy8 = str(list(tsp_geo_match_df['dest_voy8'])[0])
        deb_voy9 = str(list(tsp_geo_match_df['deb_voy9'])[0])
        fin_voy9 = str(list(tsp_geo_match_df['fin_voy9'])[0])
        dest_voy9 = str(list(tsp_geo_match_df['dest_voy9'])[0])
        deb_voy10 = str(list(tsp_geo_match_df['deb_voy10'])[0])
        fin_voy10 = str(list(tsp_geo_match_df['fin_voy10'])[0])
        dest_voy10 = str(list(tsp_geo_match_df['dest_voy10'])[0])
        deb_voy11 = str(list(tsp_geo_match_df['deb_voy11'])[0])
        fin_voy11 = str(list(tsp_geo_match_df['fin_voy11'])[0])
        dest_voy11 = str(list(tsp_geo_match_df['dest_voy11'])[0])
        deb_voy12 = str(list(tsp_geo_match_df['deb_voy12'])[0])
        fin_voy12 = str(list(tsp_geo_match_df['fin_voy12'])[0])
        dest_voy12 = str(list(tsp_geo_match_df['dest_voy12'])[0])
        deb_voy13 = str(list(tsp_geo_match_df['deb_voy13'])[0])
        fin_voy13 = str(list(tsp_geo_match_df['fin_voy13'])[0])
        dest_voy13 = str(list(tsp_geo_match_df['dest_voy13'])[0])

        return(tuple(map(GetVal,(patient_id,code_hopital,nom_hopital,adresse_hopital,date_prelev,BiobankId,date_envois_genome_quebec,travel_history,ct,outbreak,deb_voy1,fin_voy1,dest_voy1,deb_voy2,fin_voy2,dest_voy2,deb_voy3,fin_voy3,dest_voy3,deb_voy4,fin_voy4,dest_voy4,deb_voy5,fin_voy5,dest_voy5,deb_voy6,fin_voy6,dest_voy6,deb_voy7,fin_voy7,dest_voy7,deb_voy8,fin_voy8,dest_voy8,deb_voy9,fin_voy9,dest_voy9,deb_voy10,fin_voy10,dest_voy10,deb_voy11,fin_voy11,dest_voy11,deb_voy12,fin_voy12,dest_voy12,deb_voy13,fin_voy13,dest_voy13,CovbankId,BiobankId))))
        

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
            temp_df = pd.DataFrame({'Nom':[nom],'Prénom':[prenom],'NAM':[nam],'Date de naissance':[date_naiss],'Date de prélèvement':[date_prelev],'# Requête':[req]},columns = ['Nom','Prénom','NAM','Date de naissance','Date de prélèvement','# Requête'])
            if self.nomatch_envoisgq_tspgeo.CheckIfNoMatchAlreadyInNoMatches(temp_df) > 0:
                pass
            else:
                self.nomatch_envoisgq_tspgeo.nomatch_out_df = pd.concat([self.nomatch_envoisgq_tspgeo.nomatch_out_df,temp_df])
            return match_df
        elif match_df.shape[0] > 1 :
            self.multiplematch_tspGeo_envoisGenomeQc_df.loc[self.nb_multiplematch_tspGeo_envoisGenomeQc] = {'Nom':nom,'Prénom':prenom,'NAM':nam,'Date de naissance':date_naiss,'Date de prélèvement':date_prelev,'# Requête':req}
            self.nb_multiplematch_tspGeo_envoisGenomeQc += 1
            match_df = match_df[0:0]
            return match_df

        return match_df


    def ComputeDatePrelevDiff(self,match_df,date_prelev):
        match_df['DATE_DIFF'] = match_df['date_prel'] - date_prelev
        match_df['DATE_DIFF'] = match_df['DATE_DIFF'].abs()

    def WriteMultipleMatchTspGeoToEnvoisGenomeQcToFile(self):
        self.multiplematch_tspGeo_envoisGenomeQc_df.to_excel(self.multiplematch_tspGeo_envoisGenomeQc_out,sheet_name='Sheet1')

    def WriteMissingOutBreakSample(self):
        if _mode_ == 'init':
            self.missing_old_outbreak_sample_id_df.to_excel(self.missing_old_outbreak_sample_id_out,sheet_name='Sheet1')
        else:
            self.missing_new_outbreak_sample_id_df.to_excel(self.missing_new_outbreak_sample_id_out,sheet_name='Sheet1')

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


class OutbreakDataV2:
    def __init__(self):
        #DANS LA BD IL FAUT METTRE LES ID QUI SONT DANS SGIL EXTRACT ET LISTE ENVOI ET NON PAS LES ID BELUGA CAR J EN TIENT COMPTE DEJA DANS LE SCRIPT GetFastaForNextstrain_v2.py

        self.old_outbreak_file_list = []
        self.new_outbreak_file_list = []

        logging.info("In OutbreakDataV2")
        if _debug_:
            self.base_dir_old_outbreak = "/data/Databases/CovBanQ_Epi/DEBUG/OUTBREAK_OLD/"
            self.base_dir_new_outbreak = "/data/Databases/CovBanQ_Epi/DEBUG/OUTBREAK_NEW/"
        else:
            self.base_dir_old_outbreak = "/data/Databases/CovBanQ_Epi/OUTBREAK_OLD/"
            self.base_dir_new_outbreak = "/data/Databases/CovBanQ_Epi/OUTBREAK_NEW/"

        for old_outbreak_file in glob.glob(self.base_dir_old_outbreak + "*.list"):
            self.old_outbreak_file_list.append(old_outbreak_file)

        for new_outbreak_file in glob.glob(self.base_dir_new_outbreak + "*.list"):
            self.new_outbreak_file_list.append(new_outbreak_file)

        if _mode_ == 'init':
            self.BuildOldOutbreakDf()
        else:
            self.BuildNewOutbreakDf()

    def BuildOldOutbreakDf(self):
        self.old_outbreak_df = pd.DataFrame(columns=['COVBANK','BIOBANK','Outbreak'])

        for old_outbreak_file in self.old_outbreak_file_list:
            file_name = os.path.basename(old_outbreak_file)
            outbreak_name = re.sub(r'\.list','',file_name)
            temp_df = pd.read_csv(old_outbreak_file,sep="\t",index_col=False)
            temp_df['Outbreak'] = outbreak_name
            self.old_outbreak_df = pd.concat([self.old_outbreak_df,temp_df])

    def BuildNewOutbreakDf(self):

        self.new_outbreak_df = pd.DataFrame(columns=['COVBANK','BIOBANK','Outbreak'])

        for new_outbreak_file in self.new_outbreak_file_list:
            file_name = os.path.basename(new_outbreak_file)
            outbreak_name = re.sub(r'\.list','',file_name)
            temp_df = pd.read_csv(new_outbreak_file,sep="\t",index_col=False)
            temp_df['Outbreak'] = outbreak_name
            self.new_outbreak_df = pd.concat([self.new_outbreak_df,temp_df])
        #print(self.new_outbreak_df)

    def GetOldOutbreakDf(self):
        return self.old_outbreak_df

    def GetNewOutbreakDf(self):
        return self.new_outbreak_df


class SGILdata:
    def __init__(self,outbreak_obj_v2):
        logging.info("In SGILdata")

        self.outbreak_obj_v2 = outbreak_obj_v2

        if _debug_:
            self.base_dir = "/data/Databases/CovBanQ_Epi/DEBUG/"
            table_data = sgil_in_file
        else:
            self.base_dir = "/data/Databases/CovBanQ_Epi/SGIL_EXTRACT/"
            table_data = sgil_in_file

        self.pd_df = pd.read_table(os.path.join(self.base_dir,table_data))
        self.Format()
        if _mode_ == 'outbreak':
            self.ExtractOutbreakSample()

    def ExtractOutbreakSample(self):
        outbreak_df = self.outbreak_obj_v2.GetNewOutbreakDf()
        merge_df = pd.merge(outbreak_df,self.pd_df,left_on='COVBANK',right_on='NUMERO_SGIL',how='inner')

        self.pd_df = merge_df
        self.pd_df['NUMERO_SGIL'] = self.pd_df['BIOBANK']
        #print(self.pd_df)  

    def Format(self):
        self.pd_df['NOM'] = self.pd_df['NOM'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace("É","E").str.replace(",","")
        self.pd_df['PRENOM'] = self.pd_df['PRENOM'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace("É","E").str.replace(",","")
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
        
        if _debug_:
            self.base_dir = "/data/Databases/CovBanQ_Epi/DEBUG/"
            excel_data = tsp_in_file
        else:
            self.base_dir = "/data/Databases/CovBanQ_Epi/TSP_GEO/"
            excel_data = tsp_in_file
        
        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)
        self.Format()

    def Format(self):
        #TODO SI MANQUE DATE NAISS ON PEUT L OBTENIR A PARTIR DU NAM
        self.pd_df['nom'] = self.pd_df['nom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace('-','').str.replace(' ','').str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace("É","E").str.replace(",","")
        self.pd_df['nom'] = self.pd_df['nom'].str.replace('È','E')
        
        self.pd_df['prenom'] = self.pd_df['prenom'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace('-','').str.replace(' ','').str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace("É","E").str.replace(",","")
        self.pd_df['prenom'] = self.pd_df['prenom'].str.replace('È','E')

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

class NoMatchEnvoisGqTspGeo:
    def __init__(self):
        logging.info("In NoMatchEnvoisGqTspGeo")

        if _debug_:
            self.base_dir_in = "/data/Databases/CovBanQ_Epi/DEBUG/"
            self.base_dir_out = "/data/Databases/CovBanQ_Epi/DEBUG/NOMATCH_ENVOISGQ_TSPGEO_OUT/"
            nomatch_in = mm_in_file
        else: 
            self.base_dir_in = "/data/Databases/CovBanQ_Epi/NOMATCH_ENVOISGQ_TSPGEO/IN/"
            self.base_dir_out = "/data/Databases/CovBanQ_Epi/NOMATCH_ENVOISGQ_TSPGEO/OUT/"
            nomatch_in = mm_in_file

        self.nb_no_matches = 0

        self.nomatch_in_df = pd.read_excel(os.path.join(self.base_dir_in,nomatch_in),sheet_name=0)
        self.nomatch_out_df = self.nomatch_in_df.copy()

        self.nomatch_in_df['Date de naissance TSP_GEO'] = pd.to_datetime(self.nomatch_in_df['Date de naissance TSP_GEO'],format='%Y%m%d',errors='coerce')
        self.nomatch_in_df['Nom TSP_GEO'] = self.nomatch_in_df['Nom TSP_GEO'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace('-','').str.replace(' ','').str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace("É","E").str.replace(",","").str.strip(' ').str.upper()
        self.nomatch_in_df['Prenom TSP_GEO'] = self.nomatch_in_df['Prenom TSP_GEO'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.replace('-','').str.replace(' ','').str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace("É","E").str.replace(",","").str.strip(' ').str.upper()
        
        self.nomatch_in_df['NAM TSP_GEO'] = self.nomatch_in_df['NAM TSP_GEO'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace("'","").str.strip(' ').str.upper()


        yaml_conn_param = open('CovBankParam.yaml')
        param = yaml.load(yaml_conn_param,Loader=yaml.FullLoader)
        database = param['database']
        
        self.nomatch_out = os.path.join(self.base_dir_out,"nomatch_tspGeo_envoisGenomeQc_" + database + ".xlsx")


    def WriteNewNoMatch(self):
        self.nomatch_out_df.to_excel(self.nomatch_out,sheet_name='Sheet1')

    def CheckIfNoMatchAlreadyInNoMatches(self,check_df):
        merge_df = pd.merge(self.nomatch_in_df,check_df,how='inner',on = ['# Requête'])
        return(merge_df.shape[0])

    def GetNoMatchInDf(self):
        return(self.nomatch_in_df)


class BBC19data:
    def __init__(self):
        logging.info("In BB19data")

        if _debug_:
            self.base_dir = "/data/Databases/CovBanQ_Epi/DEBUG/"
            excel_data = bbc19_file
            #print("BBC file ",excel_data)
        else:
            self.base_dir = "/data/Databases/CovBanQ_Epi/BB_C19/"
            excel_data = bbc19_file

        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)
        self.Format()

    def Format(self):
        self.pd_df = self.pd_df[['ID2','ID_COVBANQ']]

        self.pd_df['GENOME_CENTER_ID'] = self.pd_df.apply(lambda x: self.SetGenomeCenterId(x.ID2,x.ID_COVBANQ),axis = 1)
        self.pd_df['# Requête']  = self.pd_df.apply(lambda x: self.SetHopitalReq(x.ID2,x.ID_COVBANQ),axis = 1) 
        self.pd_df['# Requête'] = self.pd_df['# Requête'].str.replace(' ','')
        self.pd_df = self.pd_df[['# Requête','GENOME_CENTER_ID']]

    def GetPdDf(self):
        return(self.pd_df)

    def GetHopitalReqFromId2(self,id2):
        hopital_req = ""

        if re.search(r'^ARG-',id2):
            hopital_req = re.sub(r'(^ARG-\d{7})\d{3}$',r'\1',id2)
        elif re.search(r'^CHAL-',id2):
            hopital_req = re.sub(r'(^CHAL-)\d{3}(\d{7})$',r'\1\2',id2)
        elif re.search(r'^HCLM-',id2):
            hopital_req = re.sub(r'(^HCLM-)\d{3}(\d{7})$',r'\1\2',id2)
        elif re.search(r'^HGA-',id2):
            hopital_req = re.sub(r'(^HGA-\S{8})\S{2}$',r'\1',id2)
        elif re.search(r'^HHM-',id2):
            hopital_req = re.sub(r'(^HHM-\S{8})\S{2}$',r'\1',id2)
        elif re.search(r'^HPLG-',id2):
            hopital_req = re.sub(r'(^HPLG-\S{8})\S{2}$',r'\1',id2)
        elif re.search(r'^HSCM-',id2):
            hopital_req = re.sub(r'(^HSCM-\S{8})\S{2}$',r'\1',id2)
        elif re.search(r'^HSU-',id2):
            hopital_req = re.sub(r'(^HSU-)\S{3}(\S{7}$)',r'\1\2',id2)
        elif re.search(r'^HVE-',id2):
            hopital_req = re.sub(r'(^HVE-\S{8})\S{2}$',r'\1',id2)
        elif re.search(r'^ICM-',id2):
            hopital_req = re.sub(r'(^ICM-\S{8})\S{3}$',r'\1',id2)
        elif re.search(r'^JOL-',id2):
            hopital_req = re.sub(r'(^JOL-\S{8})\S{2}$',r'\1',id2)
        elif re.search(r'^JUS-',id2):
            hopital_req = re.sub(r'(^JUS-\S{8})\S{2}$',r'\1',id2)
        elif re.search(r'^LAKE-',id2):
            hopital_req = re.sub(r'(^LAKE-\S{8})\S{2}$',r'\1',id2)
        else:
            hopital_req = id2

        if len(hopital_req) == 0:
            logging.error("Error parsing ", id2)
            return(id2)

        return(hopital_req)

    def SetHopitalReq(self,id2,id_covbanq):
        if isinstance(id_covbanq,float):
            return(self.GetHopitalReqFromId2(id2))
        else:
            return(id_covbanq)

    def SetGenomeCenterId(self,id2,id_covbanq):
        #print(type(id2)," ---- ", type(id_covbanq), isinstance(id_covbanq,float))
        if isinstance(id_covbanq,float):
            return(id2)
        else:
            return(id_covbanq)       
 
class EnvoisGenomeQuebecData:
    def __init__(self,outbreak_obj_v2,bbc19_obj):
        logging.info("In EnvoisGenomeQuebecData")

        self.outbreak_obj_v2 = outbreak_obj_v2
        self.bbc19_obj = bbc19_obj

        if _debug_:
            self.base_dir = "/data/Databases/CovBanQ_Epi/DEBUG/"
            excel_data = gq_in_file
        else:
            self.base_dir = "/data/Databases/CovBanQ_Epi/LISTE_ENVOIS_GENOME_QUEBEC/"
            excel_data = gq_in_file

        self.pd_df = pd.read_excel(os.path.join(self.base_dir,excel_data),sheet_name=0)
        self.Format()

        self.MergeWithBBC19()
         
        print(self.pd_df)

        if _mode_ == 'outbreak':
            self.ExtractOutbreakSample()

    def MergeWithBBC19(self):
        self.pd_df = pd.merge(self.pd_df,self.bbc19_obj.GetPdDf(),how='left',on='# Requête')
        self.pd_df['GENOME_CENTER_ID'] = self.pd_df['GENOME_CENTER_ID'].replace(np.nan,self.pd_df['# Requête'])


    def Format(self):
        self.pd_df = self.pd_df.loc[~self.pd_df['# Requête'].str.contains('^LSPQ-',regex=True),:]
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
        self.pd_df['NAM'] = self.pd_df['NAM'].str.replace(r'^0{1,}','')

        self.pd_df['Nom'].str.replace('É','E').str.replace('È','E').str.replace(',','').str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace(",","").str.replace("'","")
        self.pd_df['Prénom'].str.replace('É','E').str.replace('È','E').str.replace(',','').str.replace("*","").str.replace("(","").str.replace(")","").str.replace("&","").str.replace(".","").str.replace(",","").str.replace("'","")

        self.pd_df['Date de naissance'] = pd.to_datetime(self.pd_df['Date de naissance'],format='%Y-%m-%d',errors='coerce')
        self.pd_df['Date de prélèvement'] = pd.to_datetime(self.pd_df['Date de prélèvement'],format='%Y-%m-%d',errors='coerce')
        self.pd_df['DateEnvoiGenomeQuebec'] = pd.to_datetime(self.pd_df['DateEnvoiGenomeQuebec'],format='%Y-%m-%d',errors='coerce')
        
        self.pd_df = self.pd_df.dropna(subset = ['Date de prélèvement'])


    def ExtractOutbreakSample(self):
        outbreak_df = self.outbreak_obj_v2.GetNewOutbreakDf()
        #merge_df = pd.merge(outbreak_df,self.pd_df,left_on='COVBANK',right_on='# Requête',how='inner')
        merge_df = pd.merge(outbreak_df,self.pd_df,left_on='BIOBANK',right_on='GENOME_CENTER_ID',how='inner')

        self.pd_df = merge_df
        
        #Pas besoin de la ligne ci-dessous 
        #self.pd_df['# Requête'] = self.pd_df['COVBANK']

def Main():
    logging.info("Begin update")

    bbc19_obj = BBC19data()

    outbreak_obj_v2 = OutbreakDataV2()

    hopital_list_obj = HopitalList()

    sgil_obj = SGILdata(outbreak_obj_v2)
    envois_genome_qc_obj = EnvoisGenomeQuebecData(outbreak_obj_v2,bbc19_obj) 
    #exit(1)
    tsp_geo_obj = TspGeoData()
    nomatch_envoisgq_tspgeo = NoMatchEnvoisGqTspGeo()

    #exit(1)

    cov_bank_db = CovBankDB(tsp_geo_obj,envois_genome_qc_obj,hopital_list_obj,sgil_obj,outbreak_obj_v2,nomatch_envoisgq_tspgeo)

    cov_bank_db.Insert()

    if _mode_ == 'init':
        cov_bank_db.UpdateWithOldOutbreak()
    else:
        #Plus besoin de la ligne ci-dessous
        #cov_bank_db.UpdateGenomeQuebecRequete()
        cov_bank_db.SetMissingNewOutbreakSamples() 

    cov_bank_db.WriteMissingOutBreakSample()
    cov_bank_db.WriteReqNoChCodeToFile()
    
    cov_bank_db.WriteMultipleMatchTspGeoToEnvoisGenomeQcToFile()
    cov_bank_db.CloseConnection()

    nomatch_envoisgq_tspgeo.WriteNewNoMatch()

    hopital_list_obj.WriteMissingChCodeToFile()

    today = datetime.datetime.now().strftime("%Y-%m-%d@%H:%M:%S")

    with open("/data/Databases/CovBanQ_Epi/SCRIPT_OUT/" + cov_bank_db.GetDatabaseName() + ".log",'w') as log:
        log.write("Terminé @ " + today + "\n")
    log.close()

if __name__ == '__main__':
    Main()


