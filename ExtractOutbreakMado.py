# -*- coding: utf-8 -*-

"""
Eric Fournier 2020-12-09

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

global _debug
_debug = False

global basedir_in
basedir_in =  "/data/Databases/CovBanQ_Epi/FOR_EXTRACTION/"

global basedir_out
basedir_out =  "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/"

class CovBankDB:
    def __init__(self,outbreak_mado_obj):
        self.yaml_conn_param = open('CovBankParam.yaml')
        self.ReadConnParam()
        self.connection = self.SetConnection()
        self.outbreak_mado_obj = outbreak_mado_obj

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

    def ReadConnParam(self):
        param = yaml.load(self.yaml_conn_param,Loader=yaml.FullLoader)
        self.host = param['host']
        self.user = param['user']
        self.password = param['password']
        self.database = param['database']

    def SelectOutbreakMado(self):
        self.nb_select = 0
        self.outbreak_mado_with_req_df = pd.DataFrame()
        outbreak_mado_df = self.outbreak_mado_obj.GetDf()
        self.outbreak_mado_not_found_df = pd.DataFrame(columns=outbreak_mado_df.columns)

        for index, row in outbreak_mado_df.loc[:,].iterrows():
            #print(row)
            self.nb_select += 1
            sys.stdout.write("select >>> %d\r"%self.nb_select)
            sys.stdout.flush()

            nom = row['NOM']
            prenom = row['PRENOM']
            nam = str(row['NAM'])
            dt_naiss = row['DATE_NAISS']

            #print('NOM ',nom, ' PRENOM ',prenom, ' NAM ',nam, 'DTNAISS ',dt_naiss)

            sql = ""

            if len(str(nam)) > 8:
                sql = "SELECT pa.ID_PATIENT,pa.PRENOM,pa.NOM,pa.DTNAISS,pa.RTA,pa.NAM, pr.DATE_ENVOI_GENOME_QUEBEC, pr.GENOME_QUEBEC_REQUETE, pr.CT, pr.DATE_PRELEV from Patients pa inner join Prelevements pr on pa.ID_PATIENT = pr.ID_PATIENT WHERE pa.NAM = '{0}' ".format(nam)
            else:
                sql = "SELECT pa.ID_PATIENT,pa.PRENOM,pa.NOM,pa.DTNAISS,pa.RTA,pa.NAM,pr.DATE_ENVOI_GENOME_QUEBEC, pr.GENOME_QUEBEC_REQUETE, pr.CT, pr.DATE_PRELEV from Patients pa inner join Prelevements pr on pa.ID_PATIENT = pr.ID_PATIENT   WHERE pa.NOM = '{0}' and pa.PRENOM = '{1}' and pa.DTNAISS = '{2}' ".format(nom,prenom,dt_naiss)

            df = pd.read_sql(sql,con=self.GetConnection())
            nb_found = df.shape[0]
            #print(nb_found)
            if str(nb_found) == '0':
                self.outbreak_mado_not_found_df = pd.concat([self.outbreak_mado_not_found_df,pd.DataFrame({'NOM':[nom],'PRENOM':prenom,'NAM':nam,'DATE_NAISS':dt_naiss})])

            self.outbreak_mado_with_req_df = pd.concat([self.outbreak_mado_with_req_df,df])

    def SaveOutbreakMadoWithReq(self):
        self.outbreak_mado_obj.SaveOutbreakMadoWithReq(self.outbreak_mado_with_req_df,self.outbreak_mado_not_found_df)
        

class OutbreakMado:
    def __init__(self):
        if _debug:
            self.in_file = os.path.join(basedir_in,"CAS_ECLOSION_MADO_20201222_small.xlsx")
        else:
            self.in_file = os.path.join(basedir_in,"CAS_ECLOSION_MADO_20201222.xlsx")

        self.outfile = os.path.join(basedir_out,"OutbreakMado.xlsx")
        self.outfile_not_found = os.path.join(basedir_out,"OutbreakMadoNotFound.xlsx")

        self.SetOutbreakMadoDf()

    def SaveOutbreakMadoWithReq(self,df_found,df_notfound):
        print("OUT >>>>>>>>>>>>> ",self.outfile)
        df_found.to_excel(self.outfile,sheet_name='Sheet1')
        df_notfound.to_excel(self.outfile_not_found,sheet_name = 'Sheet1')

    def SetOutbreakMadoDf(self):
        self.outbreak_mado_df = pd.read_excel(self.in_file,sheet_name='Feuil1')

        self.outbreak_mado_df['NOM'] = self.outbreak_mado_df['NOM'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace('-','').str.replace(' ','').str.strip(' ').str.upper()
        self.outbreak_mado_df['PRENOM'] = self.outbreak_mado_df['PRENOM'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace('-','').str.replace(' ','').str.strip(' ').str.upper()

    def GetDf(self):
        return(self.outbreak_mado_df)


def Main():
    logging.info("Begin select")

    outbreak_mado_obj = OutbreakMado()

    db_obj = CovBankDB(outbreak_mado_obj)
    db_obj.SelectOutbreakMado()
    db_obj.SaveOutbreakMadoWithReq()

if __name__ == '__main__':
    Main()
