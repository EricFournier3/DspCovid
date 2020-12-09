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
_debug = True

global basedir_in
basedir_in =  "/data/Databases/CovBanQ_Epi/FOR_EXTRACTION/"

global basedir_out
basedir_out =  "/data/Databases/CovBanQ_Epi/SCRIPT_OUT/"


class CovBankDB:
    def __init__(self,reinfection_obj):
        self.yaml_conn_param = open('CovBankParam.yaml')
        self.ReadConnParam()
        self.connection = self.SetConnection()
        self.reinfection_obj = reinfection_obj

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

    def SelectReinfectionPrelev(self):
        self.reinfection_prelev_df = pd.DataFrame(columns= ['PRENOM','NOM','DTNAISS','RTA','NAM'])

        reinfection_df = self.reinfection_obj.GetReinfectionDf()

        for index, row in reinfection_df.loc[:,].iterrows():
            #print(row)
            nam = row['namLabo']
            nom = row['nomLabo']
            prenom = row['prenomLabo']
            dt_naiss = row['dtNaiss_calc_p']
            rta = row['codePostalLabo_calc_p']

            sql = ""

            #print('NAM: ',nam, 'nom: ',nom,' prenom: ',prenom,' dt_naiss: ',dt_naiss, ' RTA: ',rta)
            if len(str(nam)) > 8:
                sql = "SELECT pa.ID_PATIENT,pa.PRENOM,pa.NOM,pa.DTNAISS,pa.RTA,pa.NAM from Patients pa WHERE pa.NAM = '{0}' ".format(nam)
            else:
                sql = "SELECT ID_PATIENT,PRENOM,NOM,DTNAISS,RTA,NAM from Patients WHERE NOM = '{0}' and PRENOM = '{1}' and DTNAISS = '{2}' and RTA = '{3}' ".format(nom,prenom,dt_naiss,rta)  # PRENOM,NOM,DTNAISS,RTA,
            print(sql)

            df = pd.read_sql(sql,con=self.GetConnection())
            #print(df)
            self.reinfection_prelev_df =  pd.concat([self.reinfection_prelev_df,df])

        #print(self.reinfection_prelev_df)

class Reinfection:
    def __init__(self):
        if _debug:
            self.in_file = os.path.join(basedir_in,"BD_phylogenie_20201208_small.xlsx")
        else:
            self.in_file = os.path.join(basedir_in,"BD_phylogenie_20201208.xlsx")

        #self.SetPhyloDf()
        self.SetReinfectionDf()
        #self.MergeReinfectionToPhylo()

    def SetPhyloDf(self):
        self.phylo_df = pd.read_excel(self.in_file,sheet_name='phylogenie')

    def SetReinfectionDf(self):
        self.reinfection_df = pd.read_excel(self.in_file,sheet_name='reinfection_60_90j')
        #print(self.reinfection_df.columns)
        self.reinfection_df = self.reinfection_df[['namLabo','nomLabo','prenomLabo','dtNaiss_calc_p','codePostalLabo_calc_p']]
        self.reinfection_df['codePostalLabo_calc_p'] = self.reinfection_df['codePostalLabo_calc_p'].str.slice(0,3).str.upper()

        self.reinfection_df['nomLabo'] = self.reinfection_df['nomLabo'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace('-','').str.replace(' ','')
        self.reinfection_df['prenomLabo'] = self.reinfection_df['prenomLabo'].str.replace('é','e').str.replace('è','e').str.replace('ç','c').str.replace('-','').str.replace(' ','')

        self.reinfection_df['nomLabo'] = self.reinfection_df['nomLabo'].str.strip(' ').str.upper()
        self.reinfection_df['prenomLabo'] = self.reinfection_df['prenomLabo'].str.strip(' ').str.upper()


        #print(self.reinfection_df)

    def MergeReinfectionToPhylo(self):
        self.merged_reinfection_phylo = pd.merge(self.reinfection_df,self.phylo_df,on='idPersonneUniq')

    def GetReinfectionDf(self):
        return(self.reinfection_df)


def Main():
    logging.info("Begin update")

    reinfection_obj = Reinfection()
    
    db_obj = CovBankDB(reinfection_obj)
    db_obj.SelectReinfectionPrelev()

if __name__ == '__main__':
    Main()
