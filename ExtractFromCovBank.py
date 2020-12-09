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
        self.nb_select = 0
        self.reinfection_prelev_df = pd.DataFrame()
        self.reinfection_prelev_df_notfound = pd.DataFrame(columns=['NOM','PRENOM','NAM','DT_NAISS','RTA'])

        reinfection_df = self.reinfection_obj.GetReinfectionDf()

        for index, row in reinfection_df.loc[:,].iterrows():
            #print(row)
            sys.stdout.write("select >>> %d\r"%self.nb_select)
            sys.stdout.flush()

            nam = row['namLabo']
            nom = row['nomLabo']
            prenom = row['prenomLabo']
            dt_naiss = row['dtNaiss_calc_p']
            rta = row['codePostalLabo_calc_p']

            sql = ""

            #print('NAM: ',nam, 'nom: ',nom,' prenom: ',prenom,' dt_naiss: ',dt_naiss, ' RTA: ',rta)
            if len(str(nam)) > 8:
                sql = "SELECT pa.ID_PATIENT,pa.PRENOM,pa.NOM,pa.DTNAISS,pa.RTA,pa.NAM, pr.DATE_ENVOI_GENOME_QUEBEC, pr.GENOME_QUEBEC_REQUETE, pr.CT, pr.DATE_PRELEV from Patients pa inner join Prelevements pr on pa.ID_PATIENT = pr.ID_PATIENT WHERE pa.NAM = '{0}' ".format(nam)
            else:
                sql = "SELECT pa.ID_PATIENT,pa.PRENOM,pa.NOM,pa.DTNAISS,pa.RTA,pa.NAM,pr.DATE_ENVOI_GENOME_QUEBEC, pr.GENOME_QUEBEC_REQUETE, pr.CT, pr.DATE_PRELEV from Patients pa inner join Prelevements pr on pa.ID_PATIENT = pr.ID_PATIENT   WHERE pa.NOM = '{0}' and pa.PRENOM = '{1}' and pa.DTNAISS = '{2}' and pa.RTA = '{3}' ".format(nom,prenom,dt_naiss,rta)
            #print(sql)

            #DATE_ENVOI_GENOME_QUEBEC GENOME_QUEBEC_REQUETE CT DATE_PRELEV

            df = pd.read_sql(sql,con=self.GetConnection())
            nb_found = df.shape[0]
            if str(nb_found) == '0':
                self.reinfection_prelev_df_notfound = pd.concat([self.reinfection_prelev_df_notfound,pd.DataFrame({'NOM':[nom],'PRENOM':[prenom],'NAM':[nam],'DT_NAISS':[dt_naiss]})])
                pass
            #print(df)
            self.reinfection_prelev_df =  pd.concat([self.reinfection_prelev_df,df])

        #print(self.reinfection_prelev_df)

    def SaveReinfectionPrelev(self):
        self.reinfection_obj.SaveReinfectionPrelev(self.reinfection_prelev_df,self.reinfection_prelev_df_notfound)

class Reinfection:
    def __init__(self):
        if _debug:
            self.in_file = os.path.join(basedir_in,"BD_phylogenie_20201208_small.xlsx")
        else:
            self.in_file = os.path.join(basedir_in,"BD_phylogenie_20201208.xlsx")

        self.outfile = os.path.join(basedir_out,"ReinfectionPrelev.xlsx")
        self.outfile_notfound = os.path.join(basedir_out,"ReinfectionPrelevNotFound.xlsx")

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

    def SaveReinfectionPrelev(self,df_found,df_notfound):
        df_found.to_excel(self.outfile,sheet_name='Sheet1')
        df_notfound.to_excel(self.outfile_notfound,sheet_name='Sheet1')

    def MergeReinfectionToPhylo(self):
        self.merged_reinfection_phylo = pd.merge(self.reinfection_df,self.phylo_df,on='idPersonneUniq')

    def GetReinfectionDf(self):
        return(self.reinfection_df)


def Main():
    logging.info("Begin select")

    reinfection_obj = Reinfection()
    
    db_obj = CovBankDB(reinfection_obj)
    db_obj.SelectReinfectionPrelev()
    db_obj.SaveReinfectionPrelev()

if __name__ == '__main__':
    Main()
