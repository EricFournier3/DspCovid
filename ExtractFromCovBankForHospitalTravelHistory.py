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
import yaml
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
    def __init__(self,hospital_samples_obj):
        self.yaml_conn_param = open('CovBankParam.yaml')
        self.ReadConnParam()
        self.connection = self.SetConnection()
        self.hospital_samples_obj = hospital_samples_obj

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

    def SelectHospitalSamplesPrelev(self):
        self.nb_select = 0
        self.hospital_samples_prelev_df = pd.DataFrame()
        self.hospital_samples_prelev_df_notfound = pd.DataFrame(columns=['sample'])

        hospital_samples_df = self.hospital_samples_obj.GetHospitalSamplesDf()

        #print(hospital_samples_df)

        for index,row in hospital_samples_df.loc[:,].iterrows():
            #print("ROW ",row)
            self.nb_select += 1
            sys.stdout.write("select >>> %d\r"%self.nb_select)
            sys.stdout.flush()

            sql = ""
            req_select = "UPPER(GENOME_QUEBEC_REQUETE)"          
            sample = row['sample']

            sample_ = sample

            if re.search('^HGA-',sample):
                sample_ = re.sub('2D$','',sample)
                req_select = "CONCAT(UPPER(GENOME_QUEBEC_REQUETE),\"2D\")" 

            sql1 = "SELECT {0} as SAMPLE,DEB_VOY1,FIN_VOY1,DEST_VOY1,DEB_VOY2,FIN_VOY2,DEST_VOY2,DEB_VOY3,FIN_VOY3,DEST_VOY3,DEB_VOY4,FIN_VOY4,DEST_VOY4,DEB_VOY5,FIN_VOY5,DEST_VOY5,DEB_VOY6,FIN_VOY6,DEST_VOY6,DEB_VOY7,FIN_VOY7,DEST_VOY7,DEB_VOY8,FIN_VOY8,DEST_VOY8,DEB_VOY9,FIN_VOY9,DEST_VOY9,DEB_VOY10,FIN_VOY10,DEST_VOY10,DEB_VOY11,FIN_VOY11,DEST_VOY11,DEB_VOY12,FIN_VOY12,DEST_VOY12,DEB_VOY13,FIN_VOY13,DEST_VOY13 FROM Prelevements where UPPER(GENOME_QUEBEC_REQUETE) = UPPER('{1}') and DEST_VOY1 <> 'nan'".format(req_select,sample_)
            sql2 = "SELECT {0} as SAMPLE,DEB_VOY1,FIN_VOY1,DEST_VOY1 FROM Prelevements where UPPER(GENOME_QUEBEC_REQUETE) = UPPER('{1}')".format(req_select,sample_)

            df1 = pd.read_sql(sql1,con=self.GetConnection())
            df2 = pd.read_sql(sql2,con=self.GetConnection())
            
            nb_found1 = df1.shape[0]
            nb_found2 = df2.shape[0]

            if str(nb_found2) == '0':
                self.hospital_samples_prelev_df_notfound = pd.concat([self.hospital_samples_prelev_df_notfound,pd.DataFrame({'sample':[sample]})])

            self.hospital_samples_prelev_df = pd.concat([self.hospital_samples_prelev_df,df1])
            
    def SaveHospitalSamplesPrelev(self):
        self.hospital_samples_obj.SaveHospitalSamplesPrelev(self.hospital_samples_prelev_df,self.hospital_samples_prelev_df_notfound)

class HospitalSamples():

    def __init__(self):
        self.in_file = os.path.join(basedir_in,"metadata_from_data_20201030_PASS_FLAG_minmaxSampleDate_2020-02-01_2020-06-01_QuebecOnly.tsv")
        self.outfile = os.path.join(basedir_out,"HospitalSamplesTravelHistory.tsv")
        self.outfile_notfound = os.path.join(basedir_out,"HospitalSamplesNotFound.tsv")
        self.SetHospitalSamplesDf()

    def SetHospitalSamplesDf(self):
        self.hospital_samples_df = pd.read_csv(self.in_file,sep="\t",index_col=False,usecols=['sample'])
        self.hospital_samples_df = self.hospital_samples_df.loc[~self.hospital_samples_df['sample'].str.contains('^L00',regex=True),['sample']]
        #print(self.hospital_samples_df)

    def SaveHospitalSamplesPrelev(self,df_found,df_not_found):
        df_found.to_csv(self.outfile,sep="\t",index=False)
        df_not_found.to_csv(self.outfile_notfound,sep="\t",index=False)

    def GetHospitalSamplesDf(self):
        return(self.hospital_samples_df)
        

def Main():
    logging.info("Begin select")

    hospital_samples_obj = HospitalSamples()
    db_obj = CovBankDB(hospital_samples_obj)
    db_obj.SelectHospitalSamplesPrelev()
    db_obj.SaveHospitalSamplesPrelev()

if __name__ == '__main__':
    Main()
