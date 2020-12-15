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
            print("ROW ",row)
            self.nb_select += 1
            sys.stdout.write("select >>> %d\r"%self.nb_select)
            sys.stdout.flush()

            sample = row['sample']
            if re.search('^HGA-',sample):
                sample_ = re.sub('2D$','',sample)

            sql = "SELECT DEB_VOY1,FIN_VOY1,DEST_VOY1 FROM Prelevements"
            



    def SaveHospitalSamplesPrelev(self):
        pass

class HospitalSamples():

    def __init__(self):
        self.in_file = os.path.join(basedir_in,"metadata_from_data_20201030_PASS_FLAG_minmaxSampleDate_2020-02-01_2020-06-01_QuebecOnly.tsv")
        self.outfile = os.path.join(basedir_out,"HospitalSamplesTravelHistory.tsv")
        self.outfile = os.path.join(basedir_out,"HospitalSamplesNotFound.tsv")
        self.SetHospitalSamplesDf()

    def SetHospitalSamplesDf(self):
        self.hospital_samples_df = pd.read_csv(self.in_file,sep="\t",index_col=False,usecols=['sample'])
        self.hospital_samples_df = self.hospital_samples_df.loc[~self.hospital_samples_df['sample'].str.contains('^L00',regex=True),['sample']]
        print(self.hospital_samples_df)


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
