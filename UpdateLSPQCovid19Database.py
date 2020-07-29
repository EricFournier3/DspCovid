# -*- coding: utf-8 -*-

"""
Eric Fournier 2020-07-29


"""
#TODO
"""
HPLG pas de date de naissance => utiliser le NAM

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


_debug_ = True

pd.options.display.max_columns = 100
logging.basicConfig(level=logging.DEBUG)


class DSPdata:
    def __init__(self,excel_manager):
        self.excel_manager = excel_manager

        self.renamed_columns_dict = {'laboratoire':'CODE_HOPITAL_DSP','nomInfo':'NOMINFO','prenomInfo':'PRENOMINFO','dtNaissInfo':'DTNAISSINFO','Date_prelev_1':'DATE_PRELEV_1','statut':'STATUT','RSS_LSPQ_cas':'RSS_LSPQ_CAS','Date_conf_LSPQ_1':'DATE_CONF_LSPQ_1',
'noBenef':'NOBENEF','NAM__LSPQ_':'NAM_LSPQ','sexeInfo':'SEXEINFO','Date_conf_LSPQ_2':'DATE_CONF_LSPQ_2','Date_prelev_2':'DATE_PRELEV_2',
'ID_Phylo':'ID_PHYLO'}

        self.SetPandaDataFrame()

    def SetPandaDataFrame(self):
        self.pd_df = self.excel_manager.ReadDspDataFile()
        self.RenameColumns()
        self.ToUpperCase()
        self.FillNA()
        self.CreateIdPatient()
        self.ToDateTime()

    def ToDateTime(self):
        self.pd_df['DATE_PRELEV_1'] = pd.to_datetime(self.pd_df['DATE_PRELEV_1'])
        self.pd_df['DATE_PRELEV_2'] = pd.to_datetime(self.pd_df['DATE_PRELEV_2'])
        self.pd_df['DTNAISSINFO'] = pd.to_datetime(self.pd_df['DTNAISSINFO'])

    def CreateIdPatient(self):
        self.pd_df['ID_PATIENT'] = self.pd_df['NOBENEF'].apply(str) + "-" + self.pd_df['NAM_LSPQ']
        

    def FillNA(self):
        self.pd_df = pd.concat([self.pd_df[['NOBENEF','NAM_LSPQ']].fillna(''),self.pd_df[['SEXEINFO']].fillna('missing'),self.pd_df[['CODE_HOPITAL_DSP','NOMINFO','PRENOMINFO','DTNAISSINFO','DATE_PRELEV_1','DATE_CONF_LSPQ_1','DATE_CONF_LSPQ_2','DATE_PRELEV_2','ID_PHYLO']],self.pd_df[['STATUT']].fillna('missing'),self.pd_df[['RSS_LSPQ_CAS']].fillna('missing')],axis=1)

    def ToUpperCase(self):
        self.pd_df['NOMINFO'] = self.pd_df['NOMINFO'].str.upper()
        self.pd_df['PRENOMINFO'] = self.pd_df['PRENOMINFO'].str.upper()
        
    def PrintColumns(self):
        print("******************* DSPdata columns ********************")
        for col in self.pd_df.columns:
            print("* ",col)
        print("********************************************************")

    def RenameColumns(self):
        pass
        self.pd_df = self.pd_df.rename(columns=self.renamed_columns_dict)

    def GetPandaDataFrame(self):
        return self.pd_df

class DSPtoLSPQmatcher:
    def __init__(self,excel_manager):
        self.excel_manager = excel_manager
        self.renames_columns_dict = {'ETABLISSEMENTS':'NOM_HOPITAL','PrefixLSPQ':'CODE_HOPITAL_LSPQ',
'PrefixDSP':'CODE_HOPITAL_DSP'}

        self.SetPandaDataFrame()

    def SetPandaDataFrame(self):
        self.pd_df = self.excel_manager.ReadChDSP2LSPQ()

class EnvoisGenomeQuebec:
    def __init__(self,excel_manager):
        self.excel_manager = excel_manager
        self.renamed_columns_dict = {'# Requête':'GENOME_QUEBEC_REQUETE','Nom':'NOMINFO','Prénom':'PRENOMINFO',
'Date de naissance':'DTNAISSINFO','Date de prélèvement':'DATE_PRELEV','# Boîte':'NO_BOITE','DateEnvoiGenomeQuebec':'DATE_ENVOI_GENOME_QUEBEC'}

        self.SetPandaDataFrame()

        self.final_pd_df = pd.DataFrame(columns=self.pd_df.columns)

        self.RemoveWrongDateFormatRecord()

    def SetPandaDataFrame(self):
        self.pd_df = self.excel_manager.ReadEnvoisGenomeQuebecFile()
        self.RenameColumns()
        self.ToUpperCase()
        self.CreateCodeChLspq()

    def RemoveWrongDateFormatRecord(self):
        self.pd_df_with_wrong_date_format = pd.DataFrame(columns=self.pd_df.columns)        

        index_good = 0
        index_bad = 0

        for index, row in self.pd_df.loc[:,].iterrows():
            if((Utils.CheckDateFormat(str(type(row['DTNAISSINFO'])))) and (Utils.CheckDateFormat(str(type(row['DATE_PRELEV'])))) and (Utils.CheckDateFormat(str(type(row['DATE_ENVOI_GENOME_QUEBEC']))))):
                
                self.final_pd_df.loc[index_good] = row
                index_good += 1
            else:
                self.pd_df_with_wrong_date_format.loc[index_bad] = row
                index_bad += 1 

        self.excel_manager.WriteToExcel(self.pd_df_with_wrong_date_format,"EnvoisWithBadDate.xlsx")

        del self.pd_df
        gc.collect()

    def CreateCodeChLspq(self):
        self.pd_df['CODE_HOPITAL_LSPQ'] = self.pd_df['GENOME_QUEBEC_REQUETE'].str.extract(r'(\S+-)\S+')

    def ToUpperCase(self):
        self.pd_df['NOMINFO'] = self.pd_df['NOMINFO'].str.upper() 
        self.pd_df['PRENOMINFO'] = self.pd_df['PRENOMINFO'].str.upper() 

    def RenameColumns(self):
        self.pd_df = self.pd_df.rename(columns=self.renamed_columns_dict)

    def GetPandaDataFrame(self):
        return self.final_pd_df

    def PrintColumns(self):
        print("******************* DSPdata EnvoisGenomeQuebec ********************")
        for col in self.final_pd_df.columns:
            print("* ",col)
        print("*******************************************************************")

class MySQLcovid19:
    def __init__(self):
        self.host = 'localhost'
        self.user = 'root'
        self.password = 'lspq2019'
        self.database = 'TestCovid19v3'
        self.connection = self.SetConnection()

    def SetConnection(self):
        return mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database)

    def GetConnection(self):
        return self.connection

    def GetCursor(self):
        return self.GetConnection().cursor()
        

class MySQLcovid19Updator:
    def __init__(self):
        pass

class MySQLcovid19Selector:
    def __init__(self):
        pass

class Utils:
    def __init__(self):
        pass

    @staticmethod
    def CheckDateFormat(value):

        ok = True

        if ((value == "<class 'datetime.datetime'>") or (value == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>")):
            pass
        else:
            ok = False

        return ok 

class ExcelManager:
    def __init__(self,_debug):
        self._debug = _debug
        self.basedir = "/data/Databases/COVID19_DSP/"
        self.basedir_dsp_data = os.path.join(self.basedir,"BD_PHYLOGENIE")
        self.basedir_envois_genome_quebec = os.path.join(self.basedir,"LISTE_ENVOIS_GENOME_QUEBEC")
        self.basedir_script_out = os.path.join(self.basedir,"SCRIPT_OUT")
        self.dsp_data_file = None
        self.envois_genome_quebec_file = None
        self.ch_dsp2lspq_file = None
        self.SetFilePath()

    def SetFilePath(self):
        
        self.ch_dsp2lspq_file = os.path.join(self.basedir_envois_genome_quebec,'PREFIX_CH_LSPQvsDSP.xlsx')

        if self._debug:
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2.xlsm')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small.xlsx')
            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small_withbaddate.xlsx')
        else:
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie.xlsm')
            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec.xlsx')

    def ReadDspDataFile(self):
        return pd.read_excel(self.dsp_data_file,sheet_name='BD_Phylogenie')

    def ReadEnvoisGenomeQuebecFile(self):
        return pd.read_excel(self.envois_genome_quebec_file,sheet_name='Feuil1')

    def ReadChDSP2LSPQ(self):
        return pd.read_excel(self.ch_dsp2lspq_file,sheet_name='Feuil1')

    def WriteToExcel(self,df,file_name):
        df.to_excel(os.path.join(self.basedir_script_out,file_name),sheet_name='Sheet1')
       
 
def Main():
    def Inspect():
        pass
        #print(dsp_data.GetPandaDataFrame())
        #print(envois_genome_quebec_data.GetPandaDataFrame())
        dsp_data.PrintColumns()
        envois_genome_quebec_data.PrintColumns()

    logging.info("In Main()")
    excel_manager = ExcelManager(_debug_)
    db_covid19 = MySQLcovid19()
    dsp_2_lspq_matcher =  DSPtoLSPQmatcher(excel_manager) 
    dsp_data = DSPdata(excel_manager)
    envois_genome_quebec_data = EnvoisGenomeQuebec(excel_manager)
    Inspect()


if __name__ == '__main__':
    Main()
