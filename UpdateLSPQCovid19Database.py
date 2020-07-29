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
        

    def RenameColumns(self):
        pass
        self.pd_df = self.pd_df.rename(columns=self.renamed_columns_dict)

    def GetPandaDataFrame(self):
        return self.pd_df

class EnvoisGenomeQuebec:
    def __init__(self):
        pass


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

class ExcelManager:
    def __init__(self,_debug):
        self._debug = _debug
        self.basedir = "/data/Databases/COVID19_DSP/"
        self.basedir_dsp_data = os.path.join(self.basedir,"BD_PHYLOGENIE")
        self.basedir_envois_genome_quebec = os.path.join(self.basedir,"LISTE_ENVOIS_GENOME_QUEBEC")
        self.dsp_data_file = None
        self.envois_genome_quebec_file = None

        self.SetFilePath()



    def SetFilePath(self):
        if self._debug:
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2.xlsm')
            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small.xlsx')
        else:
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie.xlsm')
            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec.xlsx')

    def ReadDspDataFile(self):
        return pd.read_excel(self.dsp_data_file,sheet_name='BD_Phylogenie')

def Main():
    logging.info("In Main()")
    excel_manager = ExcelManager(_debug_)
    db_covid19 = MySQLcovid19() 
    dsp_data = DSPdata(excel_manager)
    print(dsp_data.GetPandaDataFrame())


if __name__ == '__main__':
    Main()
