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


_debug_ = False

pd.options.display.max_columns = 100
logging.basicConfig(level=logging.DEBUG)


class DSPdata:
    def __init__(self):
        pass

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
        self.SetFilePath()

        self.dsp_data_file = None
        self.envois_genome_quebec_file = None

    def SetFilePath(self):
        if self._debug:
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2.xlsm')
            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small.xlsx')
        else:
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie.xlsm')
            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec.xlsx')


def Main():
    logging.info("In Main()")
    excel_manager = ExcelManager(_debug_)
    db_covid19 = MySQLcovid19() 


if __name__ == '__main__':
    Main()
