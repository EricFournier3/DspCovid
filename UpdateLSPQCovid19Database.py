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

class CH_DSPtoLSPQ:
    def __init__(self,excel_manager):
        self.excel_manager = excel_manager
        self.renames_columns_dict = {'ETABLISSEMENTS':'NOM_HOPITAL','PrefixLSPQ':'CODE_HOPITAL_LSPQ',
'PrefixDSP':'CODE_HOPITAL_DSP'}

        self.SetPandaDataFrame()

    def SetPandaDataFrame(self):
        self.pd_df = self.excel_manager.ReadChDSP2LSPQ()

    def GetPandaDataFrame(self):
        return self.pd_df

class EnvoisGenomeQuebec:
    def __init__(self,excel_manager):
        self.excel_manager = excel_manager
        self.renamed_columns_dict = {'# Requête':'GENOME_QUEBEC_REQUETE','Nom':'NOMINFO','Prénom':'PRENOMINFO',
'Date de naissance':'DTNAISSINFO','Date de prélèvement':'DATE_PRELEV','# Boîte':'NO_BOITE','DateEnvoiGenomeQuebec':'DATE_ENVOI_GENOME_QUEBEC'}

        self.SetPandaDataFrame()

        self.final_pd_df = pd.DataFrame(columns=self.pd_df.columns)

        self.RemoveWrongDateFormatRecord()
        self.SortByDatePrelev()

    def SortByDatePrelev(self):
        self.final_pd_df = self.final_pd_df.sort_values(by=['DATE_PRELEV'],ascending=True)

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
        print("******************* EnvoisGenomeQuebec columns ********************")
        for col in self.final_pd_df.columns:
            print("* ",col)
        print("*******************************************************************")

    def GetColumns(self):
        return ','.join(self.final_pd_df.columns.values)

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
    def __init__(self,dsp_dat,envois_gq,ch_dsp2lspq_matcher,envois_genome_quebec_2_dsp_dat_matcher,db_covid19):
        self.dsp_dat = dsp_dat
        self.envois_gq = envois_gq
        self.ch_dsp2lspq_matcher = ch_dsp2lspq_matcher
        self.envois_genome_quebec_2_dsp_dat_matcher = envois_genome_quebec_2_dsp_dat_matcher
        self.db = db_covid19

        self.patients_columns_as_string = self.GetPatientsColumnsAsString()
        
    def GetChDspCode(self,lspq_ch_code):
        return self.ch_dsp2lspq_matcher.GetChDspCode(lspq_ch_code)

    def GetDSPmatch(self,nom,prenom,dt_naiss,dt_prelev,ch_dsp_code): 
        return self.envois_genome_quebec_2_dsp_dat_matcher.GetDSPmatch(nom,prenom,dt_naiss,dt_prelev,ch_dsp_code)

    def GetPatientsColumnsAsString(self):
        columns_list = MySQLcovid19Selector.GetPatientsColumn(self.db.GetCursor())
        patients_columns_as_string = ','.join(columns_list)
        return patients_columns_as_string

    def InsertPatient(self,val_to_insert):
        exist = MySQLcovid19Selector.CheckIfPatientExist(self.db.GetCursor(),val_to_insert['id_patient'])
        
        if not exist:
            try:
                pass
                ncols = self.patients_columns_as_string.count(",") + 1
                sql_insert = "INSERT INTO Patients ({0}) values ({1})".format(self.patients_columns_as_string,str("%s,"*ncols)[:-1])
                #print(sql_insert)

            except mysql.connector.Error as err:
                pass

    def InsertPrelevement(self):
        pass

    def GetValuesToInsert(self,dsp_dat_match_rec,current_envoi,ch_name):
        val = {"id_patient": dsp_dat_match_rec['ID_PATIENT'], "prenom": dsp_dat_match_rec['PRENOMINFO'],"nom":dsp_dat_match_rec['NOMINFO'],
        "sex":dsp_dat_match_rec['SEXEINFO'],"dt_naiss":dsp_dat_match_rec['DTNAISSINFO'],"statut":dsp_dat_match_rec['STATUT'],"rss":dsp_dat_match_rec['RSS_LSPQ_CAS'],"code_ch_dsp":dsp_dat_match_rec['CODE_HOPITAL_DSP'],"code_ch_lspq":current_envoi['CODE_HOPITAL_LSPQ'],"ch_name":ch_name,"dt_prelev_1":dsp_dat_match_rec['DATE_PRELEV_1'],"dt_conf_lspq_1":dsp_dat_match_rec['DATE_CONF_LSPQ_1'],"dt_prelev_2":dsp_dat_match_rec['DATE_PRELEV_2'],"dt_conf_lspq_2":dsp_dat_match_rec['DATE_CONF_LSPQ_2'],"date_prelev_in_envois_genomequebec":current_envoi['DATE_PRELEV'],"genomequebec_request":current_envoi['GENOME_QUEBEC_REQUETE'],"dt_envoi_genomequebec":current_envoi['DATE_ENVOI_GENOME_QUEBEC'],"id_phylo":dsp_dat_match_rec['ID_PHYLO']} 

        return(val)


    def Insert(self):
        for index,row in self.envois_gq.GetPandaDataFrame().loc[:,].iterrows():
            ch_dsp2lspq_val = self.ch_dsp2lspq_matcher.GetChDspCode(row['CODE_HOPITAL_LSPQ']) 
            ch_dsp2lspq_val = self.GetChDspCode(row['CODE_HOPITAL_LSPQ'])
            ch_dsp_code = ch_dsp2lspq_val[0]
            ch_name = ch_dsp2lspq_val[1]

            dsp_dat_match_rec =  self.GetDSPmatch(row['NOMINFO'],row['PRENOMINFO'],row['DTNAISSINFO'],row['DATE_PRELEV'],ch_dsp_code)

            if dsp_dat_match_rec.shape[0] != 0:
                val_to_insert = self.GetValuesToInsert(dsp_dat_match_rec,row,ch_name)
                self.InsertPatient(val_to_insert)

class MySQLcovid19Selector:
    def __init__(self):
        pass

    @staticmethod
    def CheckIfPatientExist(cursor,id_patient):
        cursor.execute("select count(*) from Patients where ID_PATIENT = '{0}'".format(id_patient))
        nb_match = cursor.fetchone()[0]
        if nb_match == 0:
            return False
        else:
            return True
        
    @staticmethod
    def GetPatientsColumn(cursor):
        cursor.execute("select COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Patients'")
        res = cursor.fetchall()
        columns = []
        for x in res:
            columns.append(x[0])
        return columns

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

    @staticmethod
    def Inspect(dsp_data,envois_genome_quebec_data):
        dsp_data.PrintColumns()
        envois_genome_quebec_data.PrintColumns()


class EnvoisGenomeQuebec_2_DSPdata_Matcher():
    def __init__(self,dsp_dat):
        self.dsp_dat = dsp_dat

    def ComputeDatePrelevDiff(self,match_df,dt_prelev):
        match_df['DATE_DIFF_1'] = match_df['DATE_PRELEV_1'] - dt_prelev
        match_df['DATE_DIFF_1'] = match_df['DATE_DIFF_1'].abs()
        match_df['DATE_DIFF_2'] = match_df['DATE_PRELEV_2'] - dt_prelev
        match_df['DATE_DIFF_2'] = match_df['DATE_DIFF_2'].abs()

        try:
            match_df['MIN_DELTA'] = match_df[['DATE_DIFF_1','DATE_DIFF_2']].min(axis=1)
        except:
            pass
        
    def GetDSPmatch(self,nom,prenom,dt_naiss,dt_prelev,dsp_ch_code):
        
        nom = nom.strip(' ')
        prenom = prenom.strip(' ')
        dsp_ch_code = dsp_ch_code.strip(' ')

        try:
            dsp_dat_df = self.dsp_dat.GetPandaDataFrame()
            match_df = dsp_dat_df.loc[(dsp_dat_df['NOMINFO'].str.strip(' ')==nom) & (dsp_dat_df['PRENOMINFO'].str.strip(' ')==prenom) & 
            (dsp_dat_df['DTNAISSINFO']==dt_naiss) & (dsp_dat_df['CODE_HOPITAL_DSP'].str.strip()==dsp_ch_code),:].copy()

            if match_df.shape[0] == 0:
                return match_df

            self.ComputeDatePrelevDiff(match_df,dt_prelev)

            return(match_df[match_df.MIN_DELTA == match_df.MIN_DELTA.min()])

        except AttributeError as e:
            print(e)
            return pd.DataFrame(columns=dsp_dat_df.columns)


class CH_DSP_2_LSPQ_Matcher():
    def __init__(self,ch_dsp_2_lspq):
        self.ch_dsp_2_lspq = ch_dsp_2_lspq

    def GetChDspCode(self,lspq_ch_code):
        df = self.ch_dsp_2_lspq.GetPandaDataFrame()
        val = df.loc[df['PrefixLSPQ'] == lspq_ch_code,['PrefixDSP','ETABLISSEMENTS']].values[0]
        dsp_ch_code = val[0]
        ch_name = val[1]
        return [dsp_ch_code,ch_name]

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

    logging.info("In Main()")
    excel_manager = ExcelManager(_debug_)
    db_covid19 = MySQLcovid19()
    ch_dsp_2_lspq =  CH_DSPtoLSPQ(excel_manager) 
    dsp_data = DSPdata(excel_manager)
    envois_genome_quebec_data = EnvoisGenomeQuebec(excel_manager)
        
    ch_dsp2lspq_matcher = CH_DSP_2_LSPQ_Matcher(ch_dsp_2_lspq)
    envois_genome_quebec_2_dsp_dat_matcher = EnvoisGenomeQuebec_2_DSPdata_Matcher(dsp_data)

    sql_updator = MySQLcovid19Updator(dsp_data,envois_genome_quebec_data,ch_dsp2lspq_matcher,envois_genome_quebec_2_dsp_dat_matcher,db_covid19)
    sql_updator.Insert()

    #Utils.Inspect(dsp_data,envois_genome_quebec_data)

if __name__ == '__main__':
    Main()
