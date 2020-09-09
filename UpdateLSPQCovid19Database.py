# -*- coding: utf-8 -*-

"""
Eric Fournier 2020-07-29


"""

#TODO
"""
Update statut dans table Patients
Ajout des metrics de sequences

filter date de prelevement 1900 exemple HSCM-P8120611  dans table prelevement
Integer Key pour ID_PATIENT
faire recherche de Patient de BDphylo vers ListeEnvois au lieu de l inverse
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

_use_ch_mapping = False
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
        #https://stackoverflow.com/questions/39604094/pandas-delete-all-rows-that-are-not-a-datetime-type
        self.pd_df['DATE_PRELEV_1'] = pd.to_datetime(self.pd_df['DATE_PRELEV_1'],errors='coerce')
        self.pd_df['DATE_PRELEV_2'] = pd.to_datetime(self.pd_df['DATE_PRELEV_2'],errors='coerce')
        self.pd_df['DTNAISSINFO'] = pd.to_datetime(self.pd_df['DTNAISSINFO'],errors='coerce')
        self.pd_df = self.pd_df.dropna(subset= ['DATE_PRELEV_1','DATE_PRELEV_2','DTNAISSINFO'])

        print("Nb lines in DSP  dat ",self.pd_df.shape[0])

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
    
    @staticmethod
    def Get_CH_Sorel_Info():
        code = 'HCLM'
        name = 'Hôtel-Dieu de Sorel'
        adresse = "400, avenue de l\'Hôtel-Dieu , Sorel-Tracy, QC, Canada"
        return [code,name,adresse]


class EnvoisGenomeQuebec:
    def __init__(self,excel_manager):
        self.excel_manager = excel_manager
        self.renamed_columns_dict = {'# Requête':'GENOME_QUEBEC_REQUETE','Nom':'NOMINFO','Prénom':'PRENOMINFO',
'Date de naissance':'DTNAISSINFO','Date de prélèvement':'DATE_PRELEV','# Boîte':'NO_BOITE','DateEnvoiGenomeQuebec':'DATE_ENVOI_GENOME_QUEBEC','NAM':'NAM'}
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
        logging.info("Remove wrong date format form Envois Genome Quebec")
        self.pd_df_with_wrong_date_format = pd.DataFrame(columns=self.pd_df.columns)        

        index_good = 0
        index_bad = 0
        tested_row = 0
        for index, row in self.pd_df.loc[:,].iterrows():
            nam = row['NAM']

            tested_row += 1
            sys.stdout.write("Test row >>> %d\r"%tested_row)
            sys.stdout.flush()

            if((Utils.CheckDateFormat(str(type(row['DTNAISSINFO'])))) and (Utils.CheckDateFormat(str(type(row['DATE_PRELEV'])))) and (Utils.CheckDateFormat(str(type(row['DATE_ENVOI_GENOME_QUEBEC']))))):
                
                self.final_pd_df.loc[index_good] = row
                index_good += 1

            elif isinstance(nam,str) or isinstance(row['DATE_PRELEV'],str):

                if isinstance(nam,str):
                    dt_naiss = Utils.GetDateNaissFromNAM(nam)

                    if not dt_naiss:
                        self.pd_df_with_wrong_date_format.loc[index_bad] = row
                        index_bad += 1 
                        continue

                    row['DTNAISSINFO'] = dt_naiss 

                if isinstance(row['DATE_PRELEV'],str):
                    str_dt_prelev = row['DATE_PRELEV']
                    dt_prelev = Utils.GetDateFromStrDate(str_dt_prelev)
                    if not dt_prelev:
                        self.pd_df_with_wrong_date_format.loc[index_bad] = row
                        index_bad += 1 
                        continue

                    row['DATE_PRELEV'] = dt_prelev

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
        self.pd_df['CODE_HOPITAL_LSPQ_PLUS'] = self.pd_df['GENOME_QUEBEC_REQUETE'].str.extract(r'(\S+-\S)\S+')
        self.pd_df['CODE_HOPITAL_LSPQ_IS_CH_SOREL'] = np.where(self.pd_df.CODE_HOPITAL_LSPQ_PLUS == 'HDS-S',True,False)

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

class SGILdata:
    def __init__(self,excel_manager):
        self.excel_manager = excel_manager
        self.SetPandaDataFrame()

        self.final_pd_df = pd.DataFrame(columns=self.pd_df.columns)
        self.pd_df_with_wrong_date_format = pd.DataFrame(columns=self.pd_df.columns)

        self.RemoveWrongDateFormatRecord()
        self.SortByDatePrelev()


    def SortByDatePrelev(self):
        self.final_pd_df = self.final_pd_df.sort_values(by=['SAMPLED_DATE'],ascending=True)

    def GetPandaDataFrame(self):
        return self.final_pd_df

    def GetColumns(self):
        return ','.join(self.final_pd_df.columns.values)

    def RemoveWrongDateFormatRecord(self):
        logging.info("Remove wrong date format form SGIL data")
        
        index_good = 0
        index_bad = 0
        tested_row = 0

        for index, row in self.pd_df.loc[:,].iterrows():
            tested_row += 1
            sys.stdout.write("Test row >>> %d\r"%tested_row)
            sys.stdout.flush()

            if((Utils.CheckDateFormat(str(type(row['SAMPLED_DATE'])))) and (Utils.CheckDateFormat(str(type(row['DATE_NAISS']))))):
                self.final_pd_df.loc[index_good] = row
                index_good += 1
            else:
                self.pd_df_with_wrong_date_format.loc[index_bad] = row
                index_bad += 1

        self.excel_manager.WriteToExcel(self.pd_df_with_wrong_date_format,"SGILdataWithBadDate.xlsx")

        del self.pd_df
        gc.collect()

    def SetPandaDataFrame(self):
        self.pd_df = self.excel_manager.ReadSGILdataFile()
        self.ToUpperCase()
        self.ToDateTime()

    def ToUpperCase(self):
        self.pd_df['NOM'] = self.pd_df['NOM'].str.upper()
        self.pd_df['PRENOM'] = self.pd_df['PRENOM'].str.upper() 

    def ToDateTime(self):
        self.pd_df['SAMPLED_DATE'] =  pd.to_datetime(self.pd_df['SAMPLED_DATE'],errors='coerce')
        self.pd_df['DATE_NAISS'] =  pd.to_datetime(self.pd_df['DATE_NAISS'],errors='coerce')


class MySQLcovid19:
    def __init__(self):
        self.host = 'localhost'
        self.user = 'root'
        self.password = 'lspq2019'
        #self.database = 'TestCovid19v7'
        self.database = 'TestCovid19_TEST'
        self.connection = self.SetConnection()

    def SetConnection(self):
        return mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=self.database)

    def GetConnection(self):
        return self.connection

    def GetCursor(self):
        return self.GetConnection().cursor()

    def Commit(self):
        self.connection.commit()


class MySQLcovid19Updator:
    def __init__(self,dsp_dat,sgil_dat,envois_gq,ch_dsp2lspq_matcher,envois_genome_quebec_2_dsp_dat_matcher,db_covid19,sgil_dat_2_dsp_dat_matcher):
        self.dsp_dat = dsp_dat
        self.sgil_dat = sgil_dat
        self.envois_gq = envois_gq
        self.ch_dsp2lspq_matcher = ch_dsp2lspq_matcher
        self.envois_genome_quebec_2_dsp_dat_matcher = envois_genome_quebec_2_dsp_dat_matcher
        self.sgil_dat_2_dsp_dat_matcher = sgil_dat_2_dsp_dat_matcher
        self.db = db_covid19
        self.nb_patients_inserted = 0
        self.nb_prelevements_inserted = 0

        self.patients_columns_as_string = self.GetPatientsColumnsAsString()
        self.SetPrelevementsColumnsAsString()

    def GetChDspCode(self,lspq_ch_code):
        return self.ch_dsp2lspq_matcher.GetChDspCode(lspq_ch_code)

    def GetDSPmatch(self,nom,prenom,dt_naiss,dt_prelev,ch_dsp_code,use_ch):
        return self.envois_genome_quebec_2_dsp_dat_matcher.GetDSPmatch(nom,prenom,dt_naiss,dt_prelev,ch_dsp_code,use_ch)


    def GetDSPmatchWithSGILdat(self,nom,prenom,dt_naiss,dt_prelev,ch,numero_sgil):
        return self.sgil_dat_2_dsp_dat_matcher.GetDSPmatch(nom,prenom,dt_naiss,dt_prelev,ch,numero_sgil)

    def SetPrelevementsColumnsAsString(self):
        columns_list =  MySQLcovid19Selector.GetPrelevementsColumn(self.db.GetCursor())
        
        self.prelevements_columns_as_string = ','.join(columns_list[0])
        self.prelevements_columns_as_string_for_sgil_insert = ','.join(columns_list[1])

    def GetPatientsColumnsAsString(self):
        columns_list = MySQLcovid19Selector.GetPatientsColumn(self.db.GetCursor())
        patients_columns_as_string = ','.join(columns_list)
        return patients_columns_as_string

    def InsertPatient(self,val_to_insert):
        exist = MySQLcovid19Selector.CheckIfPatientExist(self.db.GetCursor(),val_to_insert[0])
        
        if not exist:
            try:
                ncols = self.patients_columns_as_string.count(",") + 1
                sql_insert = "INSERT INTO Patients ({0}) values ({1})".format(self.patients_columns_as_string,str("%s,"*ncols)[:-1])
                self.db.GetCursor().execute(sql_insert,val_to_insert)
                self.db.Commit()
                self.nb_patients_inserted += 1
                sys.stdout.write("Insert in Patients >>> %d\r"%self.nb_patients_inserted)
                sys.stdout.flush()
            except mysql.connector.Error as err:
                logging.error("Erreur d'insertion dans la table Patients avec le record " + str(val_to_insert) )
                print(err)

    def InsertPrelevement(self,val_to_insert,is_sgil):
        exist = MySQLcovid19Selector.CheckIfPrelevementExist(self.db.GetCursor(),val_to_insert[10])

        if not exist:
            try:
                if not is_sgil:
                    ncols = self.prelevements_columns_as_string.count(",") + 1
                    sql_insert = "INSERT INTO Prelevements ({0}) values ({1})".format(self.prelevements_columns_as_string,str("%s,"*ncols)[:-1])
                else:
                    ncols = self.prelevements_columns_as_string_for_sgil_insert.count(",") + 1
                    sql_insert = "INSERT INTO Prelevements ({0}) values ({1})".format(self.prelevements_columns_as_string_for_sgil_insert,str("%s,"*ncols)[:-1])


                self.db.GetCursor().execute(sql_insert,val_to_insert)
                self.db.Commit()
                self.nb_prelevements_inserted += 1
                sys.stdout.write("Insert in Prelevements >>> %d\r"%self.nb_prelevements_inserted)
                sys.stdout.flush()
            except mysql.connector.Error as err:
                logging.error("Erreur d'insertion dans la table Prelevements avec le record " + str(val_to_insert))
                print(err)

    def GetValuesToInsert(self,dsp_dat_match_rec,current_envoi,ch_name,table,is_sgil,ch_adresse):
        def GetVal(x):
            if isinstance(x,pd.Timestamp):
                return str(x)
            elif isinstance(x,str):
                return x
            elif str(x.dtype) == 'datetime64[ns]':
                return str(x.values[0])
            else:
                return x.values[0]

        if table == 'Patients':
            return(tuple(map(GetVal,(dsp_dat_match_rec['ID_PATIENT'],dsp_dat_match_rec['PRENOMINFO'],dsp_dat_match_rec['NOMINFO'],dsp_dat_match_rec['SEXEINFO'],dsp_dat_match_rec['DTNAISSINFO'],dsp_dat_match_rec['STATUT'],dsp_dat_match_rec['RSS_LSPQ_CAS']))))

        elif table == 'Prelevements' and not is_sgil:
            return(tuple(map(GetVal,(dsp_dat_match_rec['ID_PATIENT'],dsp_dat_match_rec['STATUT'],dsp_dat_match_rec['CODE_HOPITAL_DSP'],current_envoi['CODE_HOPITAL_LSPQ'],ch_name,ch_adresse,dsp_dat_match_rec['DATE_PRELEV_1'],dsp_dat_match_rec['DATE_CONF_LSPQ_1'],dsp_dat_match_rec['DATE_PRELEV_2'],dsp_dat_match_rec['DATE_CONF_LSPQ_2'],current_envoi['DATE_PRELEV'],current_envoi['GENOME_QUEBEC_REQUETE'],current_envoi['DATE_ENVOI_GENOME_QUEBEC'],dsp_dat_match_rec['ID_PHYLO']))))

        elif table == 'Prelevements' and  is_sgil:
            return(tuple(map(GetVal,(dsp_dat_match_rec['ID_PATIENT'],dsp_dat_match_rec['STATUT'],dsp_dat_match_rec['CODE_HOPITAL_DSP'],'LSPQ',ch_name,ch_adresse,dsp_dat_match_rec['DATE_PRELEV_1'],dsp_dat_match_rec['DATE_CONF_LSPQ_1'],dsp_dat_match_rec['DATE_PRELEV_2'],dsp_dat_match_rec['DATE_CONF_LSPQ_2'],current_envoi['SAMPLED_DATE'],current_envoi['NUMERO_SGIL'],dsp_dat_match_rec['ID_PHYLO']))))

    def Insert(self,use_ch):
        logging.info("Begin insert")

        ch_sorel_info = CH_DSPtoLSPQ.Get_CH_Sorel_Info()

        for index,row in self.envois_gq.GetPandaDataFrame().loc[:,].iterrows():
            is_ch_sorel = row['CODE_HOPITAL_LSPQ_IS_CH_SOREL']
            ch_dsp2lspq_val = self.GetChDspCode(row['CODE_HOPITAL_LSPQ'])
            
            try:

                if is_ch_sorel: 
                    ch_dsp_code = ch_sorel_info[0]
                    ch_name = ch_sorel_info[1]
                    ch_adresse = ch_sorel_info[2] 
                else:
                    ch_dsp_code = ch_dsp2lspq_val[0]
                    ch_name = ch_dsp2lspq_val[1]
                    ch_adresse = ch_dsp2lspq_val[2]

                dsp_dat_match_rec =  self.GetDSPmatch(row['NOMINFO'],row['PRENOMINFO'],row['DTNAISSINFO'],row['DATE_PRELEV'],ch_dsp_code,use_ch)

                if  dsp_dat_match_rec.shape[0] != 0:
                    val_to_insert = self.GetValuesToInsert(dsp_dat_match_rec,row,ch_name,'Patients',False,ch_adresse)
                    self.InsertPatient(val_to_insert)

                    val_to_insert = self.GetValuesToInsert(dsp_dat_match_rec,row,ch_name,'Prelevements',False,ch_adresse)
                    self.InsertPrelevement(val_to_insert,False)

                else:
                    pass

            except IndexError as err:
                logging.error("No match for CH " + row['CODE_HOPITAL_LSPQ'])

        '''
        for index,row in self.sgil_dat.GetPandaDataFrame().loc[:,].iterrows():
            try:
                dsp_dat_match_rec = self.GetDSPmatchWithSGILdat(row['PID'],row['SAMPLED_DATE'],'LSPQ',row['NUMERO_SGIL'])
                if dsp_dat_match_rec.shape[0] != 0:
                    val_to_insert =  self.GetValuesToInsert(dsp_dat_match_rec,row,row['CH_NAME'],'Patients',True,row['CH_ADRESS'])
                    self.InsertPatient(val_to_insert)
                    val_to_insert =  self.GetValuesToInsert(dsp_dat_match_rec,row,row['CH_NAME'],'Prelevements',True,row['CH_ADRESS'])
                    self.InsertPrelevement(val_to_insert,True)
            except:
                pass
        '''

        for index,row in self.sgil_dat.GetPandaDataFrame().loc[:,].iterrows():
            try:
                dsp_dat_match_rec = self.GetDSPmatchWithSGILdat(row['NOM'],row['PRENOM'],row['DATE_NAISS'],row['SAMPLED_DATE'],'LSPQ',row['NUMERO_SGIL'])
                if dsp_dat_match_rec.shape[0] != 0:
                    val_to_insert =  self.GetValuesToInsert(dsp_dat_match_rec,row,row['CH_NAME'],'Patients',True,row['CH_ADRESS'])
                    self.InsertPatient(val_to_insert)
                    val_to_insert =  self.GetValuesToInsert(dsp_dat_match_rec,row,row['CH_NAME'],'Prelevements',True,row['CH_ADRESS'])
                    self.InsertPrelevement(val_to_insert,True)
            except:
                pass

    def SaveNoMatchToExcel(self):
        self.ch_dsp2lspq_matcher.WriteProblematicLSPQchToExcel()
        self.envois_genome_quebec_2_dsp_dat_matcher.WriteNoMatchToExcel()
        self.sgil_dat_2_dsp_dat_matcher.WriteNoMatchToExcel()

class MySQLcovid19Selector:
    def __init__(self):
        pass

    @staticmethod
    def IsMatch(cursor): 
        nb_match = cursor.fetchone()[0]
        if nb_match == 0:
            return False
        else:
            return True

    @staticmethod
    def CheckIfPatientExist(cursor,id_patient):
        cursor.execute("select count(*) from Patients where ID_PATIENT = '{0}'".format(id_patient))
        return MySQLcovid19Selector.IsMatch(cursor)
       
    @staticmethod
    def CheckIfPrelevementExist(cursor,no_req):
        cursor.execute("select count(*) from Prelevements where GENOME_QUEBEC_REQUETE  = '{0}'".format(no_req))
        return MySQLcovid19Selector.IsMatch(cursor)
         
    @staticmethod
    def GetColumns(cursor,table):
        if table == 'Prelevements':
            res = cursor.fetchall()
            id_prelev_col = 'ID_PRELEV'
            date_envoi_genome_quebec_col = 'DATE_ENVOI_GENOME_QUEBEC'
            columns = []
            columns_for_sgil_insert = []
        
            for x in res:
                if x[0] not in columns and x[0] != id_prelev_col:
                    columns.append(x[0])
                if x[0] not in columns_for_sgil_insert  and x[0] != id_prelev_col and x[0] != date_envoi_genome_quebec_col:
                    columns_for_sgil_insert.append(x[0])

            columns = list(columns)
            columns_for_sgil_insert = list(columns_for_sgil_insert)

            return [columns,columns_for_sgil_insert]

        elif table == 'Patients':
            res = cursor.fetchall()
            columns = []
            for x in res:
                if x[0] not in columns:
                    columns.append(x[0])
            return list(columns)

    @staticmethod
    def GetPatientsColumn(cursor):
        cursor.execute("select COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Patients'")
        return MySQLcovid19Selector.GetColumns(cursor,'Patients')

    @staticmethod
    def GetPrelevementsColumn(cursor):
        cursor.execute("select COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Prelevements'")
        return MySQLcovid19Selector.GetColumns(cursor,'Prelevements') 


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

    @staticmethod
    def GetDateNaissFromNAM(nam):

        def GetCompleteBirthYear(birth_year_last_two_digit):
            max_age = 105

            current_year = str(datetime.date.today().year)
            current_year_last_two_digit = current_year[-2:]

            if birth_year_last_two_digit > current_year_last_two_digit:
                return "19" + birth_year_last_two_digit

            elif  int(current_year) - int("19"+birth_year_last_two_digit) > max_age:
                return "20"+birth_year_last_two_digit

            else:
               return None

        if len(nam) == 12:
            try:
                pattern_obj = re.compile(r'(\S{4})(\d{6})(\S{2})')
                search_obj = pattern_obj.search(nam)
                name_info = search_obj.group(1)
                dt_naiss = search_obj.group(2)
                suffix = search_obj.group(3)

                complete_birth_year = GetCompleteBirthYear(dt_naiss[0:2])
                month_naiss = dt_naiss[2:4]
                day_naiss = dt_naiss[4:6]
            except Exception:
                return None
            
            if complete_birth_year:

                try:
                    if int(month_naiss) > 12:
                        month_naiss = str(int(month_naiss) - 50) #pour les femmes

                    date_naiss = complete_birth_year + "-" +  month_naiss + "-" + day_naiss
                    date_naiss_as_date = datetime.datetime.strptime(date_naiss,'%Y-%m-%d')
                    return date_naiss_as_date
                except ValueError as e:
                    return None

            return None

    @staticmethod
    def GetDateFromStrDate(str_date):

        date_as_date = ""

        try:
            match_obj = re.compile(r'(\d?\d)/(\d\d?)')
            search_obj = match_obj.search(str_date)
            day = search_obj.group(1)
            month =search_obj.group(2)
            
            current_year = str(datetime.datetime.today().year)

            if len(day) == 1:
                day = "0" + day
            if len(month) == 1:
                month = "0" + month

            date_as_date = datetime.datetime.strptime(current_year + "-" + month + "-" + day,'%Y-%m-%d')
        except:
            print(sys.exc_info())
            return None

        return date_as_date


class SGILdata_2_DSPdata_Matcher():
    def __init__(self,dsp_dat,excel_manager):
        self.dsp_dat = dsp_dat
        self.excel_manager = excel_manager
        self.no_match_df = pd.DataFrame(columns=['NUMERO_SGIL','NOM','PRENOM','DT_NAISS','DT_PRELEV'])
        self.nb_no_match = 0

    def GetDSPmatch(self,nom,prenom,dt_naiss,dt_prelev,ch,numero_sgil):
        try:
            nom = nom.strip(' ')
            nom_no_space = re.sub(r' |-','',nom)
            prenom = prenom.strip(' ')
            prenom_no_space = re.sub(r' |-','',prenom)
        except:
            self.no_match_df.loc[self.nb_no_match] = {'NUMERO_SGIL':numero_sgil,'NOM':nom,'PRENOM':prenom,'DT_NAISS':dt_naiss,'DT_PRELEV':dt_prelev}
            self.nb_no_match += 1
            return pd.DataFrame()

        try:
            dsp_dat_df = self.dsp_dat.GetPandaDataFrame()
            
            match_df = dsp_dat_df.loc[(dsp_dat_df['NOMINFO'].str.strip(' ').isin([nom,nom_no_space])) & (dsp_dat_df['PRENOMINFO'].str.strip(' ').isin([prenom,prenom_no_space])) & (dsp_dat_df['DTNAISSINFO']==dt_naiss) ,:].copy()

            if match_df.shape[0] == 0:
                self.no_match_df.loc[self.nb_no_match] =  {'NUMERO_SGIL':numero_sgil,'NOM':nom,'PRENOM':prenom,'DT_NAISS':dt_naiss,'DT_PRELEV':dt_prelev}
                self.nb_no_match += 1
                return match_df

            self.ComputeDatePrelevDiff(match_df,dt_prelev)
            return(match_df[match_df.MIN_DELTA == match_df.MIN_DELTA.min()])

        except AttributeError as e:
            print(e)
            return pd.DataFrame(columns=dsp_dat_df.columns)

    def ComputeDatePrelevDiff(self,match_df,dt_prelev):
        match_df['DATE_DIFF_1'] = match_df['DATE_PRELEV_1'] - dt_prelev
        match_df['DATE_DIFF_1'] = match_df['DATE_DIFF_1'].abs()
        match_df['DATE_DIFF_2'] = match_df['DATE_PRELEV_2'] - dt_prelev
        match_df['DATE_DIFF_2'] = match_df['DATE_DIFF_2'].abs()

        try:
            match_df['MIN_DELTA'] = match_df[['DATE_DIFF_1','DATE_DIFF_2']].min(axis=1)
        except:
            pass

    def WriteNoMatchToExcel(self):
        self.excel_manager.WriteToExcel(self.no_match_df,"NoDSPPatientfound_versus_sgil.xlsx")

class EnvoisGenomeQuebec_2_DSPdata_Matcher():
    def __init__(self,dsp_dat,excel_manager):
        self.dsp_dat = dsp_dat
        self.excel_manager = excel_manager
        self.no_match_df = pd.DataFrame(columns=['NOMINFO','PRENOMINFO','DTNAISSINFO','CODE_HOPITAL_DSP'])
        self.duplicate_match_by_ch_df = pd.DataFrame(columns=['NOMINFO','PRENOMINFO','DTNAISSINFO','CODE_HOPITAL_DSP'])

        self.nb_no_match = 0
        self.nb_duplicate_match_by_ch_df = 0

    def ComputeDatePrelevDiff(self,match_df,dt_prelev):
        match_df['DATE_DIFF_1'] = match_df['DATE_PRELEV_1'] - dt_prelev
        match_df['DATE_DIFF_1'] = match_df['DATE_DIFF_1'].abs()
        match_df['DATE_DIFF_2'] = match_df['DATE_PRELEV_2'] - dt_prelev
        match_df['DATE_DIFF_2'] = match_df['DATE_DIFF_2'].abs()

        try:
            match_df['MIN_DELTA'] = match_df[['DATE_DIFF_1','DATE_DIFF_2']].min(axis=1)
        except:
            pass
      
 
    def GetDSPmatch(self,nom,prenom,dt_naiss,dt_prelev,dsp_ch_code,use_ch):
       
        try: 
            nom = nom.strip(' ')
            nom_no_space = re.sub(r' |-','',nom)
            prenom = prenom.strip(' ')
            prenom_no_space = re.sub(r' |-','',prenom)
            dsp_ch_code = dsp_ch_code.strip(' ')
        except:
                self.no_match_df.loc[self.nb_no_match] = {'NOMINFO':nom,'PRENOMINFO':prenom,'DTNAISSINFO':dt_naiss,'CODE_HOPITAL_DSP':dsp_ch_code}
                self.nb_no_match += 1
                return pd.DataFrame()
        
        try:
            dsp_dat_df = self.dsp_dat.GetPandaDataFrame()
            if use_ch:
                match_df = dsp_dat_df.loc[(dsp_dat_df['NOMINFO'].str.strip(' ').isin([nom,nom_no_space])) & (dsp_dat_df['PRENOMINFO'].str.strip(' ').isin([prenom,prenom_no_space])) & (dsp_dat_df['DTNAISSINFO']==dt_naiss) & (dsp_dat_df['CODE_HOPITAL_DSP'].str.strip()==dsp_ch_code),:].copy()
            else:
                match_df = dsp_dat_df.loc[(dsp_dat_df['NOMINFO'].str.strip(' ').isin([nom,nom_no_space])) & (dsp_dat_df['PRENOMINFO'].str.strip(' ').isin([prenom,prenom_no_space])) & (dsp_dat_df['DTNAISSINFO']==dt_naiss) ,:].copy()

            if match_df.shape[0] == 0:
                self.no_match_df.loc[self.nb_no_match] = {'NOMINFO':nom,'PRENOMINFO':prenom,'DTNAISSINFO':dt_naiss,'CODE_HOPITAL_DSP':dsp_ch_code}
                self.nb_no_match += 1
                return match_df

            if not use_ch:
                ch_list = match_df['CODE_HOPITAL_DSP'].unique()
                if len(ch_list) > 1:
                    logging.info("Multiple CH for " + nom + " " + prenom + " " + str(dt_naiss) + " : " + str(ch_list))
                    self.duplicate_match_by_ch_df.loc[self.nb_duplicate_match_by_ch_df] = {'NOMINFO':nom,'PRENOMINFO':prenom,'DTNAISSINFO':dt_naiss,'CODE_HOPITAL_DSP':dsp_ch_code}
                    self.nb_duplicate_match_by_ch_df += 1
                    return pd.DataFrame()

            self.ComputeDatePrelevDiff(match_df,dt_prelev)

            return(match_df[match_df.MIN_DELTA == match_df.MIN_DELTA.min()])

        except AttributeError as e:
            print(e)
            return pd.DataFrame(columns=dsp_dat_df.columns)

    def WriteNoMatchToExcel(self):
        self.excel_manager.WriteToExcel(self.no_match_df,"NoDSPPatientfound.xlsx")
        self.excel_manager.WriteToExcel(self.duplicate_match_by_ch_df,"DSPduplicateByCh.xlsx")


class CH_DSP_2_LSPQ_Matcher():
    def __init__(self,ch_dsp_2_lspq,excel_manager):
        self.excel_manager = excel_manager
        self.ch_dsp_2_lspq = ch_dsp_2_lspq
        self.missing_match_df = pd.DataFrame(columns=['LSPQ_CH'])
        self.over_one_match_df = pd.DataFrame(columns=['LSPQ_CH'])

        self.nb_missing_match = 0
        self.nb_over_one_match = 0

    def GetChDspCode(self,lspq_ch_code):
        df = self.ch_dsp_2_lspq.GetPandaDataFrame()
        res = df.loc[df['PrefixLSPQ'] == lspq_ch_code,['PrefixDSP','ETABLISSEMENTS','ADRESSE']].values

        if len(res) == 0:
            self.missing_match_df.loc[self.nb_missing_match] = lspq_ch_code
            self.nb_missing_match += 1
            return []
        elif len(res) > 1 and lspq_ch_code != 'HDS-':
            self.over_one_match_df.loc[self.nb_over_one_match] = lspq_ch_code
            self.nb_over_one_match += 1
            return []
        else:
            val = res[0]
            dsp_ch_code = val[0]
            if not isinstance(dsp_ch_code,str):
                self.missing_match_df.loc[self.nb_missing_match] = lspq_ch_code
                return []
            else:
                ch_name = val[1]
                ch_adresse = val[2]
                return [dsp_ch_code,ch_name,ch_adresse]

    def WriteProblematicLSPQchToExcel(self):
        self.excel_manager.WriteToExcel(self.missing_match_df,"LSPQ_CH_missing_match.xlsx")
        self.excel_manager.WriteToExcel(self.over_one_match_df,"LSPQ_CH_over_one_match.xlsx")

class ExcelManager:
    def __init__(self,_debug):
        self._debug = _debug
        self.basedir = "/data/Databases/COVID19_DSP/"
        self.basedir_dsp_data = os.path.join(self.basedir,"BD_PHYLOGENIE")
        self.basedir_envois_genome_quebec = os.path.join(self.basedir,"LISTE_ENVOIS_GENOME_QUEBEC")
        self.basedir_sgil_data = os.path.join(self.basedir,"SGIL_EXTRACT")
        self.basedir_script_out = os.path.join(self.basedir,"SCRIPT_OUT")
        self.dsp_data_file = None
        self.envois_genome_quebec_file = None
        self.ch_dsp2lspq_file = None
        self.SetFilePath()

    def SetFilePath(self):
        
        self.ch_dsp2lspq_file = os.path.join(self.basedir_envois_genome_quebec,'PREFIX_CH_LSPQvsDSP.xlsx')

        if self._debug:
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small3.xlsm')
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2.xlsm')
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2_HDS.xlsm')
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2_HPLG.xlsm')
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2_SJ.xlsm')
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2_DUPLICATE.xlsm')
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2_BADDATE.xlsm')
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_small2_TESTSPACE.xlsm')

            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small3.xlsx')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small.xlsx')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small_nochmatch.xlsx')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small_nopatientmatch.xlsx')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small_withbaddate.xlsx')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small_HPLG.xlsx')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small_SJ.xlsx')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small_TESTSPACE.xlsx')

            self.sgil_data_file = os.path.join(self.basedir_sgil_data,'export_20200817_minimal_small3.txt')
            #self.sgil_data_file = os.path.join(self.basedir_sgil_data,'export_20200817_minimal_small.txt')
        else:
            #self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie.xlsm')
            self.dsp_data_file = os.path.join(self.basedir_dsp_data,'BD_phylogenie_31072020.xlsm')
            #self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec.xlsx')
            self.envois_genome_quebec_file = os.path.join(self.basedir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_2020-07-22_CORR.xlsx')
            self.sgil_data_file = os.path.join(self.basedir_sgil_data,'export_20200817_minimal.txt')

    def ReadDspDataFile(self):
        return pd.read_excel(self.dsp_data_file,sheet_name='BD_Phylogenie')

    def ReadEnvoisGenomeQuebecFile(self):
        return pd.read_excel(self.envois_genome_quebec_file,sheet_name='Feuil1')

    def ReadChDSP2LSPQ(self):
        return pd.read_excel(self.ch_dsp2lspq_file,sheet_name='Feuil1')

    def ReadSGILdataFile(self):
        return pd.read_table(self.sgil_data_file)

    def WriteToExcel(self,df,file_name):
        df.to_excel(os.path.join(self.basedir_script_out,file_name),sheet_name='Sheet1')
       
 
def Main():

    logging.info("In Main()")
    excel_manager = ExcelManager(_debug_)
    db_covid19 = MySQLcovid19()
    ch_dsp_2_lspq =  CH_DSPtoLSPQ(excel_manager) 
    dsp_data = DSPdata(excel_manager)
    envois_genome_quebec_data = EnvoisGenomeQuebec(excel_manager)
    sgil_data = SGILdata(excel_manager)
    ch_dsp2lspq_matcher = CH_DSP_2_LSPQ_Matcher(ch_dsp_2_lspq,excel_manager)
    envois_genome_quebec_2_dsp_dat_matcher = EnvoisGenomeQuebec_2_DSPdata_Matcher(dsp_data,excel_manager)
    sgil_dat_2_dsp_dat_matcher = SGILdata_2_DSPdata_Matcher(dsp_data,excel_manager)
    sql_updator = MySQLcovid19Updator(dsp_data,sgil_data,envois_genome_quebec_data,ch_dsp2lspq_matcher,envois_genome_quebec_2_dsp_dat_matcher,db_covid19,sgil_dat_2_dsp_dat_matcher)

    sql_updator.Insert(_use_ch_mapping)
    
    sql_updator.SaveNoMatchToExcel()

    #Utils.Inspect(dsp_data,envois_genome_quebec_data)

if __name__ == '__main__':
    Main()
