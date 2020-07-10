# -*- coding: utf-8 -*-

import mysql.connector
import datetime
import pandas as pd
import os
import numpy as np
import re

base_dir_bd = "/data/Databases/COVID19_DSP/"
base_dir_bd_phylo = os.path.join(base_dir_bd,'BD_PHYLOGENIE')
base_dir_envois_genome_quebec = os.path.join(base_dir_bd,'LISTE_ENVOIS_GENOME_QUEBEC')

pd_bd_phylo = pd.read_excel(os.path.join(base_dir_bd_phylo,'BD_phylogenie_small.xlsm'),sheet_name='BD_Phylogenie')
#pd_bd_phylo = pd.read_excel(os.path.join(base_dir_bd_phylo,'BD_phylogenie.xlsm'),sheet_name='BD_Phylogenie')
pd_bd_phylo['Date_prelev_1'] = pd.to_datetime(pd_bd_phylo['Date_prelev_1'],format='%Y%m%d').dt.strftime('%Y-%m-%d')
pd_bd_phylo['Date_conf_LSPQ_1'] = pd.to_datetime(pd_bd_phylo['Date_conf_LSPQ_1'],format='%Y%m%d').dt.strftime('%Y-%m-%d')
pd_bd_phylo['dtNaissInfo'] = pd.to_datetime(pd_bd_phylo['dtNaissInfo'],format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
pd_bd_phylo['Date_conf_LSPQ_2'] = pd.to_datetime(pd_bd_phylo['Date_conf_LSPQ_2'],format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
pd_bd_phylo['nomInfo'] = pd_bd_phylo['nomInfo'].str.upper()
pd_bd_phylo['prenomInfo'] = pd_bd_phylo['prenomInfo'].str.upper()



pd_envois_genome_quebec = pd.read_excel(os.path.join(base_dir_envois_genome_quebec,'ListeEnvoisGenomeQuebec_small.xlsx'),sheet_name='Feuil1')
pd_envois_genome_quebec['Date de naissance'] = pd.to_datetime(pd_envois_genome_quebec['Date de naissance'],format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
pd_envois_genome_quebec['Date de prélèvement'] = pd.to_datetime(pd_envois_genome_quebec['Date de prélèvement'],format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
pd_envois_genome_quebec['DateEnvoiGenomeQuebec'] = pd.to_datetime(pd_envois_genome_quebec['DateEnvoiGenomeQuebec'],format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

pd_envois_genome_quebec['Nom'] = pd_envois_genome_quebec['Nom'].str.upper()
pd_envois_genome_quebec['Prénom'] = pd_envois_genome_quebec['Prénom'].str.upper()


pd_envois_genome_quebec['LSPQ_CH_CODE'] = pd_envois_genome_quebec['# Requête'].str.extract(r'(\S+-)\S+')

pd_prefix_ch_lspq2dsp = pd.read_excel(os.path.join(base_dir_envois_genome_quebec,'PREFIX_CH_LSPQvsDSP_TEST.xlsx'),sheet_name='Feuil1')

pd_bd_phylo["REQ_ID"] = np.nan

mydb = mysql.connector.connect(host="localhost",user="root",password="lspq2019",database="TestCovid19")
mycursor = mydb.cursor()

def Match_CH_DSP2LSPQ(dsp_ch_code):
    #TODO SI LES CODE DSP EST ABSENT DU FICHIER DE MAPPING
    lspq_ch_code = pd_prefix_ch_lspq2dsp.loc[pd_prefix_ch_lspq2dsp['PrefixDSP'] == dsp_ch_code,['PrefixLSPQ','ETABLISSEMENTS']].values[0][0]
    ch_name = pd_prefix_ch_lspq2dsp.loc[pd_prefix_ch_lspq2dsp['PrefixDSP'] == dsp_ch_code,['PrefixLSPQ','ETABLISSEMENTS']].values[0][1]

    #print(" [0] ", pd_prefix_ch_lspq2dsp.loc[pd_prefix_ch_lspq2dsp['PrefixDSP'] == dsp_ch_code,['PrefixLSPQ','ETABLISSEMENTS']].values[0])
    #print("code [0] ", pd_prefix_ch_lspq2dsp.loc[pd_prefix_ch_lspq2dsp['PrefixDSP'] == dsp_ch_code,['PrefixLSPQ','ETABLISSEMENTS']].values[0][0])
    #print("code [1] ", pd_prefix_ch_lspq2dsp.loc[pd_prefix_ch_lspq2dsp['PrefixDSP'] == dsp_ch_code,['PrefixLSPQ','ETABLISSEMENTS']].values[0][1])

    #print("FOR ",dsp_ch_code, " LSPQ CODE IS ",lspq_ch_code)
    return([lspq_ch_code,ch_name])


def TryInsertInDB(id_phylo,statut,rss_lspq_cas,date_prelev_1,date_conf_lspq_1,code_hopital_dsp,no_benef,nam_lspq,nom_info,prenom_info,dt_naissance,sexe_info,date_conf_lspq_2,date_prelev_2,no_requete,ch_name,lspq_ch_code,date_envoi_genome_quebec):
    
    print("************* TRY TO INSERT ***************")
    print("ID_PHYLO : ", id_phylo)
    print("STATUT : ", statut)
    print("RSS_LSPQ_CAS : ", rss_lspq_cas)
    print("DATE_PRELEV_1 : ", date_prelev_1)
    print("DATE_CONF_LSPQ_1 ",date_conf_lspq_1)
    print("CODE_HOPITAL_DSP : ",code_hopital_dsp)
    print("NOBENEF : ", no_benef)
    print("NAM_LSPQ : ", nam_lspq)
    print("NOMINFO : ", nom_info)
    print("PRENOMINFO : ", prenom_info)
    print("DTNAISSINFO : ",dt_naissance)
    print("SEXEINFO : ",sexe_info)
    print("DATE_CONF_LSPQ_2 : ",date_conf_lspq_2)
    print("DATE_PRELEV_2 : ", date_prelev_2)
    print("GENOME_QUEBEC_REQUETE : ", no_requete)
    print("NOM_HOPITAL : ", ch_name)
    print("CODE_HOPITAL_LSPQ : ", lspq_ch_code)
    print("DATE_ENVOI_GENOME_QUEBEC : ", date_envoi_genome_quebec)

    mycursor.execute("select count(*) from CovidPositive where GENOME_QUEBEC_REQUETE = {0}".format("'" + no_requete  + "'"))
    nb_occurence = mycursor.fetchone()
    
    sql = "INSERT INTO CovidPositive (ID_PHYLO, STATUT, RSS_LSPQ_CAS , DATE_PRELEV_1, DATE_CONF_LSPQ_1,CODE_HOPITAL_DSP, NOBENEF,NAM_LSPQ, NOMINFO,PRENOMINFO, DTNAISSINFO, SEXEINFO, DATE_CONF_LSPQ_2, DATE_PRELEV_2,GENOME_QUEBEC_REQUETE,NOM_HOPITAL,CODE_HOPITAL_LSPQ,DATE_ENVOI_GENOME_QUEBEC)  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    
    if nb_occurence[0] == 0:
        try:
            mycursor.execute(sql,(id_phylo,statut,rss_lspq_cas,date_prelev_1,date_conf_lspq_1,code_hopital_dsp, no_benef,nam_lspq,
            nom_info,prenom_info,dt_naissance,sexe_info,date_conf_lspq_2,date_prelev_2,no_requete,ch_name,lspq_ch_code,date_envoi_genome_quebec))

            mydb.commit()
        except mysql.connector.Error as err:
            print("Insert error {}".format(err))
    

#for index, row in pd_bd_phylo.loc[pd_bd_phylo['laboratoire'].isin(['CHUM','HMR2'])].iterrows():
for index, row in pd_bd_phylo.loc[:,].iterrows():
    '''
    print(row['laboratoire'])
    print(row['nomInfo'])
    print(row['prenomInfo'])
    print(row['dtNaissInfo'])
    print(row['Date_prelev_1'])
    print(row['NAM__LSPQ_'])
    '''

    match = Match_CH_DSP2LSPQ(row['laboratoire'])
    lspq_ch_code = match[0]
    ch_name = match[1]
    #print("CH NAME ",ch_name)
    #print("LSPQ CH CODE ", lspq_ch_code)
    #print(pd_envois_genome_quebec['LSPQ_CH_CODE'])
    #print(pd_envois_genome_quebec.loc[pd_envois_genome_quebec['LSPQ_CH_CODE'] == 'HMR'])
    #print(pd_envois_genome_quebec.loc[(pd_envois_genome_quebec['Nom'] == row['nomInfo']) & (pd_envois_genome_quebec['Prénom'] == row['prenomInfo']) & (pd_envois_genome_quebec['Date de naissance'] == row['dtNaissInfo'])  & (pd_envois_genome_quebec['Date de prélèvement'] == row['Date_prelev_1']) & (pd_envois_genome_quebec['LSPQ_CH_CODE'] == lspq_ch_code)])

    pd_match_envois_genome_quebec =   pd_envois_genome_quebec.loc[(pd_envois_genome_quebec['Nom'] == row['nomInfo']) & (pd_envois_genome_quebec['Prénom'] == row['prenomInfo']) & (pd_envois_genome_quebec['Date de naissance'] == row['dtNaissInfo'])  & (pd_envois_genome_quebec['Date de prélèvement'] == row['Date_prelev_1']) & (pd_envois_genome_quebec['LSPQ_CH_CODE'] == lspq_ch_code)]

    nb_match = int(pd_match_envois_genome_quebec.loc[:,['# Requête']].count())
    try:
        if nb_match == 1:
            no_requete = pd_match_envois_genome_quebec.loc[:,['# Requête']].values[0][0]
            date_envoi_genome_quebec = pd_match_envois_genome_quebec.loc[:,['DateEnvoiGenomeQuebec']].values[0][0]
            

            #print("NO REQUETE ",no_requete)
            TryInsertInDB(row['ID_Phylo'],row['statut'],row['RSS_LSPQ_cas'],row['Date_prelev_1'],row['Date_conf_LSPQ_1'],row['laboratoire'],row['noBenef'],row['NAM__LSPQ_'],row['nomInfo'],row['prenomInfo'],row['dtNaissInfo'],row['sexeInfo'],row['Date_conf_LSPQ_2'],row['Date_prelev_2'],no_requete,ch_name,lspq_ch_code,date_envoi_genome_quebec)
            print("**************")
        elif nb_match > 1:
            #print("MULTIPLE MATCH for ", row['nomInfo'], " ", row['prenomInfo'], " ", row['dtNaissInfo'], " ", row['Date_prelev_1'], " ",lspq_ch_code)
            pass
    except mysql.connector.Error as err: 
        print('no match')
        #print(err)
        print("**************")


exit(1)



bd_file="/data/Databases/COVID19_DSP/BD_phylogenie.csv"

mydb = mysql.connector.connect(host="localhost",user="root",password="lspq2019",database="TestCovid19")

mycursor = mydb.cursor()

'''
mycursor.execute("select * from CovidPositive")

myresult = mycursor.fetchall()

for x in myresult:
    print(x)

'''




def CheckDate(row_to_check,index):
    try:
        datetime.datetime.strptime(row_to_check[index],'%Y-%m-%d')
    except:
        row_to_check[index] = '0000-00-00' 

with open(bd_file,encoding='ISO-8859-1') as readf:
    readf.readline()
    for line in readf:
        values = line.split(';')
        for index in [2,3,9,11,12]:
            CheckDate(values,index)
        values[-1] = values[-1].strip('\n')
        values = tuple(values)
        #print(values)
        sql = "INSERT INTO CovidPositive (STATUT, RSS_LSPQ_CAS , DATE_PRELEV_1, DATE_CONF_LSPQ_1,LABORATOIRE , NOBENEF,NAM_LSPQ, NOMINFO,PRENOMINFO, DTNAISSINFO, SEXEINFO, DATE_CONF_LSPQ_2, DATE_PRELEV_2,ID_PHYLO)  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            mycursor.execute(sql,values)
            mydb.commit()
            #print(mycursor.rowcount,"record inserted")
        except mysql.connector.Error as err:
            print("Error {}".format(err))

