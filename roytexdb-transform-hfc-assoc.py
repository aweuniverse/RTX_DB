# -*- coding: utf-8 -*-
"""
STATEMENT OF PURPOSE:
    Run this program will read any file with the naming format 'SOURCE_HFC_ASSOC_XX-XX.csv' located in ROYTEXDB folder and upload them into SQL table HFC_ASSOC by season
    the source file comes from PROCOMM 10.40.45, pulled for a specific season. So that season name needs to be added to the end of the file name in the format of "SS-YY"
PRE-REQUISITE:
    SQL table CUST_ORDER needs to be fully loaded
"""
import os
import pandas as pd
import sqlalchemy
import sys
import logging

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')

def oneSeason (aFile):
    assoc = pd.read_csv(aFile, skiprows=1, header=None, names=['GREEN_BAR', 'STYLE', 'COLOR', 'HFC'], usecols=[1, 3, 4, 5], converters={1: str, 3: str, 4:str, 5:str})
    assoc['STYLE'] = assoc['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
    assoc['COLOR'] = assoc['COLOR'].apply(lambda x: x.zfill(3))
    assoc['HFC'] = assoc['HFC'].apply(lambda x: x.split('-')[0].zfill(6))
    assoc['SEASON'] = aFile.split('.')[0][-5:]    
    return assoc

def allSeasonAssoc ():
    fileList = [each for each in os.listdir() if each[:16] == 'SOURCE_HFC_ASSOC']
    if len(fileList) >0:
        for n in range(len(fileList)):
            if n == 0:
                allAssoc = oneSeason(fileList[n])
            else:
                addone = oneSeason(fileList[n])
                allAssoc = allAssoc.append(addone)
        return allAssoc
    else:
        sys.exit('There is no file to read')

a = allSeasonAssoc()
engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
a.to_sql('#temp_hfc_assoc', con=conn, if_exists='replace', index=False)
trans = conn.begin()
try:
    conn.execute("""MERGE DBO.HFC_ASSOC AS T 
                 USING dbo.#temp_hfc_assoc AS S 
                 ON (T.GREEN_BAR = S.GREEN_BAR and T.STYLE=S.STYLE and T.COLOR=S.COLOR) 
                 WHEN MATCHED THEN UPDATE
                 SET T.HFC=S.HFC, T.SEASON=S.SEASON, T.CXL=0
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (GREEN_BAR, STYLE, COLOR, HFC, SEASON) VALUES
                 (S.GREEN_BAR, S.STYLE, S.COLOR, S.HFC, S.SEASON)
                 WHEN NOT MATCHED BY SOURCE THEN
                 UPDATE SET T.CXL=1;""")
    trans.commit()
    conn.close()
    engine.dispose()          
    print ('UPDATE COMPLETED SUCCESSFULLY!')
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    trans.rollback()
    conn.close()
    engine.dispose()
