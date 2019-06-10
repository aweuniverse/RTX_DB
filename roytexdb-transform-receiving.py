# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    Run this program will read the 'SOURCE_RECEIVING.csv' file located in the ROYTEXDB folder and upload it into SQL table RECEIVING
PREREQUISITE:
    SQL table HFC_DETAIL has to be loaded before running this program
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime as dt
import sqlalchemy
import logging

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')

pdRec = pd.read_csv('SOURCE_RECEIVING.csv', skiprows=3, header=None, usecols=[0, 1, 2, 3, 4, 5, 8], 
            names=['CTRL_NBR', 'REC_NBR', 'REC_DATE', 'HFC', 'STYLE', 'COLOR', 'REC_UNITS'], converters={0:np.int32, 1:str, 2: str, 3:str, 4:str, 5:str, 8:np.int64})
pdRec['REC_DATE'] = pdRec['REC_DATE'].apply(lambda x: dt.strptime(x, '%m/%d/%Y').date())
pdRec['REC_NBR'] = pdRec['REC_NBR'].apply(lambda x: x.zfill(6))
pdRec['HFC'] = pdRec['HFC'].apply(lambda x: x.zfill(6))
pdRec['STYLE'] = pdRec['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
pdRec['COLOR'] = pdRec['COLOR'].apply(lambda x: x.zfill(3))

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
#a = 'select HFC_NBR, STYLE, COLOR_CODE from dbo.HFC_DETAIL'
#pd.read_sql(a, con=conn).to_excel('2.xls')
pdRec.to_sql('#temp_receiving', con=conn, if_exists='replace', index=False)
trans = conn.begin()
try:
    conn.execute("""MERGE DBO.RECEIVING AS T 
                 USING dbo.#temp_receiving AS S 
                 ON (T.CTRL_NBR=S.CTRL_NBR and T.HFC_NBR = S.HFC and T.STYLE=S.STYLE and T.COLOR=S.COLOR) 
                 WHEN MATCHED THEN UPDATE
                 SET T.REC_NBR = S.REC_NBR, T.REC_DATE = S.REC_DATE, T.REC_UNITS = S.REC_UNITS 
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (CTRL_NBR, HFC_NBR, STYLE, COLOR, REC_NBR, REC_DATE, REC_UNITS) VALUES
                 (S.CTRL_NBR, S.HFC, S.STYLE, S.COLOR, S.REC_NBR, S.REC_DATE, S.REC_UNITS);""")
    trans.commit()
    conn.close()
    engine.dispose()          
    print ('RECEIVING TABLE UPDATED SUCCESSFULLY!')
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    trans.rollback()
    conn.close()
    engine.dispose()