# -*- coding: utf-8 -*-
"""
STATEMENT OF PURPOSE:
    Run this program will update SQL table CUST_ORDER using two sources:
        ~any file with the naming format 'SOURCE_CUST_ORDER_XX-XX.csv' located in ROYTEXDB folder (10.40.44 for a specific season)
        ~"SOURCE_OO.csv" (20.52 for all open orders)
    IT'S IMPORTANT that the 'SOURCE_CUST_ORDER_XX-XX.csv' files and 'SOURCE_OO.csv' file are pulled from Procomm at the same time
PRE-REQUISITE:
    SQL table STYLE_MASTER and CUSTOMER need to be fully loaded
"""

import pandas as pd
import numpy as np
from datetime import datetime as dt
import sqlalchemy
import sys
import os
import logging

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')

def oneSeasonOrder (oneSeason):
    pdOrder = pd.read_csv(oneSeason, skiprows=1, header=None, usecols=[0, 2, 4, 5, 6, 7, 8, 10, 11, 13, 14, 16, 18, 19, 21, 22], 
                          names=['DIV', 'CUST_NBR', 'CUST_PO', 'GREEN_BAR', 'ORDER_DATE', 'START_SHIP', 'CXL_SHIP', 'SEASON', 'STYLE', 'COLOR', 'SP', 'ORDERED_UNITS', 'SHIPPED_UNITS', 'SHIP_ON_DATE', 'PICK_UNITS', 'UNCONFIRMED'],
                          converters={0:np.int16, 2: str, 4: str, 5: str, 6: str, 7: str, 8: str, 10: str, 11: str, 13: str, 14: np.float64, 16: np.int64, 18: np.int64, 19: str, 21: np.int64, 22: str})
    pdOrder['CUST_NBR'] = pdOrder['CUST_NBR'].apply(lambda x: x.zfill(5))
    pdOrder['CUST_PO'] = pdOrder['CUST_PO'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
    pdOrder['ORDER_DATE'] = pdOrder['ORDER_DATE'].apply(lambda x: dt.strptime(x, '%m/%d/%Y').date())
    pdOrder['START_SHIP'] = pdOrder['START_SHIP'].apply(lambda x: dt.strptime(x, '%m/%d/%Y').date())
    pdOrder['CXL_SHIP'] = pdOrder['CXL_SHIP'].apply(lambda x: dt.strptime(x, '%m/%d/%Y').date())
    pdOrder['SHIP_ON_DATE'] = pdOrder['SHIP_ON_DATE'].apply(lambda x: dt.strptime(x, '%m/%d/%Y').date() if x != '' else pd.NaT)
    pdOrder['SEASON'] = pdOrder['SEASON'].apply(lambda x: x[:2].upper() + '-' + x[-2:])
    pdOrder['UNCONFIRMED'] = pdOrder['UNCONFIRMED'].apply(lambda x: 0 if x == '' else 1)
    pdOrder['STYLE'] = pdOrder['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
    pdOrder['COLOR'] = pdOrder['COLOR'].apply(lambda x: x.zfill(3))   
    return pdOrder

def multiSeasonOrder ():
    fileList = [each for each in os.listdir() if each[:17] == 'SOURCE_CUST_ORDER']
    if len(fileList) >0:
        for n in range(len(fileList)):
            if n == 0:
                multiSeason = oneSeasonOrder(fileList[n])
            else:
                addone = oneSeasonOrder(fileList[n])
                multiSeason = multiSeason.append(addone)
        pdOO = pd.read_csv('SOURCE_OO.csv', skiprows=1, header=None, usecols=[4, 10, 12, 16, 17, 19, 20, 21, 22, 23, 24, 25, 26], 
                   names=['GREEN_BAR', 'STYLE', 'COLOR', 'SHIPPED', 'BAL', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8'], converters={4: str, 10: str, 12: str})
        pdOO['STYLE'] = pdOO['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
        pdOO['COLOR'] = pdOO['COLOR'].apply(lambda x: x.zfill(3))
        newMulti = pd.merge(multiSeason, pdOO[['GREEN_BAR', 'STYLE', 'COLOR', 'BAL']], how='left', on=['GREEN_BAR', 'STYLE', 'COLOR'])
        newMulti['BAL'].fillna(0, inplace=True)
        newMulti['COMMENT'] = newMulti.apply(lambda row: 'DEACTIVE' if ((row.SHIPPED_UNITS == 0) & (pd.isnull(row.SHIP_ON_DATE) == False) & (row.BAL == 0))
                else 'COMPLETE' if ((row.SHIPPED_UNITS >= row.ORDERED_UNITS) & (pd.isnull(row.SHIP_ON_DATE) == False))
                else 'PARTIAL' if ((row.BAL > 0) & (pd.isnull(row.SHIP_ON_DATE) == False))
                else 'ACTIVE', axis=1)
        
        ###ADD CODE TO IDENTIFY ORDER CATEGORY: UPFRONT, OR OFF-PRICE###
        
        pdOrder = pdOO[(pdOO['SHIPPED'] == 0) & (pdOO['BAL'] > 0)]
        pdOrder = pdOrder[['GREEN_BAR', 'STYLE', 'COLOR', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8']]
        
        pdPartial = pdOO[(pdOO['SHIPPED'] > 0) & (pdOO['BAL'] > 0)]
        pdPartial = pdPartial[['GREEN_BAR', 'STYLE', 'COLOR', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8']]
       
#        return newMulti
        engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
        conn = engine.connect()
        newMulti.to_sql('#temp_cust_order', con=conn, if_exists='replace', index=False)
        pdOrder.to_sql('#temp_full_order', con=conn, if_exists='replace', index=False)
        pdPartial.to_sql('#temp_partial_order', con=conn, if_exists='replace', index=False)
        trans = conn.begin()
        try:
            conn.execute("""MERGE DBO.CUST_ORDER AS T 
                         USING dbo.#temp_cust_order AS S 
                         ON (T.GREEN_BAR = S.GREEN_BAR and T.STYLE = S.STYLE and T.COLOR = S.COLOR)
                         WHEN MATCHED THEN UPDATE
                         SET T.DIV=S.DIV, T.CUST_NBR=S.CUST_NBR, T.ORDER_DATE=S.ORDER_DATE, T.START_SHIP=S.START_SHIP, T.CXL_SHIP=S.CXL_SHIP, T.CUST_PO=S.CUST_PO, 
                         T.SEASON=S.SEASON, T.SP=S.SP, T.ORDERED_UNITS=S.ORDERED_UNITS, T.SHIPPED_UNITS=S.SHIPPED_UNITS, T.BAL_UNITS=S.BAL, T.PICK_UNITS=S.PICK_UNITS, 
                         T.SHIP_ON_DATE = S.SHIP_ON_DATE, T.UNCONFIRMED = S.UNCONFIRMED, T.COMMENT=S.COMMENT, T.CXL=0 
                         WHEN NOT MATCHED BY TARGET THEN 
                         INSERT (GREEN_BAR, STYLE, COLOR, DIV, CUST_NBR, ORDER_DATE, START_SHIP, CXL_SHIP, CUST_PO, SEASON, SP, ORDERED_UNITS, SHIPPED_UNITS, BAL_UNITS, PICK_UNITS, SHIP_ON_DATE, UNCONFIRMED, COMMENT) VALUES
                         (S.GREEN_BAR, S.STYLE, S.COLOR, S.DIV, S.CUST_NBR, S.ORDER_DATE, S.START_SHIP, S.CXL_SHIP, S.CUST_PO, S.SEASON, S.SP, S.ORDERED_UNITS, S.SHIPPED_UNITS, S.BAL, S.PICK_UNITS, S.SHIP_ON_DATE, S.UNCONFIRMED, S.COMMENT)
                         WHEN NOT MATCHED BY SOURCE AND (T.SEASON IN (select distinct SEASON from #temp_cust_order)) THEN
                         UPDATE SET T.CXL=1;""")
            conn.execute("""UPDATE T
                         SET T.ORDERED_S1 = S.S1, T.ORDERED_S2 = S.S2, T.ORDERED_S3 = S.S3, T.ORDERED_S4 = S.S4, T.ORDERED_S5 = S.S5, T.ORDERED_S6 = S.S6, T.ORDERED_S7 = S.S7, T.ORDERED_S8 = S.S8, 
                         T.BAL_S1 = S.S1, T.BAL_S2 = S.S2, T.BAL_S3 = S.S3, T.BAL_S4 = S.S4, T.BAL_S5 = S.S5, T.BAL_S6 = S.S6, T.BAL_S7 = S.S7, T.BAL_S8 = S.S8
                         FROM DBO.CUST_ORDER AS T INNER JOIN #temp_full_order AS S ON (S.GREEN_BAR = T.GREEN_BAR and S.STYLE = T.STYLE and S.COLOR = T.COLOR);""") 
            conn.execute("""UPDATE T
                         SET T.BAL_S1 = S.S1, T.BAL_S2 = S.S2, T.BAL_S3 = S.S3, T.BAL_S4 = S.S4, T.BAL_S5 = S.S5, T.BAL_S6 = S.S6, T.BAL_S7 = S.S7, T.BAL_S8 = S.S8
                         FROM DBO.CUST_ORDER AS T INNER JOIN #temp_partial_order AS S ON (S.GREEN_BAR = T.GREEN_BAR and S.STYLE = T.STYLE and S.COLOR = T.COLOR);""")
            conn.execute("""UPDATE T
                         SET T.BAL_S1 = 0, T.BAL_S2 = 0, T.BAL_S3 = 0, T.BAL_S4 = 0, T.BAL_S5 = 0, T.BAL_S6 = 0, T.BAL_S7 = 0, T.BAL_S8 = 0
                         FROM DBO.CUST_ORDER AS T WHERE T.COMMENT = 'COMPLETE';""")
            trans.commit()
            print ('UPDATE COMPLETED SUCCESSFULLY!')
            conn.close()
            engine.dispose()
        except Exception as e:
            logger = logging.Logger('Catch_All')
            logger.error(str(e))
            trans.rollback()
            conn.close()
            engine.dispose()
    else:
        sys.exit('There is no "SOURCE_CUST_ORDER" file to read')

multiSeasonOrder()
