# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 13:05:13 2019

@author: PBu
"""

import pandas as pd
#import numpy as np
import os
import sqlalchemy
import logging
import sys

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()

pdHeader = pd.read_excel('SOURCE_COMMERCIAL_INVOICE.xlsx', 'INVOICE_HEADER')
pdDetail = pd.read_excel('SOURCE_COMMERCIAL_INVOICE.xlsx', 'INVOICE_DETAIL', converters={0:str, 1:str, 2:str, 3:int})
pdDetail['LINE_AMT'] = (pdDetail['PRICE']+pdDetail['HANGER_COST'])*pdDetail['PCS']

### data quality check - make sure HFC/STYLE listed on factory's commercial invoices are valid
HFC_STYLE_SQL = '''select distinct HFC_NBR, STYLE from dbo.HFC_DETAIL where cxl = 0'''
validHfcStyle = pd.read_sql(HFC_STYLE_SQL, con=conn)
validHfcStyle.rename(columns={'HFC_NBR':'HFC'}, inplace=True)
validHfcStyle['VALID'] = 1
pdDetail_check = pd.merge(pdDetail, validHfcStyle, how='left', on=['HFC', 'STYLE'])
pdDetail_check = pdDetail_check[pdDetail_check['VALID'].isnull()]
if pdDetail_check.shape[0] != 0:
    print(pdDetail_check)
    conn.close()
    engine.dispose()
    sys.exit('HFC/Style combo in above invoice(s) are not valid. Please review and fix!')

pdHeader.to_sql('#temp_invoice_header', con=conn, if_exists='replace', index=False)
pdDetail.to_sql('#temp_invoice_detail', con=conn, if_exists='replace', index=False)
trans=conn.begin()
try:
    conn.execute("""MERGE DBO.INVOICE_HEADER AS T
                 USING #temp_invoice_header AS S
                 ON (T.INVOICE_NBR = S.INVOICE_NBR)
                 WHEN MATCHED THEN UPDATE
                 SET T.LC_NBR = S.LC_NBR, T.DN_NBR = S.DN_NBR, T.DN_AMT=S.DN_AMT
                 WHEN NOT MATCHED BY TARGET THEN
                 INSERT (INVOICE_NBR, LC_NBR, DN_NBR, DN_AMT) VALUES (S.INVOICE_NBR, S.LC_NBR, S.DN_NBR, S.DN_AMT);""")
    conn.execute("""MERGE DBO.INVOICE_DETAIL AS T
                 USING #temp_invoice_detail AS S
                 ON (T.INVOICE_NBR = S.INVOICE_NBR and T.HFC=S.HFC and T.STYLE=S.STYLE)
                 WHEN MATCHED THEN UPDATE
                 SET T.PCS=S.PCS, T.PRICE=S.PRICE, T.HANGER_COST=S.HANGER_COST, T.LINE_AMT=S.LINE_AMT
                 WHEN NOT MATCHED BY TARGET THEN
                 INSERT (INVOICE_NBR, HFC, STYLE, PCS, PRICE, HANGER_COST, LINE_AMT) VALUES (S.INVOICE_NBR, S.HFC, S.STYLE, S.PCS, S.PRICE, S.HANGER_COST, S.LINE_AMT);""")
    trans.commit()
    print('INVOICE_HEADER & INVOICE_DETAIL tables have been updated')
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    trans.rollback()
    conn.close()
    engine.dispose()
    


