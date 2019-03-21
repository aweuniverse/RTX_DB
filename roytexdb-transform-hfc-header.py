# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 15:48:50 2018

@author: PBu
"""

import pandas as pd
import pyodbc
import sqlalchemy as db
import logging


hfc_header = pd.read_csv('W:\Roytex - The Method\Ping\ROYTEXDB\BUYSUMMARY.csv', header=0, usecols=[0, 2, 3, 4,6, 10, 15, 35, 37, 39],
                         names = ['div', 'cust', 'ssn', 'hfc', 'x_orient', 'x_ship', 'cat', 'agent', 'coo', 'carton_size'])

hfc_header = hfc_header.drop_duplicates(keep='first').reset_index(drop=True)

hfc_header['cust'] = hfc_header['cust'].astype(str)
hfc_header['hfc'] = hfc_header['hfc'].astype(str).apply(lambda x: x.zfill(6))
#
#hfc_header.to_csv('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SQL_HFC_HEADER.csv', index=False)

engine = db.create_engine("mssql+pyodbc://@sqlDSN")
hfc_header.to_sql(name='temp_hfc_header', con=engine, if_exists='replace', index=False)

conn = pyodbc.connect('Driver={SQL Server};'
                  'Server=DESKTOP-5JROCDL\SQLEXPRESS;'
                  'Database=roytexdb;'
                  'Trusted_Connection=yes;')
c = conn.cursor()  

try:
    c.execute('''
              MERGE DBO.HFC_HEADER AS T
              USING temp_hfc_header AS S
              ON (T.HFC_NBR = S.hfc)
              WHEN MATCHED THEN
              UPDATE SET T.DIV = S.div, T.CUST_NBR = S.cust, T.SEASON = S.ssn, T.X_ORIENT = S.x_orient, T.SHIP_DATE = S.x_ship, T.CAT = S.cat, T.AGENT = S.agent, T.COO = S.coo, T.CARTON_SIZE = S.carton_size, T.CXL = 0
              WHEN NOT MATCHED BY TARGET THEN
              INSERT (DIV, CUST_NBR, SEASON, HFC_NBR, X_ORIENT, SHIP_DATE, CAT, AGENT, COO, CARTON_SIZE)  VALUES (S.div, S.cust, S.ssn, S.hfc, S.x_orient, S.x_ship, S.cat, S.agent, S.coo, S.carton_size)
              WHEN NOT MATCHED BY SOURCE AND T.SEASON IN (SELECT DISTINCT ssn FROM temp_hfc_header) THEN
              UPDATE SET T.CXL = 1 ;
              ''')
    c.execute('''drop table temp_hfc_header;''')
    conn.commit()
    conn.close()
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    conn.close()