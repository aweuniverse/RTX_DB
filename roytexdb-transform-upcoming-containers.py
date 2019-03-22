# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 10:10:59 2019

@author: PBu

"""

import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy
import logging
import sys
import re
from datetime import datetime


"""
from excel source create a dataframe that maps to HFC_CONTAINER table
data quality control#1: no null HFC
"""
pd_container = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
                             usecols=[2,3,4,5],names=['ETA', 'CONTAINER_NBR', 'HFC_NBR', 'CARTON_CTN'], converters={1: np.str, 2: np.str, 3: np.int32})
pd_container = pd_container.iloc[:-1]

if sum(pd_container['HFC_NBR'].isnull()) > 0:
    print ('WARNING: There are null HFC values in the data source!')
    sys.exit('FIX ABOVE MENTIONED ERROR(S)')

pd_container['HFC_NBR'] = pd_container['HFC_NBR'].apply(lambda x:x.zfill(6))
pd_container['CONTAINER_NBR'] = pd_container['CONTAINER_NBR'].fillna(method='ffill')
pd_container['ETA'] = pd_container['ETA'].fillna(method='ffill')
pd_container['ETA'] = pd_container['ETA'].dt.date
pd_container.reset_index(drop=True, inplace=True)


"""
data quality control#2 - read all valid HFC_NBR from SQL and make sure pd_container has one of the valid HFC numbers
***this requires that all the HFCs be loaded in SQL HFC_HEADER table first
"""
engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
pd_valid_hfc = pd.read_sql_table('HFC_HEADER', con=engine, columns = ['HFC_NBR'])

if sum(~pd_container['HFC_NBR'].isin(pd_valid_hfc['HFC_NBR'])) > 0:
    pd_exception = pd_container['HFC_NBR'].isin(pd_valid_hfc['HFC_NBR']).to_frame()
    exception_index = pd_exception[pd_exception['HFC_NBR']==False].index.values.tolist()
    for n in range(len(exception_index)):
        print ('WARNING: ' + pd_container['HFC_NBR'].iloc[exception_index[n]] + ' in the container file is not a valid HFC')
    sys.exit('FIX ABOVE MENTIONED ERROR(S)')


"""
from the same excel source create a dataframe that gets total carton count by container
data quality control#3: container number fits format ^[A-Z]{4}[0-9]{7}$ if it's not AIR
"""
pd_container_ttl = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
                             usecols=[2,3,6],names=['ETA', 'CONTAINER_NBR', 'TTL_CTN'])
pd_container_ttl = pd_container_ttl.iloc[:-1]
pd_container_ttl.dropna(inplace=True)
pd_container_ttl['ETA'] = pd_container_ttl['ETA'].dt.date
pd_container_ttl.reset_index(drop=True, inplace=True)

pattern = re.compile('^[A-Z]{4}[0-9]{7}$')
for n in range(pd_container_ttl.shape[0]):
    if pd_container_ttl['CONTAINER_NBR'][n] != 'AIR':
        if not pattern.match(pd_container_ttl['CONTAINER_NBR'][n]):
            print ('WARNING: CONTAINER NBR '+ pd_container_ttl['CONTAINER_NBR'][n] + ' is invalid!')
            sys.exit('FIX ABOVE MENTIONED ERROR(S)')

#pd_container_ttl = pd.pivot_table(pd_container_ttl, index=['CONTAINER_NBR'], aggfunc=np.sum).reset_index()

"""
Once all data quality checks passed, proceed to update SQL HFC_CONTAINER table
Quality control#4: 
Once HFC_CONTAINER table updated, read updated total carton count by container and compare it to the original pd_container_ttl table
if different, raise error 
"""
pd_container.to_sql(name='temp_hfc_container', con=engine, if_exists='replace', index=False)
pd_container_ttl.to_sql(name='temp_container_ttl', con=engine, if_exists='replace', index=False)

conn = pyodbc.connect('Driver={SQL Server};'
                  'Server=DESKTOP-5JROCDL\SQLEXPRESS;'
                  'Database=roytexdb;'
                  'Trusted_Connection=yes;')
c = conn.cursor()  

try:
    c.execute('''
              MERGE DBO.HFC_CONTAINER AS T
              USING temp_hfc_container AS S
              ON (T.HFC_NBR = S.HFC_NBR and T.CONTAINER_NBR = S.CONTAINER_NBR)
              WHEN MATCHED THEN
              UPDATE SET T.CARTON_CTN=S.CARTON_CTN, T.ETA=S.ETA
              WHEN NOT MATCHED BY TARGET THEN
              INSERT (HFC_NBR, CONTAINER_NBR, CARTON_CTN, ETA) VALUES (S.HFC_NBR, S.CONTAINER_NBR, S.CARTON_CTN, S.ETA);
              ''')
    c.execute('''drop table temp_hfc_container;''')
    after_update = '''
                   SELECT CONTAINER_NBR, ETA, SUM (CARTON_CTN) AS TTL_CARTON 
                   FROM DBO.HFC_CONTAINER WHERE CONTAINER_NBR IN (SELECT DISTINCT CONTAINER_NBR FROM dbo.temp_container_ttl)
                   GROUP BY CONTAINER_NBR, ETA
                   '''
    pd_container_ttl_after = pd.read_sql(after_update, conn)
    c.execute('''drop table temp_container_ttl;''')
    conn.commit()
    conn.close()
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    conn.close()

pd_container_ttl_after['ETA'] = pd_container_ttl_after['ETA'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())
pd_validate = pd.merge(pd_container_ttl, pd_container_ttl_after, how='left', 
                       left_on=['CONTAINER_NBR', 'ETA'], right_on=['CONTAINER_NBR', 'ETA'])
for n in range(pd_validate.shape[0]):
    if pd_validate['TTL_CTN'][n] - pd_validate['TTL_CARTON'][n] != 0:
        print ('WARNING: Container ' + pd_validate['CONTAINER_NBR'][n] + ' with ETA ' + pd_validate['ETA'][n].strftime('%Y-%m-%d') + ' has error!')
        sys.exit('FIX ABOVE MENTIONED ERROR(S)')

print ('UPDATE COMPLETED SUCCESSFULLY!')


