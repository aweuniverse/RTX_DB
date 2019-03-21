# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 10:10:59 2019

@author: PBu

log 3/19/2019
need data input control:
    1), contorl container# fits a certain format
    2), control HFC# is valid (FK references HFC_HEADER table)
    3), control that sum (CARTON_CTN) by container in HFC_CONTAINER table matches TTL_CARTON in CONTAINER table

"""

import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy
import logging
import sys


"""
from excel source create a dataframe that maps to HFC_CONTAINER table
"""
pd_container = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
                             usecols=[3,4,5],names=['CONTAINER_NBR', 'HFC_NBR', 'CARTON_CTN'], converters={0: np.str, 1: np.str, 2: np.int32})
pd_container = pd_container.iloc[:-1]

if sum(pd_container['HFC_NBR'].isnull()) > 0:
    print ('WARNING: There are null HFC values in the data source!')
    sys.exit('FIX ABOVE MENTIONED ERROR(S)')

pd_container['HFC_NBR'] = pd_container['HFC_NBR'].apply(lambda x:x.zfill(6))
pd_container['CONTAINER_NBR'] = pd_container['CONTAINER_NBR'].fillna(method='ffill')
pd_container.reset_index(drop=True, inplace=True)

"""
from the same excel source create a dataframe that maps to CONTAINER table
"""
pd_container_ttl = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
                             usecols=[2,3,6],names=['ETA', 'CONTAINER_NBR', 'TTL_CTN'])
pd_container_ttl = pd_container_ttl.iloc[:-1]
pd_container_ttl.dropna(inplace=True)
pd_container_ttl['ETA'] = pd_container_ttl['ETA'].dt.date
pd_container_ttl.reset_index(drop=True, inplace=True)

"""
data input control #1 - read all valid HFC_NBR from SQL and make sure pd_container has one of the valid HFC numbers
"""
engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
pd_valid_hfc = pd.read_sql_table('HFC_HEADER', con=engine, columns = ['HFC_NBR'])

if sum(~pd_container['HFC_NBR'].isin(pd_valid_hfc['HFC_NBR'])) > 0:
    pd_exception = pd_container['HFC_NBR'].isin(pd_valid_hfc['HFC_NBR']).to_frame()
    exception_index = pd_exception[pd_exception['HFC_NBR']==False].index.values.tolist()
    for n in range(len(exception_index)):
        print ('WARNING: ' + pd_container['HFC_NBR'].iloc[exception_index[n]] + ' in the container file is not a valid HFC')
    sys.exit('FIX ABOVE MENTIONED ERROR(S)')

#pd_container.to_sql(name='temp_hfc_container', con=engine, if_exists='replace', index=False)
#
#
#conn = pyodbc.connect('Driver={SQL Server};'
#                  'Server=DESKTOP-5JROCDL\SQLEXPRESS;'
#                  'Database=roytexdb;'
#                  'Trusted_Connection=yes;')
#c = conn.cursor()  
#
##c.execute('''
##         create table #temp_hfc_container
##         (ETA date not null, CONTAINER_NBR varchar(15) not null, HFC_NBR varchar(6) not null, CARTON_CTN int not null)
##         ''')
#
#try:
#    c.execute('''
#              MERGE DBO.HFC_CONTAINER AS T
#              USING temp_hfc_container AS S
#              ON (T.HFC_NBR = S.HFC_NBR and T.CONTAINER_NBR = S.CONTAINER_NBR)
#              WHEN MATCHED THEN
#              UPDATE SET T.ETA = S.ETA, T.CARTON_CTN=S.CARTON_CTN
#              WHEN NOT MATCHED BY TARGET THEN
#              INSERT (HFC_NBR, CONTAINER_NBR, ETA, CARTON_CTN) VALUES (S.HFC_NBR, S.CONTAINER_NBR, S.ETA, S.CARTON_CTN);
#              ''')
#    c.execute('''drop table temp_hfc_container;''')
#    conn.commit()
#    conn.close()
#except Exception as e:
##    c.execute('''drop table temp_hfc_container;''')
##    conn.commit()
#    logger = logging.Logger('Catch_All')
#    logger.error(str(e))
#    conn.close()
#
#
#
