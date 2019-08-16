# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    this program uploads the UPCOMING CONTAINERS excel file received in email from Glenda every Friday into SQL table HFC_CONTAINER
    It also read manually maintained file 'SOURCE_CONTAINER_RECVD.xlsx' to update the RECVD indicator (1 means received)
Note: save the Excel attachment to the right location 'W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SOURCE_UPCOMING_CONTAINERS.xlsx'
        everytime a receiving is done, 'SOURCE_CONTAINER_RECVD.xlsx' needs to get updated
PREREQUISITE:
    SQL table HFC_HEADER has to be loaded before running this program
IMPORTANT (data pitfall):
    due to the nature of data source being manually maintained by Import in Excel, typos are often and unpredicatable.
    although this program built in as many data integrity checks as it could, there is one important typo it would NOT be able to catch: 
        if a container number was initially typed as "ABCD1234567" but later changed to "ABCE1234567", or any change that was made to a container# that fit
        the format, the program would READ THE UPDATED CONTAINER# AS A NEW CONTAINER and upload it twice.
    it is therefore important to always check the total carton count that pulled back is correct at the end of the program
"""

import pandas as pd
import numpy as np
#import pyodbc
import sqlalchemy
import logging
import sys
import re
from datetime import datetime

"""
from excel source create a dataframe that maps to HFC_CONTAINER table
data quality control#1: no null HFC
"""
pd_container = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SOURCE_UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
                             usecols=[2,4,5,6],names=['ETA', 'CONTAINER_NBR', 'HFC_NBR', 'CARTON_CTN'], converters={1: np.str, 2: np.str, 3: np.int32})
pd_container = pd_container.iloc[:-1]

if sum(pd_container['HFC_NBR'].isnull()) > 0:
    print ('WARNING: There are null HFC values in the data source!')
    sys.exit('FIX ABOVE MENTIONED ERROR(S)')

pd_container['HFC_NBR'] = pd_container['HFC_NBR'].apply(lambda x:x.zfill(6))
pd_container['CONTAINER_NBR'] = pd_container['CONTAINER_NBR'].fillna(method='ffill')
pd_container['CONTAINER_NBR'] = pd_container['CONTAINER_NBR'].apply(lambda x: x.upper())
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
pd_container_ttl = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SOURCE_UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
                             usecols=[2,4,7],names=['ETA', 'CONTAINER_NBR', 'TTL_CTN'])
pd_container_ttl = pd_container_ttl.iloc[:-1]
pd_container_ttl.dropna(inplace=True)
pd_container_ttl['ETA'] = pd_container_ttl['ETA'].dt.date
pd_container_ttl['CONTAINER_NBR'] = pd_container_ttl['CONTAINER_NBR'].apply(lambda x: x.upper())
pd_container_ttl.reset_index(drop=True, inplace=True)

pattern = re.compile('^[A-Z]{4}[0-9]{7}$')
for n in range(pd_container_ttl.shape[0]):
    if (pd_container_ttl['CONTAINER_NBR'][n] != 'AIR') and (pd_container_ttl['CONTAINER_NBR'][n] != 'FEDEX'):
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
pdRec = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SOURCE_CONTAINER_RECVD.xlsx', converters={1: str})
pdRec['HFC'] = pdRec['HFC'].apply(lambda x: x.zfill(6))

conn = engine.connect()
pd_container.to_sql(name='#temp_hfc_container', con=conn, if_exists='replace', index=False)
pdRec.to_sql(name='#temp_container_rec', con=conn, if_exists='replace', index=False)
             
trans = conn.begin()
try:
    conn.execute("""MERGE DBO.HFC_CONTAINER AS T 
                 USING dbo.#temp_hfc_container AS S 
                 ON (T.HFC_NBR = S.HFC_NBR and T.CONTAINER_NBR = S.CONTAINER_NBR) 
                 WHEN MATCHED THEN UPDATE SET T.CARTON_CTN=S.CARTON_CTN, T.ETA=S.ETA 
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (HFC_NBR, CONTAINER_NBR, CARTON_CTN, ETA, RECVD) VALUES (S.HFC_NBR, S.CONTAINER_NBR, S.CARTON_CTN, S.ETA, 0);""")
    conn.execute("""UPDATE T SET T.RECVD = 1 FROM DBO.HFC_CONTAINER AS T JOIN #temp_container_rec AS S ON (T.CONTAINER_NBR = S.CONT AND T.HFC_NBR = S.HFC)""")
    trans.commit()
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    trans.rollback()
    conn.close()
    engine.dispose()

pd_container_ttl.to_sql(name='#temp_container_ttl', con=conn, if_exists='replace', index=False)
after_update = '''
               SELECT CONTAINER_NBR, ETA, SUM (CARTON_CTN) AS TTL_CARTON 
               FROM DBO.HFC_CONTAINER WHERE CONTAINER_NBR IN (SELECT DISTINCT CONTAINER_NBR FROM dbo.#temp_container_ttl)
               GROUP BY CONTAINER_NBR, ETA
               '''  
pd_container_ttl_after = pd.read_sql(after_update, con=conn)

# pull total carton count from database
review_ttl = '''select sum(carton_ctn) as ctn from dbo.HFC_CONTAINER'''
ttl_carton = pd.read_sql(review_ttl, con=conn)
ttl_carton = ttl_carton.iloc[0,0]

conn.close()
engine.dispose()

#                        
#conn = pyodbc.connect('Driver={SQL Server};'
#                  'Server=DESKTOP-5JROCDL\SQLEXPRESS;'
#                  'Database=roytexdb;'
#                  'Trusted_Connection=yes;')
#c = conn.cursor()  
#
#try:
#    c.execute('''
#              MERGE DBO.HFC_CONTAINER AS T
#              USING temp_hfc_container AS S
#              ON (T.HFC_NBR = S.HFC_NBR and T.CONTAINER_NBR = S.CONTAINER_NBR)
#              WHEN MATCHED THEN
#              UPDATE SET T.CARTON_CTN=S.CARTON_CTN, T.ETA=S.ETA
#              WHEN NOT MATCHED BY TARGET THEN
#              INSERT (HFC_NBR, CONTAINER_NBR, CARTON_CTN, ETA) VALUES (S.HFC_NBR, S.CONTAINER_NBR, S.CARTON_CTN, S.ETA);
#              ''')
#    c.execute('''drop table temp_hfc_container;''')
#    after_update = '''
#                   SELECT CONTAINER_NBR, ETA, SUM (CARTON_CTN) AS TTL_CARTON 
#                   FROM DBO.HFC_CONTAINER WHERE CONTAINER_NBR IN (SELECT DISTINCT CONTAINER_NBR FROM dbo.temp_container_ttl)
#                   GROUP BY CONTAINER_NBR, ETA
#                   '''
#    pd_container_ttl_after = pd.read_sql(after_update, conn)
#    c.execute('''drop table temp_container_ttl;''')
#    conn.commit()
#    conn.close()
#except Exception as e:
#    logger = logging.Logger('Catch_All')
#    logger.error(str(e))
#    conn.close()

pd_container_ttl_after['ETA'] = pd_container_ttl_after['ETA'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())
pd_validate = pd.merge(pd_container_ttl, pd_container_ttl_after, how='left', 
                       left_on=['CONTAINER_NBR', 'ETA'], right_on=['CONTAINER_NBR', 'ETA'])
for n in range(pd_validate.shape[0]):
    if pd_validate['TTL_CTN'][n] - pd_validate['TTL_CARTON'][n] != 0:
        print ('WARNING: Container ' + pd_validate['CONTAINER_NBR'][n] + ' with ETA ' + pd_validate['ETA'][n].strftime('%Y-%m-%d') + ' has error!')
        sys.exit('FIX ABOVE MENTIONED ERROR(S)')

print ('UPDATE COMPLETED SUCCESSFULLY!')
print ('MAKE SURE TOTAL CARTON COUNT OF ' + str(ttl_carton) + ' IS CORRECT!')



