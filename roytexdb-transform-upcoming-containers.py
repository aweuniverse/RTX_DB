# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    this program uploads THREE SQL TABLES at once:
    1), updates SHORT_OVER table using file 'SOURCE_SHORT-OVER.xlsx'. This table is being used to auto calculate CTN_TO_COME column in the HFC_CONTAINER table
    2), updates RECEIVING table using file 'SOURCE_RECEIVING_NEW.xlsx'
    3), updates HFC_CONTAINER table using A), UPCOMING CONTAINERS excel file received in email from Glenda every Friday
                                          B), 'SOURCE_RECEIVING_NEW.xlsx' (2 TABS) to update RECVD, INVOICE_NBR, & WHSE_REC_DATE columns
Note: 1), save the Excel attachment to the right location 'W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SOURCE_UPCOMING_CONTAINERS.xlsx'
      2), everytime a receiving is done, or Sharon sends an updated container report, 'SOURCE_RECEIVING_NEW.xlsx' needs to get updated
PREREQUISITE:
    SQL table HFC_HEADER has to be loaded before running this program
    SQL table SHORT_OVER needs to be up-to-date so that CTN_TO_COME column can calculate correctly (embedded in as a module and run at the beginning)
IMPORTANT (data pitfall):
    due to the nature of data source being manually maintained by Import in Excel, typos are often and unpredicatable.
    although this program built in as many data integrity checks as it could, there is one important typo it would NOT be able to catch: 
        if a container number was initially typed as "ABCD1234567" but later changed to "ABCE1234567", or any change that was made to a container# that fit
        the format, the program would READ THE UPDATED CONTAINER# AS A NEW CONTAINER and upload it twice.
    it is therefore important to always check the total carton count that pulled back is correct at the end of the program
"""

import pandas as pd
import numpy as np
import os
#import pyodbc
import sqlalchemy
import logging
import sys
import re
from datetime import datetime as dt

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')
engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()

"""
1), as a pre-requisite, update SHORT_OVER table so later CTN_TO_COME field can be correctly calculated
"""
def update_short_over():
    over = pd.read_excel('SOURCE_SHORT-OVER.xlsx', converters={0:str, 1:str, 2:str})
    over.to_sql('#temp_over_short', con=conn, if_exists='replace', index=False)
    trans = conn.begin()
    try:
        conn.execute("""
                     MERGE DBO.SHORT_OVER AS T
                     USING #temp_over_short AS S
                     ON (T.HFC = S.PO AND T.STYLE = S.STYLE AND T.COLOR=S.COLOR)
                     WHEN MATCHED THEN UPDATE SET T.DIFF = S.DIFF
                     WHEN NOT MATCHED BY TARGET THEN
                     INSERT (HFC, STYLE, COLOR, DIFF) VALUES (S.PO, S.STYLE, S.COLOR, S.DIFF);""")
        trans.commit()
        print ('SHORT_OVER TABLE UPDATED')
    except Exception as e:
        logger = logging.Logger('Catch_All')
        logger.error(str(e))
        trans.rollback()
        conn.close()
        engine.dispose()
        sys.exit('ERROR ENCOUNTERED IN UPDATING SHORT_OVER TABLE')
update_short_over()

"""
2), update RECEIVING table
"""
pdRec = pd.read_excel('SOURCE_RECEIVING_NEW.xlsx', 'REC', converters={0:str, 1:str, 2: str, 3:str, 4:int, 5:int, 6:int, 7:int, 8:int, 9:int, 10:int, 11:int, 12:int, 13:int})
pdRec['REC_DATE'] = pdRec['REC_DATE'].apply(lambda x: x.date())
pdRec['HFC'] = pdRec['HFC'].apply(lambda x: x.zfill(6))
pdRec['STYLE'] = pdRec['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
pdRec['COLOR'] = pdRec['COLOR'].apply(lambda x: x.zfill(3))
pdRec.to_sql('#temp_receiving', con=conn, if_exists='replace', index=False)
trans = conn.begin()
try:
    conn.execute("""
                 MERGE DBO.RECEIVING AS T
                 USING #temp_receiving AS S
                 ON (T.HFC_NBR = S.HFC AND T.STYLE = S.STYLE AND T.COLOR=S.COLOR AND T.CTRL_NBR=S.CTRL_NBR AND T.CONTAINER_NBR=S.CONTAINER_NBR)
                 WHEN MATCHED THEN UPDATE SET T.REC_DATE = S.REC_DATE, T.REC_S1 = S.S1, T.REC_S2 = S.S2, T.REC_S3 = S.S3, T.REC_S4 = S.S4,
                 T.REC_S5 = S.S5, T.REC_S6 = S.S6, T.REC_S7 = S.S7, T.REC_S8 = S.S8, T.REC_UNITS = S.REC_UNITS
                 WHEN NOT MATCHED BY TARGET THEN
                 INSERT (CTRL_NBR, HFC_NBR, STYLE, COLOR, CONTAINER_NBR, REC_DATE, REC_S1, REC_S2, REC_S3, REC_S4, REC_S5, REC_S6, REC_S7, REC_S8, REC_UNITS)
                 VALUES (S.CTRL_NBR, S.HFC, S.STYLE, S.COLOR, S.CONTAINER_NBR, S.REC_DATE, S.S1, S.S2, S.S3, S.S4, S.S5, S.S6, S.S7, S.S8, S.REC_UNITS);""")
    trans.commit()
    print ('RECEIVING TABLE UPDATED')
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    trans.rollback()
    conn.close()
    engine.dispose()
    sys.exit('ERROR ENCOUNTERED IN UPDATING RECEIVING TABLE')

"""
from excel source create a dataframe that maps to HFC_CONTAINER table
data quality control#1: no null HFC
"""
pd_container = pd.read_excel('SOURCE_UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
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
pd_container_ttl = pd.read_excel('SOURCE_UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
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

"""
data quality control#5: 
make sure containers listed on Sharon's container report are those already in HFC_CONTAINER table (i.e. Glenda's report) to prevent typos
(No need to load any container that is AIR or FEDEX)
"""
distinct_container_sql = '''select distinct CONTAINER_NBR from dbo.HFC_CONTAINER'''
distinct_container = pd.read_sql(distinct_container_sql, con=conn)
rec_at_whse = pd.read_excel('SOURCE_RECEIVING_NEW.xlsx', 'WHSE', skiprows=1)
rec_at_whse['WHSE_REC_DATE'] = rec_at_whse['WHSE_REC_DATE'].apply(lambda x: x.date())
### nice syntax to check if one df includes all elements in another df
rec_at_whse = rec_at_whse.assign(Valid=rec_at_whse.CONTAINER_NBR.isin(distinct_container.CONTAINER_NBR).astype(int))
rec_exception = rec_at_whse[rec_at_whse['Valid'] == 0]
if rec_exception.shape[0] != 0:
    print(rec_exception)
    sys.exit("Container report from Sharon has typo. Please fix above error(s)")    

"""
Once all data quality checks passed, proceed to update SQL HFC_CONTAINER table
Quality control#4: 
Once HFC_CONTAINER table updated, read updated total carton count by container and compare it to the original pd_container_ttl table
if different, raise error 
"""
pdRec_2 = pdRec[['CONTAINER_NBR', 'HFC', 'INVOICE_NBR']].dropna().drop_duplicates().reset_index(drop=True)

pd_container.to_sql(name='#temp_hfc_container', con=conn, if_exists='replace', index=False)
pdRec_2.to_sql(name='#temp_container_rec', con=conn, if_exists='replace', index=False)
rec_at_whse.to_sql(name='#temp_whse_container', con=conn, if_exists='replace', index=False)
               
trans = conn.begin()
try:
    conn.execute("""MERGE DBO.HFC_CONTAINER AS T 
                 USING dbo.#temp_hfc_container AS S 
                 ON (T.HFC_NBR = S.HFC_NBR and T.CONTAINER_NBR = S.CONTAINER_NBR) 
                 WHEN MATCHED THEN UPDATE SET T.CARTON_CTN=S.CARTON_CTN, T.ETA = S.ETA
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (HFC_NBR, CONTAINER_NBR, CARTON_CTN, ETA, RECVD) VALUES (S.HFC_NBR, S.CONTAINER_NBR, S.CARTON_CTN, S.ETA, 0);""")
    conn.execute("""UPDATE T SET T.RECVD = 1, T.INVOICE_NBR = S.INVOICE_NBR FROM DBO.HFC_CONTAINER AS T JOIN #temp_container_rec AS S ON (T.CONTAINER_NBR = S.CONTAINER_NBR AND T.HFC_NBR = S.HFC)""")
    conn.execute("""UPDATE T SET T.WHSE_REC_DATE = S.WHSE_REC_DATE FROM DBO.HFC_CONTAINER T JOIN #temp_whse_container S ON T.CONTAINER_NBR = S.CONTAINER_NBR""")
    # below execution was added to automatically calculate CTN_TO_COME field 
    # CTN_TO_COME: positive number means more to come; 0 means shipped complete; negative number means over-shipped (should not happen)
    # code is set to look at only non-DIV8 FA-19 and SP-20 HFC's
    conn.execute("""UPDATE DBO.HFC_CONTAINER
                 SET CTN_TO_COME = T.SHIPPED_DIFF
                 FROM DBO.HFC_CONTAINER C INNER JOIN
                 (select D.HFC_NBR, SUM(D.TTL_QTY + ISNULL(S.DIFF, 0)) / H.CARTON_SIZE - ISNULL(C.SHIPPED_CTN,0) AS SHIPPED_DIFF
                  from DBO.HFC_DETAIL D 
                  JOIN DBO.HFC_HEADER H ON D.HFC_NBR = H.HFC_NBR
                  LEFT JOIN DBO.SHORT_OVER S ON D.HFC_NBR = S.HFC AND D.STYLE=S.STYLE AND D.COLOR_CODE=S.COLOR
                  LEFT JOIN (SELECT HFC_NBR, SUM(CARTON_CTN) AS SHIPPED_CTN FROM DBO.HFC_CONTAINER GROUP BY HFC_NBR) C ON D.HFC_NBR=C.HFC_NBR
                  WHERE D.CXL = 0 AND H.SEASON IN ('FA-19', 'SP-20') AND H.DIV <> 8
                  GROUP BY D.HFC_NBR, H.CARTON_SIZE, C.SHIPPED_CTN) T
                  ON C.HFC_NBR = T.HFC_NBR""")
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

pd_container_ttl_after['ETA'] = pd_container_ttl_after['ETA'].apply(lambda x: dt.strptime(x, '%Y-%m-%d').date())
pd_validate = pd.merge(pd_container_ttl, pd_container_ttl_after, how='left', 
                       left_on=['CONTAINER_NBR', 'ETA'], right_on=['CONTAINER_NBR', 'ETA'])
for n in range(pd_validate.shape[0]):
    if pd_validate['TTL_CTN'][n] - pd_validate['TTL_CARTON'][n] != 0:
        print ('WARNING: Container ' + pd_validate['CONTAINER_NBR'][n] + ' with ETA ' + pd_validate['ETA'][n].strftime('%Y-%m-%d') + ' has error!')
        sys.exit('FIX ABOVE MENTIONED ERROR(S)')

print ('UPDATE COMPLETED SUCCESSFULLY!')
print ('MAKE SURE TOTAL CARTON COUNT OF ' + str(ttl_carton) + ' IS CORRECT!')



