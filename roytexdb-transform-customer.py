# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
Run this program will read the 'SOURCE_CUSTOMER.csv' file located in ROYTEXDB folder and upload it into SQL table CUSTOMER
"""

import pandas as pd
import sqlalchemy
import logging

pd_customer = pd.read_csv('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SOURCE_CUSTOMER.csv')
pd_customer['CUST_NBR'] = pd_customer['CUST_NBR'].apply(lambda x: str(x).zfill(5))

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
pd_customer.to_sql(name='#temp_customer', con=conn, if_exists='replace', index=False)
                   
trans = conn.begin()
try:
    conn.execute("""MERGE DBO.CUSTOMER AS T 
                 USING dbo.#temp_customer AS S 
                 ON T.CUST_NBR = S.CUST_NBR
                 WHEN MATCHED THEN UPDATE SET T.CUST_NAME = S.CUST_NAME 
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (CUST_NBR, CUST_NAME) VALUES (S.CUST_NBR, S.CUST_NAME);""")
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