# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    Run this program will read the 'SOURCE_SIZE_RANGE_MASTER.csv' file located in ROYTEXDB folder and upload it into SQL table SIZE_RANGE
PREREQUISITE:
    None
"""

import pandas as pd
import numpy as np
import sqlalchemy
import logging

df_size_range = pd.read_csv('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\SOURCE_SIZE_RANGE_MASTER.csv', converters={0: str})
df_size_range.columns = ['SIZE_RANGE_CODE', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8']
df_size_range.replace(np.nan, '', inplace=True)


engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
df_size_range.to_sql(name='#temp_size_range', con=conn, if_exists='replace', index=False)
trans = conn.begin()

try:
    conn.execute("""MERGE DBO.SIZE_RANGE AS T 
                 USING dbo.#temp_size_range AS S 
                 ON T.SIZE_RANGE_CODE = S.SIZE_RANGE_CODE 
                 WHEN MATCHED THEN UPDATE 
                 SET T.S1=S.S1, T.S2=S.S2, T.S3=S.S3, T.S4=S.S4, T.S5=S.S5, T.S6=S.S6, T.S7=S.S7, T.S8=S.S8
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (SIZE_RANGE_CODE, S1, S2, S3, S4, S5, S6, S7, S8) 
                 VALUES (S.SIZE_RANGE_CODE, S.S1, S.S2, S.S3, S.S4, S.S5, S.S6, S.S7, S.S8);""")
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