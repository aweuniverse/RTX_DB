# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    Running this program will update:
        1), SHORT_OVER table
        2), Also update the CTN_TO_COME column in the HFC_HEADER table
"""

import pandas as pd
#import numpy as np
import os
#import pyodbc
#import sqlalchemy
import logging
#import sys
#import re
#from datetime import datetime as dt
from utilities import new_engine, new_sqlconn

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')

def update_short_over():
    over = pd.read_excel('SOURCE_SHORT-OVER.xlsx', converters={0:str, 1:str, 2:str})
    over['PO'] = over['PO'].apply(lambda x: x.zfill(6))
    over['STYLE'] = over['STYLE'].apply(lambda x: x.zfill(6))
    over['COLOR'] = over['COLOR'].apply(lambda x: x.zfill(3))
    
    with new_sqlconn(engine) as conn:
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
            conn.execute("""
                         UPDATE DBO.HFC_HEADER
                         SET CTN_TO_COME = T.SHIPPED_DIFF
                         FROM DBO.HFC_HEADER C INNER JOIN
                         (select D.HFC_NBR, SUM(D.TTL_QTY + ISNULL(S.DIFF, 0)) / H.CARTON_SIZE - ISNULL(C.SHIPPED_CTN,0) AS SHIPPED_DIFF
                          from DBO.HFC_DETAIL D 
                          JOIN DBO.HFC_HEADER H ON D.HFC_NBR = H.HFC_NBR
                          LEFT JOIN DBO.SHORT_OVER S ON D.HFC_NBR = S.HFC AND D.STYLE=S.STYLE AND D.COLOR_CODE=S.COLOR
                          LEFT JOIN (SELECT HFC_NBR, SUM(CARTON_CTN) AS SHIPPED_CTN FROM DBO.HFC_CONTAINER GROUP BY HFC_NBR) C ON D.HFC_NBR=C.HFC_NBR
                          WHERE D.CXL = 0 AND H.SEASON IN ('FA-20', 'SP-20') AND H.DIV <> 8
                          GROUP BY D.HFC_NBR, H.CARTON_SIZE, C.SHIPPED_CTN) T
                          ON C.HFC_NBR = T.HFC_NBR
                          """)
            trans.commit()
            print ('SHORT_OVER TABLE UDPATED \nCTN_TO_COME IN SP-20 & FA-20 HFCs UPDATED')
        except Exception as e:
            logger = logging.Logger('Catch_All')
            logger.error(str(e))
            trans.rollback()
            #sys.exit('ERROR ENCOUNTERED IN UPDATING SHORT_OVER TABLE')

if __name__ == '__main__':
    with new_engine() as engine:
        update_short_over()