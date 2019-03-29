# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 16:07:02 2018

@author: PBu
"""
   
import pandas as pd
import numpy as np
import sqlalchemy
import logging

df_style = pd.read_csv('W:\Roytex - The Method\Ping\ROYTEXDB\SOURCE_STYLE_MASTER.csv', header=None,
                            names=['STYLE', 'STYLE_DESC', 'PROTO', 'SIZE_RANGE', 'OPEN_SSN', 'OPEN_YR', 'DIV', 'LABEL', 'SP', 'ELC', 'ILC', 'BRAND_NAME', 'FABRIC', 'ROYALTY', 'PPK' ],
                            usecols=[1, 2, 3, 4, 5, 6, 7,8, 11, 12, 13, 15, 16, 30, 34], skiprows=2, 
                            dtype={'STYLE': str, 'STYLE_DESC':str, 'PROTO':str, 'SIZE_RANGE':str, 'OPEN_SSN':str, 'OPEN_YR':np.int, 'DIV': np.int, 'LABEL':str, 'SP':np.float64, 'ELC':np.float64, 'ILC': np.float64, 'BRAND_NAME':str, 'FABRIC':str, 'ROYALTY':np.float64, 'PPK':str })

df_style['STYLE'] = df_style['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
df_style['OPEN_SSN'] = df_style['OPEN_SSN'].apply(lambda x: x.upper())
df_style['CAT'] = df_style['STYLE_DESC'].apply(lambda x: 'K' if ' KS ' in x or ' KL ' in x else ('W' if ' WS ' in x or ' WL ' in x else ''))
df_style['SLV'] = df_style['STYLE_DESC'].apply(lambda x: 'L' if ' KL ' in x or ' WL ' in x else ('S' if ' WS ' in x or ' KS ' in x else ''))


engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
df_style.to_sql(name='#temp_style', con=conn, if_exists='replace', index=False)
trans = conn.begin()


try:
    conn.execute("""MERGE DBO.STYLE_MASTER AS T 
                 USING dbo.#temp_style AS S 
                 ON T.STYLE = S.STYLE 
                 WHEN MATCHED THEN UPDATE 
                 SET T.STYLE_DESC = S.STYLE_DESC, T.DIV=S.DIV, T.PROTO=S.PROTO, T.SIZE_RANGE_CODE = S.SIZE_RANGE, 
                 T.OPEN_SSN = S.OPEN_SSN, T.OPEN_YEAR = S.OPEN_YR, T.LABEL_CODE=S.LABEL, 
                 T.SP=S.SP, T.ELC=S.ELC, T.ILC=S.ILC, T.CAT=S.CAT, T.SLV=S.SLV, T.BRAND_NAME=S.BRAND_NAME, 
                 T.FABRIC=S.FABRIC, T.ROYALTY = S.ROYALTY, T.PPK = S.PPK, T.CXL=0
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (STYLE, STYLE_DESC, DIV, PROTO, SIZE_RANGE_CODE, OPEN_SSN, OPEN_YEAR, CAT, SLV, ILC, ELC, SP, LABEL_CODE, BRAND_NAME, FABRIC, ROYALTY, PPK) 
                 VALUES (S.STYLE, S.STYLE_DESC, S.DIV, S.PROTO, S.SIZE_RANGE, S.OPEN_SSN, S.OPEN_YR, S.CAT, S.SLV, S.ILC, S.ELC, S.SP, S.LABEL, S.BRAND_NAME, S.FABRIC, S.ROYALTY, S.PPK)
                 WHEN NOT MATCHED BY SOURCE THEN
                 UPDATE SET T.CXL = 1;""")
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









#import csv
#
#with open('W:\Roytex - The Method\Ping\ROYTEXDB\STYLE_MASTER_SOURCE.csv', 'r') as f:
#    csv_f = csv.reader(f)
#    next(csv_f)
#    next(csv_f)
#    styleMasterList = []
#    for row in csv_f:
#      if ' KS ' in row[2]:
#          styleMasterList.append([row[1], int(row[7]),  row[3], row[4], row[5].upper(), row[6], '', 'KS', float(row[13]), float(row[12]), float(row[11]), 0, row[2]])
#      elif ' KL ' in row[2]:
#          styleMasterList.append([row[1], int(row[7]),  row[3], row[4], row[5].upper(), row[6], '', 'KL', float(row[13]), float(row[12]), float(row[11]), 0, row[2]])
#      elif ' WS ' in row[2]:
#          styleMasterList.append([row[1], int(row[7]),  row[3], row[4], row[5].upper(), row[6], '', 'WS', float(row[13]), float(row[12]), float(row[11]), 0, row[2]])
#      elif ' WL ' in row[2]:
#          styleMasterList.append([row[1], int(row[7]),  row[3], row[4], row[5].upper(), row[6], '', 'WL', float(row[13]), float(row[12]), float(row[11]), 0, row[2]])
#      else:
#          styleMasterList.append([row[1], int(row[7]),  row[3], row[4], row[5].upper(), row[6], '', '', float(row[13]), float(row[12]), float(row[11]), 0, row[2]])
#
#with open('W:\Roytex - The Method\Ping\ROYTEXDB\STYLE_MASTER_SQL.csv', 'w', newline='') as f:
#    writer_f = csv.writer(f, delimiter=str(','))
#    writer_f.writerow(['STYLE', 'DIV', 'PROTO', 'SIZE RANGE', 'SSN OPENED', 'YEAR OPENED', 'REG/BT', 'CAT', 'ILC', 'ELC', 'SP', 'CXL', 'STYLE DESC'])
#    writer_f.writerows(styleMasterList)
