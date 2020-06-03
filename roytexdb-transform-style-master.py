# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    Run this program will read the 'SOURCE_STYLE_MASTER.csv' file located in ROYTEXDB folder and upload it into SQL table STYLE_MASTER
    It will also udpate manually maintained 'SOURCE_STYLE_MASTER_SLV_AND_SIZE.csv' as complementary info into STYLE_MASTER
PREREQUISITE:
    SQL table SIZE_RANGE has to be loaded before running this program
"""
import sys
import pandas as pd
import numpy as np
import sqlalchemy
import logging

df_style = pd.read_csv('W:\Roytex - The Method\Ping\ROYTEXDB\SOURCE_STYLE_MASTER.csv', header=None,
                            names=['STYLE', 'STYLE_DESC', 'PROTO', 'SIZE_RANGE', 'OPEN_SSN', 'OPEN_YR', 'DIV', 'LABEL', 'SP', 'ELC', 'ILC', 'BRAND_NAME', 'FABRIC', 'RESERVE', 'ROYALTY', 'PPK' ],
                            usecols=[1, 2, 3, 4, 5, 6, 7,8, 11, 12, 13, 15, 16, 29, 30, 34], skiprows=2, 
                            dtype={'STYLE': str, 'STYLE_DESC':str, 'PROTO':str, 'SIZE_RANGE':str, 'OPEN_SSN':str, 'OPEN_YR':str, 'DIV': np.int, 'LABEL':str, 'SP':np.float64, 'ELC':np.float64, 'ILC': np.float64, 'BRAND_NAME':str, 'FABRIC':str, 'RESERVE': np.float64, 'ROYALTY':np.float64, 'PPK':str })

df_style['STYLE'] = df_style['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
df_style['SEASON'] = df_style.apply(lambda row: 'SP-' + row.OPEN_YR[-2:] if row.OPEN_SSN.upper() == 'S' else 'FA-' + row.OPEN_YR[-2:], axis=1)
#df_style['OPEN_SSN'] = df_style['OPEN_SSN'].apply(lambda x: x.upper())
df_style['CAT'] = df_style['STYLE_DESC'].apply(lambda x: '' if type(x)==np.float else 'K' if ' KS ' in x or ' KL ' in x else ('W' if ' WS ' in x or ' WL ' in x else ''))
df_style['SLV'] = df_style['STYLE_DESC'].apply(lambda x: '' if type(x)==np.float else 'L/S' if ' KL ' in x or ' WL ' in x else ('S/S' if ' WS ' in x or ' KS ' in x else ''))

slvAndSize = pd.read_csv('W:\Roytex - The Method\Ping\ROYTEXDB\SOURCE_STYLE_MASTER_SLV_AND_SIZE.csv', converters={0:str})
slvAndSize['STYLE'] = slvAndSize['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)

styleNew = pd.merge(df_style, slvAndSize, how='left', on='STYLE')
styleNew.fillna({'CAT_2': ''}, inplace=True)
styleNew.fillna({'SLV_2': ''}, inplace=True)
# CHECKING DISCREPANCY BETWEEN PROCOMM DESCRIPTION AND THE LIST I MANUALLY MAINTAIN IS A GOOD WAY TO MAKE SURE MY LIST IS CORRECT
exceptionCAT = styleNew[(styleNew['CAT'] != '') & (styleNew['CAT_2'] != '') & (styleNew['CAT'] != styleNew['CAT_2'])]
exceptionCAT['TYPE'] = 'CAT'
exceptionSLV = styleNew[(styleNew['SLV'] != '') & (styleNew['SLV_2'] != '') & (styleNew['SLV'] != styleNew['SLV_2'])]
exceptionSLV['TYPE'] = 'SLV'
exception = pd.concat([exceptionCAT, exceptionSLV], ignore_index=True)
if exception.shape[0] > 0:
    exception.to_excel('ExceptionReport_STYLE_MASTER.xlsx', index=False)
    choice = str(input('STYLE MASTER EXCEPTION REPORT OFFLOADED. PLEASE REVIEW. IS IT OKAY TO PROCEED? Y OR N')) #there should not be any style from 2019 & forward
    if choice.upper() != 'Y':
        sys.exit('PROGRAM STOPPED')
# for the CAT & SLV, use the list I manually maintained as the final answer
styleNew['CAT'] = styleNew.apply(lambda row: row.CAT if row.CAT_2 == '' else row.CAT_2, axis=1)
styleNew['SLV'] = styleNew.apply(lambda row: row.SLV if row.SLV_2 == '' else row.SLV_2, axis=1)
styleNew['INV_GROUP'].fillna('', inplace=True)
styleNew['BUY_GROUP_MAIN'].fillna('', inplace=True)
styleNew['BUY_GROUP_SUB'].fillna('', inplace=True)
styleNew['RM_GROUP'] = styleNew.apply(lambda row: "POLY KNITS" if ((row.INV_GROUP == 'POLY POLO') | (row.INV_GROUP == 'POLY QTR ZIP') | (row.INV_GROUP == 'PRINT POLY (KPR GROUP)'))
                                                #else "MICROFIBER" if ((row.INV_GROUP == 'MICROFIBER') | (row.INV_GROUP == 'MICROFIBER PRINT'))
                                                else "MICROFIBER" if (row.INV_GROUP == 'MICROFIBER') 
                                                else "OTHER KNITS" if (row.CAT == 'K')
                                                else "OTHER WOVEN" if (row.CAT == 'W') else '', axis = 1)
#
#slvAndSize2 = pd.read_csv('W:\Roytex - The Method\Ping\ROYTEXDB\SOURCE_STYLE_MASTER_SLV_AND_SIZE_2.csv', converters={0:str})
#slvAndSize2['STYLE'] = slvAndSize2['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
#engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
#conn = engine.connect()
#slvAndSize2.to_sql(name='#temp_slv_and_size', con=conn, if_exists='replace', index=False)

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
styleNew.to_sql(name='#temp_style', con=conn, if_exists='replace', index=False)
#slvAndSize.to_sql(name='#temp_slv_and_size', con=conn, if_exists='replace', index=False)
trans = conn.begin()
try:
    conn.execute("""MERGE DBO.STYLE_MASTER AS T
                 USING dbo.#temp_style AS S 
                 ON T.STYLE = S.STYLE
                 WHEN MATCHED THEN UPDATE 
                 SET T.STYLE_DESC = S.STYLE_DESC, T.DIV=S.DIV, T.PROTO=S.PROTO, T.SIZE_RANGE_CODE = S.SIZE_RANGE, T.SLV = S.SLV,
                 T.SEASON = S.SEASON, T.LABEL_CODE=S.LABEL, T.CAT = S.CAT, T.INV_GROUP = S.INV_GROUP, T.BUY_GROUP_MAIN = S.BUY_GROUP_MAIN, T.BUY_GROUP_SUB = S.BUY_GROUP_SUB, T.RM_GROUP = S.RM_GROUP,
                 T.SP=S.SP, T.ELC=S.ELC, T.ILC=S.ILC, T.BRAND_NAME=S.BRAND_NAME, 
                 T.FABRIC=S.FABRIC, T.RESERVE=S.RESERVE, T.ROYALTY = S.ROYALTY, T.PPK = S.PPK, T.REG_BT = S.SIZE, T.CXL=0
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (STYLE, STYLE_DESC, DIV, PROTO, SIZE_RANGE_CODE, SEASON, ILC, ELC, SP, SLV, LABEL_CODE, BRAND_NAME, FABRIC, RESERVE, ROYALTY, PPK, REG_BT, CAT, INV_GROUP, BUY_GROUP_MAIN, BUY_GROUP_SUB, RM_GROUP) 
                 VALUES (S.STYLE, S.STYLE_DESC, S.DIV, S.PROTO, S.SIZE_RANGE, S.SEASON, S.ILC, S.ELC, S.SP, S.SLV, S.LABEL, S.BRAND_NAME, S.FABRIC, S.RESERVE, S.ROYALTY, S.PPK, S.SIZE, S.CAT, S.INV_GROUP, S.BUY_GROUP_MAIN, S.BUY_GROUP_SUB, S.RM_GROUP)
                 WHEN NOT MATCHED BY SOURCE THEN
                 UPDATE SET T.CXL = 1;""")
#    conn.execute("""update T set T.REG_BT = S.SIZE, T.SLV = S.SLV_2, T.CAT=S.CAT_2, T.INV_GROUP=S.INV_GROUP from #temp_slv_and_size as S inner join dbo.STYLE_MASTER as T on T.STYLE=S.STYLE""")
    trans.commit()
#    conn.close()
#    engine.dispose()
    print ('STYLE_MASTER TABLE UPDATE COMPLETED SUCCESSFULLY!')
except Exception as e:
    logger = logging.Logger('Catch_All')
    logger.error(str(e))
    trans.rollback()
    conn.close()
    engine.dispose()
    sys.exit('PROGRAM STOPPED')

"""
Below code is added for data integrity check:
    ~to verify that the season code entered in style master indeed match the season the style is ordered in
"""
sql_check_ssn_code = '''
                    SELECT F.STYLE, F.HFC_SSN, S.STYLE_SSN FROM 
                    (SELECT DISTINCT D.STYLE, H.SEASON AS HFC_SSN FROM DBO.HFC_DETAIL D JOIN DBO.HFC_HEADER H ON D.HFC_NBR = H.HFC_NBR) AS F 
                    LEFT JOIN (SELECT STYLE, SEASON AS STYLE_SSN FROM DBO.STYLE_MASTER) AS S 
                    ON F.STYLE = S.STYLE
                    '''
check_ssn_code = pd.read_sql(sql_check_ssn_code, con = conn)
conn.close()
engine.dispose()
ssn_code_exception = check_ssn_code[check_ssn_code['HFC_SSN'] != check_ssn_code['STYLE_SSN']]
if ssn_code_exception.shape[0] > 0:
    print ('Some styles might have season code issue. Please review below data')
    print (ssn_code_exception.reset_index(drop=True))
else:
    print ('All styles passed season code check')


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
