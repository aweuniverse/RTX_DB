# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    Run this program will read the 'SOURCE_BUYSUMMARY.csv' file located in the ROYTEXDB folder and upload it into SQL table HFC_HEADER
    It will also read the "SOURCE_BUY_MAKER_ECOM.csv" file, which is manually maintained (only have seasons SPRING 19 and forward), and update the
    MAKER and IS_ECOM fields in the HFC_HEADER table
Note: this program will generate an ExceptionReport for 'mixed prepack' (i.e. prepack with two styles in one carton). 
      Any HFC in the exception report will have the size scale changed to 'Z9' assorted sizes
      User should review the offloaded Excel report '''ExceptionReport_HFC_HEADER.xlsx''' before choosing 'Y' or 'N' to proceed
PREREQUISITE: 
    SQL tables CUSTOMER and SIZE_SCALE have to be loaded before running this program
"""
import pandas as pd
import sqlalchemy
import sys
import os
import logging

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')

# read SOURCE_BUYSUMMARY into POHeader dataframe
POheader = pd.read_csv('SOURCE_BUYSUMMARY.csv', skiprows=1, header=None, usecols=[0, 2, 3, 4, 6, 10, 15, 35, 37, 39, 40], names=['DIV', 'CUST_NBR', 'SSN', 'HFC', 'X_ORIENT', 'X_SHIP', 'CAT', 'AGENT', 'COO', 'CARTON_SIZE', 'HFC_SIZE_SCALE'])
POheader.drop_duplicates(subset='HFC', keep='first', inplace=True)
POheader['CUST_NBR'] = POheader['CUST_NBR'].apply(lambda x: str(x).zfill(5))
POheader['HFC'] = POheader['HFC'].apply(lambda x: str(x).zfill(6))
POheader['HFC_SIZE_SCALE'] = POheader['HFC_SIZE_SCALE'].apply(lambda x: str(x).zfill(2))
POheader.reset_index(drop=True, inplace=True)

# read from SQL SIZE_SCALE table qty by size_scale
readSizeScale = "select SIZE_SCALE_CODE, (S1_QTY + S2_QTY +S3_QTY +S4_QTY +S5_QTY +S6_QTY +S7_QTY +S8_QTY) as TTL_QTY from dbo.SIZE_SCALE"
sizeScale = pd.read_sql_query(readSizeScale, con=conn)

# filter in POheader how many prepack HFC's have carton size not matching size scale qty. 
POheader2 = pd.merge(POheader, sizeScale, how='left', left_on='HFC_SIZE_SCALE', right_on='SIZE_SCALE_CODE')
POheader2['PPK_DIFF'] = POheader2['CARTON_SIZE'] - POheader2['TTL_QTY']
exception = POheader2[(POheader2['HFC_SIZE_SCALE'] != '00') & (POheader2['PPK_DIFF'] != 0)]
exception.to_excel('ExceptionReport_HFC_HEADER.xlsx', index=False)

#ask use to verify the exception report and pick 'Y' or 'N'. 'Y' to proceed to change those not matching to 'Z9' size scale and upload to SQL. 'N' to abort
x = str(input("EXCEPTION REPORT 'ExceptionReport_HFC_HEADER.xlsx' OFFLOADED. IS IT OKAY TO PROCEED? ENTER 'Y' OR 'N' "))
if x.upper() == 'Y':
    exceptionls = exception.index.tolist()
    for n in range(len(exceptionls)):
        POheader.at[exceptionls[n], 'HFC_SIZE_SCALE'] = 'Z9'
else:
    sys.exit('PROGRAM STOPPED')

# Read the manually maintained "SOURCE_BUY_MAKER_ECOM.csv" file
add = pd.read_csv('SOURCE_BUY_MAKER_ECOM.csv', converters={0: str})
add['HFC'] = add['HFC'].apply(lambda x: x.zfill(6))
add['ECOM'].fillna(0, inplace=True)

# upload data into SQL  
POheader.to_sql('#temp_hfc_header', con=conn, if_exists='replace', index=False)
add.to_sql('#temp_maker_ecom', con=conn, if_exists='replace', index=False)
trans = conn.begin()
try:
    conn.execute("""MERGE DBO.HFC_HEADER AS T 
                 USING dbo.#temp_hfc_header AS S 
                 ON T.HFC_NBR = S.HFC 
                 WHEN MATCHED THEN UPDATE
                 SET T.SHIP_DATE = S.X_SHIP, T.SEASON=S.SSN, T.DIV=S.DIV, T.CUST_NBR = S.CUST_NBR, T.CARTON_SIZE = S.CARTON_SIZE, T.X_ORIENT = S.X_ORIENT, T.AGENT = S.AGENT, T.COO = S.COO, T.HFC_SIZE_SCALE_CODE = S.HFC_SIZE_SCALE, T.CAT=S.CAT, T.CXL=0 
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (HFC_NBR, SHIP_DATE, SEASON, DIV, CUST_NBR, CARTON_SIZE, X_ORIENT, AGENT, COO, HFC_SIZE_SCALE_CODE, CAT) VALUES
                 (S.HFC, S.X_SHIP, S.SSN, S.DIV, S.CUST_NBR, S.CARTON_SIZE, S.X_ORIENT, S.AGENT, S.COO, S.HFC_SIZE_SCALE, S.CAT)
                 WHEN NOT MATCHED BY SOURCE AND T.SEASON IN (select distinct SSN from #temp_hfc_header) THEN
                 UPDATE SET T.CXL=1;""")
    conn.execute("""UPDATE DBO.HFC_HEADER SET MAKER = M.MAKER, IS_ECOM = M.ECOM FROM DBO.HFC_HEADER H JOIN #temp_maker_ecom M ON H.HFC_NBR = M.HFC;""")
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

