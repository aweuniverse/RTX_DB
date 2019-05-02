# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 15:48:50 2018

@author: PBu
"""
import pandas as pd
import numpy as np
import sqlalchemy
import sys
import datetime
import os
import logging

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
masterdir = "U:\\ROYTEX PO'S\\SPRING 2018 PO'S\\DIV # "

def readHFCHeader (raw_hfc):
    """
    input: raw dataframe read from an excel HFC
    output: a list with HFC header info that matches the SQL table HFC_HEADER 
    """
    hfc_nbr = str(raw_hfc.iloc[2,2]).zfill(6)
    print('processing HFC ', hfc_nbr)
    
    div = raw_hfc.iloc[6,2]
    
    try:
        ship_date = raw_hfc.iloc[12,2].date()
    except:
        ship_date = datetime.date(1900, 1,1)
    
    try:
        x_orient = raw_hfc.iloc[10,2].date()
    except:
        x_orient = datetime.date(1900, 1,1)
    
    if raw_hfc.iloc[8,16].replace(' ', '') == '(X)':
        cat = 'K'
    elif raw_hfc.iloc[9,16].replace(' ', '') == '(X)':
        cat = 'W'
    else:
        cat = '?'
    
    try:
        cust_nbr = raw_hfc.iloc[2,10].split(':')[0]
    except:
        cust_nbr = '0'
    
    if 'E-COM' in raw_hfc.iloc[2,10] or 'ECOM' in raw_hfc.iloc[2,10] or '.COM' in raw_hfc.iloc[2,10]:
        is_ecom = 1
    else:
        is_ecom = 0
    
    agent = raw_hfc.iloc[11, 16]
    
    maker = raw_hfc.iloc[13, 16]
    
    coo = raw_hfc.iloc[17, 15]
    
    try:
        ssn = raw_hfc.iloc[2,7].split(':')[-1].strip()[:2] + '-' + raw_hfc.iloc[2,7].split(':')[-1].strip()[-2:]
    except:
        ssn = ''
    
    try:
        carton_size = int(raw_hfc.iloc[8,11].split(':')[1].strip()[:-1])
    except:
        carton_size = 0
    
    try:
        hfc_size_scale_code = str(raw_hfc.iloc[3,7]).split('/')[0]
    except:
        hfc_size_scale_code = ''
    
    return [hfc_nbr, div, ship_date, x_orient, cat, cust_nbr, is_ecom, agent, maker, coo, ssn, carton_size, hfc_size_scale_code]


def updateHFCHeader(masterdir):
    poHeaderls = []
    for n in range(10):
        subdir = masterdir + str(n+1)
        files = os.listdir(subdir)
        for each in files:
            if each[-4:] == '.xls' and each[-10:-4].upper() != 'CANCEL':
                raw_hfc = pd.read_excel(subdir+"\\"+each)
                poHeaderls.append(readHFCHeader(raw_hfc))
    
    poHeaderdf = pd.DataFrame(poHeaderls)            
    poHeaderdf.columns = ['hfc_nbr', 'div', 'ship_date', 'x_orient', 'cat', 'cust_nbr', 'is_ecom', 'agent', 'maker', 'coo', 'ssn', 'carton_size', 'hfc_size_scale_code']
    poHeaderdf.to_sql(name='#temp_hfc_header', con=conn, if_exists='replace', index=False)
    
    trans = conn.begin()
    try:
        conn.execute("""MERGE DBO.HFC_HEADER AS T 
                     USING dbo.#temp_hfc_header AS S 
                     ON T.HFC_NBR = S.hfc_nbr 
                     WHEN MATCHED THEN UPDATE
                     SET T.SHIP_DATE = S.ship_date, T.SEASON=S.ssn, T.DIV=S.div, T.CUST_NBR = S.cust_nbr, T.CARTON_SIZE = S.carton_size, T.X_ORIENT = S.x_orient, T.AGENT = S.agent, T.COO = S.coo, T.IS_ECOM = S.is_ecom, T.MAKER=S.maker, T.HFC_SIZE_SCALE_CODE = S.hfc_size_scale_code, T.CAT=S.cat, T.CXL = 0 
                     WHEN NOT MATCHED BY TARGET THEN 
                     INSERT (HFC_NBR, SHIP_DATE, SEASON, DIV, CUST_NBR, CARTON_SIZE, X_ORIENT, AGENT, COO, IS_ECOM, MAKER, HFC_SIZE_SCALE_CODE, CAT) VALUES
                     (S.hfc_nbr, S.ship_date, S.ssn, S.div, S.cust_nbr, S.carton_size, S.x_orient, S.agent, S.coo, S.is_ecom, S.maker, S.hfc_size_scale_code, S.cat)
                     WHEN NOT MATCHED BY SOURCE AND T.SEASON IN (select distinct ssn from #temp_hfc_header) THEN
                     UPDATE SET T.CXL = 1;""")
        trans.commit()        
        print ('UPDATE COMPLETED SUCCESSFULLY!')
    except Exception as e:
        logger = logging.Logger('Catch_All')
        logger.error(str(e))
        trans.rollback()
        conn.close()
        engine.dispose()

updateHFCHeader(masterdir)
#x = pd.read_excel("U:\ROYTEX PO'S\SPRING 2018 PO'S\DIV # 10\\085001 - SS PY-600.xls")
a = 'CANCELLED PO'
print ('CANCEL' in a)
   
