# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    this program uploads TWO SQL TABLES at once:
    1), updates INVOICE_HEADER table using two sources:
            a), 'INVOICE_HEADER' tab in 'SOURCE_COMMERCIAL_INVOICE.xlsx' 
            b), 'INVOICE_PAID.xlsx' file updated by Accounting in the Accouting Shared drive
    2), updates INVOICE_DETAIL table using 'INVOICE_DETAIL' tab in 'SOURCE_COMMERCIAL_INVOICE.xlsx' 
"""

import pandas as pd
#import numpy as np
import os
import sqlalchemy
import logging
import sys
from datetime import datetime as dt
import timeit

def main():
    os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')
    
    engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
    conn = engine.connect()
    
    pdHeader = pd.read_excel('SOURCE_COMMERCIAL_INVOICE.xlsx', 'INVOICE_HEADER')
    pdDetail = pd.read_excel('SOURCE_COMMERCIAL_INVOICE.xlsx', 'INVOICE_DETAIL', usecols=[0,1,2,3,4,5], converters={0:str, 1:str, 2:str, 3:int})
    pdDetail['LINE_AMT'] = (pdDetail['PRICE']+pdDetail['HANGER_COST'])*pdDetail['PCS']
    
    ### data quality check - make sure HFC/STYLE listed on factory's commercial invoices are valid
    HFC_STYLE_SQL = '''select distinct HFC_NBR, STYLE from dbo.HFC_DETAIL where cxl = 0'''
    validHfcStyle = pd.read_sql(HFC_STYLE_SQL, con=conn)
    validHfcStyle.rename(columns={'HFC_NBR':'HFC'}, inplace=True)
    validHfcStyle['VALID'] = 1
    pdDetail_check = pd.merge(pdDetail, validHfcStyle, how='left', on=['HFC', 'STYLE'])
    pdDetail_check = pdDetail_check[pdDetail_check['VALID'].isnull()]
    if pdDetail_check.shape[0] != 0:
        print(pdDetail_check)
        conn.close()
        engine.dispose()
        sys.exit('HFC/Style combo in above invoice(s) are not valid. Please review and fix!')
    
    ### Below block of code added to read Accounting's file about each invoice's payment date ###
    paid = pd.read_excel('U:\\Accounting Shared\\INVOICE_PAID.xlsx')    
    paid['PAID_DATE'] = paid['PAID_DATE'].dt.date
    paid['COMMISSION_PAID_DATE'] = paid['COMMISSION_PAID_DATE'].dt.date
    paid['ACH_PAID_DATE'] = paid['ACH_PAID_DATE'].dt.date
    file = paid[['FILE_NBR', 'ACH_PAID_DATE', 'ACH_AMOUNT']].dropna()
    paid = pd.merge(paid[['INVOICE_NBR', 'PAID_DATE', 'COMMISSION_PAID_DATE', 'FILE_NBR', 'CARTON_COUNT']], file, how='left', on='FILE_NBR')
    
    pdHeader = pd.merge(pdHeader, paid, how='outer', on='INVOICE_NBR')
    if sum(pdHeader['LC_NBR'].isnull()) != 0:
        print (pdHeader[pdHeader['LC_NBR'].isnull()])
        conn.close()
        engine.dispose()
        sys.exit('Please review! Above invoice(s) has a paid_date in accounting file but not in Invoice_Header')  
    
    pdHeader.to_sql('#temp_invoice_header', con=conn, if_exists='replace', index=False)
    pdDetail.to_sql('#temp_invoice_detail', con=conn, if_exists='replace', index=False)
    trans=conn.begin()
    try:
        conn.execute("""MERGE DBO.INVOICE_HEADER AS T
                     USING #temp_invoice_header AS S
                     ON (T.INVOICE_NBR = S.INVOICE_NBR)
                     WHEN MATCHED THEN UPDATE
                     SET T.LC_NBR = S.LC_NBR, T.DN_NBR = S.DN_NBR, T.DN_AMT=S.DN_AMT, T.PAID_DATE = S.PAID_DATE, T.COM_PAID_DATE = S.COMMISSION_PAID_DATE, 
                     T.FILE_NBR = S.FILE_NBR, T.ACH_PAID_DATE = S.ACH_PAID_DATE, T.ACH_AMT = S.ACH_AMOUNT, T.CARTON_CTN = S.CARTON_COUNT
                     WHEN NOT MATCHED BY TARGET THEN
                     INSERT (INVOICE_NBR, LC_NBR, DN_NBR, DN_AMT, PAID_DATE, COM_PAID_DATE, FILE_NBR, ACH_PAID_DATE, ACH_AMT, CARTON_CTN) VALUES
                     (S.INVOICE_NBR, S.LC_NBR, S.DN_NBR, S.DN_AMT, S.PAID_DATE, S.COMMISSION_PAID_DATE, S.FILE_NBR, S.ACH_PAID_DATE, S.ACH_AMOUNT, S.CARTON_COUNT);""")
        conn.execute("""MERGE DBO.INVOICE_DETAIL AS T
                     USING #temp_invoice_detail AS S
                     ON (T.INVOICE_NBR = S.INVOICE_NBR and T.HFC=S.HFC and T.STYLE=S.STYLE)
                     WHEN MATCHED THEN UPDATE
                     SET T.PCS=S.PCS, T.PRICE=S.PRICE, T.HANGER_COST=S.HANGER_COST, T.LINE_AMT=S.LINE_AMT
                     WHEN NOT MATCHED BY TARGET THEN
                     INSERT (INVOICE_NBR, HFC, STYLE, PCS, PRICE, HANGER_COST, LINE_AMT) VALUES (S.INVOICE_NBR, S.HFC, S.STYLE, S.PCS, S.PRICE, S.HANGER_COST, S.LINE_AMT);""")
        trans.commit()
        print('INVOICE_HEADER & INVOICE_DETAIL tables have been updated')
    except Exception as e:
        logger = logging.Logger('Catch_All')
        logger.error(str(e))
        trans.rollback()
        conn.close()
        engine.dispose()


if __name__ == '__main__':
    main()
    
#print(timeit.timeit(main, number=20)/20)



