# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    1), updates INVOICE_HEADER table using two sources:
        a), 'INVOICE_HEADER' tab in 'SOURCE_COMMERCIAL_INVOICE.xlsx' 
        b), 'INVOICE_PAID.xlsx' file updated by Accounting in the Accouting Shared drive
    2), updates INVOICE_DETAIL table using 'INVOICE_DETAIL' tab in 'SOURCE_COMMERCIAL_INVOICE.xlsx'
    3), updates INVOICE_NBR column in HFC_CONTAINER table (this requires multiple data quality checks)

Running this script essentially performs extensive quality checks on data previously loaded in HFC_CONTAINER table via the Loading Result file
It ties together data from 1), Loading result; 2), payment summary; 3), packing list to make sure they all match one another
"""

import pandas as pd
import numpy as np
import os
#import sqlalchemy
import logging
import sys
#from datetime import datetime as dt
#import timeit
from utilities import new_engine, new_sqlconn

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')

#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_columns', None)
#pd.set_option('display.width', None)
#pd.set_option('display.max_colwidth', -1)

def update_invoices():
#    with new_engine() as engine:
    pdHeader = pd.read_excel('SOURCE_COMMERCIAL_INVOICE.xlsx', 'INVOICE_HEADER', converters={0:str, 1:str, 2:str, 3:np.float, 4:str, 5:np.float, 6:np.float, 7:int, 8:int})
    pdHeader['INVOICE_NBR']=pdHeader['INVOICE_NBR'].apply(lambda x: x.replace(' ', '').upper())
    pdHeader['LC_NBR']=pdHeader['LC_NBR'].apply(lambda x: x.replace(' ', '').upper())
    pdHeader['OBL_AWB']=pdHeader['OBL_AWB'].apply(lambda x: x.replace(' ', '').upper() if not pd.isnull(x) else x)
    
    pdDetail = pd.read_excel('SOURCE_COMMERCIAL_INVOICE.xlsx', 'INVOICE_DETAIL', usecols=[0,1,2,3,4,5,6], converters={0:str, 1:str, 2:str, 3:int})
    pdDetail['INVOICE_NBR']=pdDetail['INVOICE_NBR'].apply(lambda x: x.replace(' ', '').upper())
    pdDetail['HFC'] = pdDetail['HFC'].apply(lambda x: x.zfill(6))
    pdDetail['STYLE'] = pdDetail['STYLE'].apply(lambda x: x.zfill(6))
    pdDetail['LINE_AMT'] = (pdDetail['PRICE']+pdDetail['HANGER_COST'])*pdDetail['PCS']
    pdDetail['DUTY_FREE_LINE_AMT'] = pdDetail.apply(lambda row: 0 if np.isnan(row.DUTY_FREE_PRICE) else (row.DUTY_FREE_PRICE+row.HANGER_COST)*row.PCS, axis=1)
    
    ### data quality check - make sure HFC/STYLE listed on factory's commercial invoices are valid
    with new_sqlconn(engine) as conn:
        HFC_STYLE_SQL = '''select distinct HFC_NBR, STYLE from dbo.HFC_DETAIL where cxl = 0'''
        validHfcStyle = pd.read_sql(HFC_STYLE_SQL, con=conn)
    validHfcStyle.rename(columns={'HFC_NBR':'HFC'}, inplace=True)
    validHfcStyle['VALID'] = 1
    pdDetail_check = pd.merge(pdDetail, validHfcStyle, how='left', on=['HFC', 'STYLE'])
    pdDetail_check = pdDetail_check[pdDetail_check['VALID'].isnull()]
    if pdDetail_check.shape[0] != 0:
        print(pdDetail_check)
        sys.exit('HFC/Style combo in above invoice(s) are not valid. Please review and fix!')
    
    ### Below block of code added to read Accounting's file about each invoice's payment date ###
    paid = pd.read_excel('U:\\Accounting Transfer\\INVOICE_PAID.xlsm', converters={0:str})
    paid['INVOICE_NBR']=paid['INVOICE_NBR'].apply(lambda x: x.replace(' ', '').upper())
    paid['PAID_DATE'] = paid['PAID_DATE'].dt.date
    paid['COMMISSION_PAID_DATE'] = paid['COMMISSION_PAID_DATE'].dt.date
    paid['ACH_PAID_DATE'] = paid['ACH_PAID_DATE'].dt.date
    file = paid[['FILE_NBR', 'ACH_PAID_DATE', 'ACH_AMOUNT']].dropna()
    paid = pd.merge(paid[['INVOICE_NBR', 'PAID_DATE', 'COMMISSION_PAID_DATE', 'FILE_NBR', 'CARTON_COUNT']], file, how='left', on='FILE_NBR')

    pdHeader = pd.merge(pdHeader, paid, how='outer', on='INVOICE_NBR')
    pdHeader['LC_NBR'] = pdHeader['LC_NBR'].apply(lambda x: x.replace(' ', ''))
    pdHeader['OBL_AWB'] = pdHeader['OBL_AWB'].apply(lambda x: x.replace(' ', '').upper() if not pd.isnull(x) else x)
    if sum(pdHeader['LC_NBR'].isnull()) != 0:
        print (pdHeader[pdHeader['LC_NBR'].isnull()])
        sys.exit('Please review! Above invoice(s) has a paid_date in accounting file but not in Invoice_Header')  
    
    ### Below block of code verifies the total qty, LC amt, and firt sales amt entered into invoice detail matches what agents give us on their payment summary
    invoice_summary_agent = pdHeader[~pdHeader['OBL_AWB'].isnull()]
    invoice_summary_cal = pd.pivot_table(pdDetail, index='INVOICE_NBR', values=['PCS', 'LINE_AMT', 'DUTY_FREE_LINE_AMT'], aggfunc=np.sum).reset_index()
    invoice_summary_agent = pd.merge(invoice_summary_agent, invoice_summary_cal, how='left', on='INVOICE_NBR')
    invoice_summary_agent['FIRST_SALE_AMT_DIFF'] = invoice_summary_agent['DUTY_FREE_LINE_AMT'] - invoice_summary_agent['FIRST_SALE_AMT']
    invoice_summary_agent['LC_AMT_DIFF'] = invoice_summary_agent['LINE_AMT'] - invoice_summary_agent['LC_AMT']
    invoice_summary_agent['PCS_DIFF'] = invoice_summary_agent['PCS'] - invoice_summary_agent['QTY']
    if invoice_summary_agent[abs(invoice_summary_agent['FIRST_SALE_AMT_DIFF'])>=0.01].shape[0]+invoice_summary_agent[abs(invoice_summary_agent['LC_AMT_DIFF'])>=0.01].shape[0]+invoice_summary_agent[abs(invoice_summary_agent['PCS_DIFF'])>0].shape[0] > 0:
        invoice_summary_agent['TTL_DIFF'] = abs(invoice_summary_agent['FIRST_SALE_AMT_DIFF']) + abs(invoice_summary_agent['LC_AMT_DIFF']) + abs(invoice_summary_agent['PCS_DIFF'])
        print(invoice_summary_agent[invoice_summary_agent['TTL_DIFF']>=0.01])
        sys.exit('Please review! Above invoice(s) has calculated totals from invoice detail not matching up to totals given by agents')
    
    ### Below code updates INVOICE_NBR column in HFC_CONTAINER table via two ways: (updated 8.18.2020 - turns out that OBL_AWB to INVOICE is not one-to-one relationship
    ###                                                                                                 therefore the only way is through CONTAINER_NBR & HFC combo)
    ### 1), verify OBL_AWB# already exists in HFC_CONTAINER
    ### 2), uses CONTAINER_NBR & HFC combination in INVOICE CONTAINER tab to look up INVOICE_NBR (verify the cont/hfc combo already exists in HFC_CONTAINER)
    ### The two methods should yield the SAME INVOICE_NBR. If not, stop the program and fix error(s) --- no longer valid. Got rid of Error#3 check on 8.18.2020
    ### 3), tally up total carton count by INVOICE_NBR and match it to what agent gave us (on the INVOICE_HEADER tab)
    error_count = 0
    
    with new_sqlconn(engine) as conn:
        sql = '''select CONTAINER_NBR, HFC_NBR, OBL_AWB, CARTON_CTN from dbo.HFC_CONTAINER where OBL_AWB is not null'''
        cont_hfc = pd.read_sql(sql, con=conn)
    all_obl = cont_hfc[['OBL_AWB']].drop_duplicates()
    all_obl['VALID'] = 1
    invoice_summary_agent = pd.merge(invoice_summary_agent, all_obl, how='left', on='OBL_AWB')
    exception_1 = invoice_summary_agent[invoice_summary_agent['VALID'].isnull()]
    if exception_1.shape[0] > 0:
        print(exception_1)
        exception_1.to_excel('ExceptionReport_COMMERCIAL_INVOICE_Error1.xlsx')
        print('Error #1: OBL/AWB in the payment summary sent by agents does not exist in the Loading Result (a.k.a. HFC_CONTAINER table)')
        error_count += 1
#    cont_hfc_v1 = pd.merge(cont_hfc, invoice_summary_agent[['INVOICE_NBR', 'OBL_AWB']], how='inner', on='OBL_AWB')  ### This is the list using OBL_AWB to look up INVOICE_NBR
    
    inv_cont = pd.read_excel('SOURCE_COMMERCIAL_INVOICE.xlsx', 'INVOICE_CONTAINER', usecols=[0,1,2], converters={0:str, 1:str, 2:str})
    inv_cont['INVOICE_NBR'] = inv_cont['INVOICE_NBR'].apply(lambda x: x.replace(' ', '').upper())
    inv_cont['CONTAINER_NBR'] = inv_cont['CONTAINER_NBR'].apply(lambda x: x.replace(' ', '').upper())
    inv_cont['HFC'] = inv_cont['HFC'].apply(lambda x: x.zfill(6))
    inv_cont.rename(columns={'HFC':'HFC_NBR'}, inplace=True)
    inv_cont = pd.merge(inv_cont, cont_hfc, how='left', on=['CONTAINER_NBR', 'HFC_NBR'])
    exception_2 = inv_cont[inv_cont['OBL_AWB'].isnull()]
    if exception_2.shape[0] > 0:
        print(exception_2)
        exception_2.to_excel('ExceptionReport_COMMERCIAL_INVOICE_Error2.xlsx')
        print('Error #2: Container/HFC combo(s) listed in the P/L does not exist in the Loading Result (a.k.a. HFC_CONTAINER table)')
        error_count += 1    
    cont_hfc_v2 = pd.merge(cont_hfc, inv_cont[['INVOICE_NBR', 'CONTAINER_NBR', 'HFC_NBR']], how='inner', on=['CONTAINER_NBR', 'HFC_NBR'])  ### This is the list using CONTAINER/HFC combo to look up INVOICE_NBR
    
#    cont_hfc_final = pd.merge(cont_hfc_v1, cont_hfc_v2, how='outer', on=['CONTAINER_NBR', 'HFC_NBR'])
#    cont_hfc_final.rename(columns={'INVOICE_NBR_x':'INVOICE_FROM_OBL','INVOICE_NBR_y':'INVOICE_FROM_CONT_HFC'}, inplace=True)
#    exception_3 = cont_hfc_final[cont_hfc_final['INVOICE_FROM_OBL'] != cont_hfc_final['INVOICE_FROM_CONT_HFC']]
#    if exception_3.shape[0] > 0:
#        print(exception_3)
#        exception_3.to_excel('ExceptionReport_COMMERCIAL_INVOICE_Error3.xlsx')
#        print('Error #3: INVOICE_NBR looked up by OBL_AWB vs by CONTAINER/HFC are different')
#        error_count += 1 
    
    if error_count > 0:
        sys.exit('{} error(s) happened. Please review and fix'.format(str(error_count)))

    carton_by_invoice = pd.pivot_table(cont_hfc_v2[['INVOICE_NBR', 'CARTON_CTN']], index=['INVOICE_NBR'], values='CARTON_CTN', aggfunc=sum).reset_index()
    carton_by_invoice.rename(columns={'CARTON_CTN': 'CTN_SUM'}, inplace=True)
    invoice_summary_agent = pd.merge(invoice_summary_agent, carton_by_invoice, how='left', on='INVOICE_NBR')
    invoice_summary_agent['CTN_SUM'].fillna(value=0, inplace=True)
    exception_4 = invoice_summary_agent[invoice_summary_agent['CTN_SUM'] != invoice_summary_agent['CTNS']]
    if exception_4.shape[0] > 0:
        print(exception_4[['INVOICE_NBR', 'OBL_AWB', 'CTNS', 'CTN_SUM']])
        sys.exit('Please review! Above invoice(s) has different carton count provided by agent than the SUM calculated from HFC_CONTAINER table')
   
    carton_by_invoice_by_hfc = pd.pivot_table(cont_hfc_v2[['INVOICE_NBR', 'HFC_NBR', 'CARTON_CTN']], index=['INVOICE_NBR','HFC_NBR'], values='CARTON_CTN', aggfunc=sum).reset_index()
    carton_by_invoice_by_hfc.rename(columns={'CARTON_CTN': 'CTN_SUM', 'HFC_NBR':'HFC'}, inplace=True)
    pcs_by_invoice_by_hfc = pd.pivot_table(pdDetail[['INVOICE_NBR', 'HFC', 'PCS']], index=['INVOICE_NBR', 'HFC'], values='PCS', aggfunc=sum).reset_index()
    with new_sqlconn(engine) as conn:
        sql = '''SELECT HFC_NBR as HFC, CARTON_SIZE FROM DBO.HFC_HEADER WHERE CXL = 0'''
        hfc_size = pd.read_sql(sql, con=conn)
    pcs_by_invoice_by_hfc = pd.merge(pcs_by_invoice_by_hfc, hfc_size, how='left', on='HFC')
    pcs_by_invoice_by_hfc['CARTONS'] = pcs_by_invoice_by_hfc['PCS'] / pcs_by_invoice_by_hfc['CARTON_SIZE']
    exception_5 = pd.merge(pcs_by_invoice_by_hfc, carton_by_invoice_by_hfc, how='inner', on=['INVOICE_NBR', 'HFC'])
    exception_5 = exception_5[exception_5['CARTONS'] != exception_5['CTN_SUM']]
    if exception_5.shape[0] > 0:
        print (exception_5[['INVOICE_NBR', 'HFC', 'CARTONS', 'CTN_SUM']])
        sys.exit('Please review! Above INVOICE/HFC combos have different carton count in INVOICE_DETAIL vs HFC_CONTAINER')
    
    ### START UPDATING INVOICE_HEADER table, INVOICE_DETAIL table, and INVOICE_NBR in HFC_CONTAINER table
    with new_sqlconn(engine) as conn:
        pdHeader.to_sql('#temp_invoice_header', con=conn, if_exists='replace', index=False)
        pdDetail.to_sql('#temp_invoice_detail', con=conn, if_exists='replace', index=False)
        cont_hfc_v2[['CONTAINER_NBR', 'HFC_NBR', 'INVOICE_NBR']].to_sql(name='#temp_invoice_nbr', con=conn, if_exists='replace', index=False)
        trans = conn.begin()
        try:
            conn.execute("""MERGE DBO.INVOICE_HEADER AS T
                     USING #temp_invoice_header AS S
                     ON (T.INVOICE_NBR = S.INVOICE_NBR)
                     WHEN MATCHED THEN UPDATE
                     SET T.LC_NBR = S.LC_NBR, T.DN_NBR = S.DN_NBR, T.DN_AMT=S.DN_AMT, T.PAID_DATE = S.PAID_DATE, T.COM_PAID_DATE = S.COMMISSION_PAID_DATE, 
                     T.FILE_NBR = S.FILE_NBR, T.ACH_PAID_DATE = S.ACH_PAID_DATE, T.ACH_AMT = S.ACH_AMOUNT, T.CARTON_CTN = S.CARTON_COUNT, T.OBL_AWB = S.OBL_AWB, 
                     T.FIRST_SALE_AMT = S.FIRST_SALE_AMT, T.LC_AMT = S.LC_AMT, T.QTY=S.QTY, T.CTNS = S.CTNS
                     WHEN NOT MATCHED BY TARGET THEN
                     INSERT (INVOICE_NBR, LC_NBR, OBL_AWB, FIRST_SALE_AMT, LC_AMT, QTY, CTNS, DN_NBR, DN_AMT, PAID_DATE, COM_PAID_DATE, FILE_NBR, ACH_PAID_DATE, ACH_AMT, CARTON_CTN) VALUES
                     (S.INVOICE_NBR, S.LC_NBR, S.OBL_AWB, S.FIRST_SALE_AMT, S.LC_AMT, S.QTY, S.CTNS, S.DN_NBR, S.DN_AMT, S.PAID_DATE, S.COMMISSION_PAID_DATE, S.FILE_NBR, S.ACH_PAID_DATE, S.ACH_AMOUNT, S.CARTON_COUNT);""")
            conn.execute("""MERGE DBO.INVOICE_DETAIL AS T
                     USING #temp_invoice_detail AS S
                     ON (T.INVOICE_NBR = S.INVOICE_NBR and T.HFC=S.HFC and T.STYLE=S.STYLE)
                     WHEN MATCHED THEN UPDATE
                     SET T.PCS=S.PCS, T.PRICE=S.PRICE, T.HANGER_COST=S.HANGER_COST, T.LINE_AMT=S.LINE_AMT, T.DUTY_FREE_PRICE = S.DUTY_FREE_PRICE, T.DUTY_FREE_LINE_AMT = S.DUTY_FREE_LINE_AMT 
                     WHEN NOT MATCHED BY TARGET THEN
                     INSERT (INVOICE_NBR, HFC, STYLE, PCS, PRICE, HANGER_COST, LINE_AMT, DUTY_FREE_PRICE, DUTY_FREE_LINE_AMT) VALUES (S.INVOICE_NBR, S.HFC, S.STYLE, S.PCS, S.PRICE, S.HANGER_COST, S.LINE_AMT, S.DUTY_FREE_PRICE, S.DUTY_FREE_LINE_AMT);""")
            conn.execute("""UPDATE T
                     SET T.INVOICE_NBR = S.INVOICE_NBR
                     FROM DBO.HFC_CONTAINER AS T
                     JOIN dbo.#temp_invoice_nbr AS S 
                     ON (T.HFC_NBR = S.HFC_NBR and T.CONTAINER_NBR = S.CONTAINER_NBR);""")
            trans.commit()
            print ('INVOICE_HEADER table updated \n'
                   'INVOICE DETAIL table updated \n'
                   'INVOICE_NBR column UPDATED IN HFC_CONTAINER TABLE')
        except Exception as e:
            logger = logging.Logger('Catch_All')
            logger.error(str(e))
            trans.rollback()
   

if __name__ == '__main__':
    with new_engine() as engine:
        update_invoices()
    
#print(timeit.timeit(main, number=20)/20)



