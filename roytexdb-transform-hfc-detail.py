# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
    This program has 3 functions. They can be run altogether or separately. 
    ***readBuySummary function:
        will read the 'SOURCE_BUYSUMMARY.csv' file located in the ROYTEXDB folder and upload it into SQL table HFC_HEADER
    ***readBuySizeBreak function:
        will read the specified file (naming convention 'SOURCE_BUY_XX-XX.csv') and upload any HFC size break into SQL table HFC_HEADER
        multiple files can be run at once if calling this function multiple times
        this csv file comes from Open Order report 20.53 and filtered to only one specific season we are interested in uploading the size break for
    ***reviewSizeBreak function:
        review every HFC/style/color line to make sure sum of size break matches ttl_qty since they come from two different sources
        offload exception report to show the ones that don't match.
        excluded are DIV 8 and VARIOUS HFC's
    NOTE: BUYSUMMARY for Spring '19 in procomm has data integrity issue as below JCP HFC's have style 637759 / color 749 listed twice:
        697016/017/018/019/030/031/032
        so when SOURCE_BUYSUMMARY.csv included raw data from Spring 2019 optional code section needs to be activated
PREREQUISITE: 
    SQL tables HFC_HEADER, STYLE_MASTER, and SIZE_SCALE have to be loaded before running this program
"""

import pandas as pd
import sqlalchemy
import os
import logging

def readBuySummary():
    engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
    conn = engine.connect()

    POdetail = pd.read_csv('SOURCE_BUYSUMMARY.csv', skiprows=1, header=None, usecols=[3, 4, 17,19,21,22, 23, 24, 25, 27, 40], names=['SSN','HFC', 'STYLE', 'ASST', 'COLOR', 'FOB', 'ELC', 'SP', 'MSRP', 'UNITS', 'HFC_LINE_SIZE_SCALE'])
    POdetail['STYLE'] = POdetail['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
    POdetail['COLOR_DESC'] = POdetail['COLOR'].apply(lambda x: x.split('-')[1] if '-' in x else '')
    POdetail['COLOR'] = POdetail['COLOR'].apply(lambda x: x.split('-')[0].zfill(3))
    POdetail['HFC'] = POdetail['HFC'].apply(lambda x: str(x).zfill(6))
    POdetail['HFC_LINE_SIZE_SCALE'] = POdetail['HFC_LINE_SIZE_SCALE'].apply(lambda x: str(x).zfill(2))

    ###########OPTIONAL CODE BEGINS: to fix data issue in Spring '19 JCP
    ###########OPTIONAL CODE BEGINS: to fix data issue in Spring '19 JCP
    ###########OPTIONAL CODE BEGINS: to fix data issue in Spring '19 JCP
    #REMEMBER TO DEACTIVATE THIS SECTION OF CODE WHEN BUY SUMMARY NO LONGER CONTAINS SP-19
    x = POdetail[POdetail['STYLE']=='637759']
    y = pd.pivot_table(x, values=['UNITS'], index=['HFC', 'STYLE', 'COLOR'], aggfunc=sum).reset_index()
    POdetail.drop_duplicates(subset=['HFC', 'STYLE', 'COLOR'], inplace=True)
    for n in range(y.shape[0]):
        POdetail.at[POdetail[(POdetail['HFC'] == y['HFC'][n]) & (POdetail['STYLE']=='637759')].index.values.astype(int)[0], 'UNITS'] = y['UNITS'][n]
    ##################OPTIONAL CODE ENDS#########################
    ##################OPTIONAL CODE ENDS#########################
    ##################OPTIONAL CODE ENDS#########################
    
    POdetail.to_sql('#temp_hfc_detail', con=conn, if_exists='replace', index=False)
    trans = conn.begin()
    try:
        conn.execute("""MERGE DBO.HFC_DETAIL AS T 
                     USING dbo.#temp_hfc_detail AS S 
                     ON (T.HFC_NBR = S.HFC and T.STYLE = S.STYLE and T.COLOR_CODE = S.COLOR)
                     WHEN MATCHED THEN UPDATE
                     SET T.COLOR_DESC = S.COLOR_DESC, T.HFC_LINE_SIZE_SCALE_CODE=S.HFC_LINE_SIZE_SCALE, T.ASST=S.ASST, T.FOB = S.FOB, T.ELC = S.ELC, T.SP = S.SP, T.MSRP = S.MSRP, T.TTL_QTY = S.UNITS 
                     WHEN NOT MATCHED BY TARGET THEN 
                     INSERT (HFC_NBR, STYLE, COLOR_CODE, COLOR_DESC, HFC_LINE_SIZE_SCALE_CODE, ASST, FOB, ELC, SP, MSRP, TTL_QTY) VALUES
                     (S.HFC, S.STYLE, S.COLOR, S.COLOR_DESC, S.HFC_LINE_SIZE_SCALE, S.ASST, S.FOB, S.ELC, S.SP, S.MSRP, S.UNITS)
                     WHEN NOT MATCHED BY SOURCE AND 
                     ((select SEASON from dbo.HFC_HEADER where HFC_NBR = T.HFC_NBR) IN (select distinct SSN from #temp_hfc_detail)) THEN
                     DELETE;""")
        trans.commit()         
        print ('BUY SUMMARY UPDATE COMPLETED SUCCESSFULLY!')
        conn.close()
        engine.dispose()
    except Exception as e:
        logger = logging.Logger('Catch_All')
        logger.error(str(e))
        trans.rollback()
        conn.close()
        engine.dispose()

def readBuySizeBreak(aFile):
    engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
    conn = engine.connect()
    
    POline = pd.read_csv(aFile, skiprows=1, header=None, usecols=[3, 10, 12, 19, 20, 21, 22, 23, 24, 25, 26], names=['HFC', 'STYLE', 'COLOR', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8'], converters={12: str})
    POline = POline[['HFC' in each for each in POline['HFC']]]
    POline['HFC'] = POline['HFC'].apply(lambda x: x[3:])
    POline['STYLE'] = POline['STYLE'].apply(lambda x: x.zfill(6) if len(x) < 6 else x)
    POline['COLOR'] = POline['COLOR'].apply(lambda x: x.zfill(3))
    
    POline.to_sql('#temp_hfc_line_detail', con=conn, if_exists='replace', index=False)
    trans = conn.begin()
    try:
        conn.execute("""UPDATE T SET T.S1_QTY = S.S1, T.S2_QTY = S.S2,T.S3_QTY = S.S3,T.S4_QTY = S.S4,T.S5_QTY = S.S5,T.S6_QTY = S.S6,T.S7_QTY = S.S7,T.S8_QTY = S.S8
                     FROM #temp_hfc_line_detail as S INNER JOIN dbo.HFC_DETAIL as T ON 
                     (T.HFC_NBR=S.HFC and T.STYLE=S.STYLE and T.COLOR_CODE=S.COLOR);""")
        trans.commit()         
        print (aFile.split('.')[0]+' SIZE BREAK UPDATE COMPLETED SUCCESSFULLY!')
        conn.close()
        engine.dispose()
    except Exception as e:
        logger = logging.Logger('Catch_All')
        logger.error(str(e))
        trans.rollback()
        conn.close()
        engine.dispose()

def reviewSizeBreak():
    engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
    conn = engine.connect()    
    ### review all but DIV 8 and VARIOUS HFC's
    sqlcheck = '''
                select H.SEASON, D.HFC_NBR, D.STYLE, D.COLOR_CODE, D.TTL_QTY, SUM(D.S1_QTY)+SUM(D.S2_QTY)+SUM(D.S3_QTY)+SUM(D.S4_QTY)+SUM(D.S5_QTY)+SUM(D.S6_QTY)+SUM(D.S7_QTY)+SUM(D.S8_QTY) as TTL_SIZE_BREAK
                from dbo.HFC_DETAIL D join dbo.HFC_HEADER H on D.HFC_NBR=H.HFC_NBR where  H.DIV <> 8 and H.CUST_NBR<>'87700' GROUP BY H.SEASON, D.HFC_NBR, D.STYLE, D.COLOR_CODE, D.TTL_QTY
                '''
    POcheck = pd.read_sql(sqlcheck, con=conn)
    conn.close()
    engine.dispose()
    POcheck = POcheck[POcheck['TTL_QTY'] != POcheck['TTL_SIZE_BREAK']]
    if POcheck.shape[0] == 0:
        print ('SIZE REVIEW COMPLETED. ALL GOOD')
    else:
        POcheck.to_excel('ExceptionReport_HFC_DETAIL.xlsx', index=False)
        print ('ExceptionReport_HFC_DETAIL.xlsx report offloaded. PLEASE REVIEW')

os.chdir('W:\\Roytex - The Method\\Ping\\ROYTEXDB')
readBuySummary()
#readBuySizeBreak('SOURCE_BUY_FA-18.csv')
#readBuySizeBreak('SOURCE_BUY_SP-19.csv')
readBuySizeBreak('SOURCE_BUY_FA-19_1.csv')
readBuySizeBreak('SOURCE_BUY_FA-19_2.csv')
reviewSizeBreak()
