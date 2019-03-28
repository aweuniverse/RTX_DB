# -*- coding: utf-8 -*-
"""
@author: PBu
STATEMENT OF PURPOSE:
this program reads the 'SIZE SCALES.xlsx' file located on U drive and upload it into SQL table SIZE_SCALE
"""

import pandas as pd
import numpy as np
import sys
import sqlalchemy
import logging

"""
STEP 1: format excel file into dataframe that matches SQL table
"""

df1 = pd.read_excel('U:\\SIZE SCALES.xlsx', skiprows=2)
df_scale = pd.DataFrame()
df_desc = pd.DataFrame()

for n in range(len(df1.columns)):
    if 'SCALE' in df1.columns[n]:
        df_scale = pd.concat([df_scale, df1[df1.columns[n]]]).reset_index(drop=True)
    elif 'DESC' in df1.columns[n]:
        df_desc = pd.concat([df_desc, df1[df1.columns[n]]]).reset_index(drop=True)

df_size_scale = pd.concat([df_scale, df_desc], axis=1)
df_size_scale.columns = ['SIZE_SCALE_CODE', 'DESCRIPTION']
df_size_scale.dropna(inplace=True)
df_size_scale.reset_index(drop=True, inplace=True)
df_size_scale['SIZE_SCALE_CODE'] = df_size_scale['SIZE_SCALE_CODE'].apply(lambda x: str(int(x)).zfill(2) if type(x) == float else x)

def splitList(aSizeScaleString):
    """
    input: aSizeScaleString is a string of size scale description
    output: a list contains two list elements:
            1st list element is a size description list of length 8
            2nd list element is a qty list of length 8
    """
    aList = aSizeScaleString.split()
    try:
        qtyList = [int(each.split('/')[0]) for each in aList if '/' in each]
        sizeList = [each.split('/')[1].strip() for each in aList if '/' in each]
        if len(qtyList) != len(sizeList):
            print ('WARNING: This size scale ' + aSizeScaleString + ' split into two different lengths of qty and size')
            sys.exit('FIX ABOVE MENTIONED ERROR(S)')
        else:
            if len(qtyList) < 8:
                qtyList = qtyList + list(np.repeat([0], 8-len(qtyList)))
                sizeList = sizeList + list(np.repeat(['NONE'], 8-len(sizeList)))
            elif len(qtyList) > 8:
                print ('WARNING: This size scale ' + aSizeScaleString + ' split into more than 8 sizes')
                sys.exit('FIX ABOVE MENTIONED ERROR(S)')
        return [sizeList, qtyList]
    except:
        print('WARNING: ' + aSizeScaleString + ' ran into issues')

    
df_size_split = pd.DataFrame([splitList(each)[0] for each in df_size_scale['DESCRIPTION']], 
              columns=['S1_SIZE', 'S2_SIZE', 'S3_SIZE', 'S4_SIZE','S5_SIZE', 'S6_SIZE', 'S7_SIZE', 'S8_SIZE'])
df_qty_split = pd.DataFrame([splitList(each)[1] for each in df_size_scale['DESCRIPTION']], 
              columns=['S1_QTY', 'S2_QTY', 'S3_QTY', 'S4_QTY','S5_QTY', 'S6_QTY', 'S7_QTY', 'S8_QTY'])
df_size_scale = pd.concat([df_size_scale, df_size_split], axis=1)
df_size_scale = pd.concat([df_size_scale, df_qty_split], axis=1)

"""
STEP 2: upload into SQL table
"""

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()
df_size_scale.to_sql(name='#temp_size_scale', con=conn, if_exists='replace', index=False)
trans = conn.begin()

try:
    conn.execute("""MERGE DBO.SIZE_SCALE AS T 
                 USING dbo.#temp_size_scale AS S 
                 ON T.SIZE_SCALE_CODE = S.SIZE_SCALE_CODE 
                 WHEN MATCHED THEN UPDATE 
                 SET T.SIZE_SCALE_DESC = S.DESCRIPTION, T.S1_SIZE=S.S1_SIZE, T.S2_SIZE=S.S2_SIZE, T.S3_SIZE=S.S3_SIZE, T.S4_SIZE=S.S4_SIZE, 
                 T.S5_SIZE=S.S5_SIZE, T.S6_SIZE=S.S6_SIZE, T.S7_SIZE=S.S7_SIZE, T.S8_SIZE=S.S8_SIZE, 
                 T.S1_QTY=S.S1_QTY, T.S2_QTY=S.S2_QTY, T.S3_QTY=S.S3_QTY, T.S4_QTY=S.S4_QTY, 
                 T.S5_QTY=S.S5_QTY, T.S6_QTY=S.S6_QTY, T.S7_QTY=S.S7_QTY, T.S8_QTY=S.S8_QTY
                 WHEN NOT MATCHED BY TARGET THEN 
                 INSERT (SIZE_SCALE_CODE, SIZE_SCALE_DESC, S1_SIZE, S2_SIZE, S3_SIZE, S4_SIZE, S5_SIZE, S6_SIZE, S7_SIZE, S8_SIZE, S1_QTY, S2_QTY, S3_QTY, S4_QTY, S5_QTY, S6_QTY, S7_QTY, S8_QTY) 
                 VALUES (S.SIZE_SCALE_CODE, S.DESCRIPTION, S.S1_SIZE, S.S2_SIZE, S.S3_SIZE, S.S4_SIZE, S.S5_SIZE, S.S6_SIZE, S.S7_SIZE, S.S8_SIZE, S.S1_QTY, S.S2_QTY, S.S3_QTY, S.S4_QTY, S.S5_QTY, S.S6_QTY, S.S7_QTY, S.S8_QTY);""")
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
    
    
    
    
    
    
    
    
    
    
    