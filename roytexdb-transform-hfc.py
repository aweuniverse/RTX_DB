# -*- coding: utf-8 -*-
"""
@author: PBu
"""
import pandas as pd
import numpy as np
import sqlalchemy
import sys
import os

season = 'FA-19'
masterdir = "U:\\ROYTEX PO'S\\FALL 2019 PO'S\\DIV # "

##retrieve from SQL database a reference list with styles and size range
engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
conn = engine.connect()

sql_style_size_range = """select S.STYLE, S.SIZE_RANGE_CODE, T.S1, T.S2, T.S3, T.S4,T.S5,T.S6,T.S7,T.S8 
                            FROM DBO.STYLE_MASTER AS S JOIN DBO.SIZE_RANGE AS T ON S.SIZE_RANGE_CODE = T.SIZE_RANGE_CODE"""

df_ref = pd.read_sql_query(sql_style_size_range, con=conn)
df_ref['size_range_dict'] = df_ref['S1'] + '-' + df_ref['S2']+ '-' + df_ref['S3']+ '-' + df_ref['S4']+ '-' + df_ref['S5']+'-' + df_ref['S6']+'-' + df_ref['S7']+'-' + df_ref['S8']
df_ref['size_range_dict'] = df_ref['size_range_dict'].apply(lambda x: x.rstrip('-').split('-'))
df_ref['size_range_dict'] = df_ref['size_range_dict'].apply(lambda x: {v:0 for (k,v) in enumerate(x)})
df_ref.drop(columns=['S1', 'S2','S3','S4','S5','S6','S7','S8'], inplace=True)



def BulkOrPrepack (aHFC):
    """
    input: raw dataframe read from an excel HFC
    output: "BULK" or "PPK" 
    """
    if '00' in str(aHFC.iloc[3,7]) and 'BULK' in aHFC.iloc[3,10]:
        return 'BULK'
    elif '00' not in str(aHFC.iloc[3,7]) and 'BULK' not in aHFC.iloc[3,10]:
        return 'PPK'
    else:
        print ('WARNING: Error in identifying this HFC ' + aHFC.iloc[2,2] + ' is bulk or prepack')
        sys.exit('FIX ABOVE MENTIONED ERROR(S)')
    
def readHFCHeader (raw_hfc):
    """
    input: raw dataframe read from an excel HFC
    output: a list with HFC header info that feeds into SQL table HFC_HEADER 
    """
    hfc_nbr = str(raw_hfc.iloc[2,2]).zfill(6)
    div = raw_hfc.iloc[6,2]
    ship_date = raw_hfc.iloc[12,2].date()
    x_orient = raw_hfc.iloc[10,2].date()
    if raw_hfc.iloc[8,16] == '( X )':
        cat = 'K'
    elif raw_hfc.iloc[9,16] == '( X )':
        cat = 'W'
    cust_nbr = raw_hfc.iloc[2,10].split(':')[0]
    if 'E-COM' in raw_hfc.iloc[2,10] or 'ECOM' in raw_hfc.iloc[2,10] or '.COM' in raw_hfc.iloc[2,10]:
        is_ecom = 'Y'
    else:
        is_ecom = 'N'
    agent = raw_hfc.iloc[11, 16]
    maker = raw_hfc.iloc[13, 16]
    coo = raw_hfc.iloc[17, 15]
    ssn = raw_hfc.iloc[2,7].split(':')[-1].strip()[:2] + '-' + raw_hfc.iloc[2,7].split(':')[-1].strip()[-2:]
    carton_size = int(raw_hfc.iloc[8,11].split(':')[1].strip()[:-1])
    hfc_size_scale = str(raw_hfc.iloc[3,7]).split('/')[0]  
    
    return [hfc_nbr, div, ship_date, x_orient, cat, cust_nbr, is_ecom, agent, maker, coo, ssn, carton_size, hfc_size_scale]


def readBulkHFC (bulk_hfc):
    """
    input: one hfc that has been identified as bulk
    output: 
    """   
    hfc_nbr = str(bulk_hfc.iloc[2,2]).zfill(6)
    new_bulk = bulk_hfc.iloc[22:, :].reset_index(drop=True)
    #select rows only with style/color information; and select columns
    n = 0
    while True:
        if n+1 > new_bulk.shape[0]:
            print ('WARNING: loop exceeded max!')
            sys.exit('FIX ABOVE MENTIONED ERROR(S)')
        if type(new_bulk.iloc[:, 0][n]) == str:
            if not bool(new_bulk.iloc[:, 0][n].strip()):
                break
        else:
            if np.isnan(new_bulk.iloc[:, 0][n]):
                break
        n += 1
    new_bulk = new_bulk.iloc[:n, [0,2,3,7,8,9,10,13, 15, 16, 17]].reset_index(drop=True)
    new_bulk.columns = ['style', 'asst', 'style_desc', 'proto', 'color_code', 'ttl_qty', 'size_curve', 'fob', 'msrp', 'elc', 'sp']
    #change size curve to dictionary with size being KEY; if no size curve info then default to {'none': 0}
    new_bulk['size_curve'] = new_bulk['size_curve'].apply(lambda x: '' if pd.isnull(x) or not bool(x.strip()) else x)
    new_bulk['size_curve'] = new_bulk['size_curve'].apply(lambda x: {each.split('/')[1].strip().upper() if bool(x) else 'none' : int(each.split('/')[0].strip()) if bool(x) else 0 for each in x.split('-')})
    #add size_range_dict from pd table read from SQL database     
    df_bulk = pd.merge(new_bulk, df_ref, how='left', left_on='style', right_on='STYLE').drop(columns=['STYLE', 'SIZE_RANGE_CODE'])
    #combine size curve and size range to put qty in proper place. Also check if sum of S1--S8 add up to total    
    ls_size_curve = []
    ls_exception = []
    for n in range(len(df_bulk)):
        x = [df_bulk['size_curve'][n][each] if each in df_bulk['size_curve'][n] else 0 for each in df_bulk['size_range_dict'][n].keys()]
        if len(x) > 8:
            print ('WARNING: bulk HFC ' + hfc_nbr + ' style ' + df_bulk['style'][n] + ' color ' + df_bulk['color_code'][n] + ' size curve has more than 8 places')
            sys.exit('FIX ABOVE MENTIONED ERROR(S)')
        else:
            x.extend([0]*(8-len(x)))
            if sum(x) != df_bulk['ttl_qty'][n]:
                ls_exception.append([hfc_nbr, df_bulk['style'][n], df_bulk['color_code'][n]])
            ls_size_curve.append(x)               
    df_size_curve = pd.DataFrame(ls_size_curve)
    df_size_curve.columns = ['s1', 's2', 's3', 's4', 's5', 's6', 's7', 's8']   
    final_df_bulk = pd.concat([df_bulk, df_size_curve], sort=False, axis=1) 
    
    if len(ls_exception) > 0:
        print ("CHECK THESE BULK HFC LINES AS THEY DON'T ADD UP TO TOTAL: \n", ls_exception)
    
    return final_df_bulk
    


for n in range(10):
    subdir = masterdir + str(n+1)
    files = os.listdir(subdir)
    for each in files:
        if each[-4:] == '.xls' and each[-10:-4].upper() != 'CANCEL':
            raw_hfc = pd.read_excel(subdir+"\\"+each)

            packing = raw_hfc.iloc[3,10].split('=')[0]
            
            



testppkpd = pd.read_excel("U:\\ROYTEX PO'S\\FALL 2019 PO'S\\DIV # 10\\097228- LS  PS , DBW, TKPS.xls")
testbulkpd = pd.read_excel("W:\\Roytex - The Method\\Ping\\ROYTEXDB\\195225 - MEIJER LS JSW-801Z.xls")
testwmtpd = pd.read_excel("U:\\ROYTEX PO'S\\FALL 2019 PO'S\\DIV # 7\\797202 - WALMART SS MICRO.xls")
m = [{'s':10, 'm':40, 'l': 14, 'xl': 16, 'xxl':20},{'s':13, 'm':140, 'l': 164, 'xl': 136, 'xxl':200}, {'s':13, 'm':140, 'l': 14, 'xl': 13, 'xxl':100},{'s':1, 'm':10, 'l': 104, 'xl': 103, 'xxl':100}]
n = [{'s': 0, 'm':0, 'l':0, 'xl':0, 'xxl':0}, {'s': 0, 'm':0, 'l':0, 'xl':0, 'xxl':0}, {'s': 0, 'm':0, 'l':0, 'xl':0, 'xxl':0}, {'s': 0, 'm':0, 'l':0, 'xl':0, 'xxl':0}]

dd = pd.Series(m)
n.append(dd)

m = [[3,21,1], [4,2,21], [431,21,7]]
n = pd.DataFrame(m)
n.columns = ['a', 'b', 'c']

dd = []
for x in range(len(n)):
    dd.append({key: m[x][key] for key, value in n[x].items()})

x = {}
print('s' in x)

m = '  '
n = m.split('-')
print(bool(m))























   
print(z)