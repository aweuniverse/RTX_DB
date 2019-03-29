# -*- coding: utf-8 -*-
"""
@author: PBu
"""
import pandas as pd
import numpy as np
import sys
import os

season = 'FA-19'
masterdir = "U:\\ROYTEX PO'S\\FALL 2019 PO'S\\DIV # "

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


def readHFCDetail (raw_hfc):
    hfc_nbr = str(raw_hfc.iloc[2,2]).zfill(6)
    hfc_type = BulkOrPrepack(raw_hfc)
    hfc_size_scale = str(raw_hfc.iloc[3,7]).split('/')[0]
    
    raw_hfc = raw_hfc.iloc[22:, :].reset_index(drop=True)
    #select rows only with style/color information
    n = 0
    while True:
        if n+1 > raw_hfc.shape[0]:
            print ('WARNING: loop exceeded max!')
            sys.exit('FIX ABOVE MENTIONED ERROR(S)')
        if type(raw_hfc.iloc[:, 0][n]) == str:
            if not bool(raw_hfc.iloc[:, 0][n].strip()):
                break
        else:
            if np.isnan(raw_hfc.iloc[:, 0][n]):
                break
        n += 1
    raw_hfc = raw_hfc.iloc[:n, :].reset_index(drop=True)
    raw_hfc = raw_hfc.iloc[:, [0,2,3,7,8,9,10,13, 15, 16, 17]]
    
for n in range(10):
    subdir = masterdir + str(n+1)
    files = os.listdir(subdir)
    for each in files:
        if each[-4:] == '.xls' and each[-10:-4].upper() != 'CANCEL':
            raw_hfc = pd.read_excel(subdir+"\\"+each)

#            packing = raw_hfc.iloc[3,10].split('=')[0]
            
            



testppkpd = pd.read_excel("U:\\ROYTEX PO'S\\FALL 2019 PO'S\\DIV # 10\\097228- LS  PS , DBW, TKPS.xls")
testbulkpd = pd.read_excel("U:\\ROYTEX PO'S\\FALL 2019 PO'S\\DIV # 1\\195225 - MEIJER LS JSW-801Z.xls")
testwmtpd = pd.read_excel("U:\\ROYTEX PO'S\\FALL 2019 PO'S\\DIV # 7\\797202 - WALMART SS MICRO.xls")
             