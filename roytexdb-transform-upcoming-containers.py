# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 10:10:59 2019

@author: PBu
"""

import pandas as pd
import os
import pyodbc
import sqlalchemy



pd_container = pd.read_excel('W:\\Roytex - The Method\\Ping\\ROYTEXDB\\UPCOMING_CONTAINERS.xlsx', skiprows=3, header=None, 
                             usecols=[2,3,4,5],names=['ETA', 'CONTAINER_NBR', 'HFC_NBR', 'CARTON_CTN'])
pd_container = pd_container.iloc[:-1]

pd_container['HFC_NBR'] = pd_container['HFC_NBR'].astype(int).astype(str).apply(lambda x:x.zfill(6))
pd_container['ETA'] = pd_container['ETA'].fillna(method='ffill')
pd_container['ETA'] = pd_container['ETA'].dt.date
pd_container['CONTAINER_NBR'] = pd_container['CONTAINER_NBR'].fillna(method='ffill')

conn = pyodbc.connect('Driver={SQL Server};'
                  'Server=DESKTOP-5JROCDL\SQLEXPRESS;'
                  'Database=roytexdb;'
                  'Trusted_Connection=yes;')
c = conn.cursor()   
c.execute('''
         create table #temp_hfc_container
         (ETA date not null, CONTAINER_NBR varchar(15) not null, HFC_NBR varchar(6) not null, CARTON_CTN int not null)
         ''')

engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
pd_container.to_sql(name='temp_hfc_container', con=engine, if_exists='append', index=False)

c.execute('''
          MERGE DBO.HFC_CONTAINER AS T
          USING temp_hfc_container AS S
          ON (T.HFC_NBR = S.HFC_NBR and T.CONTAINER_NBR = S.CONTAINER_NBR)
          WHEN MATCHED THEN
          UPDATE SET T.ETA = S.ETA, T.CARTON_CTN=S.CARTON_CTN
          WHEN NOT MATCHED BY TARGET THEN
          INSERT (HFC_NBR, CONTAINER_NBR, ETA, CARTON_CTN) VALUES (S.HFC_NBR, S.CONTAINER_NBR, S.ETA, S.CARTON_CTN);
          ''')

c.execute('''drop table temp_hfc_container;''')

conn.commit()
conn.close()




