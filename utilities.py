# -*- coding: utf-8 -*-
"""
Created on Fri Dec 27 16:15:39 2019

@author: PBu
"""
import multiprocessing as mp
import numpy as np
import pandas as pd
import timeit
import time
from contextlib import contextmanager
import sqlalchemy

counter = 0
def stopwatch(text = 'time elapsed : {} seconds', end=False):
    global counter
    global start
    if end:
        if counter == 0:
            print('nothing happens!')
        else:
            end = time.time()
            elapsed = end - start
            print (text.format(elapsed))
            counter = 0
    else:
        if counter == 0:
            start = time.time()
            print('start time recorded')
        else:
            end = time.time()
            elapsed = end - start
            print (text.format(elapsed))
            start = end
        counter += 1


@contextmanager
def new_engine():
    try:
        engine = sqlalchemy.create_engine("mssql+pyodbc://@sqlDSN")
        yield engine
    finally:
        engine.dispose()
        print('engine disposed')

@contextmanager
def new_sqlconn(engine):
    try:
        conn= engine.connect()
        yield conn
    finally:
        conn.close()
        print('connection closed')
        

def testFunc(df):
    df['max'] = df.apply(lambda row: row.Random_1 if row.Random_1 > row.Random_2 else row.Random_2, axis=1)
    return df

def testFunc_2(df):
    df['max'] = pd.Series(max(a, b) for a, b in zip(df['Random_1'], df['Random_2']))
    return df

def parallelize_processing(df, func):
    df_split = np.array_split(df, cpu_count)
    pool = mp.Pool(cpu_count)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def time_apply_row():
    setup_code = '''
import numpy as np
import pandas as pd
from __main__ import testFunc'''
                    
    stmt_code = '''
n=100000
test = pd.DataFrame.from_dict({'Random_1': list(np.random.randint(100, size=n)), 'Random_2': list(np.random.randint(100, size=n))})
testFunc(test)'''
    
    print('apply row without parallel processing is: {}'.format(timeit.timeit(setup=setup_code, stmt = stmt_code, number= 5)/5))

def time_apply_row_with_parallel():
    setup_code = '''
import numpy as np
import pandas as pd
import multiprocessing as mp
from __main__ import testFunc
from __main__ import parallelize_processing'''

    stmt_code = '''
n=100000
cpu_count = mp.cpu_count()
test = pd.DataFrame.from_dict({'Random_1': list(np.random.randint(100, size=n)), 'Random_2': list(np.random.randint(100, size=n))})
parallelize_processing(test, testFunc)'''

    print('apply row with parallel processing is: {}'.format(timeit.timeit(setup=setup_code, stmt = stmt_code, number= 5)/5))

def time_use_numpy():
    setup_code = '''
import numpy as np
import pandas as pd
from __main__ import testFunc_2'''
                    
    stmt_code = '''
n=100000
test = pd.DataFrame.from_dict({'Random_1': list(np.random.randint(100, size=n)), 'Random_2': list(np.random.randint(100, size=n))})
testFunc_2(test)'''
    
    print('do not use apply row but use numpy data series is: {}'.format(timeit.timeit(setup=setup_code, stmt = stmt_code, number= 5)/5))


if __name__ == '__main__':
    time_apply_row()
    time_apply_row_with_parallel()
    time_use_numpy()