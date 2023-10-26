from pytz import timezone
import os
import pandas as pd
import duckdb

local_tz = timezone('Asia/Ho_Chi_Minh')

# create connection to duckdb
conn = duckdb.connect(database=r'D:\DuckDB\duckdb.db', read_only=False)

# define function
def LoadtoDuck(filename):
    '''function load csv file to duckdb table'''
    extension = '.csv'
    parent = r'D:\flatfile'
    csv_source = os.path.join(parent, f'{filename}{extension}')

    try:
        df = pd.read_csv(csv_source, header=0)
    except Exception as e:
        raise e.__str__()

    # load dataframe to duckdb
    sql = """
        drop table if exists {TABLE_NAME};
        create table {TABLE_NAME} as select * from df;
    """.format(TABLE_NAME=filename)
    conn.execute('SET GLOBAL pandas_analyze_sample=100000')
    conn.execute(sql)

LoadtoDuck('exchange')
