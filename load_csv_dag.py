from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
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
        drop table if exists {TABLE_NAME}
        create table {TABLE_NAME} as select * from df;
    """.format(TABLE_NAME=filename)
    conn.execute('SET GLOBAL pandas_analyze_sample=100000')
    conn.execute(sql)


# define DAG
default_args = {
    'owner': 'tubt',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 26, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

main_dag = DAG(
    dag_id='load_csv_dag',
    default_args=default_args,
    description='load data from csv to duckdb',
    schedule_interval='@daily'
)

# Định nghĩa các Task
start_process = BashOperator(
    task_id='start_process',
    bash_command='echo "START"',
    dag=main_dag,
)

loadcsv1 = PythonOperator(
    task_id='loadcsv1',
    python_callable=LoadtoDuck(filename='customer'),
    dag=main_dag,
)

loadcsv2 = PythonOperator(
    task_id='task3',
    python_callable=LoadtoDuck(filename='deposit'),
    dag=main_dag,
)

end_process = BashOperator(
    task_id='end_process',
    bash_command='echo "FINISH"',
    dag=main_dag
)
# define workflow of task

start_process >> [loadcsv1,loadcsv2] >> end_process

if __name__ == "__main__":
    main_dag.cli()
