from airflow import  DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from MssqlPyodbc import MssqlPyodbc
import logging




log = logging.getLogger()


defaults_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1)
}

dag = DAG(
        'sql_test', default_args=defaults_args, schedule_interval=timedelta(days=1)
        )

# custom operator

class CustomOperator:

    def __init__(self, debug=False, *args, **kwargs):
        mssql = MssqlPyodbc()
        self.conn = mssql.get_conn()

        pass

    def sql_load(self):
        # sql select data
        # return df.values
        return [0]*10

    def csv_write(self, values):
        # write file to csv
        pass

    def process(self):
        log.info("processing task")
        values = self.sql_load()
        self.csv_write(values)
        log.info("Finishing task")

        # convert datatypes, format data columns
        pass

    def execute(self):#, context):
        log.info("starting task")
        pd.read_sql("sp_tables", self.conn)
        self.process()


Op = CustomOperator()

t1 = PythonOperator(
        task_id='custom_ETL',
        python_callable=Op.execute,        
        dag=dag)
