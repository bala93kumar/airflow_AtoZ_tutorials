from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators import EmailOperator
import os 

from airflow.operators.mysql_operator import MySqlOperator 

from datacleaner import data_cleaner


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 18),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

yesterday_date = datetime.strftime(datetime.now() - timedelta(1),'%Y-%m-%d' )


dag = DAG("stores_dag", default_args=default_args, template_searchpath=['/usr/local/airflow/sql_files'], schedule_interval='@daily',catchup=False)

t1=BashOperator(
    task_id='check_file',
    bash_command='shasum ~/store_files/raw_store_transactions.csv',
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag
)

t2 = PythonOperator(
    task_id='data_cleaner',
    python_callable=data_cleaner,
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag
)

t3 = MySqlOperator(
    task_id  = 'create_MySql_table',
    mysql_conn_id = 'mysql_conn',
    sql="create_table.sql",
    dag=dag
)

t4 = MySqlOperator(
    task_id  = 'insert_into_MySql_table',
    mysql_conn_id = 'mysql_conn',
    sql="insert_into_table.sql",
    dag=dag
)

t5 = MySqlOperator(
    task_id  = 'select_data_from_mysql',
    mysql_conn_id = 'mysql_conn',
    sql="select_from_table.sql",
    dag=dag
)

t6 = BashOperator(task_id='move_file1', bash_command='cat ~/store_files/location_wise_profit.csv && mv ~/store_files/location_wise_profit.csv ~/store_files/location_wise_profit_%s.csv' % yesterday_date,dag=dag)

t7 = BashOperator(task_id='move_file2', bash_command='cat ~/store_files/store_wise_profit.csv && mv ~/store_files/store_wise_profit.csv ~/store_files/store_wise_profit_%s.csv' % yesterday_date,dag=dag)


t8= EmailOperator(
    task_id='email',
    subject='Daily report generated',
    to='youremail',
    html_content=""" <h1> Congratulation! Your store reports are here  """,
    files=['/usr/local/airflow/store_files/location_wise_profit_%s.csv' % yesterday_date, '/usr/local/airflow/store_files/store_wise_profit_%s.csv' % yesterday_date],
    dag=dag
)


t9 = BashOperator(task_id='rename_raw', 
                  bash_command='mv ~/store_files/raw_store_transactions.csv ~/store_files/raw_store_transactions_%s.csv' % yesterday_date, 
                  dag=dag)



t1 >> t2 >> t3 >> t4 >> t5 >> [t6,t7] >> t8  >> t9