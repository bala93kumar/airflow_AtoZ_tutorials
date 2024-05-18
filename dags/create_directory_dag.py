from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os 

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

dag = DAG("firstDag", default_args=default_args, schedule_interval=timedelta(1),catchup=False)


dir = "/usr/local/airflow/dags/test_dir"

def createDirectory():   
    os.makedirs(dir,exist_ok=True)

def chekcIfDirectoryExists(dir,**kwargs):
    if os.path.isdir(dir):
        print("path exists")
    else :
        print(f"The directory  '{dir}' does not exists ")    

t1 = PythonOperator(
    task_id= 'create_dir',
    retries= 2, 
    python_callable=createDirectory,
    dag=dag
)

t2= PythonOperator(
    task_id = 'check_if_dir_exists',
    retries = 2, 
    python_callable=chekcIfDirectoryExists,
    op_kwargs={"dir":dir},
    dag=dag
) 

t1 >> t2