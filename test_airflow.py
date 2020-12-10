from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/root/airflow/dags/automatisation_groupe4/")

dag = DAG(
dag_id = ("group4_dag_prime"),
start_date = datetime(2020, 12, 9),
schedule_interval = timedelta(days=1))
 
task1 = BashOperator(
task_id = "hello_world",
bash_command = "echo hello world",
dag = dag)
 
task2 = BashOperator(
task_id = "execute_python",
bash_command = "python hello.py",
dag = dag)

task1 >> task2