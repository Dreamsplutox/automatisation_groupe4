from airflow import DAG
 from datetime import timedelta

dag = DAG(
dag_id = ("toto_2020_12_09"),
start_date = "2020_12_09",
schedule_interval = timedelta(days=1))
 
task1 = BashOperator(
task_id = "toto1",
bash_command = "echo hello world",
dag = dag)
 
task2 = BashOperator(
task_id = "toto2",
bash_command = "echo {{ ds }}",
dag = dag)
 
task3 = BashOperator(
task_id = "toto3",
bash_command = "sleep 5",
dag = dag)