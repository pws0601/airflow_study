import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="chapter8_bash_operator_no_command",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

BashOperator(
    task_id="this_should_fail", 
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag
)


