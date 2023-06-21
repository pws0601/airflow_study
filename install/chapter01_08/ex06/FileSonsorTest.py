from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    'file_sensor_example_with_tasks',
    default_args=default_args,
    description='An example DAG with a FileSensor and tasks',
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/tmp/my_file.txt',
        poke_interval=10, # check every 10 seconds
        timeout=300, # timeout after 5 minutes
    )

    process_file = BashOperator(
        task_id='process_file',
        bash_command='echo "Processing file: ./data/my_file.txt"',
    )

    wait_for_file >> process_file  # set the task dependencies