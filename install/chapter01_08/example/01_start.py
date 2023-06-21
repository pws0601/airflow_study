import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator


with DAG(
    dag_id="01_start",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    fetch_sales = DummyOperator(task_id="fetch_sales")
    clean_sales = DummyOperator(task_id="clean_sales")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    join_datasets = DummyOperator(task_id="join_datasets")
    train_model = DummyOperator(task_id="train_model")
    deploy_model = DummyOperator(task_id="deploy_model")

    start >> [fetch_sales, fetch_weather] #Fan-in 의존성
    fetch_sales >> clean_sales # 선형 의존성
    fetch_weather >> clean_weather # 선형 의존성
    [clean_sales, clean_weather] >> join_datasets # Fan-out 의존성
    join_datasets >> train_model >> deploy_model # 선형 의존성

    # DummyOperator 더미 오퍼레이터 : 아무것도 수행하지 않는 오퍼레이터, 
    # 아무것도 실행하지는 않으나 성공적으로 실행된것으로 간주하여 다음 단계로 진행됩니다.
    # 복잡한 작업 흐름을 구현하는데 사용할 수 있습니다.