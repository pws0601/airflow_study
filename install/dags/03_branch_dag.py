import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1)


def _pick_erp_system(**context):
    if context["execution_date"] < ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"


def _fetch_sales_old(**context):
    print("Fetching sales data (OLD)...")


def _fetch_sales_new(**context):
    print("Fetching sales data (NEW)...")


def _clean_sales_old(**context):
    print("Preprocessing sales data (OLD)...")


def _clean_sales_new(**context):
    print("Preprocessing sales data (NEW)...")


with DAG(
    dag_id="03_branch_dag",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    # BranchPythonOperator 을 이용하여 명시적으로 브랜치를 나눌수 있습니다.
    pick_erp_system = BranchPythonOperator(
        task_id="pick_erp_system", python_callable=_pick_erp_system
    )

    fetch_sales_old = PythonOperator(
        task_id="fetch_sales_old", python_callable=_fetch_sales_old
    )
    clean_sales_old = PythonOperator(
        task_id="clean_sales_old", python_callable=_clean_sales_old
    )

    fetch_sales_new = PythonOperator(
        task_id="fetch_sales_new", python_callable=_fetch_sales_new
    )
    clean_sales_new = PythonOperator(
        task_id="clean_sales_new", python_callable=_clean_sales_new
    )

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    # Using the wrong trigger rule ("all_success") results in tasks being skipped downstream.
    # join_datasets = DummyOperator(task_id="join_datasets")
    # 업스트림의 태스크중 하나를 건너뛰더라도 계속 트리거가 진행되도록 트리거 규칙 변경
    # trigger_rule="none_failed"
    # 트리거 규칙을 변경하지 않는다면 브랜치된 없스트림의 태스크들이 모두 성공이 되지 않아 join_datasets 태스크와
    # 모든 다운 스트림 태스크가 실행되지 않습니다.
    join_datasets = DummyOperator(
        task_id="join_datasets", 
        trigger_rule="none_failed"
    )
    train_model = DummyOperator(task_id="train_model")
    deploy_model = DummyOperator(task_id="deploy_model")

    start >> [pick_erp_system, fetch_weather]
    pick_erp_system >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new
    fetch_weather >> clean_weather
    [clean_sales_old, clean_sales_new, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model