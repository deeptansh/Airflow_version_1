from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def load_data():
    df = pd.read_csv('/opt/airflow/data/sales_data.csv')
    return df

def compute_metrics(ti):
    df = ti.xcom_pull(task_ids='load_data')
    metrics = {
        'total_sales': df['amount'].sum(),
        'average_sales': df['amount'].mean()
    }
    return metrics

def save_results(ti):
    results = ti.xcom_pull(task_ids='compute_metrics')
    with open('/opt/airflow/data/results.txt', 'w') as f:
        for key, value in results.items():
            f.write(f"{key}: {value}\n")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_analysis',
    default_args=default_args,
    description='A DAG for analyzing sales data',
    schedule_interval='@daily',
    catchup=False,
)

t1 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='compute_metrics',
    python_callable=compute_metrics,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='save_results',
    python_callable=save_results,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3