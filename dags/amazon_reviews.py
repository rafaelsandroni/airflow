from airflow import  DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

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
        'amazon_reviews', default_args=defaults_args, schedule_interval=timedelta(days=1)
        )

t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)


templated_command = """
    {% for i in range(0,5) %}
        echo "{{ ds }}"
        echo "{{ params.my_params }}"
    {% endfor %}
    """

t2 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
        params={'my_params': 'Parameter I passed in'},
        dag=dag)


t2.set_upstream(t1)
