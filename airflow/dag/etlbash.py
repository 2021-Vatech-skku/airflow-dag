from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# define arguments and dag
args = {
      'owner' : 'SKKU-Sanhak',
      'start_date' : days_ago(2),
      'retries' : 2,
      'retry_delay' : timedelta(minutes=2),
}

dag = DAG('ETL_workflow_usingBash', schedule_interval = '@daily', default_args = args, max_active_runs=1, tags=['test', 'Clever', 'ETL'],)

t1 = BashOperator(
    task_id="chart_etl", bash_command="./submit.sh chart-etl.py", dag=dag
)

t2 = BashOperator(
    task_id="chart_etl", bash_command="./submit.sh chart-etl.py", dag=dag
)

t1 >> t2
