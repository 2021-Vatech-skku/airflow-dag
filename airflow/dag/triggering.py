from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# define arguments and dag
args = {
      'owner' : 'SKKU-Sanhak',
      'start_date' : days_ago(1),
      'retries' : 2,
      'retry_delay' : timedelta(minutes=3),
}

dag = DAG('Triggering_Test', schedule_interval = '@once', default_args = args, max_active_runs=1)

t0 = DummyOperator(task_id="Start", dag=dag)

t1 = TriggerDagRunOperator(task_id="Trigger", trigger_dag_id="Triggered_Test", dag=dag)
t0 >> t1
