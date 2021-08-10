from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# define arguments and dag
args = {
      'owner' : 'SKKU-Sanhak',
      'start_date' : days_ago(1),
      'retries' : 2,
      'retry_delay' : timedelta(minutes=3),
}

dag = DAG('Triggered_Test', schedule_interval = '*/2 * * * *', default_args = args, max_active_runs=1)

t0 = DummyOperator(task_id="Start", dag=dag)

t1 = KubernetesPodOperator(
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["chart-etl.py"],
    labels={"foo": "bar"},
    image_pull_policy="Always",
    name="chart-cdc",
    task_id="Chart-etl",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64'},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)

t0 >> t1
