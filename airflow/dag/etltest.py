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
      'start_date' : days_ago(2),
      'retries' : 2,
      'retry_delay' : timedelta(minutes=2),
}

dag = DAG('ETL_workflow_daily', schedule_interval = '@daily', default_args = args, max_active_runs=1)

t0 = KubernetesPodOperator(
    namespace="spark",
    image="cmcm0012/spark:v2",
    cmds=["printenv"],
    labels={"foo": "bar"},
    image_pull_policy="Always",
    name="dummy",
    task_id="start",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64'},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)

t1 = KubernetesPodOperator(
    namespace="spark",
    image="cmcm0012/spark:v2",
    cmds=["./submit.sh"],
    arguments=["chart-etl.py"],
    labels={"foo": "bar"},
    image_pull_policy="Always",
    name="chart-cdc",
    task_id="Chart-etl",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', 'PATH' : '$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin'},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)

t2 = KubernetesPodOperator(
    namespace="spark",
    image="cmcm0012/spark:v2",
    cmds=["./submit.sh"],
    arguments=["patient-etl.py"],
    labels={"foo": "bar"},
    image_pull_policy="Always",
    name="patient-cdc",
    task_id="Patient-etl",
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)

t0 >> t1 >> t2
