from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
#from kubernetes.client import models as k8s

def branch_func(**kwargs):
    ti = kwargs['ti']
    xcom_value = int(ti.xcom_pull(task_ids='initial_job_Cleared'))
    if xcom_value >= 5:
        return 'daily'
    else:
        return 'overwrite'

args = {
      'owner' : 'Sanhak',
      'start_date' : days_ago(1),    #'start_date': datetime(2021, 8, 8),
      'retries' : 2,
      'retry_delay' : timedelta(minutes=3),
}

dag = DAG('BranchTest', schedule_interval = '0 0 * * *', default_args = args, max_active_runs=1)

t0 = DummyOperator(task_id="Start", dag=dag)

t1 = KubernetesPodOperator(
    task_id="insert_chart",
    name="insert_chart",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)
t2 = KubernetesPodOperator(
    task_id="update_chart",
    name="update_chart",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py -t update"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)
t3 = KubernetesPodOperator(
    task_id="insert_patient",
    name="insert_patient",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py -c jee.clever.dev0-patient.filtered.test"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)
t4 = KubernetesPodOperator(
    task_id="update_patient",
    name="update_patient",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py -c jee.clever.dev0-patient.filtered.test -t update"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)
t5 = KubernetesPodOperator(
    task_id="initial_insert_chart",
    name="initial_insert_chart",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py -y 0"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)
t6 = KubernetesPodOperator(
    task_id="initial_update_chart",
    name="initial_update_chart",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py -t update -y 0"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)
t7 = KubernetesPodOperator(
    task_id="initial_insert_patient",
    name="initial_insert_patient",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py -c jee.clever.dev0-patient.filtered.test -y 0"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)
t8 = KubernetesPodOperator(
    task_id="initial_update_patient",
    name="initial_update_patient",
    namespace="spark",
    image="cmcm0012/spark:latest",
    cmds=["./submit.sh"],
    arguments=["clever-fetch.py -c jee.clever.dev0-patient.filtered.test -t update -y 0"],
    image_pull_policy="Always",
    env_vars={'SPARK_HOME' : '/opt/spark', 'JAVA_HOME' : '/usr/lib/jvm/java-11-openjdk-amd64', "SPARK_LOCAL_HOSTNAME" : "localhost"},
    is_delete_operator_pod=False,
    get_logs=True,
    dag=dag
)


check = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    dag=dag
)
Initial = BashOperator(
    task_id='initial_job_Cleared',
    bash_command="echo 5",
    xcom_push=True,
    dag=dag
)
overwrite = DummyOperator(task_id="overwrite", dag=dag)
daily = DummyOperator(task_id="daily", dag=dag)

# schedule
t0 >> check >> overwrite
overwrite >> t5 >> t6 >> Initial
overwrite >> t7 >> t8 >> Initial

t0 >> check >> daily
daily >> t1 >> t2
daily >> t3 >> t4
