#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import timedelta
from os import name

from kubernetes.client import models as k8s
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from sqlalchemy.sql.operators import op

args = {
    'owner': 'Heegwan Son',
}

node_affinity = k8s.V1Affinity(
    node_affinity = k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=50,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(
                            key="node-roles.kubernetes.io/control-plane",
                            operator="DoesNotExist"
                        )
                    ]
                ),
            )
        ]
    ),
)

port = k8s.V1ContainerPort(name='http-faker', container_port=8000)

with DAG(
    dag_id='Kubernetes_faker_daily',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'faker'],
) as dag:

    StartOperator = DummyOperator(
        task_id='start',
    )

    kubernetes_min_pod = KubernetesPodOperator(
        task_id='pod-ex-minimum',
        name='pod-ex-minimum',
        cmds=['echo Hello!; sleep 30; exit 0'],
        namespace='airflow',
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4'
    )

    FakerOperator = KubernetesPodOperator(
        task_id='elasticfaker-test',
        name="elastic_faker",
        namespace="elasticsearch",
        image="gmlrhks95/elastic-faker",
        ports=[port],
        env_vars={'PYTHONUNBUFFERED' : 'True'},
        labels={'sidecar.istio.io/inject' : 'false'},
        affinity=node_affinity,
    )

    StartOperator >> kubernetes_min_pod >> FakerOperator

if __name__ == "__main__":
    dag.cli()