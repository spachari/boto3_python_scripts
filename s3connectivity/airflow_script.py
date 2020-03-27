import os
import sys

import logging
import airflow.utils

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

from ruamel import yaml


dag_params = yaml.round_trip_load(Variable.get('MKT-HDE-SqlServer-Sync'))


def setup_sys_path(params):
    logging.info('setup sys path')

    for p in params['append_to_dag_system_path']:
        if not os.path.exists(p):
            raise ValueError('Invalid path: {}'.format(p))
        sys.path.append(os.path.abspath(p))


setup_sys_path(dag_params)

from hde_utils import generate_emr_step, add_emr_step, add_emr_settings_overrides

CUSTOM_EMR = add_emr_settings_overrides(dag_params['bucket'], dag_params['key'])
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 01),
    'email': [dag_params['notifications_email']],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '30 * * * *'
}


def create_emr_tasks_chain_with_yml_key(
        dag, dag_default_args, dag_params, step_key, param_step_key):

    get_task_id = 'get_{}_step'.format(step_key)
    get_step_name = '{}_step'.format(step_key)

    t_get_step = PythonOperator(
        task_id=get_task_id,
        default_args=dag_default_args,
        provide_context=True,
        python_callable=generate_emr_step,
        op_kwargs={
            'step_name': get_step_name,
            'step_template': dag_params[param_step_key],
            'env_info': dag_params
        },
        dag=dag
    )

    add_step_task_id = 'add_{}_step'.format(step_key)

    t_add_step = PythonOperator(
        task_id=add_step_task_id,
        default_args=dag_default_args,
        provide_context=True,
        python_callable=add_emr_step,
        op_kwargs={
            'aws_conn_id': dag_params['aws_conn_id'],
            'create_job_flow_task': 'create_emr_et_job_flow',
            'get_step_task': get_task_id
        },
        dag=dag
    )

    check_step_task_id = 'check_{}_step'.format(step_key)

    s_check_step = EmrStepSensor(
        task_id=check_step_task_id,
        default_args=dag_default_args,
        job_flow_id="{{ task_instance.xcom_pull('create_emr_et_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('" + add_step_task_id + "', key='return_value')[0] }}",
        aws_conn_id=dag_params['aws_conn_id'],
        dag=dag
    )

    return t_get_step, t_add_step, s_check_step


def get_load_funnel_base_names():
    for name in ["CUSTOMER_ACCOUNT", "LOYALTY_ACCOUNT", "LOYALTY_ACCOUNT_TIER_HISTORY"]:
        yield name


def create_funnel_operators(dag, dag_default_args, dag_params):

    for funnel_name in get_load_funnel_base_names():

        param_key = '{}'.format(funnel_name)

        yield create_emr_tasks_chain_with_yml_key(
            dag=dag,
            dag_default_args=dag_default_args,
            dag_params=dag_params,
            step_key=param_key,
            param_step_key=param_key,
        )


def create_dag(dag_name):

    dag = DAG(
        dag_name,
        default_args=default_args,
        schedule_interval="0 5 * * *",
        catchup=False,
        concurrency=1,
        max_active_runs=1
    )

    t_emr_cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_emr_et_job_flow',
        job_flow_overrides=CUSTOM_EMR,
        aws_conn_id=dag_params['aws_conn_id'],
        emr_conn_id=dag_params['emr_conn_id'],
        dag=dag
    )

    t_emr_cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_emr_et_job_flow',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_et_job_flow', key='return_value') }}",
        aws_conn_id=dag_params['aws_conn_id'],
        dag=dag
    )

    s_cluster_sensor = EmrJobFlowSensor(
        task_id='check_exacttarget_tracking_flow_sensor',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_et_job_flow', key='return_value') }}",
        aws_conn_id=dag_params['aws_conn_id'],
        dag=dag
    )

    airflow.utils.helpers.chain(
        t_emr_cluster_creator,

    )

    for funnel_step_ops in create_funnel_operators(
            dag=dag, dag_default_args=default_args, dag_params=dag_params):

        t_emr_cluster_creator >> funnel_step_ops[0]
        airflow.utils.helpers.chain(*funnel_step_ops)
        funnel_step_ops[-1] >> t_emr_cluster_remover

    airflow.utils.helpers.chain(
        t_emr_cluster_remover,
        s_cluster_sensor
    )

    return dag


dag = create_dag(dag_name='MKT-HDE-SqlServer-Sync')
