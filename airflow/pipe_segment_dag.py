from datetime import datetime, timedelta
import posixpath as pp
import os

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


CONNECTION_ID = 'google_cloud_default'

CONFIG = Variable.get('pipe_segment', deserialize_json=True)
CONFIG['ds_nodash'] = '{{ ds_nodash }}'
CONFIG['first_day_of_month'] = '{{ execution_date.replace(day=1).strftime("%Y-%m-%d") }}'
CONFIG['last_day_of_month'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y-%m-%d") }}'
CONFIG['first_day_of_month_nodash'] = '{{ execution_date.replace(day=1).strftime("%Y%m%d") }}'
CONFIG['last_day_of_month_nodash'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y%m%d") }}'

def get_start_date():
    date_str = CONFIG.get('pipeline_start_date', Variable.get('PIPELINE_START_DATE', ''))
    if date_str:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    else:
        return datetime.utcnow() - timedelta(days=3)

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': get_start_date(),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'project_id': CONFIG['project_id'],
    'dataset_id': CONFIG['pipeline_dataset'],
    'bucket': CONFIG['pipeline_bucket'],
    'bigquery_conn_id': CONNECTION_ID,
    'gcp_conn_id': CONNECTION_ID,
    'google_cloud_conn_id': CONNECTION_ID,
    'write_disposition': 'WRITE_TRUNCATE',
    'allow_large_results': True,
}


def table_sensor(dataset, table, date):
    return BigQueryTableSensor(
        task_id='source_exists_{}'.format(table),
        table_id='{}{}'.format(table, date),
        poke_interval=10,   # check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60)
    )


def build_dag(dag_id, schedule_interval='@daily', extra_default_args=None, extra_config=None):

    default_args=DEFAULT_ARGS.copy()
    default_args.update(extra_default_args or {})

    config=CONFIG.copy()
    config.update(extra_config or {})

    if schedule_interval == '@daily':
        source_sensor_date = '{{ ds_nodash }}'
        date_range = '{{ ds }},{{ ds }}'
    elif schedule_interval == '@monthly':
        source_sensor_date = '{last_day_of_month_nodash}'.format(**config)
        date_range = '{first_day_of_month},{last_day_of_month}'.format(**config)
    else:
        raise ValueError('Unsupported schedule interval {}'.format(schedule_interval))

    with DAG(dag_id, schedule_interval=schedule_interval, default_args=default_args) as dag:

        project = config['project_id']
        dataset = config['pipeline_dataset']
        source_tables = config['normalized_tables'].split(',')
        source_sensors = [table_sensor(dataset, table, source_sensor_date) for table in source_tables]
        source_paths = ['bq://{}:{}.{}'.format(project, dataset, table) for table in source_tables]

        segment = DataFlowPythonOperator(
            task_id='segment',
            pool='dataflow',
            depends_on_past=True,
            py_file=Variable.get('DATAFLOW_WRAPPER_STUB'),
            priority_weight=10,
            options=dict(
                command='{docker_run} {docker_image} segment'.format(**config),
                startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'), 'pipe_segment/segment.log'),
                date_range=date_range,
                source=','.join(source_paths),
                dest='bq://{project_id}:{pipeline_dataset}.{messages_table}'.format(**config),
                segments='bq://{project_id}:{pipeline_dataset}.{segments_table}'.format(**config),
                runner='DataflowRunner',
                project=config['project_id'],
                disk_size_gb="50",
                max_num_workers="100",
                worker_machine_type='n1-highmem-2',
                temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                requirements_file='./requirements.txt',
                setup_file='./setup.py',
                experiments='shuffle_mode=service'
            )
        )

        identity_messages_monthly = BashOperator(
            task_id='identity_messages_monthly',
            pool='bigquery',
            bash_command='{docker_run} {docker_image} identity_messages_monthly '
                         '{project_id}:{pipeline_dataset}.{messages_table} '
                         '{project_id}:{pipeline_dataset}.{identity_messages_monthly_table}{first_day_of_month_nodash} '
                         '{first_day_of_month} {last_day_of_month}'.format(**config)
        )

        segment_identity = BashOperator(
            task_id='segment_identity',
            pool='bigquery',
            bash_command='{docker_run} {docker_image} segment_identity '
                         '{project_id}:{pipeline_dataset}.{identity_messages_monthly_table}{first_day_of_month_nodash} '
                         '{project_id}:{pipeline_dataset}.{segment_identity_table}{first_day_of_month_nodash} '.format(**config)
        )

        for sensor in source_sensors:
            dag >> sensor >> segment

        segment >> identity_messages_monthly >> segment_identity

        return dag


segment_daily_dag = build_dag('pipe_segment_daily', '@daily')
segment_monthly_dag = build_dag('pipe_segment_monthly', '@monthly')