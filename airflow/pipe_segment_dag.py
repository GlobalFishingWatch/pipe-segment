from datetime import datetime, timedelta
import posixpath as pp
import six
import re

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from pipe_tools.airflow.dataflow_operator import DataFlowDirectRunnerOperator
from pipe_tools.airflow.config import load_config
from pipe_tools.airflow.config import default_args


CONFIG=load_config('pipe_segment')
DEFAULT_ARGS=default_args(CONFIG)


def table_sensor(dataset, table, date):
    return BigQueryTableSensor(
        task_id='source_exists_{}'.format(table),
        table_id='{}{}'.format(table, date),
        dataset_id=dataset,
        poke_interval=10,   # check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60),
        retry_exponential_backoff=False
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
        dataset = config['source_dataset']
        source_tables = config['normalized_tables'].split(',')
        source_sensors = [table_sensor(dataset, table, source_sensor_date) for table in source_tables]
        source_paths = ['bq://{}:{}.{}'.format(project, dataset, table) for table in source_tables]

        segment = DataFlowDirectRunnerOperator(
            task_id='segment',
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
                temp_shards_per_day="200",
                runner='{dataflow_runner}'.format(**config),
                project=config['project_id'],
                disk_size_gb="50",
                max_num_workers="{max_num_workers}".format(**config),
                worker_machine_type='custom-1-15360-ext',
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
                         '{project_id}:{pipeline_dataset}.{identity_messages_monthly_table} '
                         '{project_id}:{pipeline_dataset}.{segments_table} '
                         '{first_day_of_month} {last_day_of_month} '
                         '{project_id}:{pipeline_dataset}.{segment_identity_table}{first_day_of_month_nodash} '.format(**config)
        )

        segment_info = BashOperator(
            task_id='segment_info',
            pool='bigquery',
            bash_command='{docker_run} {docker_image} segment_info '
                         '{project_id}:{pipeline_dataset}.{segment_identity_table} '
                         '{project_id}:{pipeline_dataset}.{segment_info_table} '.format(**config)
        )


        for sensor in source_sensors:
            dag >> sensor >> segment

        segment >> identity_messages_monthly >> segment_identity >> segment_info

        return dag


segment_daily_dag = build_dag('pipe_segment_daily', '@daily')
segment_monthly_dag = build_dag('pipe_segment_monthly', '@monthly')