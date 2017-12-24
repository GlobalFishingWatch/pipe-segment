from datetime import datetime, timedelta
import posixpath as pp
import os

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable

CONNECTION_ID = 'google_cloud_default'
THIS_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FILES = THIS_SCRIPT_DIR
with open(pp.join(DAG_FILES, 'identity_messages_monthly.sql.j2')) as f:
    IDENTITY_MESSAGES_MONTHLY_SQL = f.read()
with open(pp.join(DAG_FILES, 'segment_identity.sql.j2')) as f:
    SEGEMENT_IDENTITY_SQL = f.read()
FIRST_DAY_OF_MONTH='{{ execution_date.replace(day=1).strftime("%Y%m%d") }}'

config = Variable.get('pipe_segment', deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2017, 11, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'project_id': config['project_id'],
    'dataset_id': config['pipeline_dataset'],
    'bucket': config['pipeline_bucket'],
    'bigquery_conn_id': CONNECTION_ID,
    'gcp_conn_id': CONNECTION_ID,
    'google_cloud_conn_id': CONNECTION_ID,
    'write_disposition': 'WRITE_TRUNCATE',
    'allow_large_results': True,
}


with DAG('pipe_segment', schedule_interval=timedelta(days=1), default_args=default_args) as dag:

    source_exists= BigQueryTableSensor(
        task_id='source_exists',
        table_id="{normalized_table}{ds}".format(ds="{{ ds_nodash }}", **config),
        poke_interval=10,   #check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60),
        dag=dag
    )

    segment=DataFlowPythonOperator(
        task_id='segment',
        py_file=Variable.get('DATAFLOW_WRAPPER_STUB'),
        options=dict(
            command='{docker_run} {docker_image} segment'.format(**config),
            startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'), 'pipe_segment/segment.log'),
            date_range='{{ ds }},{{ ds }}',
            source='bq://{project_id}:{pipeline_dataset}.{normalized_table}'.format(**config),
            dest='bq://{project_id}:{pipeline_dataset}.{messages_table}'.format(**config),
            segments='bq://{project_id}:{pipeline_dataset}.{segments_table}'.format(**config),
            runner='DataflowRunner',
            project=config['project_id'],
            disk_size_gb="50",
            max_num_workers="100",
            temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
            staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
            requirements_file='./requirements.txt',
            setup_file='./setup.py'
        ),
        dag=dag
    )

    identity_messages_monthly = BigQueryOperator(
        task_id='identity_messages_monthly',
        bql=IDENTITY_MESSAGES_MONTHLY_SQL,
        destination_dataset_table='{project_id}'
                                  ':{pipeline_dataset}'
                                  '.{identity_messages_monthly_table}'
                                  '{first_day_of_month}'.format(first_day_of_month=FIRST_DAY_OF_MONTH, **config),
        params={
            'messages_table': '{messages_table}'.format(**config)
        }
    )

    segment_identity = BigQueryOperator(
        task_id = 'segment_identity',
        bql=SEGEMENT_IDENTITY_SQL,
        destination_dataset_table='{project_id}'
                                  ':{pipeline_dataset}'
                                  '.{segment_identity_table}'
                                  '{first_day_of_month}'.format(first_day_of_month=FIRST_DAY_OF_MONTH, **config),
        params={
            'identity_messages_table': '{identity_messages_monthly_table}'.format(**config),
        },
        dag=dag
    )
    source_exists >> segment >> identity_messages_monthly >> segment_identity
