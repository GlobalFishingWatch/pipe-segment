from datetime import datetime, timedelta
import posixpath as pp

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.models import Variable

CONNECTION_ID = 'google_cloud_default'

config = Variable.get('pipe_segment', deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
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

    source_exists >> segment
