from datetime import datetime, timedelta
import posixpath as pp
import six
import re

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator
from airflow_ext.gfw.config import load_config
from airflow_ext.gfw.config import default_args
from airflow_ext.gfw.models import DagFactory

PIPELINE = "pipe_segment"

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


class PipeSegmentDagFactory(DagFactory):
    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipeSegmentDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def build(self, dag_id):
        config = self.config
        config['date_range'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)
    
            project = config['project_id']
            dataset = config['source_dataset']
            source_tables = config['source_tables'].split(',')
            source_paths = ['bq://{}:{}.{}'.format(project, dataset, table) for table in source_tables]
    
            segment = DataFlowDirectRunnerOperator(
                task_id='segment',
                depends_on_past=True,
                py_file=Variable.get('DATAFLOW_WRAPPER_STUB'),
                priority_weight=10,
                options=dict(
                    command='{docker_run} {docker_image} segment'.format(**config),
                    startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'), 'pipe_segment/segment.log'),
                    date_range='{date_range}'.format(**config),
                    source=','.join(source_paths),
                    dest_table='bq://{project_id}:{pipeline_dataset}.{messages_table}'.format(**config),
                    segments='bq://{project_id}:{pipeline_dataset}.{segments_table}'.format(**config),

                    temp_shards_per_day="200",
                    runner='{dataflow_runner}'.format(**config),
                    project=config['project_id'],
                    max_num_workers='{dataflow_max_num_workers}'.format(**config),
                    disk_size_gb='{dataflow_disk_size_gb}'.format(**config),
                    worker_machine_type='{dataflow_machine_type}'.format(**config),
                    temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                    staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                    requirements_file='./requirements.txt',
                    setup_file='./setup.py',
                    experiments='shuffle_mode=service'
                )
            )
    
            segment_identity_daily = BashOperator(
                task_id='segment_identity_daily',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} segment_identity_daily '
                             '{date_range} '
                             '{project_id}:{pipeline_dataset}.{messages_table} '
                             '{project_id}:{pipeline_dataset}.{segments_table} '
                             '{project_id}:{pipeline_dataset}.{segment_identity_daily_table} '.format(**config)
            )
    
            segment_vessel_daily = BashOperator(
                task_id='segment_vessel_daily',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} segment_vessel_daily '
                             '{date_range} '
                             '{window_days} '
                             '{single_ident_min_freq} '
                             '{most_common_min_freq} '
                             '{spoofing_threshold} '
                             '{project_id}:{pipeline_dataset}.{segment_identity_daily_table} '
                             '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table} '.format(**config)
            )
    
            segment_info = BashOperator(
                task_id='segment_info',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} segment_info '
                             '{project_id}:{pipeline_dataset}.{segment_identity_daily_table} '
                             '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table} '
                             '{most_common_min_freq} '
                             '{project_id}:{pipeline_dataset}.{segment_info_table} '.format(**config)
            )
    
            vessel_info = BashOperator(
                task_id='vessel_info',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} vessel_info '
                             '{project_id}:{pipeline_dataset}.{segment_identity_daily_table} '
                             '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table} '
                             '{most_common_min_freq} '
                             '{project_id}:{pipeline_dataset}.{vessel_info_table} '.format(**config)
            )
    
            segment_vessel = BashOperator(
                task_id='segment_vessel',
                pool='bigquery',
                bash_command='{docker_run} {docker_image} segment_vessel '
                             '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table} '
                             '{project_id}:{pipeline_dataset}.{segment_vessel_table} '.format(**config)
            )

            for sensor in source_sensors:
                dag >> sensor >> segment
                
            segment >> segment_identity_daily
            segment_identity_daily >> segment_info
            segment_identity_daily >> segment_vessel_daily
            segment_vessel_daily >> vessel_info
            segment_vessel_daily >> segment_vessel
    
            return dag


segment_daily_dag = PipeSegmentDagFactory().build('pipe_segment_daily')
segment_monthly_dag = PipeSegmentDagFactory(schedule_interval='@monthly').build('pipe_segment_monthly')
segment_yearly_dag = PipeSegmentDagFactory(schedule_interval='@yearly').build('pipe_segment_yearly')
