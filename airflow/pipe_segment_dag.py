from airflow import DAG
from airflow.models import Variable

from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator

import posixpath as pp


PIPELINE = "pipe_segment"

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
                    pipeline_start_date=self.default_args['start_date'].strftime("%Y-%m-%d"),
                    source=','.join(source_paths),
                    msg_dest='bq://{project_id}:{pipeline_dataset}.{messages_table}'.format(**config),
                    legacy_seg_v1_dest='bq://{project_id}:{pipeline_dataset}.{legacy_segment_v1_table}'.format(**config),
                    seg_dest='bq://{project_id}:{pipeline_dataset}.{segments_table}'.format(**config),
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

            segment_identity_daily = DataFlowDirectRunnerOperator(
                task_id='segment_identity_daily',
                py_file=Variable.get('DATAFLOW_WRAPPER_STUB'),
                priority_weight=10,
                options=dict(
                    command='{docker_run} {docker_image} segment_identity_daily'.format(**config),
                    startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'), 'pipe_segment/segment.log'),
                    date_range='{date_range}'.format(**config),
                    source='bq://{project_id}:{pipeline_dataset}.{segments_table}'.format(**config),
                    segment_identity_daily_dest='bq://{project_id}:{pipeline_dataset}.{segment_identity_daily_table}',
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

            segment_vessel_daily = self.build_docker_task({
                'task_id':'segment_vessel_daily',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'segment-identity-daily',
                'dag':dag,
                'arguments':['segment_vessel_daily',
                             '{date_range}'.format(**config),
                             '{window_days}'.format(**config),
                             '{single_ident_min_freq}'.format(**config),
                             '{most_common_min_freq}'.format(**config),
                             '{spoofing_threshold}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{segment_identity_daily_table}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table}'.format(**config)]
            })


            for sensor in source_sensors:
                dag >> sensor >> segment

            segment >> segment_identity_daily

            segment_identity_daily >> segment_vessel_daily

            if config.get('enable_aggregate_tables', False):
                segment_info = self.build_docker_task({
                    'task_id':'segment_info',
                    'pool':'k8operators_limit',
                    'docker_run':'{docker_run}'.format(**config),
                    'image':'{docker_image}'.format(**config),
                    'name':'segment-info',
                    'dag':dag,
                    'arguments':['segment_info',
                                 '{project_id}:{pipeline_dataset}.{segment_identity_daily_table}'.format(**config),
                                 '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table}'.format(**config),
                                 '{most_common_min_freq}'.format(**config),
                                 '{project_id}:{pipeline_dataset}.{segment_info_table}'.format(**config)]
                })

                vessel_info = self.build_docker_task({
                    'task_id':'vessel_info',
                    'pool':'k8operators_limit',
                    'docker_run':'{docker_run}'.format(**config),
                    'image':'{docker_image}'.format(**config),
                    'name':'vessel-info',
                    'dag':dag,
                    'arguments':['vessel_info',
                                 '{project_id}:{pipeline_dataset}.{segment_identity_daily_table}'.format(**config),
                                 '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table}'.format(**config),
                                 '{most_common_min_freq}'.format(**config),
                                 '{project_id}:{pipeline_dataset}.{vessel_info_table}'.format(**config)]
                })

                segment_vessel = self.build_docker_task({
                    'task_id':'segment_vessel',
                    'pool':'k8operators_limit',
                    'docker_run':'{docker_run}'.format(**config),
                    'image':'{docker_image}'.format(**config),
                    'name':'segment-vessel',
                    'dag':dag,
                    'arguments':['segment_vessel',
                                 '{project_id}:{pipeline_dataset}.{segment_vessel_daily_table}'.format(**config),
                                 '{project_id}:{pipeline_dataset}.{segment_vessel_table} '.format(**config)]
                })

                segment_identity_daily >> segment_info
                segment_vessel_daily >> vessel_info
                segment_vessel_daily >> segment_vessel

            return dag

for mode in ['daily','monthly', 'yearly']:
    dag_id = '{}_{}'.format(PIPELINE, mode)
    globals()[dag_id] = PipeSegmentDagFactory(schedule_interval='@{}'.format(mode)).build(dag_id)
