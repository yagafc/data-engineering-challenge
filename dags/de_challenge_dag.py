from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}



with DAG(
        'de_challenge',
        default_args=default_args,
        description='Download and process links',
        schedule_interval=None,
        start_date=datetime(2024, 12, 20),
        catchup=False,
        tags=['commoncrawl', 'download', 'elaboration', 'report', 'metrics'],
) as dag:

    base_path = Variable.get("base_path", default_var="/opt/airflow")
    file_format = Variable.get("file_format", default_var="WET")
    crawl_version = Variable.get("crawl_data_version", default_var="CC-MAIN-2024-46")
    num_segments = Variable.get("num_segments", default_var="3")
    raw_dir = Variable.get("raw_dir", default_var=f"{base_path}/data/raw/{file_format.lower()}")
    max_filtered = Variable.get("max_filtered", default_var="0")

    links_dir = Variable.get("link_dir", default_var=f"{base_path}/data/extracted/{file_format.lower()}")
    transformed_dir = Variable.get("transformed_dir", default_var=f"{base_path}/data/transformed/")
    metrics_dir = Variable.get("metrics_dir", default_var=f"{base_path}/data/metrics/")

    start_task = DummyOperator(
        task_id='start',
    )

    download_files = BashOperator(
        task_id='download_files',
        bash_command=f'bash {base_path}/scripts/bash/download_segment.sh '
                     f'--crawl-data-version {crawl_version} '
                     f'--n-segment {num_segments} '
                     f'--destination {raw_dir} '
                     f'--format {file_format} '
                     f'--max-filtered {max_filtered}',
        retries=2,
        retry_delay=timedelta(seconds=5),
    )

    extract_links_from_files = BashOperator(
        task_id=f'extract_links_from_files',
        bash_command=f'python {base_path}/scripts/python/extract_links.py '
                     f'--source {raw_dir} '
                     f'--destination {links_dir} '
                     f'--format {file_format.lower()} '
    )

    load_links = BashOperator(
        task_id='load_links',
        bash_command=f'python {base_path}/scripts/python/load_links.py '
                     f'--source {links_dir} '
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=f'python {base_path}/scripts/python/transform.py '
                     f'--destination {transformed_dir} '
    )

    end_task = DummyOperator(
        task_id='end',
    )

    start_task >> download_files >> extract_links_from_files >> load_links >> transform_data >> end_task
