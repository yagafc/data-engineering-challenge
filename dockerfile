FROM apache/airflow:2.10.4

USER airflow
COPY config/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt
