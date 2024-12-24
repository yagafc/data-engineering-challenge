FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y \
    libpq-dev gcc python3-dev

USER airflow
COPY config/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt
