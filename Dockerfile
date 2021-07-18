FROM apache/airflow:latest
WORKDIR /airflow
COPY requirement.txt requirement.txt
RUN pip install -r requirement.txt



