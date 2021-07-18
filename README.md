## ETL End-To-End
ETL data penerbangan menggunakan `Postgres - Airflow - Hadoop`

### Requirement
- Spark / Pyspark

### How to run
- Airflow initiation : `docker-compose up airflow-init`
- Run : `docker-compose up`
- Destroy : `docker-compose down`

ETL belum selesai, masih terdapat error yang belum bisa saya selesaikan

### Error
1. DAG Import Error `ModuleNotFoundError: No module named 'pyspark'`

![DAG Import Error](/images/1.JPG)

- Module pyspark sudah terinstall, tapi tetap terjadi error `ModuleNotFoundError: No module named 'pyspark'`

![Module pyspark](/images/2.JPG)

2. Berhasil menulis ke hadoop

![Hdfs Data](/images/3.JPG)

- Meskipun berhasil menulis ke hadoop dan data sudah masuk ke hdfs, tetapi masih terdapat error

![Hdfs Error](/images/4.JPG)

