## ETL End-To-End
ETL dataset keterlambatan penerbangan menggunakan `Postgres - Airflow - Hadoop`

### Requirement
- Docker
- Spark / Pyspark

### How to run
- Airflow initiation : `docker-compose up airflow-init`
- Run : `docker-compose up`
- Destroy : `docker-compose down`

### Explanation
ETL untuk mengolah data keterlambatan penerbangan. Menggunakan **Airflow** untuk melakukan otomatisasi ETL Script, ETL script dibuat dengan bahasa pemrograman **Python**, **Spark digunakan untuk meng-ekstrak data dan menulis ke database, dan **Hadoop** digunakan untuk menyimpan hasil data yang telah diolah. 

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

