import os
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession

args = {
    "owner": "user",
    "start_date": datetime.datetime(2021, 7, 11)
}

def read_write():
    url = "jdbc:postgresql://localhost:5432/digitalskola"
    user = "digitalskola"
    password = "digitalskola"
    driver = "org.postgresql.Driver"

    spark = SparkSession.builder \
        .appName("Spark Postgres") \
        .config("spark.jars", "./jar/postgresql-42.2.23.jar") \
        .getOrCreate()
    
    df1 = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "airlines") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()
    
    df2 = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "airports") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()

    df3 = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "flights") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()
    
    df1.write.parquet("hdfs://0.0.0.0:9000/data/data_penerbangan.parquet", mode="append")
    df2.write.parquet("hdfs://0.0.0.0:9000/data/data_penerbangan.parquet", mode="append")
    df3.write.parquet("hdfs://0.0.0.0:9000/data/data_penerbangan.parquet", mode="append")


with DAG(
    dag_id="etl-end-to-end",
    default_args=args,
    schedule_interval="@daily"
) as dag:

    task_pertama = PythonOperator(
        task_id="panda_read",
        python_callable=read_write
    )
    
task_pertama

