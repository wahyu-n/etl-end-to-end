import datetime
import pandas as panda
import psycopg2

from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

args = {
    "owner": "user",
    "start_date": datetime.datetime(2021, 7, 11)
}

def panda_read():
    engine = create_engine('postgresql://digitalskola:digitalskola@postgres/digitalskola')
    connection = engine.connect()
    df1 = panda.read_sql("select * from airlines", connection)
    print(df1)

def spark():
    spark = SparkSession.builder \
        .appName("Spark Postgres") \
        .config("spark.jars", "./jar/postgresql-42.2.23.jar") \
        .getOrCreate()
    
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/digitalskola") \
        .option("dbtable", "flights") \
        .option("user", "digitalskola") \
        .option("password", "digitalskola") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df.printSchema()

with DAG(
    dag_id="etl-end-to-end",
    default_args=args,
    schedule_interval="@daily"
) as dag:

    task_pertama = PythonOperator(
        task_id="panda_read",
        python_callable=panda_read
    )

    task_kedua = PostgresOperator(
        task_id="postgres_operator",
        postgres_conn_id="postgres_connecion",
        sql="SELECT * FROM airlines;"
    )

    task_ketiga = PythonOperator(
        task_id="spark_read",
        python_callable=spark
    )

task_pertama >> task_kedua >> task_ketiga


