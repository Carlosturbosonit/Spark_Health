from datetime import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

RAW = "/data/raw/fluview"
CLEAN = "/data/clean/fluview"
JAR = "/jars/spark-health-etl.jar"

with DAG(
    dag_id="fluview_weekly_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["fluview", "spark"],
) as dag:

    wait_raw = FileSensor(
        task_id="wait_raw",
        filepath=f"{RAW}/ingest_date={{{{ ds }}}}/_SUCCESS",
        poke_interval=30,
        timeout=600,
        fs_conn_id="fs_default",
    )

    clean = SparkSubmitOperator(
        task_id="spark_clean",
        conn_id="spark_standalone",
        application=JAR,
        java_class="com.sparkhealth.FluviewETL",
        application_args=[
            f"{RAW}/ingest_date={{{{ ds }}}}",
            f"{CLEAN}/ingest_date={{{{ ds }}}}",
        ],
    )

    wait_raw >> clean
