from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator



default_args = {
    "owner": "Jenifer",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
        dag_id= "Food_dag",
        description="This is dag to manage the data lake",
        schedule_interval="@weekly",
        start_date=datetime(2024, 0o2, 0o7),
        default_args=default_args
) as dag:
    pass
    ingest_task = BashOperator(
        task_id='sh_script_execution',
        bash_command= "/home/ubuntu/airflow/file.sh {{ ds }}",
    )

    processed_task = SparkSubmitOperator(
        task_id='formatted_task_name',  # formatted
        conn_id='spark_default',
        application='/home/ubuntu/Downloads/mnmcount/scala/target/scala-2.12/main-scala-mnm_2.12-1.0.jar',
        java_class='main.scala.mnm.Jobformated',
        conf={
            'spark.airflow.execution_date': '{{ ds }}'
        },
    )

    '''combined_task = SparkSubmitOperator(
        task_id='usage_task',  # combined
        conn_id='spark_default',
        application='jar_path',
        java_class='main.scala.mnm...',
        conf={
            'spark.airflow.execution_date': '{{ ds }}'
        },
    )'''

    ingest_task >> processed_task
    # combined_task