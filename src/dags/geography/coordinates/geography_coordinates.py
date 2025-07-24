import os
import pendulum

from airflow.sdk import dag

from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.sdk import Variable

@dag(
    schedule="*/3 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Maceio"),
    catchup=False,
    tags=["pyspark", "delta", "minio"],
)
def geography_coordinates():

    return PapermillOperator(
        task_id="geography_coordinates",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "src_lnd_geography_coordinates.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/src_lnd_geography_coordinates_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )


geography_coordinates()
