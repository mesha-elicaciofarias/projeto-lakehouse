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
def ibge_ipca_amplo():

    landing = PapermillOperator(
        task_id="landing_ibge_ipca_amplo",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "landing", "src_lnd_ibge_ipca_amplo.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_ibge_ipca_amplo_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    # bronze = PapermillOperator(
    #     task_id="bronze_ibge_ipca_amplo",
    #     input_nb=os.path.join(
    #         os.path.dirname(os.path.realpath(__file__)), "tasks", "bronze","lnd_brz_ibge_ipca_amplo.ipynb"
    #     ),
    #     output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_ibge_ipca_amplo_{{ ts_nodash }}.ipynb",
    #     parameters={"minio_connection": Variable.get("minio_connection")}
    # )

    # silver = PapermillOperator(
    #     task_id="silver_ibge_ipca_amplo",
    #     input_nb=os.path.join(
    #         os.path.dirname(os.path.realpath(__file__)), "tasks", "silver","brz_slv_ibge_ipca_amplo.ipynb"
    #     ),
    #     output_nb="/opt/airflow/logs/tasks/silver/brz_slv_ibge_ipca_amplo_{{ ts_nodash }}.ipynb",
    #     parameters={"minio_connection": Variable.get("minio_connection")}
    # )

    landing 
    # >> bronze >> silver

ibge_ipca_amplo()
