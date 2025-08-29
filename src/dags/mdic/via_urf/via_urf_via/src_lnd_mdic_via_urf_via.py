import os

import pendulum
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.sdk import Variable, dag


@dag(
    schedule="*/3 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Maceio"),
    catchup=False,
    tags=["pyspark", "delta", "minio"],
)
def mdic_via_urf_via():

    landing_mdic_via_urf_via = PapermillOperator(
        task_id="landing_mdic_via_urf_via",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_mdic_via_urf_via.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_mdic_via_urf_via_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )



    # bronze_mdic_municipio_ncm = PapermillOperator(
    #     task_id="bronze_mdic_municipio_ncm",
    #     input_nb=os.path.join(
    #         os.path.dirname(os.path.realpath(__file__)), 
    #         "tasks", 
    #         "bronze",
    #         "lnd_brz_mdic_municipio_ncm.ipynb"
    #     ),
    #     output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_mdic_municipio_ncm_{{ ts_nodash }}.ipynb",
    #     parameters={"minio_connection": Variable.get("minio_connection")}
    # )

    landing_mdic_via_urf_via.set_downstream(bronze_mdic_via_urf_via)


mdic_via_urf_via()
