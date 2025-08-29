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
def mdic_paises_blocos_blocos():

    landing_mdic_paises_blocos_blocos = PapermillOperator(
        task_id="landing_mdic_paises_blocos_blocos",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_mdic_paises_blocos_blocos.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_mdic_paises_blocos_blocos_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )



    # bronze_mdic_paises_blocos_blocos = PapermillOperator(
    #     task_id="bronze_mdic_paises_blocos_blocos",
    #     input_nb=os.path.join(
    #         os.path.dirname(os.path.realpath(__file__)), 
    #         "tasks", 
    #         "bronze",
    #         "lnd_brz_mdic_paises_blocos_blocos.ipynb"
    #     ),
    #     output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_mdic_paises_blocos_blocos_{{ ts_nodash }}.ipynb",
    #     parameters={"minio_connection": Variable.get("minio_connection")}
    # )

    landing_mdic_paises_blocos_blocos.set_downstream(bronze_mdic_paises_blocos_blocos)


mdic_paises_blocos_blocos()
