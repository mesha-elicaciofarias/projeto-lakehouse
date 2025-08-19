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
def rfb_cno():

    landing_rfb_cno = PapermillOperator(
        task_id="landing_rfb_cno",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks",
            "landing",
            "src_lnd_rfb_cno.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rfb_cno_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")},
    )

    bronze_rfb_cno_areas = PapermillOperator(
        task_id="bronze_rfb_cno_areas",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks",
            "bronze",
            "lnd_brz_rfb_cno_areas.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_rfb_cno_areas_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")},
    )

    bronze_rfb_cno_cnaes = PapermillOperator(
        task_id="bronze_rfb_cno_cnaes",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks",
            "bronze",
            "lnd_brz_rfb_cno_cnaes.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_rfb_cno_cnaes_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")},
    )

    bronze_rfb_cno = PapermillOperator(
        task_id="bronze_rfb_cno",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks",
            "bronze",
            "lnd_brz_rfb_cno.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_rfb_cno_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")},
    )

    bronze_rfb_cno_totais = PapermillOperator(
        task_id="bronze_rfb_cno_totais",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks",
            "bronze",
            "lnd_brz_rfb_cno_totais.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_rfb_cno_totais_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")},
    )

    bronze_rfb_cno_vinculos = PapermillOperator(
        task_id="bronze_rfb_cno_vinculos",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks",
            "bronze",
            "lnd_brz_rfb_cno_vinculos.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_rfb_cno_vinculos_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")},
    )

    landing_rfb_cno.set_downstream(
        [
            bronze_rfb_cno_areas,
            bronze_rfb_cno_cnaes,
            bronze_rfb_cno,
            bronze_rfb_cno_totais,
            bronze_rfb_cno_vinculos,
        ]
    )


rfb_cno()
