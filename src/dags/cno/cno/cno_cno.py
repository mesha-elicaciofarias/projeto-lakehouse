import os
import pendulum

from airflow.sdk import dag

from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.sdk import Variable
from airflow.operators.bash import BashOperator


@dag(
    schedule="*/3 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Maceio"),
    catchup=False,
    tags=["pyspark", "delta", "minio"],
)
def cno_cno():

    # Tarefa para criar o diretório "bronze" antes da execução dos notebooks
    create_bronze_dir = BashOperator(
        task_id="create_bronze_dir",
        bash_command="mkdir -p /opt/airflow/logs/tasks/bronze"
    )

    landing = PapermillOperator(
        task_id="landing_cno_cno",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "landing", "src_lnd_cno_cno.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_cno_cno_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    bronze = PapermillOperator(
        task_id="bronze_cno_cno_areas",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "bronze","lnd_brz_cno_cno_areas.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_cno_cno_areas_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    bronze = PapermillOperator(
        task_id="bronze_cno_cno_cnaes",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "bronze","lnd_brz_cno_cno_cnaes.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_cno_cno_cnaes_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    bronze = PapermillOperator(
        task_id="bronze_cno_cno_cno",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "bronze","lnd_brz_cno_cno_cno.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_cno_cno_cno_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    bronze = PapermillOperator(
        task_id="bronze_cno_cno_totais",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "bronze","lnd_brz_cno_cno_totais.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_cno_cno_totais_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    bronze = PapermillOperator(
        task_id="bronze_cno_cno_vinculos",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "tasks", "bronze","lnd_brz_cno_cno_vinculos.ipynb"
        ),
        output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_cno_cno_vinculos_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    # silver = PapermillOperator(
    #     task_id="silver_cno_cno",
    #     input_nb=os.path.join(
    #         os.path.dirname(os.path.realpath(__file__)), "tasks", "silver","brz_slv_cno_cno.ipynb"
    #     ),
    #     output_nb="/opt/airflow/logs/tasks/silver/brz_slv_cno_cno_{{ ts_nodash }}.ipynb",
    #     parameters={"minio_connection": Variable.get("minio_connection")}
    # )

    landing >> bronze 
    #>> silver

cno_cno()
