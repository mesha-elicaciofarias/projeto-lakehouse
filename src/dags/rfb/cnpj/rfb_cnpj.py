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
def rbf_cnpj():

    landing_rbf_cnpj_cnaes = PapermillOperator(
        task_id="landing_rbf_cnpj_cnaes",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_cnaes.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_cnaes_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )


    landing_rbf_cnpj_empresas = PapermillOperator(
        task_id="landing_rbf_cnpj_empresas",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_empresas.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_empresas_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_estabelecimentos = PapermillOperator(
        task_id="landing_rbf_cnpj_estabelecimentos",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_estabelecimentos.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_estabelecimentos_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_motivos = PapermillOperator(
        task_id="landing_rbf_cnpj_motivos",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_motivos.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_motivos_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_municipios = PapermillOperator(
        task_id="landing_rbf_cnpj_municipios",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_municipios.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_municipios_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_naturezas = PapermillOperator(
        task_id="landing_rbf_cnpj_naturezas",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_naturezas.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_naturezas_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_paises = PapermillOperator(
        task_id="landing_rbf_cnpj_paises",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_paises.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_paises_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_qualificacoes = PapermillOperator(
        task_id="landing_rbf_cnpj_qualificacoes",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_qualificacoes.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_qualificacoes_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_simples = PapermillOperator(
        task_id="landing_rbf_cnpj_simples",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_simples.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_simples_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rbf_cnpj_socios = PapermillOperator(
        task_id="landing_rbf_cnpj_socios",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rbf_cnpj_socios.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rbf_cnpj_socios_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )


    # bronze = PapermillOperator(
    #     task_id="bronze_cno_cno_areas",
    #     input_nb=os.path.join(
    #         os.path.dirname(os.path.realpath(__file__)), "tasks", "bronze","lnd_brz_cno_cno_areas.ipynb"
    #     ),
    #     output_nb="/opt/airflow/logs/tasks/bronze/lnd_brz_cno_cno_areas_{{ ts_nodash }}.ipynb",
    #     parameters={"minio_connection": Variable.get("minio_connection")}
    # )


    landing_rbf_cnpj_cnaes.set_downstream(landing_rbf_cnpj_empresas)
    landing_rbf_cnpj_empresas.set_downstream(landing_rbf_cnpj_estabelecimentos)
    landing_rbf_cnpj_estabelecimentos.set_downstream(landing_rbf_cnpj_motivos)
    landing_rbf_cnpj_motivos.set_downstream(landing_rbf_cnpj_municipios)
    landing_rbf_cnpj_municipios.set_downstream(landing_rbf_cnpj_naturezas)
    landing_rbf_cnpj_naturezas.set_downstream(landing_rbf_cnpj_paises)
    landing_rbf_cnpj_paises.set_downstream(landing_rbf_cnpj_qualificacoes)
    landing_rbf_cnpj_qualificacoes.set_downstream(landing_rbf_cnpj_simples)
    landing_rbf_cnpj_simples.set_downstream(landing_rbf_cnpj_socios)

rbf_cnpj()
