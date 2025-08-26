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
def rfb_regime_tributario():

    landing_rfb_regime_tributario_imunes_isentas = PapermillOperator(
        task_id="src_lnd_rfb_regime_tributario_imunes_isentas",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rfb_regime_tributario_imunes_isentas.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rfb_regime_tributario_imunes_isentas_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )


    landing_rfb_regime_tributario_lucro_arbitrado = PapermillOperator(
        task_id="src_lnd_rfb_regime_tributario_lucro_arbitrado",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rfb_regime_tributario_lucro_arbitrado",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rfb_regime_tributario_lucro_arbitrado_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    landing_rfb_regime_tributario_lucro_presumido = PapermillOperator(
        task_id="src_lnd_rfb_regime_tributario_lucro_presumido",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rfb_regime_tributario_lucro_presumido.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rfb_regime_tributario_lucro_presumido_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )


    landing_rfb_regime_tributario_lucro_real = PapermillOperator(
        task_id="src_lnd_rfb_regime_tributario_lucro_real",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "src_lnd_rfb_regime_tributario_lucro_real.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/src_lnd_rfb_regime_tributario_lucro_real_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    bronze_rfb_regime_tributario_imunes_isentas = PapermillOperator(
        task_id="lnd_brz_rfb_regime_tributario_imunes_isentas",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "lnd_brz_rfb_regime_tributario_imunes_isentas.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/lnd_brz_rfb_regime_tributario_imunes_isentas_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )


    bronze_rbf_regime_tributario_lucro_arbitrado = PapermillOperator(
        task_id="landing_rbf_regime_tributario_lucro_arbitrado",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "landing_rbf_regime_tributario_lucro_arbitrado",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/landing_rbf_regime_tributario_lucro_arbitrado_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )

    bronze_rbf_regime_tributario_lucro_presumido = PapermillOperator(
        task_id="landing_rbf_regime_tributario_lucro_presumido",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "lnd_brz_rfb_regime_tributario_lucro_presumido.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/lnd_brz_rfb_regime_tributario_lucro_presumido_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )


    bronze_rbf_regime_tributario_lucro_real = PapermillOperator(
        task_id="landing_rbf_regime_tributario_lucro_real",
        input_nb=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "tasks", 
            "landing", 
            "lnd_brz_rfb_regime_tributario_lucro_real.ipynb",
        ),
        output_nb="/opt/airflow/logs/tasks/landing/lnd_brz_rfb_regime_tributario_lucro_real_{{ ts_nodash }}.ipynb",
        parameters={"minio_connection": Variable.get("minio_connection")}
    )





    landing_rfb_regime_tributario_imunes_isentas.set_downstream(bronze_rfb_regime_tributario_imunes_isentas)
    landing_rfb_regime_tributario_lucro_arbitrado.set_downstream(bronze_rbf_regime_tributario_lucro_arbitrado)
    landing_rfb_regime_tributario_lucro_presumido.set_downstream(bronze_rbf_regime_tributario_lucro_presumido)
    landing_rfb_regime_tributario_lucro_real.set_downstream(bronze_rbf_regime_tributario_lucro_real)
  

rfb_regime_tributario()
