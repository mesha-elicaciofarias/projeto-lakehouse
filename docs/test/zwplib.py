import os
from pyspark.sql import SparkSession, DataFrame
import boto3
from trino.dbapi import connect
import logging

from typing import Any, Dict, List

# Configuração básica de logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class LakeHouse:
    packages = [
        "io.delta:delta-spark_2.13:4.0.0",
        "org.apache.hadoop:hadoop-aws:3.4.0",
    ]

    def __init__(self):
        self.minio_user = os.environ.get("MINIO_USER")
        self.minio_password = os.environ.get("MINIO_PASSWORD")
        self.minio_endpoint = (
            f"http://{os.environ.get('MINIO_HOST')}:{os.environ.get('MINIO_API_PORT')}"
        )
        self.trino_user = os.environ.get("TRINO_USER")
        self.trino_host = os.environ.get("TRINO_HOST")
        self.trino_port = os.environ.get("TRINO_PORT")

    def _start_spark_session(self) -> None:

        # app_name: str = "Airflow Spark Delta Minio App",
        self.spark_session = (
            SparkSession.Builder()
            # .appName(app_name)
            .config("spark.hadoop.fs.s3a.access.key", self.minio_user)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_password)
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            #
            .config("spark.hadoop.delta.enableFastS3AListFrom", "true")
            #
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
                "true",
            )
            .config(
                "spark.databricks.delta.properties.defaults.enableRowTracking", "true"
            )
            #
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            #
            .config("spark.jars.packages", ",".join(self.packages))
            #
            .master("local[*]")
            .getOrCreate()
        )

    def _start_minio_session(self) -> None:

        self.minio_session = boto3.client(
            "s3",
            aws_access_key_id=self.minio_user,
            aws_secret_access_key=self.minio_password,
            endpoint_url=self.minio_endpoint,
        )

    def _start_trino_session(self) -> None:

        self.trino_session = connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
        ).cursor()

    def write_files(
        self,
        files: List[str],
        catalog: str,
        schema: str,
        volume: str,
    ) -> None:
        # start boto3 session
        self._start_minio_session()

        # try create bucket
        try:
            self.minio_session.create_bucket(Bucket=catalog)
        except Exception:
            logger.warning(f"O catalogo {catalog} ja existe")

        # try save files
        for file in files:
            self.minio_session.upload_file(file, catalog, f"{schema}/{volume}/{file}")

        # close connection
        self.minio_session.close()

    def read_files(
        self,
        catalog: str,
        schema: str,
        volume: str,
        files: List[Dict[str, Any]],
    ) -> List[DataFrame]:

        self._start_spark_session()

        dfs = []

        for file in files:
            df = (
                self.spark_session.read.format(file["format"])
                .options(**file["options"])
                .load(f"s3a://{catalog}/{schema}/{volume}/{file["path"]}")
            )

            dfs.append(df)

        return dfs

    def write_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        dataframe: Dict[str, Any],
    ):
        # start boto3 session
        self._start_minio_session()

        # try create bucket
        try:
            self.minio_session.create_bucket(Bucket=catalog)
        except Exception:
            logger.info(f"O catalogo {catalog} ja existe")

        df = (
            dataframe["dataframe"]
            .write.mode(dataframe["mode"])
            .format("delta")
            .options(**dataframe["options"])
        )

        if partitions := dataframe["partition_by"]:
            df.partitionBy(partitions)

        df.save(f"s3a://{catalog}/{schema}/{table}")

        # start trino session
        self._start_trino_session()

        self.trino_session.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
            WITH (location = 's3a://{catalog}/{schema}')
            """
        )

        try:
            self.trino_session.execute(
                f"""
                CALL {catalog}.system.register_table(
                    schema_name => '{schema}', 
                    table_name => '{table}', 
                    table_location => 's3a://{catalog}/{schema}/{table}'
                )
                """
            )
        except Exception:
            logger.info(f"A tabela {catalog}.{schema}.{table} ja existe")

        self.minio_session.close()
        self.trino_session.close()
        self.spark_session.stop()

    def read_table(
        self, catalog: str, schema: str, table: str, options: Dict[str, Any]
    ) -> DataFrame:

        self._start_spark_session()

        df = (
            self.spark_session.read.format(options["format"])
            .options(**options["options"])
            .load(f"s3a://{catalog}/{schema}/{table}")
        )

        return df
