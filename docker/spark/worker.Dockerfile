FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

COPY ./jars/hadoop-aws-3.4.0.jar /opt/spark/jars/
COPY ./jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
COPY requirements.txt .

USER root

RUN mkdir ${SPARK_HOME}/logs

RUN pip --no-cache-dir install -r requirements.txt

RUN chown -R spark:root ${SPARK_HOME}

USER spark

WORKDIR ${SPARK_HOME}

RUN chmod +x -R sbin bin

RUN sbin/spark-config.sh

RUN  bin/load-spark-env.sh

EXPOSE 8081

CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 >> logs/spark-worker.out