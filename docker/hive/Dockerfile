FROM apache/hive:standalone-metastore-4.1.0

USER root

RUN curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar && \
mv postgresql-42.7.7.jar /opt/hive/lib/

RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
mv hadoop-aws-3.4.0.jar /opt/hive/lib/

RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.32.25/bundle-2.32.25.jar && \
mv bundle-2.32.25.jar /opt/hive/lib/

COPY ./conf/metastore-site.xml /opt/hive/conf/

USER hive