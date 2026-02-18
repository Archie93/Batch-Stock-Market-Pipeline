FROM apache/spark:3.4.2
USER root

ENV SPARK_DRIVER_MEMORY=4g
ENV SPARK_EXECUTOR_MEMORY=4g
ENV SPARK_SQL_SHUFFLE_PARTITIONS=16
ENV SPARK_MEMORY_FRACTION=0.7

# JDBC driver
RUN curl -L -o /opt/spark/jars/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
COPY batch_job.py /opt/spark/app/
COPY entrypoint.sh /opt/spark/app/
RUN chmod +x /opt/spark/app/entrypoint.sh
ENTRYPOINT ["/opt/spark/app/entrypoint.sh"]
