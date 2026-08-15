# Opt-in JVM overlay: Apache Spark 3.5.5 + Delta 3.2, not Sail.
# Fabric Runtime 1.3's public engine baseline. Java 11, not the short
# apache/spark:3.5.5 tag (that one is Java 17).
FROM apache/spark:3.5.5-scala2.12-java11-python3-ubuntu
USER root
COPY agent.py /opt/spark/work-dir/agent.py
USER spark
EXPOSE 8099
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.1", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/opt/spark/work-dir/agent.py"]
