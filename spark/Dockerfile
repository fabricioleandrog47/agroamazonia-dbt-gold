FROM apache/spark:3.5.0

# Baixar Delta Lake e AWS JARs
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar -P /opt/spark/jars/

RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar -P /opt/spark/jars/

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars/

RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.500/aws-java-sdk-bundle-1.12.500.jar -P /opt/spark/jars/

ENV SPARK_HOME=/opt/spark