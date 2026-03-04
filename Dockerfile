FROM apache/spark:3.5.2-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y \
    python3-pip wget \
    && rm -rf /var/lib/apt/lists/*

# Delta + AWS
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.500/aws-java-sdk-bundle-1.12.500.jar

RUN pip3 install dbt-spark==1.7.1 pyspark==3.5.2 boto3 python-dotenv psycopg2-binary python-dotenv

WORKDIR /dbt

COPY models/ /dbt/models/
#COPY macros/ /dbt/macros/
COPY dbt_project.yml /dbt/

RUN mkdir -p /opt/dbt
COPY profile.yml /opt/dbt/profiles.yml

WORKDIR /app
COPY dbt_runner.py /app/
COPY scripts/ /app/scripts/

RUN chown -R 185:185 /dbt /opt/dbt /app

USER 185
