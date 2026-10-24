# Image Spark de VogueMind AI
#  - base officielle Apache Spark 3.5.3 (Scala 2.12, Java 17, Python 3)
#  - connecteur Kafka intégré (Structured Streaming) => aucun téléchargement au lancement
#  - dépendances Python figées pour Spark ML
ARG SPARK_IMAGE=spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

ARG SPARK_VERSION=3.5.3
ARG SCALA_BINARY=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl python3-pip \
 && rm -rf /var/lib/apt/lists/*

# Connecteur Spark <-> Kafka et ses dépendances
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
 && chmod 644 /opt/spark/jars/*.jar

# Dépendances Python (identiques sur le driver et les executors)
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /opt/voguemind /opt/spark/work && chown -R spark:spark /opt/voguemind /opt/spark/work

USER spark
WORKDIR /opt/voguemind
