FROM apache/airflow:3.2.2
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Java is needed for PySpark — install as root, then drop back to the airflow user
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
