#!/bin/bash

# Set environment variables to disable Kerberos
export HADOOP_USER_NAME=spark
export HADOOP_CONF_DIR=""
export JAVA_SECURITY_KRB5_CONF=""
export HADOOP_SECURITY_AUTHENTICATION=simple
export HADOOP_SECURITY_AUTHORIZATION=false

spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/bitnami/spark/.ivy2 \
  --conf spark.hadoop.security.authentication=simple \
  --conf spark.hadoop.security.authorization=false \
  --conf spark.hadoop.hadoop.security.authentication=simple \
  --conf spark.hadoop.hadoop.security.authorization=false \
  --conf spark.hadoop.security.credential.provider.path="" \
  --conf spark.security.credentials.hadoop.enabled=false \
  --conf spark.driver.extraJavaOptions="-Djava.security.krb5.conf=" \
  --conf spark.executor.extraJavaOptions="-Djava.security.krb5.conf=" \
  --conf spark.hadoop.hadoop.security.authentication.use_jaas=false \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-s3:1.11.1034,com.amazonaws:aws-java-sdk-core:1.11.1034 \
  /opt/bitnami/spark/jobs/spark-jobs.py 