#!/usr/bin/env bash

mvn -q clean package -DskipTests

set -x
output=${1:-output}

hadoop jar target/hadoopifications-1.0-SNAPSHOT-hadoop.jar \
  dk.kb.hadoop.nark.CDXJob \
  input/inputs.txt \
  $output

hdfs dfs -cat ${1:-output}/_SUCCESS && \
hdfs dfs -cat ${1:-output}/*