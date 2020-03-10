#!/usr/bin/env bash

# Works perfectly fine with running below line
# hadoop jar target/hadoopifications-1.0-SNAPSHOT-hadoop.jar input/inputs.txt outputtest

# Atm. get "No FileSystem for scheme: hdfs.. maybe try uploading files to Colins VM and run it there?
# java -jar target/hadoopifications-1.0-SNAPSHOT-hadoop.jar dk.kb.hadoop.nark.MROutputGetter input/inputs.txt output-test

java -jar target/hadoopifications-1.0-SNAPSHOT-withdeps.jar input/inputs.txt output-test