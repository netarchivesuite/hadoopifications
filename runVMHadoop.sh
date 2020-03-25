#!/usr/bin/env bash

# Works perfectly fine with running below line
# hadoop jar target/hadoopifications-1.0-SNAPSHOT-hadoop.jar input/inputs.txt outputtest

# hdfs://node1:8020/user/rbkr/output-test
# file:///home/vagrant/inputs.txt \
# file:///home/rbkr/Desktop/hadoopifications/src/test/resources/input/inputs.txt \
# Can only get this to work with a hdfs path for input (by default input/inputs.txt is interpreted as hdfs://.../input/inputs.txt
java -jar target/hadoopifications-1.0-SNAPSHOT-withdeps.jar \
            file:///home/rbkr/Desktop/hadoopifications/src/test/resources/input/inputs.txt \
            file:///filedir/netarkivet/fileDir/1-1-20200317124915539-00001-ciblee_2015_96a3b22ff5b5.warc
            #file:///home/csr/projects/docker-csr/bitmag/bitrepository-quickstart/var/pillar/netarkivet/fileDir/1-1-20200317124915539-00001-ciblee_2015_96a3b22ff5b5.warc