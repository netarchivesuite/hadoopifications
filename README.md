# hadoopifications

A small hadoop job to create CDX files for NetarchiveSuite


```bash
hadoop jar hadoopifications-1.0-SNAPSHOT-hadoop.jar \
    dk.kb.hadoop.nark.CDXJob \
    TEST1_list.txt \
    outputFolder
```

`inputfile` is a test file consisting of warc-file addresses. Remember to use the prefix `file://` as the addresses
are resolved on HDFS per default. This goes both for the `inputfile` and the lines in `inputfile`.

Note that `inputfile` and `outputFolder` must be available on all nodes in the cluster, so it is probably wise to use
HDFS for these. 

Get the result like this

```bash
hdfs dfs -ls outputFolder/
hdfs dfs -cat 'outputFolder/part-r-*'
```

Run this on the server `dkm_eld@narcana-suite01.statsbiblioteket.dk`. The netarchive files are available on `/mnt/netarkiv/`


## Generate input file

```bash
ssh dkm_eld@narcana-suite01.statsbiblioteket.dk
find  /mnt/netarkiv/ -type f | sed 's|\(.*\)|file://\1|' > TEST1_list.txt
hdfs dfs -copyFromLocal TEST1_list.txt .
```
