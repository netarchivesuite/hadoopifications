package dk.kb.hadoop.nark;

import dk.kb.hadoop.nark.cdx.CDXMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CDXJob extends Configured implements Tool {

    public static void main(String ... args) throws Exception {
        int exitCode = ToolRunner.run(new CDXJob(new Configuration()), args);
        System.exit(exitCode);
    }

    public CDXJob(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String ... args) throws Exception {
        Configuration conf = getConf();
        conf.set("yarn.resourcemanager.address", "node1:8032");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.defaultFS", "hdfs://node1");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"); // VM Hadoop crashes without this..
        // None of the below settings make a difference so far
        // Seems the default path to jobhistory logs are in /var/hadoop/mr-history/done/yyyy/mm/dd/*
        //File jar = new File("target/hadoopifications-1.0-SNAPSHOT-hadoop.jar");
        //conf.set("mapreduce.job.jar", jar.getAbsolutePath());
        //conf.set("mapreduce.jobhistory.webapp.https.address", "node1:19888");
        //conf.set("mapreduce.jobhistory.webapp.address", "node1:19888"); // Seems by default that this is port 10020, but this makes the url to track the job redirect to this IPC port instead of the UI on 19888
        //conf.set("mapreduce.jobhistory.done-dir", "hdfs://node1:8020/tmp/hadoop-yarn/staging/history/done");
        //conf.set("mapreduce.jobhistory.intermediate-done-dir", "/tmp/hadoop-yarn/staging/history/done_intermediate");
        Job job = Job.getInstance(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());

        /* //TODO (jolf/abr) probably better if we can give it a folder or glob rather than a file of files
        // The below code does this, but adds one new line too much - should use an iterator instead of for-each
        // when iterating to check for last element.
        job.setInputFormatClass(NLineInputFormat.class);
        File input = File.createTempFile("input", ".tmp");
        input.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(input));
        FileSystem fs = FileSystem.get(conf); // Probably shouldn't let it default to a URI?
        for (FileStatus status : fs.globStatus(new Path(args[0] + "/{*.warc,*.warc.gz}"))) {
            System.out.println("Adding input path: " + status.getPath());
            writer.write(status.getPath().toString());
            writer.newLine();
        }
        writer.close();
        NLineInputFormat.addInputPath(job, new Path(input.toPath().toString())); */
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        NLineInputFormat.setNumLinesPerSplit(job, 5);

        job.setMapperClass(CDXMap.class);

        job.setMapOutputKeyClass(NullWritable.class);
        //job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //job.setCombinerClass(CDXReduce.class);

        //job.setReducerClass(CDXReduce.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
