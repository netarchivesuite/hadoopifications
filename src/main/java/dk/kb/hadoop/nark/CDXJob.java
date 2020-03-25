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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class CDXJob extends Configured implements Tool {

    public static void main(String ... args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("yarn.resourcemanager.address", "node1:8032");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.defaultFS", "hdfs://node1");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // VM Hadoop crashes without this..
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        /*
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("vagrant");
        ugi.doAs((PrivilegedAction<Void>) () -> {
            int exitCode = 0;
            try {
                exitCode = ToolRunner.run(new CDXJob(conf), args);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.exit(exitCode); // Hmmmm
            return null;
        });
         */
        int exitCode = ToolRunner.run(new CDXJob(conf), args);
        System.exit(exitCode);
    }

    public CDXJob(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String ... args) throws Exception {
        Configuration conf = getConf();
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
