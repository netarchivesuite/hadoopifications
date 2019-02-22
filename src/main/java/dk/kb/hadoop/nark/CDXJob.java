package dk.kb.hadoop.nark;

import dk.kb.hadoop.nark.cdx.CDXCombiner;
import dk.kb.hadoop.nark.cdx.CDXMap;
import dk.kb.hadoop.nark.cdx.CDXReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop job for extracting CDX indexes from WARC files.
 * Takes 2 arguments.
 * 1. a file where each line is the reference to a WARC file to create CDX indexes from.
 *  - If run on HDFS, then each file path must have the format 'file://'
 * 2. a output directory, where the CDX index files are delivered.
 *  - These output files can easily be concatenated into a single CDX index file, as used by NAS.
 */
public class CDXJob extends Configured implements Tool {

    public static void main(String ... args) throws Exception {
        int exitCode = ToolRunner.run(new CDXJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String ... args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());

        //TODO probably better if we can give it a folder or glob rather than a file of files
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        NLineInputFormat.setNumLinesPerSplit(job, 5);


        job.setMapperClass(CDXMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(CDXCombiner.class);

        job.setReducerClass(CDXReduce.class);

        job.setNumReduceTasks(5);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));


        return job.waitForCompletion(true) ? 0 : 1;
    }
}
