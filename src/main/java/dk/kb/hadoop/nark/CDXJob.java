package dk.kb.hadoop.nark;

import dk.kb.hadoop.nark.cdx.CDXMap;
import dk.kb.hadoop.nark.cdx.CDXReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CDXJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CDXJob(), args);
        System.exit(exitCode);
    }



    @Override
    public int run(String[] strings) throws Exception {
        JobConf conf = new JobConf(getConf(), CDXJob.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapperClass(CDXMap.class);
        conf.setCombinerClass(CDXReduce.class);
        conf.setReducerClass(CDXReduce.class);


        JobClient.runJob(conf);

        return 0;
    }



}
