package dk.kb.hadoop.nark.cdx;

import dk.kb.cdx.CDXIndexer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Hadoop Mapper for creating the CDX indexes.
 *
 * The input is a key (not used) and a Text line, which we assume is the path to an WARC file.
 * The output is an exit code (not used), and the generated CDX lines.
 */
public class CDXMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    /** The CDX indexer.*/
    private CDXIndexer indexer = new CDXIndexer();

    /** The number one as an IntWritable. */
    private final static IntWritable one = new IntWritable(1);

    /**
     * Mapping method.
     *
     * @param key  The key. Is ignored.
     * @param warcPath The path to the WARC file.
     * @param outputCollector The output collector, where the results are written.
     * @param reporter The report.
     * @throws IOException If it fails to generate the CDX indexes.
     */
    @Override
    public void map(LongWritable key, Text warcPath, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        // reject empty or null warc paths.
        if(warcPath == null || warcPath.toString().isEmpty()) {
            return;
        }

        File warcFile = new File(warcPath.toString());

        List<String> cdxIndexes = indexer.indexFile(warcFile);
        outputCollector.collect(new Text(String.join("\n", cdxIndexes)), one);
    }
}
