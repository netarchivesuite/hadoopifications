package dk.kb.hadoop.nark.cdx;

import dk.kb.cdx.CDXIndexer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

/**
 * Hadoop Mapper for creating the CDX indexes.
 *
 * The input is a line number as key (not used) and a Text line, which we assume is the path to an WARC file.
 * The output is an exit code (not used), and the generated CDX lines.
 */
public class CDXMap extends Mapper<LongWritable, Text, LongWritable, Text> {

    /** The CDX indexer.*/
    private CDXIndexer indexer = new CDXIndexer();

    @Override
    protected void map(LongWritable line, Text warcPath, Context context) throws IOException, InterruptedException {
        // reject empty or null warc paths.
        if(warcPath == null || warcPath.toString().isEmpty()) {
            return;
        }

        File warcFile = new File(warcPath.toString());

        String cdxIndexes = indexer.indexFile(warcFile);
        context.write(new LongWritable(0), new Text(cdxIndexes));
    }
}
