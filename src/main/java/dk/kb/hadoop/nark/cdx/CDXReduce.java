package dk.kb.hadoop.nark.cdx;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CDXReduce extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text warcPath, Iterable<Text> cdxlines, Context context) throws IOException, InterruptedException {

        List<String> valuesString = StreamSupport.stream(cdxlines.spliterator(), false)
                .map(text -> text.toString())
                .collect(Collectors.toList());

        //Apparently this reverses the list
        valuesString = Lists.reverse(valuesString);

        // Don't write a key to avoid printing an url for each WARC-file in the cdx output
        Text valueout = new Text(String.join("\n", valuesString));


        context.write(NullWritable.get(), valueout);
    }
}
