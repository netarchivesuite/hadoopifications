package dk.kb.hadoop.nark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

public class MROutputGetter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(new CDXJob(conf), args);
        if (exitCode == 0) {
            MROutputGetter getter = new MROutputGetter();
            getter.makeFolderFromOutput(new Path(args[1]), conf, true);
            // There is no 'done' folder in hdfs because something is wrong...
            //getter.makeFolderFromOutput(new Path("hdfs://node1:8020/tmp/hadoop-yarn/staging/history/done"), conf, false);
            //getter.readStuff(new Path(args[1]), conf);
        } else {
            System.out.println("Something went wrong...");
        }
    }

    /**
     * Line by line reads all files in a given folder specified by a hadoop path and simply outputs these to stdout.
     *
     * @param path A hadoop path specifying the folder to read files in
     * @param conf The used configuration
     * @throws IOException
     */
    public void readStuff(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        // Only want output of mapping
        FileStatus[] statuses = fs.listStatus(path, filePath -> filePath.toString().contains("part-"));
        for (FileStatus status : statuses) {
            FSDataInputStream fis = fs.open(status.getPath());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fis))){
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
        }
    }

    /**
     * Copies the specified folder (hadoop path) from hdfs to local storage (HARDCODED PATH).
     * Also has boolean option to delete the source.
     *
     * @param path A hadoop path specifying the output folder path
     * @param conf The used configuration
     * @param delSrc Boolean to determine whether or not to delete the output source after copying to local storage
     * @throws IOException
     */
    public void makeFolderFromOutput(Path path, Configuration conf, boolean delSrc) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String dest = "file:///home/rbkr/Desktop/hadoopifications/"; //TODO: make this non-hardcoded
        fs.copyToLocalFile(delSrc, path, new Path(dest));
    }
}
