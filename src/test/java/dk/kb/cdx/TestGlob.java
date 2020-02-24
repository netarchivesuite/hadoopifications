package dk.kb.cdx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jaccept.structure.ExtendedTestCase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;

public class TestGlob extends ExtendedTestCase {
    @Test
    public void testGlobOnFileSystem() throws IOException {
        Configuration conf = new Configuration();
        //TODO Figure out how to not hardcode the URI.
        // I get the impression, that the below URI-string should be correct, but it simply returns 'file:///'.
        // Pretty sure the configuration just grabs a default value instead of my config. Probably use addResource()?
        //String uri = conf.get("fs.defaultFS");
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/user/rbkr"), conf);
        Path pattern = new Path("input/{*.warc,*.warc.gz}"); // works
        System.out.println("Home path: " + fs.getHomeDirectory());
        for (FileStatus status : fs.globStatus(pattern)) {
            System.out.println("Path: " + status.getPath());
        }
        // TODO: assert something
    }
}
