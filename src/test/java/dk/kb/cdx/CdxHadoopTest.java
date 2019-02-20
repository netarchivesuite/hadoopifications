package dk.kb.cdx;

import dk.kb.hadoop.nark.CDXJob;
import org.apache.hadoop.fs.FileUtil;
import org.jaccept.structure.ExtendedTestCase;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class CdxHadoopTest extends ExtendedTestCase {

    //TODO log4j file to shut up hadoop and wayback
    @Test
    public void test() throws Exception {
        File f = new File(Thread.currentThread().getContextClassLoader().getResource("CDX-test-2.warc.gz").toURI());

        Path p = Files.createTempFile("", UUID.randomUUID().toString());

        p.toFile().deleteOnExit();
        Files.write(p, Arrays.asList(f.getAbsolutePath()));


        File outputDir = new File(f.getParentFile(), UUID.randomUUID().toString());
        try {
            new CDXJob().run(p.toFile().getAbsolutePath(), outputDir.getAbsolutePath());

            Arrays.asList(outputDir.listFiles((dir, name) -> name.startsWith("part-r-")))
                    .stream()
                    .forEach(file -> {
                        List<String> cdxLines = null;
                        try {
                            cdxLines = Files.readAllLines(file.toPath());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        for (String cdxLine : cdxLines) {
                            //TODO map instead of forEach so we actually get the list
                            System.out.println(cdxLine);
                        }
                    });
            //TODO assert something here

        } finally {
            FileUtil.fullyDelete(outputDir);
        }
    }
}
