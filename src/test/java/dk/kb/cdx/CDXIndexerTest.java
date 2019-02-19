package dk.kb.cdx;

import org.jaccept.structure.ExtendedTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

public class CDXIndexerTest extends ExtendedTestCase {

    @Test
    public void testIndexingWarcFile() throws Exception {
        addDescription("");

        File warcFile = new File("src/test/resources/CDX-test-2.warc.gz");
        Assert.assertTrue(warcFile.isFile());

        CDXIndexer indexer = new CDXIndexer();
        List<String> cdxLines = indexer.indexFile(warcFile);

        Assert.assertNotNull(cdxLines);
        Assert.assertFalse(cdxLines.isEmpty());

        System.err.println("RESULTS: ");
        System.out.println("Number of lines: " + cdxLines.size());
        System.out.println(cdxLines);
    }

}
