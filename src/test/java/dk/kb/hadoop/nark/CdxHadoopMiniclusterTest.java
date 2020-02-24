package dk.kb.hadoop.nark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.jaccept.structure.ExtendedTestCase;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CdxHadoopMiniclusterTest extends ExtendedTestCase {

    private final List<String> expectedResults = Arrays.asList("metadata://netarkivet.dk/crawl/index/cdx?majorversion=2&minorversion=0&harvestid=1&jobid=1&filename=1-1-20161205124343741-00000-sb-test-har-001.statsbiblioteket.dk.warc.gz 20170202130403 metadata://netarkivet.dk/crawl/index/cdx?majorversion=2&minorversion=0&harvestid=1&jobid=1&filename=1-1-20161205124343741-00000-sb-test-har-001.statsbiblioteket.dk.warc.gz - - - - 34091 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/uri-errors.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/uri-errors.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 33734 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/scope.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/scope.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 32584 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/runtime-errors.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/runtime-errors.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 32222 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/progress-statistics.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/progress-statistics.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 31532 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/preselector.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/preselector.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 30883 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/nonfatal-errors.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/nonfatal-errors.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 30521 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/job.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/job.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 30021 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/heritrix_out.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/heritrix_out.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 29121 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/heritrix3_out.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/heritrix3_out.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 27819 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/heritrix3_err.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/heritrix3_err.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 26449 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/crawl.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/crawl.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 25017 CDX-test-2.warc.gz",
            "metadata://crawl/index/dedupcdx?majorversion=0&minorversion=0 20170202130403 metadata://crawl/index/dedupcdx?majorversion=0&minorversion=0 - - - - 24695 CDX-test-2.warc.gz",
            "metadata://crawl/index/deduplicationmigration?majorversion=0&minorversion=0 20170202130403 metadata://crawl/index/deduplicationmigration?majorversion=0&minorversion=0 - - - - 24366 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/logs/alerts.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/logs/alerts.log?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 24013 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/threads-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/threads-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 23640 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/source-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/source-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 23223 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/seeds-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/seeds-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 22811 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/responsecode-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/responsecode-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 22415 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/processors-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/processors-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 21256 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/mimetype-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/mimetype-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 20781 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/hosts-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/hosts-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 20263 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/frontier-summary-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/frontier-summary-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 18805 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/crawl-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/crawl-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 18283 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/reports/archivefiles-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/reports/archivefiles-report.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 17823 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/setup/seeds.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/setup/seeds.txt?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 17459 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/setup/harvestInfo.xml?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/setup/harvestInfo.xml?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 16851 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/setup/crawler-beans.cxml?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/setup/crawler-beans.cxml?heritrixVersion=3.3.0-LBS-2016-02&harvestid=1&jobid=1 - - - - 856 CDX-test-2.warc.gz",
            "metadata://netarkivet.dk/crawl/setup/duplicatereductionjobs?majorversion=1&minorversion=0&harvestid=1&harvestnum=1&jobid=1 20170202130403 metadata://netarkivet.dk/crawl/setup/duplicatereductionjobs?majorversion=1&minorversion=0&harvestid=1&harvestnum=1&jobid=1 - - - - 497 CDX-test-2.warc.gz");
    private MiniDFSCluster hdfsCluster;
    private File baseDir;
    private Configuration conf;
    private MiniYARNCluster miniCluster;
    private DistributedFileSystem fileSystem;

    @BeforeMethod
    public void log4jSetup() {
        PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("log4j.properties"));
    }


    @BeforeMethod
    public void setUp() throws IOException {
        org.apache.log4j.BasicConfigurator.configure(new NullAppender());

        baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        conf = new YarnConfiguration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();


        fileSystem = hdfsCluster.getFileSystem();
        System.out.println("HDFS started");

        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                               FifoScheduler.class, ResourceScheduler.class);
        miniCluster = new MiniYARNCluster("name", 1, 1, 1);
        miniCluster.init(conf);
        miniCluster.start();
        System.out.println("YARN started");
    }

    @AfterMethod
    public void tearDown() {
        miniCluster.stop();
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    // Atm. assumes map only job
    @Test
    public void testMinicluster() throws Exception {
        YarnConfiguration appConf = new YarnConfiguration(miniCluster.getConfig());


        File inputWarc = new File(Thread.currentThread().getContextClassLoader().getResource("CDX-test-2.warc.gz").toURI());
        Path inputFilenameList = Files.createTempFile("", UUID.randomUUID().toString());
        inputFilenameList.toFile().deleteOnExit();
        Files.write(inputFilenameList, Arrays.asList("file://"+inputWarc.getAbsolutePath()));


        String outputURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/"+UUID.randomUUID().toString();
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(URI.create(outputURI));

        try {
            Tool cdxJob = new CDXJob(appConf);
            ToolRunner.run(appConf, cdxJob, new String[]{"file://"+inputFilenameList.toFile().getAbsolutePath(), outputURI});

            boolean foundResult = false;
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(outputPath, true);
            while (iterator.hasNext()) {
                LocatedFileStatus next = iterator.next();
                org.apache.hadoop.fs.Path nextPath = next.getPath();

                if (nextPath.getName().startsWith("part-m")){
                    foundResult = true;
                    List<String> cdxLines = new ArrayList<>();
                    try {
                        //System.out.println(file.length());
                        cdxLines.addAll(readLinesHDFS(nextPath));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    int resultIndex = 0;

                    cdxLines = cdxLines.stream().sorted().collect(Collectors.toList());

                    List<String> expectedResultsSorted = expectedResults.stream().sorted().collect(Collectors.toList());

                    for (String cdxLine : cdxLines) {
                        Assert.assertEquals(cdxLine,expectedResultsSorted.get(resultIndex++));
                        System.out.println(cdxLine);
                    }
                    Assert.assertEquals(cdxLines.size(), expectedResults.size());
                }

            }
            Assert.assertTrue(foundResult);
        } finally {
            fileSystem.delete(outputPath,true);
        }
    }

    private ArrayList<String> readLinesHDFS(org.apache.hadoop.fs.Path nextPath) throws IOException {
        ArrayList<String> lines = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new BufferedInputStream(fileSystem.open(nextPath))))) {
            String line;
            while ((line = in.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

}
