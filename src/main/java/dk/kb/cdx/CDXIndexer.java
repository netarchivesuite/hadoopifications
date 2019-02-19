package dk.kb.cdx;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXLineAdapter;
import org.archive.wayback.resourcestore.indexer.WARCRecordToSearchResultAdapter;
import org.archive.wayback.util.url.IdentityUrlCanonicalizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for creating CDX indexed from WARC files.
 */
public class CDXIndexer {

    /**
     * Runner main method, for extracting the CDX indexes from a WARC file.
     * Delivers the CDX indexes to standard out.
     * @param args The list of WARC files to have their CDX indexes extracted.
     * @throws IOException If if fails to extract the CDX indexes from a file.
     */
    public static void main(String[] args) throws IOException {
        CDXIndexer cdxIndexer = new CDXIndexer();
        for(String s : args) {
            File f = new File(s);
            cdxIndexer.index(new FileInputStream(f), System.out, f.getName());
        }
    }


    /** The warc record searcher.*/
    protected final WARCRecordToSearchResultAdapter warcSearcher;
    /** The CDX line creator, which creates the cdx lines from the warc records.*/
    protected final SearchResultToCDXLineAdapter cdxLineCreater;

    /** Constructor.*/
    public CDXIndexer() {
        warcSearcher = new WARCRecordToSearchResultAdapter();
        cdxLineCreater = new SearchResultToCDXLineAdapter();
    }

    /**
     * Index the warc input stream and delivers the CDX lines to the output stream.
     * @param warcInputStream The input stream for the WARC file.
     * @param outputStream The output stream, where the CDX lines are delivered.
     * @param warcName The name of the WARC file.
     * @throws IOException If it fails to read or write the streams.
     */
    public void index(InputStream warcInputStream, OutputStream outputStream, String warcName) throws IOException {
        ArchiveReader archiveReader = ArchiveReaderFactory.get(warcName, warcInputStream, false);
        for(ArchiveRecord archiveRecord : archiveReader) {
            WARCRecord warcRecord = (WARCRecord) archiveRecord;
            warcSearcher.setCanonicalizer(new IdentityUrlCanonicalizer());
            CaptureSearchResult captureSearchResult = warcSearcher.adapt(warcRecord);
            if (captureSearchResult != null) {
                 outputStream.write(cdxLineCreater.adapt(captureSearchResult).getBytes());
                 outputStream.write("\n".getBytes());
            }
        }
    }

    /**
     * Create the CDX indexes from an WARC file.
     * @param warcFile The WARC file.
     * @return The CDX lines for the records in the WARC file.
     * @throws IOException If it fails to read the WARC file.
     */
    public List<String> indexFile(File warcFile) throws IOException {
        ArchiveReader archiveReader = ArchiveReaderFactory.get(warcFile.getName(), new FileInputStream(warcFile),
                false);
        List<String> res = new ArrayList<>();
        for(ArchiveRecord archiveRecord : archiveReader) {
            WARCRecord warcRecord = (WARCRecord) archiveRecord;
            warcSearcher.setCanonicalizer(new IdentityUrlCanonicalizer());
            CaptureSearchResult captureSearchResult = warcSearcher.adapt(warcRecord);
            if (captureSearchResult != null) {
                res.add(cdxLineCreater.adapt(captureSearchResult));
            }
        }
        return res;
    }
}
