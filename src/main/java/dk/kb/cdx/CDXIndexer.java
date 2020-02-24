package dk.kb.cdx;

import dk.kb.cdx.utils.WARCBatchFilter;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Class for creating CDX indexed from WARC files.
 */
public class CDXIndexer {
    /** The warc record searcher.*/
    protected final WARCRecordToSearchResultAdapter warcSearcher;
    /** The CDX line creator, which creates the cdx lines from the warc records.*/
    protected final SearchResultToCDXLineAdapter cdxLineCreater;
    //private int recordNum = 0;
    //private int actualLinesWritten = 0;

    /** Constructor.*/
    public CDXIndexer() {
        warcSearcher = new WARCRecordToSearchResultAdapter();
        cdxLineCreater = new SearchResultToCDXLineAdapter();
    }



    public List<String> index(InputStream warcInputStream, String warcName) throws IOException {
        ArchiveReader archiveReader = ArchiveReaderFactory.get(warcName, warcInputStream, false);
        return extractCdxLines(archiveReader);
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
        return extractCdxLines(archiveReader);
    }


    /**
     * returns a BatchFilter object which restricts the set of warc records in the archive on which the MapReduce job
     * is performed. Taken directly from NAS, this is simply hardcoded to exclude non-response/resource records.
     *
     * @return A filter telling which records should be indexed.
     */
    public WARCBatchFilter getFilter() {
        return WARCBatchFilter.EXCLUDE_NON_RESPONSE_RESOURCE_RECORDS;
    }

    /**
     * Method for extracting the cdx lines from an ArchiveReader.
     * @param reader The ArchiveReader which is actively reading an archive file (e.g WARC).
     * @return The list of CDX index lines for the records of the archive in the reader.
     */
    protected List<String> extractCdxLines(ArchiveReader reader) {
        List<String> res = new ArrayList<>();

        for(ArchiveRecord archiveRecord : reader) {
            //recordNum++;
            //System.out.println("Processing record #" + recordNum);
            // Add filter for only response headers (can trivially get filter in actual project)
            WARCRecord warcRecord = (WARCRecord) archiveRecord;
            if (!getFilter().accept(warcRecord)) {
                //System.out.println("Skipping non response record #" + recordNum);
                continue;
            }
            warcSearcher.setCanonicalizer(new IdentityUrlCanonicalizer());
            //TODO this returns null and prints stack trace on OutOfMemoryError. Bad code.
            CaptureSearchResult captureSearchResult = warcSearcher.adapt(warcRecord);
            if (captureSearchResult != null) {
                //actualLinesWritten++;
                //System.out.println("Actual cdx lines written: " + actualLinesWritten);
                res.add(cdxLineCreater.adapt(captureSearchResult));
            }
        }
        return res;
    }
}
