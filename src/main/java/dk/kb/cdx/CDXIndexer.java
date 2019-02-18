package dk.kb.cdx;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXLineAdapter;
import org.archive.wayback.resourcestore.indexer.WARCRecordToSearchResultAdapter;
import org.archive.wayback.util.url.IdentityUrlCanonicalizer;

import java.io.*;

public class CDXIndexer {

    public static void main(String[] args) throws IOException {
        CDXIndexer cdxIndexer = new CDXIndexer();
        File f = new File(args[0]);
        cdxIndexer.index(new FileInputStream(f), System.out, f.getName());
    }


    protected final WARCRecordToSearchResultAdapter warcSearcher;
    protected final SearchResultToCDXLineAdapter cdxLineCreater;

    public CDXIndexer() {
        warcSearcher = new WARCRecordToSearchResultAdapter();
        cdxLineCreater = new SearchResultToCDXLineAdapter();
    }

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

    public String indexFile(File warcFile) throws IOException {
        ArchiveReader archiveReader = ArchiveReaderFactory.get(warcFile.getName(), new FileInputStream(warcFile),
                false);
        StringBuffer res = new StringBuffer();
        for(ArchiveRecord archiveRecord : archiveReader) {
            WARCRecord warcRecord = (WARCRecord) archiveRecord;
            warcSearcher.setCanonicalizer(new IdentityUrlCanonicalizer());
            CaptureSearchResult captureSearchResult = warcSearcher.adapt(warcRecord);
            if (captureSearchResult != null) {
                res.append(cdxLineCreater.adapt(captureSearchResult));
                res.append("\n");
            }
        }
        return res.toString();
    }
}
