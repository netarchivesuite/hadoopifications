package dk.kb.cdx;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

public class CDXIndexerInvoker {


        /**
         * Runner main method, for extracting the CDX indexes from a WARC file.
         * Delivers the CDX indexes to standard out.
         * @param args The list of WARC files to have their CDX indexes extracted.
         * @throws IOException If if fails to extract the CDX indexes from a file.
         */
        public static void main(String[] args) throws IOException {
            CDXIndexer cdxIndexer = new CDXIndexer();

                File f = new File("input/test.warc.gz");
                try (InputStream warcInputStream = new BufferedInputStream(new FileInputStream(f))) {
                    List<String> output = cdxIndexer.index(warcInputStream, f.getName());
                    System.out.println(output.stream().collect(Collectors.joining("\n")));
                }
        }


}
