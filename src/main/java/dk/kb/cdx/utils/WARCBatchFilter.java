package dk.kb.cdx.utils;

import org.archive.io.warc.WARCRecord;

public abstract class WARCBatchFilter {
    /** The name of the BatchFilter. */
    private String name;

    /** The name of the filter that filters out non response/resource records. */
    private static final String EXCLUDE_NON_RESPONSE_RESOURCE_RECORDS_FILTER_NAME = "EXCLUDE_NON_RESPONSE_RECORDS";

    /** A default filter: Accepts on response records.
     * See https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-type-mandatory
     *
     * https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#annex-a-informative-use-cases-for-writing-warc-records
     * */
    public static final WARCBatchFilter EXCLUDE_NON_RESPONSE_RESOURCE_RECORDS = new WARCBatchFilter(
            EXCLUDE_NON_RESPONSE_RESOURCE_RECORDS_FILTER_NAME) {
        public boolean accept(WARCRecord record) {
            HeritrixArchiveRecordWrapper recordWrapper = new HeritrixArchiveRecordWrapper(record);
            String warcType = notNull(recordWrapper.getHeader().getHeaderStringValue("WARC-Type")).toLowerCase();
            switch (warcType){
                case "response":
                case "resource":
                    return true;
                case "warcinfo":
                case "request":
                case "metadata":
                case "revisit":
                case "conversion":
                case "continuation":
                default:
                    return false;

            }
        }
    };

    public static String notNull(String isNull){
        if (isNull == null){
            return "";
        }
        return isNull;
    }

    protected WARCBatchFilter(String name) {
        //ArgumentNotValid.checkNotNullOrEmpty(name, "String name");
        this.name = name;
    }

    /**
     * Check if a given record is accepted (not filtered out) by this filter.
     *
     * @param record a given WARCRecord
     * @return true, if the given record is accepted by this filter
     */
    public abstract boolean accept(WARCRecord record);
}
