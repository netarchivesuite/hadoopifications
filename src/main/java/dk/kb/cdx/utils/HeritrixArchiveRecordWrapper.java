/*
 * #%L
 * Netarchivesuite - common
 * %%
 * Copyright (C) 2005 - 2018 The Royal Danish Library,
 *             the National Library of France and the Austrian National Library.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 2.1 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 *
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */
package dk.kb.cdx.utils;

import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

import java.io.InputStream;

/**
 * Heritrix wrapper implementation of the abstract archive record interface.
 */
public class HeritrixArchiveRecordWrapper extends ArchiveRecordBase {

    /**
     * The original Heritrix record, since it is also the record payload input stream.
     */
    protected ArchiveRecord record;

    /** The wrapper archive header. */
    protected ArchiveHeaderBase header;

    /**
     * Construct a Heritrix record wrapper object.
     *
     * @param record Heritrix record object
     */
    public HeritrixArchiveRecordWrapper(ArchiveRecord record) {
        //ArgumentNotValid.checkNotNull(record, "record");
        this.record = record;
        this.header = HeritrixArchiveHeaderWrapper.wrapArchiveHeader(this, record);
        if (record instanceof ARCRecord) {
            this.bIsArc = true;
        } else if (record instanceof WARCRecord) {
            this.bIsWarc = true;
        } else {
            System.out.println("Error in HeritrixArchiveRecordWrapper");
            //throw new ArgumentNotValid("Unsupported ArchiveRecord type: " + record.getClass().getName());
        }
    }

    @Override
    public ArchiveHeaderBase getHeader() {
        return header;
    }

    @Override
    public InputStream getInputStream() {
        return record;
    }

}
