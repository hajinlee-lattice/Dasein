package com.latticeengines.cdl.workflow.steps.export.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.SegmentExportContext;
import com.latticeengines.cdl.workflow.steps.export.SegmentExportProcessor;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.query.DataPage;

@Component
public class ContactOnlyExportProcessor extends SegmentExportProcessor {

    private static final Logger log = LoggerFactory.getLogger(ContactOnlyExportProcessor.class);

    @Override
    public boolean accepts(AtlasExportType type) {
        return AtlasExportType.CONTACT.equals(type) || AtlasExportType.ORPHAN_CONTACT.equals(type);
    }

    @Override
    protected void fetchAndProcessPage(Schema schema, SegmentExportContext segmentExportContext, File localFile)
            throws IOException {
        long segmentContactsCount = contactFetcher.getCount(segmentExportContext, version);
        log.info(String.format("contactCount = %d", segmentContactsCount));

        if (segmentContactsCount > 0) {
            // process contacts that exists in segment
            long total = 0;
            long offset = 0;
            // find total number of pages needed
            int pages = (int) Math.ceil((segmentContactsCount * 1.0D) / pageSize);
            log.info("Number of minimum required loops: " + pages + ", with pageSize: " + pageSize);

            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                    new GenericDatumWriter<GenericRecord>(schema))) {
                dataFileWriter.create(schema, localFile);

                int pageNo = 0;
                while (true) {
                    // fetch and process a single page
                    total = fetchAndProcessContactsPage(segmentExportContext, segmentContactsCount, offset, pageNo,
                            dataFileWriter, schema, version);
                    if (total == offset) {
                        break;
                    } else {
                        offset = total;
                    }
                    pageNo++;
                }
                if (total < segmentContactsCount) {
                    log.warn("Export contact number less than expected.");
                }
                log.info(String.format("segmentContactsCount = %d", total));
            }
        }
    }

    protected long fetchAndProcessContactsPage(SegmentExportContext segmentExportContext, long segmentContactsCount,
            long processedSegmentContactsCount, int pageNo, DataFileWriter<GenericRecord> dataFileWriter, Schema schema,
            DataCollection.Version version) throws IOException {
        log.info(String.format("Loop #%d", pageNo));

        // fetch contacts in current page
        DataPage contactsPage = //
                contactFetcher.fetch(segmentExportContext, segmentContactsCount, processedSegmentContactsCount,
                        version);

        // process accounts in current page
        processedSegmentContactsCount += processContactsPage(segmentExportContext, contactsPage, dataFileWriter,
                schema);
        return processedSegmentContactsCount;
    }

    private long processContactsPage(SegmentExportContext segmentExportContext, DataPage contactsPage,
            DataFileWriter<GenericRecord> dataFileWriter, Schema schema) throws IOException {
        List<Map<String, Object>> contactList = contactsPage.getData();
        List<GenericRecord> records = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(contactList)) {
            for (Map<String, Object> contact : contactList) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                for (Field field : schema.getFields()) {
                    String fieldNameInContactLookupResult = field.name()
                            .substring(field.name().indexOf(SegmentExportProcessor.SEPARATOR)
                                    + SegmentExportProcessor.SEPARATOR.length());

                    setValueInAvroRecord(contact, builder, field, fieldNameInContactLookupResult);
                }
                records.add(builder.build());
            }
        }
        for (GenericRecord datum : records) {
            dataFileWriter.append(datum);
        }

        return contactList.size();

    }
}
