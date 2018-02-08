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
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.query.DataPage;

@Component
public class ContactOnlyExportProcessor extends SegmentExportProcessor {

    private static final Logger log = LoggerFactory.getLogger(ContactOnlyExportProcessor.class);

    @Override
    public boolean accepts(MetadataSegmentExportType type) {
        return type == MetadataSegmentExportType.CONTACT;
    }

    @Override
    protected void fetchAndProcessPage(Schema schema, SegmentExportContext segmentExportContext, File localFile)
            throws IOException {
        long segmentContactsCount = contactFetcher.getCount(segmentExportContext);
        log.info("contactCount = ", segmentContactsCount);

        if (segmentContactsCount > 0) {
            // process contacts that exists in segment
            long processedSegmentContactsCount = 0;
            // find total number of pages needed
            int pages = (int) Math.ceil((segmentContactsCount * 1.0D) / pageSize);
            log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                    new GenericDatumWriter<GenericRecord>(schema))) {
                dataFileWriter.create(schema, localFile);

                // loop over to required number of pages
                for (int pageNo = 0; pageNo < pages; pageNo++) {
                    // fetch and process a single page
                    processedSegmentContactsCount = fetchAndProcessContactsPage(segmentExportContext,
                            segmentContactsCount, processedSegmentContactsCount, pageNo, dataFileWriter, schema);
                }
            }
        }
    }

    protected long fetchAndProcessContactsPage(SegmentExportContext segmentExportContext, long segmentContactsCount,
            long processedSegmentContactsCount, int pageNo, DataFileWriter<GenericRecord> dataFileWriter, Schema schema)
            throws IOException {
        log.info(String.format("Loop #%d", pageNo));

        // fetch contacts in current page
        DataPage contactsPage = //
                contactFetcher.fetch(//
                        segmentExportContext, segmentContactsCount, processedSegmentContactsCount);

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
                    String fieldNameInContactLookupResult = field.name();
                    if (fieldNameInContactLookupResult.startsWith(MetadataSegmentExport.ACCOUNT_PREFIX)) {
                        fieldNameInContactLookupResult = fieldNameInContactLookupResult
                                .substring(MetadataSegmentExport.ACCOUNT_PREFIX.length());
                    } else if (fieldNameInContactLookupResult.startsWith(MetadataSegmentExport.CONTACT_PREFIX)) {
                        fieldNameInContactLookupResult = fieldNameInContactLookupResult
                                .substring(MetadataSegmentExport.CONTACT_PREFIX.length());
                    }
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
