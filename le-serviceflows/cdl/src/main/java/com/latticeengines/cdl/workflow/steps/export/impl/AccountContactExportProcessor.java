package com.latticeengines.cdl.workflow.steps.export.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.query.DataPage;

@Component
public class AccountContactExportProcessor extends SegmentExportProcessor {

    private static final Logger log = LoggerFactory.getLogger(AccountContactExportProcessor.class);

    @Override
    public boolean accepts(MetadataSegmentExportType type) {
        return type != MetadataSegmentExportType.CONTACT;
    }

    @Override
    protected void fetchAndProcessPage(Schema schema, SegmentExportContext segmentExportContext, File localFile)
            throws IOException {
        long segmentAccountsCount = accountFetcher.getCount(segmentExportContext);
        log.info("accountCount = ", segmentAccountsCount);

        if (segmentAccountsCount > 0) {
            // process accounts that exists in segment
            long processedSegmentAccountsCount = 0;
            // find total number of pages needed
            int pages = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);
            log.info("Number of required loops: " + pages + ", with pageSize: " + pageSize);

            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                    new GenericDatumWriter<GenericRecord>(schema))) {
                dataFileWriter.create(schema, localFile);

                // loop over to required number of pages
                for (int pageNo = 0; pageNo < pages; pageNo++) {
                    // fetch and process a single page
                    processedSegmentAccountsCount = fetchAndProcessPage(segmentExportContext, segmentAccountsCount,
                            processedSegmentAccountsCount, pageNo, dataFileWriter, schema);
                }
            }
        }
    }

    private long fetchAndProcessPage(SegmentExportContext segmentExportContext, long segmentAccountsCount,
            long processedSegmentAccountsCount, int pageNo, DataFileWriter<GenericRecord> dataFileWriter, Schema schema)
            throws IOException {
        log.info(String.format("Loop #%d", pageNo));

        // fetch accounts in current page
        DataPage accountsPage = //
                accountFetcher.fetch(//
                        segmentExportContext, segmentAccountsCount, processedSegmentAccountsCount);

        // process accounts in current page
        processedSegmentAccountsCount += processAccountsPage(segmentExportContext, accountsPage, dataFileWriter,
                schema);
        return processedSegmentAccountsCount;
    }

    private long processAccountsPage(SegmentExportContext segmentExportContext, DataPage accountsPage,
            DataFileWriter<GenericRecord> dataFileWriter, Schema schema) throws IOException {
        List<Object> modifiableAccountIdCollectionForContacts = segmentExportContext
                .getModifiableAccountIdCollectionForContacts();

        List<Map<String, Object>> accountList = accountsPage.getData();
        List<GenericRecord> records = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(accountList)) {

            if (segmentExportContext.getMetadataSegmentExport()
                    .getType() == MetadataSegmentExportType.ACCOUNT_AND_CONTACT) {
                List<Object> accountIds = getAccountsIds(accountList);

                // make sure to clear list of account Ids in contact query and
                // then insert list of accounts ids from current account page
                modifiableAccountIdCollectionForContacts.clear();
                modifiableAccountIdCollectionForContacts.addAll(accountIds);

                // fetch corresponding contacts and prepare map of accountIs vs
                // list of contacts
                Map<Object, List<Map<String, String>>> mapForAccountAndContactList = //
                        contactFetcher.fetch(segmentExportContext);

                if (CollectionUtils.isNotEmpty(accountList)) {
                    for (Map<String, Object> account : accountList) {
                        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                        Object accountId = account.get(InterfaceName.AccountId.name());
                        List<Map<String, String>> matchingContacts = mapForAccountAndContactList.get(accountId);

                        if (CollectionUtils.isNotEmpty(matchingContacts)) {
                            for (Map<String, String> contact : matchingContacts) {
                                for (Field field : schema.getFields()) {
                                    String fieldNameInAccountLookupResult = field.name();

                                    if (contact.get(fieldNameInAccountLookupResult) != null) {
                                        builder.set(field.name(), contact.get(fieldNameInAccountLookupResult));
                                    } else {
                                        setValueInAvroRecord(account, builder, field, fieldNameInAccountLookupResult);
                                    }
                                }

                                records.add(builder.build());
                            }
                        }
                    }
                }
            } else if (segmentExportContext.getMetadataSegmentExport().getType() == MetadataSegmentExportType.ACCOUNT //
                    || segmentExportContext.getMetadataSegmentExport()
                            .getType() == MetadataSegmentExportType.ACCOUNT_ID) {
                for (Map<String, Object> account : accountList) {
                    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                    for (Field field : schema.getFields()) {
                        String fieldNameInAccountLookupResult = field.name();
                        setValueInAvroRecord(account, builder, field, fieldNameInAccountLookupResult);
                    }
                    records.add(builder.build());
                }
            } else if (segmentExportContext.getMetadataSegmentExport().getType() == MetadataSegmentExportType.CONTACT) {
                for (Map<String, Object> account : accountList) {
                    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                    for (Field field : schema.getFields()) {
                        String fieldNameInAccountLookupResult = field.name();
                        setValueInAvroRecord(account, builder, field, fieldNameInAccountLookupResult);
                    }
                    records.add(builder.build());
                }
            }
            for (GenericRecord datum : records) {
                dataFileWriter.append(datum);
            }
        }

        return accountList.size();
    }

    private List<Object> getAccountsIds(List<Map<String, Object>> accountList) {
        List<Object> accountIds = //
                accountList//
                        .stream().parallel() //
                        .map( //
                                account -> account.get(InterfaceName.AccountId.name()) //
                        ) //
                        .collect(Collectors.toList());

        log.info(String.format("Extracting contacts for accountIds: %s",
                Arrays.deepToString(accountIds.toArray(new Object[accountIds.size()]))));
        return accountIds;
    }
}
