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
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.query.DataPage;

@Component
public class AccountContactExportProcessor extends SegmentExportProcessor {

    private static final Logger log = LoggerFactory.getLogger(AccountContactExportProcessor.class);

    @Override
    public boolean accepts(AtlasExportType type) {
        return type == AtlasExportType.ACCOUNT //
                || type == AtlasExportType.ACCOUNT_AND_CONTACT //
                || type == AtlasExportType.ACCOUNT_ID //
                || type == AtlasExportType.ORPHAN_TXN;
    }

    @Override
    protected void fetchAndProcessPage(Schema schema, SegmentExportContext segmentExportContext, File localFile)
            throws IOException {
        long segmentAccountsCount = accountFetcher.getCount(segmentExportContext, version);
        log.info(String.format("accountCount = %d", segmentAccountsCount));

        if (segmentAccountsCount > 0) {
            // process accounts that exists in segment
            long total = 0;
            long offset = 0;
            // find total number of pages needed
            int pages = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);
            log.info("Number of minimum required loops: " + pages + ", with pageSize: " + pageSize);

            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                    new GenericDatumWriter<>(schema))) {
                dataFileWriter.create(schema, localFile);

                int pageNo = 0;
                while (true) {
                    total = fetchAndProcessPage(segmentExportContext, segmentAccountsCount, offset, pageNo,
                            dataFileWriter, schema, version);
                    if (total == offset) {
                        break;
                    } else {
                        offset = total;
                    }
                    pageNo++;
                }
            }
            if (total < segmentAccountsCount) {
                log.warn("Export number less than expected.");
            }
            log.info(String.format("processedSegmentAccountsCount = %d", total));
        }
    }

    private long fetchAndProcessPage(SegmentExportContext segmentExportContext, long segmentAccountsCount,
            long processedSegmentAccountsCount, int pageNo, DataFileWriter<GenericRecord> dataFileWriter, Schema schema,
            DataCollection.Version version) throws IOException {
        log.info(String.format("Loop #%d", pageNo));

        // fetch accounts in current page
        DataPage accountsPage = //
                accountFetcher.fetch(//
                        segmentExportContext, segmentAccountsCount, processedSegmentAccountsCount, version);

        // process accounts in current page
        processedSegmentAccountsCount += processAccountsPage(segmentExportContext, accountsPage, dataFileWriter, schema,
                version);
        return processedSegmentAccountsCount;
    }

    private long processAccountsPage(SegmentExportContext segmentExportContext, DataPage accountsPage,
            DataFileWriter<GenericRecord> dataFileWriter, Schema schema, DataCollection.Version version)
            throws IOException {
        List<Object> modifiableAccountIdCollectionForContacts = segmentExportContext
                .getModifiableAccountIdCollectionForContacts();

        List<Map<String, Object>> accountList = accountsPage.getData();
        List<GenericRecord> records = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(accountList)) {

            if (segmentExportContext.getMetadataSegmentExport()
                    .getType() == AtlasExportType.ACCOUNT_AND_CONTACT) {
                List<Object> accountIds = getAccountsIds(accountList);

                // make sure to clear list of account Ids in contact query and
                // then insert list of accounts ids from current account page
                modifiableAccountIdCollectionForContacts.clear();
                modifiableAccountIdCollectionForContacts.addAll(accountIds);

                // fetch corresponding contacts and prepare map of accountIs vs
                // list of contacts
                Map<Object, List<Map<String, String>>> mapForAccountAndContactList = //
                        contactFetcher.fetch(segmentExportContext, version);

                if (CollectionUtils.isNotEmpty(accountList)) {
                    for (Map<String, Object> account : accountList) {
                        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                        Object accountId = account.get(InterfaceName.AccountId.name());
                        List<Map<String, String>> matchingContacts = mapForAccountAndContactList.get(accountId);

                        if (CollectionUtils.isNotEmpty(matchingContacts)) {
                            for (Map<String, String> contact : matchingContacts) {

                                for (Field field : schema.getFields()) {
                                    String fieldNameInAccountLookupResult = field.name()
                                            .substring(field.name().indexOf(SegmentExportProcessor.SEPARATOR)
                                                    + SegmentExportProcessor.SEPARATOR.length());

                                    if (contact.get(field.name()) != null) {
                                        builder.set(field.name(), contact.get(field.name()));
                                    } else {
                                        setValueInAvroRecord(account, builder, field, fieldNameInAccountLookupResult);
                                    }
                                }

                                records.add(builder.build());
                            }
                        }
                    }
                }
            } else if (segmentExportContext.getMetadataSegmentExport().getType() == AtlasExportType.ACCOUNT //
                    || segmentExportContext.getMetadataSegmentExport()
                            .getType() == AtlasExportType.ACCOUNT_ID) {
                for (Map<String, Object> account : accountList) {
                    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                    for (Field field : schema.getFields()) {
                        String fieldNameInAccountLookupResult = field.name()
                                .substring(field.name().indexOf(SegmentExportProcessor.SEPARATOR)
                                        + SegmentExportProcessor.SEPARATOR.length());
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
        List<Object> accountIds = accountList //
                .stream().parallel() //
                .map(account -> account.get(InterfaceName.AccountId.name())) //
                .collect(Collectors.toList());

        log.info(String.format("Extracting contacts for accountIds: %s",
                Arrays.deepToString(accountIds.toArray(new Object[accountIds.size()]))));
        return accountIds;
    }
}
