package com.latticeengines.cdl.workflow.steps.export;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.export.SegmentExportContext.Counter;
import com.latticeengines.cdl.workflow.steps.export.SegmentExportContext.SegmentExportContextBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.ExportType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component("segmentExportProcessor")
public class SegmentExportProcessor {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportProcessor.class);

    private static final String ACCOUNT_PREFIX = BusinessEntity.Account + "_";
    private static final String CONTACT_PREFIX = BusinessEntity.Contact + "_";

    @Autowired
    private ExportAccountFetcher accountFetcher;

    @Autowired
    private ExportContactFetcher contactFetcher;

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${playmaker.workflow.segment.pagesize:100}")
    private long pageSize;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public void executeExportActivity(Tenant tenant, SegmentExportStepConfiguration config,
            Configuration yarnConfiguration) {
        CustomerSpace customerSpace = config.getCustomerSpace();
        String exportId = config.getMetadataSegmentExportId();

        MetadataSegmentExport metadataSegmentExport = internalResourceRestApiProxy
                .getMetadataSegmentExport(customerSpace, exportId);
        Table segmentExportTable = metadataProxy.getTable(tenant.getId(), metadataSegmentExport.getTableName());
        long currentTimeMillis = System.currentTimeMillis();
        log.info("segmentExportTable = : " + JsonUtils.serialize(segmentExportTable));

        Schema schema = TableUtils.createSchema("SomeSchema", segmentExportTable);

        SegmentExportContext segmentExportContext = initSegmentExportContext(tenant, config, metadataSegmentExport,
                schema);

        try {
            String csvFileName = metadataSegmentExport.getFileName();
            String avroFileName = csvFileName.substring(0, csvFileName.lastIndexOf(".csv")) + ".avro";

            File localFile = new File(tenant.getName() + "_" + currentTimeMillis + "_" + avroFileName);

            if (segmentExportContext.getMetadataSegmentExport().getType() == ExportType.CONTACT) {
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
                                    segmentContactsCount, processedSegmentContactsCount, pageNo, dataFileWriter,
                                    schema);
                        }
                    }
                }
            } else {
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
                            processedSegmentAccountsCount = fetchAndProcessPage(segmentExportContext,
                                    segmentAccountsCount, processedSegmentAccountsCount, pageNo, dataFileWriter,
                                    schema);
                        }
                    }
                }
            }

            String path = metadataSegmentExport.getPath();
            String avroPath = path + "avro/";

            try {
                HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), avroPath + avroFileName);
            } finally {
                FileUtils.deleteQuietly(localFile);
            }

            Extract extract = new Extract();
            extract.setExtractionTimestamp(currentTimeMillis);
            extract.setName("segmentExport");
            extract.setPath(avroPath + "*.avro");
            extract.setTable(segmentExportTable);
            extract.setTenant(segmentExportTable.getTenant());
            segmentExportTable.addExtract(extract);
            metadataProxy.updateTable(tenant.getId(), metadataSegmentExport.getTableName(), segmentExportTable);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
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

            if (segmentExportContext.getMetadataSegmentExport().getType() == ExportType.ACCOUNT_AND_CONTACT) {
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
                                    if (fieldNameInAccountLookupResult.startsWith(ACCOUNT_PREFIX)) {
                                        fieldNameInAccountLookupResult = fieldNameInAccountLookupResult
                                                .substring(ACCOUNT_PREFIX.length());
                                    } else if (fieldNameInAccountLookupResult.startsWith(CONTACT_PREFIX)) {
                                        fieldNameInAccountLookupResult = fieldNameInAccountLookupResult
                                                .substring(CONTACT_PREFIX.length());
                                    }

                                    if (contact.get(fieldNameInAccountLookupResult) != null) {
                                        builder.set(field.name(), contact.get(fieldNameInAccountLookupResult));
                                    } else {
                                        builder.set(field.name(), account.get(fieldNameInAccountLookupResult));
                                    }
                                }

                                records.add(builder.build());
                            }
                        }
                    }
                }
            } else if (segmentExportContext.getMetadataSegmentExport().getType() == ExportType.ACCOUNT) {
                for (Map<String, Object> account : accountList) {
                    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                    for (Field field : schema.getFields()) {
                        String fieldNameInAccountLookupResult = field.name();
                        if (fieldNameInAccountLookupResult.startsWith(ACCOUNT_PREFIX)) {
                            fieldNameInAccountLookupResult = fieldNameInAccountLookupResult
                                    .substring(ACCOUNT_PREFIX.length());
                        } else if (fieldNameInAccountLookupResult.startsWith(CONTACT_PREFIX)) {
                            fieldNameInAccountLookupResult = fieldNameInAccountLookupResult
                                    .substring(CONTACT_PREFIX.length());
                        }
                        builder.set(field.name(), account.get(fieldNameInAccountLookupResult));
                    }
                    records.add(builder.build());
                }
            } else if (segmentExportContext.getMetadataSegmentExport().getType() == ExportType.CONTACT) {
                for (Map<String, Object> account : accountList) {
                    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

                    for (Field field : schema.getFields()) {
                        String fieldNameInAccountLookupResult = field.name();
                        if (fieldNameInAccountLookupResult.startsWith(ACCOUNT_PREFIX)) {
                            fieldNameInAccountLookupResult = fieldNameInAccountLookupResult
                                    .substring(ACCOUNT_PREFIX.length());
                        } else if (fieldNameInAccountLookupResult.startsWith(CONTACT_PREFIX)) {
                            fieldNameInAccountLookupResult = fieldNameInAccountLookupResult
                                    .substring(CONTACT_PREFIX.length());
                        }
                        builder.set(field.name(), account.get(fieldNameInAccountLookupResult));
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

    private long fetchAndProcessContactsPage(SegmentExportContext segmentExportContext, long segmentContactsCount,
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
                    if (fieldNameInContactLookupResult.startsWith(ACCOUNT_PREFIX)) {
                        fieldNameInContactLookupResult = fieldNameInContactLookupResult
                                .substring(ACCOUNT_PREFIX.length());
                    } else if (fieldNameInContactLookupResult.startsWith(CONTACT_PREFIX)) {
                        fieldNameInContactLookupResult = fieldNameInContactLookupResult
                                .substring(CONTACT_PREFIX.length());
                    }
                    builder.set(field.name(), contact.get(fieldNameInContactLookupResult));
                }
                records.add(builder.build());
            }
        }
        for (GenericRecord datum : records) {
            dataFileWriter.append(datum);
        }

        return contactList.size();

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

    private SegmentExportContext initSegmentExportContext(Tenant tenant, SegmentExportStepConfiguration config,
            MetadataSegmentExport metadataSegmentExport, Schema schema) {
        SegmentExportContextBuilder segmentExportContextBuilder = new SegmentExportContextBuilder();

        CustomerSpace customerSpace = config.getCustomerSpace();

        log.info(String.format("Processing MetadataSegmentExport: %s", JsonUtils.serialize(metadataSegmentExport)));

        FrontEndQuery accountFrontEndQuery = new FrontEndQuery();
        accountFrontEndQuery.setAccountRestriction(metadataSegmentExport.getAccountFrontEndRestriction());
        accountFrontEndQuery.setContactRestriction(metadataSegmentExport.getContactFrontEndRestriction());
        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);
        setSortField(BusinessEntity.Account,
                Arrays.asList(InterfaceName.LDC_Name.name(), InterfaceName.AccountId.name()), false,
                accountFrontEndQuery);

        List<Object> modifiableAccountIdCollectionForContacts = new ArrayList<>();

        FrontEndQuery contactFrontEndQuery = new FrontEndQuery();
        if (metadataSegmentExport.getType() == ExportType.ACCOUNT_AND_CONTACT) {
            FrontEndRestriction contactRestrictionWithAccountIdList = prepareContactRestriction(
                    metadataSegmentExport.getContactFrontEndRestriction().getRestriction(),
                    modifiableAccountIdCollectionForContacts);
            contactFrontEndQuery.setContactRestriction(contactRestrictionWithAccountIdList);
        } else {
            contactFrontEndQuery.setAccountRestriction(metadataSegmentExport.getAccountFrontEndRestriction());
            contactFrontEndQuery.setContactRestriction(metadataSegmentExport.getContactFrontEndRestriction());
        }
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);
        setSortField(BusinessEntity.Contact,
                Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactName.name()), false,
                contactFrontEndQuery);

        Set<String> addFieldsAccounts = new HashSet<>();
        Set<String> addFieldsContacts = new HashSet<>();
        List<BusinessEntity> addFieldsEntityType = new ArrayList<>();
        List<String> addFieldsWithFullName = new ArrayList<>();

        for (int i = 0; i < schema.getFields().size(); i++) {
            String fullFieldName = schema.getFields().get(i).name();
            if (fullFieldName.startsWith(ACCOUNT_PREFIX)) {
                addFieldsEntityType.add(BusinessEntity.Account);
                addFieldsAccounts.add(fullFieldName.substring(ACCOUNT_PREFIX.length()));
            } else if (fullFieldName.startsWith(CONTACT_PREFIX)) {
                addFieldsEntityType.add(BusinessEntity.Contact);
                addFieldsContacts.add(fullFieldName.substring(CONTACT_PREFIX.length()));
            }
            addFieldsWithFullName.add(fullFieldName);
        }

        addFieldsAccounts.add(InterfaceName.AccountId.name());

        log.info(schema.getFields().size() + " Fields <Accounts>: " + JsonUtils.serialize(addFieldsAccounts));
        log.info(schema.getFields().size() + " Fields <Contacts>: " + JsonUtils.serialize(addFieldsContacts));

        Map<BusinessEntity, List<String>> mergedAccountLookupFields = new HashMap<>();
        Map<BusinessEntity, List<String>> mergedContactLookupFields = new HashMap<>();

        if (CollectionUtils.isNotEmpty(addFieldsAccounts)) {
            mergedAccountLookupFields.put(BusinessEntity.Account, new ArrayList<>(addFieldsAccounts));
        }

        if (CollectionUtils.isNotEmpty(addFieldsContacts)) {
            mergedContactLookupFields.put(BusinessEntity.Contact, new ArrayList<>(addFieldsContacts));
        }

        prepareLookupsForFrontEndQueries(accountFrontEndQuery, mergedAccountLookupFields, contactFrontEndQuery,
                mergedContactLookupFields);

        log.info(" accountFrontEndQuery -: " + JsonUtils.serialize(accountFrontEndQuery));

        segmentExportContextBuilder //
                .customerSpace(customerSpace) //
                .tenant(tenant) //
                .metadataSegmentExport(metadataSegmentExport) //
                .accountFrontEndQuery(accountFrontEndQuery) //
                .contactFrontEndQuery(contactFrontEndQuery) //
                .modifiableAccountIdCollectionForContacts(modifiableAccountIdCollectionForContacts) //
                .counter(new Counter());

        SegmentExportContext segmentExportContext = segmentExportContextBuilder.build();

        return segmentExportContext;
    }

    private FrontEndRestriction prepareContactRestriction(Restriction extractedContactRestriction,
            Collection<Object> modifiableAccountIdCollection) {
        Restriction accountIdRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.AccountId.name()).inCollection(modifiableAccountIdCollection)
                .build();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction(
                Restriction.builder().and(extractedContactRestriction, accountIdRestriction).build());

        return frontEndRestriction;
    }

    private void prepareLookupsForFrontEndQueries(FrontEndQuery accountFrontEndQuery,
            Map<BusinessEntity, List<String>> mergedAccountLookupFields, FrontEndQuery contactFrontEndQuery,
            Map<BusinessEntity, List<String>> mergedContactLookupFields) {
        List<Lookup> accountLookups = new ArrayList<>();
        mergedAccountLookupFields //
                .keySet().stream() //
                .forEach( //
                        businessEntity -> prepareLookups(businessEntity, accountLookups, mergedAccountLookupFields));

        List<Lookup> contactLookups = new ArrayList<>();
        mergedContactLookupFields //
                .keySet().stream() //
                .forEach( //
                        businessEntity -> prepareLookups(businessEntity, contactLookups, mergedContactLookupFields));

        accountFrontEndQuery.setLookups(accountLookups);
        contactFrontEndQuery.setLookups(contactLookups);
    }

    private void prepareLookups(BusinessEntity businessEntity, List<Lookup> lookups,
            Map<BusinessEntity, List<String>> entityLookupFields) {
        entityLookupFields.get(businessEntity) //
                .stream() //
                .forEach( //
                        field -> lookups.add(new AttributeLookup(businessEntity, field)));
    }

    public void writeDataFiles(Tenant tenant, MetadataSegmentExport metadataSegmentExport,
            Configuration yarnConfiguration) throws Exception {
        Table segmentExportTable = metadataProxy.getTable(tenant.getId(), metadataSegmentExport.getTableName());
        long currentTimeMillis = System.currentTimeMillis();

        Schema schema = TableUtils.createSchema("SomeSchema", segmentExportTable);
        String csvFileName = metadataSegmentExport.getFileName();
        String avroFileName = csvFileName.substring(0, csvFileName.lastIndexOf(".csv")) + ".avro";

        File localFile = new File(tenant.getName() + "_" + currentTimeMillis + "_" + avroFileName);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
                new GenericDatumWriter<GenericRecord>(schema))) {
            dataFileWriter.create(schema, localFile);
            List<GenericRecord> records = new ArrayList<>();
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            for (int i = 0; i < 10; i++) {
                for (Field field : schema.getFields()) {
                    builder.set(field.name(), i + field.name());
                }
                records.add(builder.build());
            }
            for (GenericRecord datum : records) {
                dataFileWriter.append(datum);
            }
        }

        String path = metadataSegmentExport.getPath();
        String avroPath = path + "avro/";

        try {
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), avroPath + avroFileName);
        } finally {
            FileUtils.deleteQuietly(localFile);
        }

        Extract extract = new Extract();
        extract.setExtractionTimestamp(currentTimeMillis);
        extract.setName("segmentExport");
        extract.setPath(avroPath + "*.avro");
        extract.setTable(segmentExportTable);
        extract.setTenant(segmentExportTable.getTenant());
        segmentExportTable.addExtract(extract);
        metadataProxy.updateTable(tenant.getId(), metadataSegmentExport.getTableName(), segmentExportTable);
    }

    private void setSortField(BusinessEntity entityType, List<String> sortBy, boolean descending,
            FrontEndQuery entityFrontEndQuery) {
        if (CollectionUtils.isEmpty(sortBy)) {
            sortBy = Arrays.asList(InterfaceName.AccountId.name());
        }

        List<AttributeLookup> lookups = sortBy.stream() //
                .map(sort -> new AttributeLookup(entityType, sort)) //
                .collect(Collectors.toList());

        FrontEndSort sort = new FrontEndSort(lookups, descending);
        entityFrontEndQuery.setSort(sort);
    }

    @VisibleForTesting
    void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }

    @VisibleForTesting
    void setAccountFetcher(ExportAccountFetcher accountFetcher) {
        this.accountFetcher = accountFetcher;
    }

    @VisibleForTesting
    void setContactFetcher(ExportContactFetcher contactFetcher) {
        this.contactFetcher = contactFetcher;
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }
}
