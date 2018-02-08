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
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

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
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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

public abstract class SegmentExportProcessor {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportProcessor.class);

    @Autowired
    protected ExportAccountFetcher accountFetcher;

    @Autowired
    protected ExportContactFetcher contactFetcher;

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${playmaker.workflow.segment.pagesize:100}")
    protected long pageSize;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public abstract boolean accepts(MetadataSegmentExportType type);

    protected abstract void fetchAndProcessPage(Schema schema, SegmentExportContext segmentExportContext,
            File localFile) throws IOException;

    public void executeExportActivity(Tenant tenant, SegmentExportStepConfiguration config,
            Configuration yarnConfiguration) {
        CustomerSpace customerSpace = config.getCustomerSpace();
        String exportId = config.getMetadataSegmentExportId();

        MetadataSegmentExport metadataSegmentExport = internalResourceRestApiProxy
                .getMetadataSegmentExport(customerSpace, exportId);
        Table segmentExportTable = metadataProxy.getTable(tenant.getId(), metadataSegmentExport.getTableName());
        long currentTimeMillis = System.currentTimeMillis();
        log.info("segmentExportTable = : " + JsonUtils.serialize(segmentExportTable));

        Schema schema = TableUtils.createSchema(metadataSegmentExport.getTableName(), segmentExportTable);

        SegmentExportContext segmentExportContext = initSegmentExportContext(tenant, config, metadataSegmentExport,
                schema);

        try {
            String csvFileName = metadataSegmentExport.getFileName();
            String avroFileName = csvFileName.substring(0, csvFileName.lastIndexOf(".csv")) + ".avro";

            File localFile = new File(tenant.getName() + "_" + currentTimeMillis + "_" + avroFileName);

            fetchAndProcessPage(schema, segmentExportContext, localFile);

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
        if (metadataSegmentExport.getType() == MetadataSegmentExportType.ACCOUNT_AND_CONTACT) {
            FrontEndRestriction contactRestrictionWithAccountIdList = prepareContactRestriction(
                    metadataSegmentExport.getContactFrontEndRestriction().getRestriction(),
                    modifiableAccountIdCollectionForContacts);
            contactFrontEndQuery.setContactRestriction(contactRestrictionWithAccountIdList);
            setSortField(BusinessEntity.Contact,
                    Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactName.name()), false,
                    contactFrontEndQuery);
        } else {
            contactFrontEndQuery.setAccountRestriction(metadataSegmentExport.getAccountFrontEndRestriction());
            contactFrontEndQuery.setContactRestriction(metadataSegmentExport.getContactFrontEndRestriction());
            setSortField(BusinessEntity.Contact,
                    Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactName.name()), false,
                    contactFrontEndQuery);
        }
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);

        Set<String> addFieldsAccounts = new HashSet<>();
        Set<String> addFieldsContacts = new HashSet<>();
        List<BusinessEntity> addFieldsEntityType = new ArrayList<>();
        List<String> addFieldsWithFullName = new ArrayList<>();

        for (int i = 0; i < schema.getFields().size(); i++) {
            String fullFieldName = schema.getFields().get(i).name();
            if (fullFieldName.startsWith(MetadataSegmentExport.ACCOUNT_PREFIX)) {
                addFieldsEntityType.add(BusinessEntity.Account);
                addFieldsAccounts.add(fullFieldName.substring(MetadataSegmentExport.ACCOUNT_PREFIX.length()));
            } else if (fullFieldName.startsWith(MetadataSegmentExport.CONTACT_PREFIX)) {
                addFieldsEntityType.add(BusinessEntity.Contact);
                addFieldsContacts.add(fullFieldName.substring(MetadataSegmentExport.CONTACT_PREFIX.length()));
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

        if (metadataSegmentExport.getType() == MetadataSegmentExportType.CONTACT) {
            Lookup specialHandlingForAccountNameLookupForContacts = new AttributeLookup(BusinessEntity.Account,
                    InterfaceName.LDC_Name.name());
            contactFrontEndQuery.getLookups().add(specialHandlingForAccountNameLookupForContacts);
        }

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

    protected void setValueInAvroRecord(Map<String, Object> account, GenericRecordBuilder builder, Field field,
            String fieldNameInAccountLookupResult) {
        Object val = account.get(fieldNameInAccountLookupResult);
        builder.set(field.name(), val == null ? null : val.toString());
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
