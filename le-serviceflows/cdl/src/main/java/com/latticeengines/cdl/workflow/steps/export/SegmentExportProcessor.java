package com.latticeengines.cdl.workflow.steps.export;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.export.SegmentExportContext.Counter;
import com.latticeengines.cdl.workflow.steps.export.SegmentExportContext.SegmentExportContextBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;
import com.latticeengines.domain.exposed.util.SegmentExportUtil;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class SegmentExportProcessor {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportProcessor.class);

    public static String SEPARATOR = "___";

    @Inject
    protected ExportAccountFetcher accountFetcher;

    @Inject
    protected ExportContactFetcher contactFetcher;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Value("${playmaker.workflow.segment.pagesize:200}")
    protected long pageSize;

    private String avroFilePath;

    private List<Predefined> filterByPredefinedSelection = Collections.singletonList(Predefined.Enrichment);

    protected DataCollection.Version version;

    public abstract boolean accepts(AtlasExportType type);

    protected abstract void fetchAndProcessPage(Schema schema, SegmentExportContext segmentExportContext,
                                                File localFile) throws IOException;

    public void executeExportActivity(Tenant tenant, SegmentExportStepConfiguration config,
                                      Configuration yarnConfiguration) {
        CustomerSpace customerSpace = config.getCustomerSpace();
        version = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        log.info(String.format("Using DataCollection.Version %s", version.name()));

        MetadataSegmentExport metadataSegmentExport = config.getMetadataSegmentExport();
        if (metadataSegmentExport == null) {
            String exportId = config.getMetadataSegmentExportId();
            metadataSegmentExport = plsInternalProxy.getMetadataSegmentExport(customerSpace, exportId);
        }

        AtlasExportType exportType = metadataSegmentExport.getType();

        Map<BusinessEntity, List<Attribute>> configuredBusEntityAttrMap = new TreeMap<>();
        BusinessEntity.SEGMENT_ENTITIES.forEach(i -> configuredBusEntityAttrMap.put(i, new ArrayList<Attribute>()));

        List<Attribute> configuredAccountAttributes = configuredBusEntityAttrMap.get(BusinessEntity.Account);
        List<Attribute> configuredContactAttributes = configuredBusEntityAttrMap.get(BusinessEntity.Contact);
        List<Attribute> configuredRatingAttributes = configuredBusEntityAttrMap.get(BusinessEntity.Rating);
        List<Attribute> configuredPurHistoryAttributes = configuredBusEntityAttrMap.get(BusinessEntity.PurchaseHistory);
        List<Attribute> configuredCuratedAccAttributes = configuredBusEntityAttrMap.get(BusinessEntity.CuratedAccount);

        if (exportType == AtlasExportType.ACCOUNT || exportType == AtlasExportType.ACCOUNT_AND_CONTACT) {
            configuredAccountAttributes.addAll(getSchema(tenant.getId(), BusinessEntity.Account));

            Map<String, Attribute> defaultAccountAttributesMap = exportType.getDefaultAttributeTuples().stream() //
                    .filter(tuple -> tuple.getLeft() == BusinessEntity.Account
                            && InterfaceName.AccountId.name().equals(tuple.getMiddle())) //
                    .map(tuple -> {
                        Attribute attribute = new Attribute();
                        attribute.setName(BusinessEntity.Account.name() + SEPARATOR + tuple.getMiddle());
                        attribute.setDisplayName(tuple.getRight());
                        attribute.setSourceLogicalDataType("");
                        attribute.setPhysicalDataType(Type.STRING.name());
                        return attribute;
                    }) //
                    .collect(Collectors.toMap(Attribute::getName, att -> att));

            configuredAccountAttributes.forEach(attr -> defaultAccountAttributesMap.remove(attr.getName()));

            if (MapUtils.isNotEmpty(defaultAccountAttributesMap)) {
                configuredAccountAttributes.addAll(defaultAccountAttributesMap.values());
            }

            configuredRatingAttributes.addAll(getSchema(tenant.getId(), BusinessEntity.Rating));
            configuredPurHistoryAttributes.addAll(getSchema(tenant.getId(), BusinessEntity.PurchaseHistory));
            configuredCuratedAccAttributes.addAll(getSchema(tenant.getId(), BusinessEntity.CuratedAccount));

            Map<String, String> customizedNameMapping = getCustomizedDisplayNames(tenant.getId(),
                    BusinessEntity.Account);
            configuredAccountAttributes.forEach(attr -> {
                if (customizedNameMapping.containsKey(attr.getName())) {
                    attr.setDisplayName(customizedNameMapping.get(attr.getName()));
                }
            });
        }

        if (exportType == AtlasExportType.CONTACT || exportType == AtlasExportType.ACCOUNT_AND_CONTACT
                || exportType == AtlasExportType.ORPHAN_CONTACT) {
            configuredContactAttributes.addAll(getSchema(tenant.getId(), BusinessEntity.Contact));
            Map<String, Attribute> defaultContactAttributesMap = new HashMap<>();
            exportType.getDefaultAttributeTuples().stream() //
                    .filter(tuple -> tuple.getLeft() == BusinessEntity.Contact
                            && (InterfaceName.ContactId.name().equals(tuple.getMiddle())
                            || InterfaceName.AccountId.name().equals(tuple.getMiddle()))) //
                    .map(tuple -> {
                        Attribute attribute = new Attribute();
                        attribute.setName(BusinessEntity.Contact.name() + SEPARATOR + tuple.getMiddle());
                        attribute.setDisplayName(tuple.getRight());
                        attribute.setSourceLogicalDataType("");
                        attribute.setPhysicalDataType(Type.STRING.name());
                        return attribute;
                    }) //
                    .forEach(att -> defaultContactAttributesMap.put(att.getName(), att));
            configuredContactAttributes.forEach(attr -> defaultContactAttributesMap.remove(attr.getName()));

            if (MapUtils.isNotEmpty(defaultContactAttributesMap)) {
                configuredContactAttributes.addAll(defaultContactAttributesMap.values());
            }

            Map<String, String> customizedNameMapping = getCustomizedDisplayNames(tenant.getId(),
                    BusinessEntity.Contact);
            configuredContactAttributes.forEach(attr -> {
                if (customizedNameMapping.containsKey(attr.getName())) {
                    attr.setDisplayName(customizedNameMapping.get(attr.getName()));
                }
            });
        }

        registerTableForExport(customerSpace, metadataSegmentExport, configuredBusEntityAttrMap);
        config.setMetadataSegmentExport(metadataSegmentExport);
        SegmentExportContext segmentExportContext = initSegmentExportContext(tenant, config, metadataSegmentExport,
                configuredBusEntityAttrMap);
        fetchExtratPageByPage(tenant, metadataSegmentExport, segmentExportContext, yarnConfiguration);
    }

    private void fetchExtratPageByPage(Tenant tenant, MetadataSegmentExport metadataSegmentExport,
                                       SegmentExportContext segmentExportContext, Configuration yarnConfiguration) {
        try {
            long currentTimeMillis = System.currentTimeMillis();

            Table segmentExportTable = metadataProxy.getTable(tenant.getId(), metadataSegmentExport.getTableName());
            log.info("segmentExportTable = : " + JsonUtils.serialize(segmentExportTable));

            Schema schema = TableUtils.createSchema(metadataSegmentExport.getTableName(), segmentExportTable);

            String csvFileName = metadataSegmentExport.getFileName();
            String avroFileName = csvFileName.substring(0, csvFileName.lastIndexOf(".csv")) + ".avro";

            File localFile = new File(tenant.getName() + "_" + currentTimeMillis + "_" + avroFileName);

            fetchAndProcessPage(schema, segmentExportContext, localFile);

            String path = metadataSegmentExport.getPath();
            String avroPath = path + "avro/";

            try {
                avroFilePath = avroPath + avroFileName;
                HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), avroFilePath);
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

    private void registerTableForExport( //
                                         CustomerSpace customerSpace, MetadataSegmentExport metadataSegmentExportJob, //
                                         Map<BusinessEntity, List<Attribute>> configuredBusEntityAttrMap) {

        Tenant tenant = new Tenant(customerSpace.toString());
        Table segmentExportTable = SegmentExportUtil.constructSegmentExportTable(tenant, metadataSegmentExportJob,
                configuredBusEntityAttrMap);

        metadataProxy.createTable(tenant.getId(), segmentExportTable.getName(), segmentExportTable);
    }

    private SegmentExportContext initSegmentExportContext(Tenant tenant, //
                                                          SegmentExportStepConfiguration config, //
                                                          MetadataSegmentExport metadataSegmentExport, //
                                                          Map<BusinessEntity, List<Attribute>> configuredBusEntityAttrMap) {
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
        if (metadataSegmentExport.getType() == AtlasExportType.ACCOUNT_AND_CONTACT) {
            FrontEndRestriction contactRestrictionWithAccountIdList = prepareContactRestriction(
                    metadataSegmentExport.getContactFrontEndRestriction().getRestriction(),
                    modifiableAccountIdCollectionForContacts);
            contactFrontEndQuery.setContactRestriction(contactRestrictionWithAccountIdList);
        } else if (metadataSegmentExport.getType() == AtlasExportType.ORPHAN_CONTACT) {
            Restriction restriction = Restriction.builder().let(BusinessEntity.Account, InterfaceName.AccountId.name())
                    .isNull().build();
            FrontEndRestriction frontEndRestriction = new FrontEndRestriction(
                    Restriction.builder().or(restriction).build());
            contactFrontEndQuery.setAccountRestriction(metadataSegmentExport.getAccountFrontEndRestriction());
            contactFrontEndQuery.setContactRestriction(frontEndRestriction);
        } else {
            contactFrontEndQuery.setAccountRestriction(metadataSegmentExport.getAccountFrontEndRestriction());
            contactFrontEndQuery.setContactRestriction(metadataSegmentExport.getContactFrontEndRestriction());
        }
        setSortField(BusinessEntity.Contact,
                Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name()), false,
                contactFrontEndQuery);
        contactFrontEndQuery.setMainEntity(BusinessEntity.Contact);

        prepareLookupsForFrontEndQueries(accountFrontEndQuery, contactFrontEndQuery, configuredBusEntityAttrMap);

        if (metadataSegmentExport.getType() == AtlasExportType.CONTACT) {
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

        return segmentExportContextBuilder.build();
    }

    private FrontEndRestriction prepareContactRestriction(Restriction extractedContactRestriction,
                                                          Collection<Object> modifiableAccountIdCollection) {
        Restriction accountIdRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, InterfaceName.AccountId.name()).inCollection(modifiableAccountIdCollection)
                .build();

        return new FrontEndRestriction(
                Restriction.builder().and(extractedContactRestriction, accountIdRestriction).build());
    }

    private Map<String, String> getCustomizedDisplayNames(String tenantId, BusinessEntity business) {
        Map<String, String> nameToDisplayNameMap = new HashMap<>();
        try {
            Map<BusinessEntity, List<AttrConfig>> customDisplayNameAttrs = cdlAttrConfigProxy
                    .getCustomDisplayNames(tenantId);
            if (MapUtils.isNotEmpty(customDisplayNameAttrs)) {
                List<AttrConfig> renderedConfigList = customDisplayNameAttrs.get(business);
                if (CollectionUtils.isNotEmpty(renderedConfigList)) {
                    renderedConfigList.forEach(config -> {
                        if (StringUtil.isNotBlank(
                                config.getPropertyFinalValue(ColumnMetadataKey.DisplayName, String.class))) {
                            nameToDisplayNameMap.put(business.name() + SEPARATOR + config.getAttrName(),
                                    (String) (config.getProperty(ColumnMetadataKey.DisplayName).getCustomValue()));
                        }
                    });
                }
            }
            log.info("Customer display name: \n" + nameToDisplayNameMap);
        } catch (LedpException e) {
            log.warn("Got LedpException " + ExceptionUtils.getStackTrace(e));
            return new HashMap<>();
        }
        return nameToDisplayNameMap;
    }

    private void setSortField(BusinessEntity entityType, List<String> sortBy, boolean descending,
                                    FrontEndQuery entityFrontEndQuery) {
        if (CollectionUtils.isEmpty(sortBy)) {
            sortBy = Collections.singletonList(InterfaceName.AccountId.name());
        }

        List<AttributeLookup> lookups = sortBy.stream() //
                .map(sort -> new AttributeLookup(entityType, sort)) //
                .collect(Collectors.toList());

        FrontEndSort sort = new FrontEndSort(lookups, descending);
        entityFrontEndQuery.setSort(sort);
    }

    private void prepareLookupsForFrontEndQueries( //
                                                   FrontEndQuery accountFrontEndQuery, //
                                                   FrontEndQuery contactFrontEndQuery, //
                                                   Map<BusinessEntity, List<Attribute>> configuredBusEntityAttrMap) {
        List<Lookup> accountLookups = new ArrayList<>();
        List<Attribute> configuredAccountAttributes = configuredBusEntityAttrMap.get(BusinessEntity.Account);
        List<Attribute> configuredContactAttributes = configuredBusEntityAttrMap.get(BusinessEntity.Contact);

        // by default add AccountId lookup in account query
        AttributeLookup accountIdAttributeForLookup = new AttributeLookup(BusinessEntity.Account, //
                InterfaceName.AccountId.name());
        accountLookups.add(accountIdAttributeForLookup);

        if (CollectionUtils.isNotEmpty(configuredAccountAttributes)) {
            configuredAccountAttributes //
                    .forEach( //
                            a -> {
                                AttributeLookup attributeForLookup = new AttributeLookup(BusinessEntity.Account, //
                                        a.getName().substring((BusinessEntity.Account + SEPARATOR).length()));
                                accountLookups.add(attributeForLookup);
                            });
        }

        Set<BusinessEntity> segmentPartEntities = new HashSet<>(
                Arrays.asList(BusinessEntity.Rating, BusinessEntity.PurchaseHistory, BusinessEntity.CuratedAccount));
        for (BusinessEntity entity : segmentPartEntities) {
            List<Attribute> configuredAttributes = configuredBusEntityAttrMap.get(entity);
            if (CollectionUtils.isNotEmpty(configuredAttributes)) {
                configuredAttributes //
                        .forEach( //
                                r -> accountLookups.add(new AttributeLookup(entity, //
                                        r.getName() //
                                                .substring((entity + SEPARATOR).length()))));
            }
        }

        List<Lookup> contactLookups = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(configuredContactAttributes)) {
            configuredContactAttributes //
                    .forEach( //
                            c -> contactLookups.add(new AttributeLookup(BusinessEntity.Contact, //
                                    c.getName() //
                                            .substring((BusinessEntity.Contact + SEPARATOR).length()))));
        }
        accountFrontEndQuery.setLookups(accountLookups);
        contactFrontEndQuery.setLookups(contactLookups);
    }

    protected void setValueInAvroRecord(Map<String, Object> account, GenericRecordBuilder builder, Field field,
                                        String fieldNameInAccountLookupResult) {
        Object val = account.get(fieldNameInAccountLookupResult);
        builder.set(field.name(), val == null ? null : val.toString());
    }

    private List<Attribute> getSchema(String customerSpace, BusinessEntity entity) {
        List<ColumnMetadata> cms = servingStoreProxy
                .getDecoratedMetadata(customerSpace, entity, filterByPredefinedSelection).collectList().block();
        Mono<List<Attribute>> stream = Flux.fromIterable(cms) //
                .map(metadata -> {
                    Attribute attribute = new Attribute();
                    attribute.setName(entity.name() + SEPARATOR + metadata.getAttrName());
                    attribute.setDisplayName(metadata.getDisplayName());
                    attribute.setSourceLogicalDataType(
                            metadata.getLogicalDataType() == null ? "" : metadata.getLogicalDataType().name());
                    attribute.setPhysicalDataType(Type.STRING.name());
                    return attribute;
                }) //
                .collectList();
        return stream.block();
    }

    @VisibleForTesting
    void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }

    @VisibleForTesting
    public String getAvroFilePath() {
        return avroFilePath;
    }

}
