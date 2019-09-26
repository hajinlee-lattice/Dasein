package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.DataFeedTaskTemplateService;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("dataFeedTaskTemplateService")
public class DataFeedTaskTemplateServiceImpl implements DataFeedTaskTemplateService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskTemplateServiceImpl.class);

    private static final String DEFAULT_WEBSITE_SYSTEM = "Default_Website_System";
    private static final String USER_PREFIX = "user_";

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private CatalogEntityMgr catalogEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private BatonService batonService;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public boolean setupWebVisitTemplate(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata) {
        Preconditions.checkNotNull(simpleTemplateMetadata);
        EntityType entityType = simpleTemplateMetadata.getEntityType();
        if (!EntityType.WebVisit.equals(entityType) && !EntityType.WebVisitPathPattern.equals(entityType)) {
            throw new RuntimeException("Cannot create template for: " + entityType.getDisplayName());
        }
        S3ImportSystem websiteSystem = s3ImportSystemService.getS3ImportSystem(customerSpace, DEFAULT_WEBSITE_SYSTEM);
        if (websiteSystem != null) {
            DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, "File",
                    EntityTypeUtils.generateFullFeedType(DEFAULT_WEBSITE_SYSTEM, entityType));
            if (dataFeedTask != null) {
                throw new RuntimeException("Already created template for: " + entityType.getDisplayName());
            }
        } else {
            S3ImportSystem s3ImportSystem = new S3ImportSystem();
            String systemName = DEFAULT_WEBSITE_SYSTEM;
            s3ImportSystem.setSystemType(S3ImportSystem.SystemType.Website);
            s3ImportSystem.setName(systemName);
            s3ImportSystem.setDisplayName(systemName);
            s3ImportSystem.setTenant(tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString()));
            s3ImportSystemService.createS3ImportSystem(customerSpace, s3ImportSystem);
            dropBoxService.createFolder(customerSpace, systemName, null, null);
            websiteSystem = s3ImportSystemService.getS3ImportSystem(customerSpace, DEFAULT_WEBSITE_SYSTEM);
        }
        Table standardTable = SchemaRepository.instance().getSchema(websiteSystem.getSystemType(), entityType,
                batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace()));
        Table templateTable = generateTemplate(standardTable, simpleTemplateMetadata);
        templateTable.setName(templateTable.getName() + System.currentTimeMillis());
        metadataProxy.createImportTable(customerSpace, templateTable.getName(), templateTable);
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        dataFeedTask.setImportTemplate(templateTable);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setEntity(entityType.getEntity().name());
        dataFeedTask.setFeedType(EntityTypeUtils.generateFullFeedType(websiteSystem.getName(), entityType));
        dataFeedTask.setSource("File");
        dataFeedTask.setActiveJob("Not specified");
        dataFeedTask.setSourceConfig("Not specified");
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date(0L));
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setSubType(entityType.getSubType());
        dataFeedTask.setTemplateDisplayName(dataFeedTask.getFeedType());
        dataFeedTaskService.createDataFeedTask(customerSpace, dataFeedTask);

        if (EntityType.WebVisitPathPattern == entityType) {
            // create ptn catalog
            Catalog catalog = new Catalog();
            catalog.setTenant(websiteSystem.getTenant());
            catalog.setName(EntityType.WebVisitPathPattern.name());
            catalog.setDataFeedTask(dataFeedTask);
            catalogEntityMgr.create(catalog);
            log.info("Create WebVisitPathPattern catalog for tenant {}, catalog={}, dataFeedTaskUniqueId={}",
                    customerSpace, catalog, dataFeedTask.getUniqueId());
        }

        return true;
    }

    private Table generateTemplate(Table standardTable, SimpleTemplateMetadata simpleTemplateMetadata) {
        Preconditions.checkNotNull(standardTable);
        Preconditions.checkNotNull(simpleTemplateMetadata);
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getIgnoredStandardAttributes())) {
            standardTable.getAttributes()
                    .removeIf(attribute -> !Boolean.TRUE.equals(attribute.getRequired()) &&
                            simpleTemplateMetadata.getIgnoredStandardAttributes().contains(attribute.getName()));
        }
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getStandardAttributes())) {
            Map<String, String> nameMapping = simpleTemplateMetadata.getStandardAttributes()
                    .stream()
                    .filter(simpleTemplateAttribute -> StringUtils.isNotBlank(simpleTemplateAttribute.getName()))
                    .collect(Collectors.toMap(SimpleTemplateMetadata.SimpleTemplateAttribute::getName,
                            SimpleTemplateMetadata.SimpleTemplateAttribute::getDisplayName));
            standardTable.getAttributes().forEach(attribute -> {
                if (nameMapping.containsKey(attribute.getName())) {
                    attribute.setDisplayName(nameMapping.get(attribute.getName()));
                }
            });
        }
        standardTable.getAttributes().forEach(attribute -> {
            if (StringUtils.isEmpty(attribute.getDisplayName())) {
                attribute.setDisplayName(attribute.getName());
            }
        });
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getCustomerAttributes())) {
            standardTable.addAttributes(generateCustomerAttributes(simpleTemplateMetadata.getCustomerAttributes()));
        }
        return standardTable;
    }

    private List<Attribute> generateCustomerAttributes(List<SimpleTemplateMetadata.SimpleTemplateAttribute> simpleCustomerAttrs) {
        List<Attribute> customerAttributes = new ArrayList<>();
        for (SimpleTemplateMetadata.SimpleTemplateAttribute simpleTemplateAttr :  simpleCustomerAttrs) {
            Attribute attribute = new Attribute();
            attribute.setDisplayName(simpleTemplateAttr.getDisplayName());
            attribute.setNullable(Boolean.TRUE);
            attribute.setName(getCustomerAttributeName(simpleTemplateAttr.getName(), simpleTemplateAttr.getDisplayName()));
            attribute.setPhysicalDataType(simpleTemplateAttr.getPhysicalDataType().name());
            FundamentalType fundamentalType = getFundamentalType(simpleTemplateAttr.getFundamentalType(),
                    simpleTemplateAttr.getPhysicalDataType());
            attribute.setFundamentalType(fundamentalType);
            if (FundamentalType.DATE.equals(fundamentalType)) {
                attribute.setLogicalDataType(LogicalDataType.Date);
            }
            attribute.setTags(ModelingMetadata.INTERNAL_TAG);
            attribute.setApprovedUsageFromEnumList(getApprovedUsage(simpleTemplateAttr.getApprovedUsages()));
            customerAttributes.add(attribute);
        }
        return customerAttributes;
    }

    private String getCustomerAttributeName(String name, String displayName) {
        Preconditions.checkNotNull(displayName);
        if (StringUtils.isEmpty(name)) {
            return displayName.startsWith(USER_PREFIX) ? displayName : USER_PREFIX + displayName;
        } else {
            return name.startsWith(USER_PREFIX) ? name : USER_PREFIX + name;
        }
    }

    private FundamentalType getFundamentalType(String fundamentalType, Schema.Type dataType) {
        FundamentalType type = null;
        if (StringUtils.isNotEmpty(fundamentalType)) {
            try {
                type = FundamentalType.fromName(fundamentalType);
            } catch (IllegalArgumentException ignored) {
            }
        }
        if (type == null) {
            switch (dataType) {
                case ENUM:
                    type = FundamentalType.ENUM;
                    break;
                case INT:
                case FLOAT:
                case DOUBLE:
                    type = FundamentalType.NUMERIC;
                    break;
                case LONG:
                    type = FundamentalType.DATE;
                    break;
                case BOOLEAN:
                    type = FundamentalType.BOOLEAN;
                    break;
                case STRING:
                default:
                    type = FundamentalType.ALPHA;
                    break;
            }
        }
        return type;
    }

    private List<ApprovedUsage> getApprovedUsage(List<String> approvedUsages) {
        Set<ApprovedUsage> allApprovedUsages = new HashSet<>();
        allApprovedUsages.add(ApprovedUsage.MODEL_ALLINSIGHTS);
        if (CollectionUtils.isNotEmpty(approvedUsages)) {
            for (String approvedUsage : approvedUsages) {
                 ApprovedUsage approvedUsage1 = ApprovedUsage.fromName(approvedUsage);
                 if (!ApprovedUsage.NONE.equals(approvedUsage1)) {
                     allApprovedUsages.add(approvedUsage1);
                 }
            }
        }
        return new ArrayList<>(allApprovedUsages);
    }
}
