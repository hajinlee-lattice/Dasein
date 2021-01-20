package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ActivityTypeId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastModifiedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ModelName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ModelNameId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageNameId;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityMetricsGroupService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.DataFeedTaskTemplateService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskConfig;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.AttributeLengthValidator;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.SimpleValueFilter;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.TemplateValidator;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.ActivityStoreUtils;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.domain.exposed.util.WebVisitUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("dataFeedTaskTemplateService")
public class DataFeedTaskTemplateServiceImpl implements DataFeedTaskTemplateService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskTemplateServiceImpl.class);

    private static final String DEFAULT_WEBSITE_SYSTEM = "Default_Website_System";
    private static final String USER_PREFIX = "user_";
    private static final String LATTICE_IDS_SECTION = "Lattice IDs";
    private static final String MATCH_TO_ACCOUNT_ID_SECTION = "Match to Accounts - ID";
    private static final String MATCH_TO_CONTACT_ID_SECTION = "Match to Contacts - ID";
    private static final String ACCOUNT_FIELD_NAME = "AccountId";
    private static final List<String> CONTACT_FIELD_NAME = Arrays.asList("ContactId", "leadId", "LeadId", "prospect_id");
    private static final String DEFAULTSYSTEM = "DefaultSystem";

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private CatalogEntityMgr catalogEntityMgr;

    @Inject
    protected AtlasStreamEntityMgr streamEntityMgr;

    @Inject
    protected StreamDimensionEntityMgr dimensionEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private BatonService batonService;

    @Inject
    private S3Service s3Service;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ActivityMetricsGroupService activityMetricsGroupService;

    @Inject
    private ImportWorkflowSpecService importWorkflowSpecService;

    @Inject
    private ActionService actionService;

    @Value("${aws.customer.s3.bucket}")
    private String customerBucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Override
    public boolean setupWebVisitProfile(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata,
                                        String systemDisplayName) {
        Preconditions.checkNotNull(simpleTemplateMetadata);
        EntityType entityType = simpleTemplateMetadata.getEntityType();
        S3ImportSystem websiteSystem = setupSystems(customerSpace, entityType, S3ImportSystem.SystemType.Website,
                S3ImportSystem.SystemType.Website.getDefaultSystemName(), systemDisplayName);

        Table standardTable = SchemaRepository.instance().getSchema(websiteSystem.getSystemType(), entityType,
                batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace()),
                batonService.onlyEntityMatchGAEnabled(MultiTenantContext.getCustomerSpace()));

        DataFeedTask dataFeedTask = setupDataFeedTask(customerSpace, simpleTemplateMetadata, entityType, websiteSystem,
                standardTable);
        setupWebVisitCatalogs(customerSpace, entityType, dataFeedTask);
        return true;
    }

    public boolean setupWebVisitProfile2(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata,
                                         String systemDisplayName) {
        Preconditions.checkNotNull(simpleTemplateMetadata);
        EntityType entityType = simpleTemplateMetadata.getEntityType();
        S3ImportSystem websiteSystem = setupSystems(customerSpace, entityType, S3ImportSystem.SystemType.Website,
                S3ImportSystem.SystemType.Website.getDefaultSystemName(), systemDisplayName);

        ImportWorkflowSpec spec;
        try {
            spec = importWorkflowSpecService.loadSpecFromS3(websiteSystem.getSystemType().name(),
                    entityType.getDisplayName());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Could not create template for tenant %s, system type %s, and system object %s " +
                            "because the Spec failed to load", customerSpace, websiteSystem.getSystemType().name(),
                            entityType.getDisplayName()), e);
        }
        Table standardTable = importWorkflowSpecService.tableFromRecord(null, true, spec);

        DataFeedTask dataFeedTask = setupDataFeedTask(customerSpace, simpleTemplateMetadata, entityType, websiteSystem,
                standardTable);
        setupWebVisitCatalogs(customerSpace, entityType, dataFeedTask);
        return true;
    }

    private S3ImportSystem setupSystems(String customerSpace, EntityType entityType,
                                        S3ImportSystem.SystemType systemType, String systemName,
                                        String systemDisplayName) {
        if (!systemType.getEntityTypes().contains(entityType)) {
            log.error("Cannot create template for: {}, systemType is {}.", entityType.getDisplayName(), systemType);
            throw new RuntimeException("Cannot create template for: " + entityType.getDisplayName());
        }
        S3ImportSystem importSystem = s3ImportSystemService.getS3ImportSystem(customerSpace,
                systemName);
        if (importSystem != null) {
            DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, "File",
                    EntityTypeUtils.generateFullFeedType(systemName, entityType));
            if (dataFeedTask != null) {
                throw new RuntimeException("Already created template for: " + entityType.getDisplayName());
            }
        } else {
            importSystem = createS3ImportSystem(customerSpace, systemName, systemDisplayName, systemType);
            log.debug("Successfully created S3ImportSystem for entity type {}:\n{}", entityType,
                    JsonUtils.pprint(importSystem));
        }
        return importSystem;
    }

    private DataFeedTask setupDataFeedTask(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata,
                                           EntityType entityType, S3ImportSystem websiteSystem, Table standardTable) {
        return setupDataFeedTask(customerSpace, simpleTemplateMetadata, entityType, websiteSystem, standardTable, null);
    }

    private DataFeedTask setupDataFeedTask(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata,
                                           EntityType entityType, S3ImportSystem importSystem, Table standardTable,
                                           String systemType) {
        Table templateTable = generateTemplate(standardTable, simpleTemplateMetadata);
        templateTable.setName(templateTable.getName() + System.currentTimeMillis());
        metadataProxy.createImportTable(customerSpace, templateTable.getName(), templateTable);
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        dataFeedTask.setImportTemplate(templateTable);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setEntity(entityType.getEntity().name());
        dataFeedTask.setFeedType(EntityTypeUtils.generateFullFeedType(importSystem.getName(), entityType));
        dataFeedTask.setSource("File");
        dataFeedTask.setActiveJob("Not specified");
        dataFeedTask.setSourceConfig("Not specified");
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date(0L));
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setSubType(entityType.getSubType());
        dataFeedTask.setTemplateDisplayName(dataFeedTask.getFeedType());
        dataFeedTask.setSpecType(systemType);
        dataFeedTaskService.createDataFeedTask(customerSpace, dataFeedTask);
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
            dataFeedService.updateDataFeed(customerSpace, "", DataFeed.Status.Initialized.getName());
        }

        log.debug("Successfully created DataFeedTask with FeedType {} for entity type {}",
                dataFeedTask.getFeedType(), entityType);

        return dataFeedTask;
    }

    private void setupWebVisitCatalogs(String customerSpace, EntityType entityType, DataFeedTask dataFeedTask) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        Catalog pathPtnCatalog = catalogEntityMgr.findByNameAndTenant(EntityType.WebVisitPathPattern.name(), tenant);
        Catalog srcMediumCatalog = catalogEntityMgr.findByNameAndTenant(EntityType.WebVisitSourceMedium.name(), tenant);
        if (EntityType.WebVisit == entityType) {
            AtlasStream webVisitStream = WebVisitUtils.newWebVisitStream(tenant, dataFeedTask);
            webVisitStream.setStreamId(AtlasStream.generateId());
            streamEntityMgr.create(webVisitStream);
            log.info(
                    "Create WebVisit activity stream for tenant {}. stream PID = {}, streamId={}, dataFeedTaskUniqueId = {}",
                    tenant.getId(), webVisitStream.getPid(), webVisitStream.getStreamId(), dataFeedTask.getUniqueId());
            List<StreamDimension> dimensions = WebVisitUtils.newWebVisitDimensions(webVisitStream, pathPtnCatalog,
                    srcMediumCatalog);
            dimensions.forEach(dimensionEntityMgr::create);
            log.info("Create WebVisit stream dimensions for tenant {}. PathPatternCatalog = {}",
                    webVisitStream.getTenant().getId(), pathPtnCatalog);
            List<ActivityMetricsGroup> defaultGroups = activityMetricsGroupService.setupDefaultWebVisitGroups(
                    tenant.getId(), webVisitStream.getName());
            if (defaultGroups == null || defaultGroups.stream().anyMatch(Objects::isNull)) {
                throw new IllegalStateException(String.format(
                        "Failed to setup default web visit metric groups for tenant %s", customerSpace));
            }
        } else {
            // create src medium catalog
            Catalog catalog = new Catalog();
            catalog.setTenant(tenant);
            catalog.setName(entityType.name());
            catalog.setCatalogId(Catalog.generateId());
            catalog.setDataFeedTask(dataFeedTask);
            catalogEntityMgr.create(catalog);
            log.info("Create {} catalog for tenant {}, catalog={}, dataFeedTaskUniqueId={}", entityType.name(),
                    customerSpace, catalog, dataFeedTask.getUniqueId());
            if (EntityType.WebVisitPathPattern == entityType) {
                pathPtnCatalog = catalog;
            } else {
                srcMediumCatalog = catalog;
            }

            attachDimensionCatalog(tenant, InterfaceName.PathPatternId.name(), pathPtnCatalog);
            attachDimensionCatalog(tenant, InterfaceName.SourceMediumId.name(), srcMediumCatalog);
        }

        log.debug("Successfully set up Catalog for entity type {}", entityType);
    }

    @Override
    public String backupTemplate(String customerSpace, String uniqueTaskId) {
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, uniqueTaskId);
        if (dataFeedTask == null || dataFeedTask.getImportTemplate() == null) {
            log.warn("There's no template to backup for task: " + uniqueTaskId);
            return StringUtils.EMPTY;
        }
        Table template = dataFeedTask.getImportTemplate();
        String templateBackup = JsonUtils.serialize(dataFeedTask);
        InputStream backupStream;
        try {
            backupStream = IOUtils.toInputStream(templateBackup, "UTF-8");
        } catch (IOException e) {
            log.error("Cannot backup template: " + uniqueTaskId);
            return StringUtils.EMPTY;
        }
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String backupPath = pathBuilder.getS3AtlasTableBackupPrefix(CustomerSpace.parse(customerSpace).getTenantId(),
                uniqueTaskId);
        if (!backupPath.endsWith("/")) {
            backupPath += "/";
        }
        String backupName = template.getName() + "_" + System.currentTimeMillis() + ".json";
        s3Service.uploadInputStream(customerBucket, backupPath + backupName, backupStream, true);
        dropBoxService.removeTemplatePath(customerSpace, dataFeedTask.getFeedType());
        return backupName;
    }

    @Override
    public Table restoreTemplate(String customerSpace, String uniqueTaskId, String backupName, boolean onlyGetTable) {
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String backupPath = pathBuilder.getS3AtlasTableBackupPrefix(CustomerSpace.parse(customerSpace).getTenantId(),
                uniqueTaskId);
        if (!backupPath.endsWith("/")) {
            backupPath += "/";
        }
        if (!s3Service.objectExist(customerBucket, backupPath + backupName)) {
            log.error("Backup file {} not exists!", backupPath + backupName);
            throw new LedpException(LedpCode.LEDP_40072, new String[]{uniqueTaskId, backupName});
        }
        InputStream backupStream = s3Service.readObjectAsStream(customerBucket, backupPath + backupName);
        try {
            DataFeedTask backupTask = JsonUtils.deserialize(backupStream, DataFeedTask.class);
            if (!onlyGetTable) {
                DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, uniqueTaskId);
                if (dataFeedTask == null) {
                    dataFeedTaskService.createDataFeedTask(customerSpace, backupTask);
                } else {
                    dataFeedTask.setImportTemplate(backupTask.getImportTemplate());
                    dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask, false);
                }
            }
            dropBoxService.restoreTemplatePath(customerSpace, backupTask.getFeedType());
            return backupTask.getImportTemplate();
        } catch (Exception e) {
            log.error("Cannot deserialize backup file for task {}, backup file {}", uniqueTaskId, backupName);
            throw new LedpException(LedpCode.LEDP_40072, new String[]{uniqueTaskId, backupName});
        }
    }

    @Override
    public boolean validationOpportunity(String customerSpace, String systemName, EntityType entityType) {
        if (!EntityType.Opportunity.equals(entityType) && !EntityType.OpportunityStageName.equals(entityType)) {
            return false;
        }
        S3ImportSystem importSystem = s3ImportSystemService.getS3ImportSystem(customerSpace,
                systemName);
        log.info("importSystem is {}.", JsonUtils.serialize(importSystem));
        if (importSystem == null) {
            return false;
        }
        return StringUtils.isNotEmpty(importSystem.getAccountSystemId()) || isDefaultSystemInGATenant(importSystem);
    }

    @Override
    public boolean validationMarketing(String customerSpace, String systemName,
                                       String systemType, EntityType entityType) {
        if (!EntityType.MarketingActivity.equals(entityType) && !EntityType.MarketingActivityType.equals(entityType)) {
            log.error("entityType isn't match Marketing/MarketingActivityType, customerSpace is {}, systemName is {}," +
                    " " +
                            "systemType is {}, entityType is {}.", customerSpace, systemName, systemType, entityType);
            return false;
        }
        List<String> validSystemTypes = Arrays.asList(S3ImportSystem.SystemType.Eloqua.name().toLowerCase(),
                S3ImportSystem.SystemType.Marketo.name().toLowerCase(), "lattice", "pardot");
        if (!validSystemTypes.contains(systemType.toLowerCase())) {
            log.error("systemType isn't match list {}, customerSpace is {}, systemName is {}, systemType is " +
                    "{}," +
                    " entityType is {}.", validSystemTypes, customerSpace, systemName, systemType, entityType);
            return false;
        }
        S3ImportSystem importSystem = s3ImportSystemService.getS3ImportSystem(customerSpace,
                systemName);
        log.info("importSystem is {}. ", JsonUtils.serialize(importSystem));
        if (importSystem == null) {
            log.error("import system is null when validate Marketing. customerSpace is {}.", customerSpace);
            return false;
        }
        return StringUtils.isNotEmpty(importSystem.getContactSystemId()) || isDefaultSystemInGATenant(importSystem);
    }

    @Override
    public boolean createDefaultOpportunityTemplate(String customerSpace, String systemName) {
        log.info("setup opportunity data for tenant {}, systemName {}.", customerSpace, systemName);
        DataFeedTask opportunityDataFeedTask = createOpportunityTemplateOnly(customerSpace, systemName,
                EntityType.Opportunity,
                null);
        log.info("opportunity dataFeedTask unique id is {}.", opportunityDataFeedTask.getUniqueId());
        DataFeedTask stageDataFeedTask = createOpportunityTemplateOnly(customerSpace, systemName,
                EntityType.OpportunityStageName, null);
        log.info("Stage dataFeedTask UniqueId is {}.", stageDataFeedTask.getUniqueId());
        String opportunityAtlasStreamName = ActivityStoreUtils.getStreamName(systemName, EntityType.Opportunity);
        createOpportunityMetadata(customerSpace, opportunityAtlasStreamName, opportunityDataFeedTask, stageDataFeedTask);
        return true;
    }

    @Override
    public boolean createOpportunityTemplate(String customerSpace, String systemName, EntityType entityType,
                                                    SimpleTemplateMetadata simpleTemplateMetadata) {
        if (!EntityType.Opportunity.equals(entityType)) {
            throw new IllegalArgumentException(String.format("createOpportunityTemplate cannot support entityType %s" +
                    ".", entityType));
        }
        log.info("setup opportunity data for tenant {}, systemName {}, SimpleTemplateMetadata {}.", customerSpace,
                systemName, JsonUtils.serialize(simpleTemplateMetadata));
        DataFeedTask opportunityDataFeedTask = createOpportunityTemplateOnly(customerSpace, systemName,
                EntityType.Opportunity, simpleTemplateMetadata);
        log.info("opportunity dataFeedTask unique id is {}.", opportunityDataFeedTask.getUniqueId());
        DataFeedTask stageDataFeedTask = createOpportunityTemplateOnly(customerSpace, systemName,
                EntityType.OpportunityStageName, null);
        log.info("Stage dataFeedTask UniqueId is {}.", stageDataFeedTask.getUniqueId());
        String opportunityAtlasStreamName = ActivityStoreUtils.getStreamName(systemName, entityType);
        createOpportunityMetadata(customerSpace, opportunityAtlasStreamName, opportunityDataFeedTask, stageDataFeedTask);
        return true;
    }

    @Override
    public boolean createDefaultMarketingTemplate(String customerSpace, String systemName, String systemType) {
        log.info("setup marketing data for tenant {}, systemName {}, use {} systemType spec ", customerSpace, systemName, systemType);
        DataFeedTask marketingDataFeedTask = createMarketingTemplateOnly(customerSpace, systemName, systemType,
                EntityType.MarketingActivity, null);
        log.info("marketing dataFeedTask unique id is {}.", marketingDataFeedTask.getUniqueId());
        DataFeedTask marketingTypeDataFeedTask = createMarketingTemplateOnly(customerSpace, systemName, systemType,
                EntityType.MarketingActivityType, null);
        log.info("MarketingType dataFeedTask UniqueId is {}.", marketingTypeDataFeedTask.getUniqueId());
        String marketingAtlasStreamName = ActivityStoreUtils.getStreamName(systemName, EntityType.MarketingActivity);
        createMarketingMetadata(customerSpace, marketingAtlasStreamName, marketingDataFeedTask, marketingTypeDataFeedTask);
        return true;
    }

    @Override
    public boolean createMarketingTemplate(String customerSpace, String systemName, String systemType, EntityType entityType, SimpleTemplateMetadata simpleTemplateMetadata) {
        if (!EntityType.MarketingActivity.equals(entityType)) {
            throw new IllegalArgumentException(String.format("createMarketingTemplate cannot support entityType %s" +
                    ".", entityType));
        }
        log.info("setup marketing data for tenant {}, systemName {}, use {} systemType spec, SimpleTemplateMetadata " +
                        "{}.", customerSpace, systemName, systemType, JsonUtils.serialize(simpleTemplateMetadata));
        DataFeedTask marketingDataFeedTask = createMarketingTemplateOnly(customerSpace, systemName, systemType,
                EntityType.MarketingActivity, simpleTemplateMetadata);
        log.info("marketing dataFeedTask unique id is {}.", marketingDataFeedTask.getUniqueId());
        DataFeedTask marketingTypeDataFeedTask = createMarketingTemplateOnly(customerSpace, systemName, systemType,
                EntityType.MarketingActivityType, null);
        log.info("MarketingType dataFeedTask UniqueId is {}.", marketingTypeDataFeedTask.getUniqueId());
        String marketingAtlasStreamName = ActivityStoreUtils.getStreamName(systemName, EntityType.MarketingActivity);
        createMarketingMetadata(customerSpace, marketingAtlasStreamName, marketingDataFeedTask, marketingTypeDataFeedTask);
        return true;
    }

    @Override
    public boolean createDefaultDnbIntentDataTemplate(String customerSpace, String systemDisplayName) {
        EntityType entityType = EntityType.CustomIntent;
        S3ImportSystem importSystem = setupSystems(customerSpace, entityType, S3ImportSystem.SystemType.DnbIntent,
                S3ImportSystem.SystemType.DnbIntent.getDefaultSystemName(), systemDisplayName);
        log.info("importSystem is {}.", JsonUtils.serialize(importSystem));
        log.info("setup dnb Intent data for tenant {}, systemName {}.", customerSpace, importSystem.getName());
        DataFeedTask intentDataTask = createDnbIntentDataTemplateOnly(customerSpace, importSystem.getName(),
                entityType, null);
        log.info("DnbIntentData dataFeedTask unique Id is {}.", intentDataTask.getUniqueId());
        String streamName = ActivityStoreUtils.getStreamName(importSystem.getName(), entityType);
        createDnbIntentMetadata(customerSpace, streamName, intentDataTask);
        return true;
    }

    @Override
    public boolean createDnbIntentDataTemplate(String customerSpace, EntityType entityType,
                                               SimpleTemplateMetadata simpleTemplateMetadata,
                                               String systemDisplayName) {
        if (!EntityType.CustomIntent.equals(entityType)) {
            throw new IllegalArgumentException(String.format("createDnbIntentDataTemplate cannot support entityType " +
                    "%s.", entityType));
        }
        S3ImportSystem importSystem = setupSystems(customerSpace, entityType, S3ImportSystem.SystemType.DnbIntent,
                S3ImportSystem.SystemType.DnbIntent.getDefaultSystemName(), systemDisplayName);
        log.info("setup dnb Intent data for tenant {}, systemName {}.", customerSpace, importSystem.getName());
        DataFeedTask intentDataTask = createDnbIntentDataTemplateOnly(customerSpace, importSystem.getName(),
                entityType, simpleTemplateMetadata);
        log.info("DnbIntentData dataFeedTask unique Id is {}.", intentDataTask.getUniqueId());
        String streamName = ActivityStoreUtils.getStreamName(importSystem.getName(), entityType);
        createDnbIntentMetadata(customerSpace, streamName, intentDataTask);
        return true;
    }

    @Override
    public boolean validateGAEnabled(String customerSpace, boolean enableGA) {
        CustomerSpace customerSpace1 = MultiTenantContext.getCustomerSpace();
        return (batonService.isEntityMatchEnabled(customerSpace1) && !batonService.onlyEntityMatchGAEnabled(customerSpace1)) || (enableGA && batonService.onlyEntityMatchGAEnabled(customerSpace1));
    }

    @Override
    public boolean resetTemplate(String customerSpace, String source, String feedType, Boolean forceDelete) {
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, source, feedType);
        if (dataFeedTask == null) {
            return false;
        }
        // 1. Check if there's Action already consumed by PA
        if (CollectionUtils.isNotEmpty(getPAConsumedActions(dataFeedTask.getUniqueId()))) {
            throw new LedpException(LedpCode.LEDP_40093);
        }

        // 2. Check if there's depending template
        S3ImportSystem importSystem = dataFeedTask.getImportSystem();
        if (importSystem == null) {
            importSystem = dataFeedTaskService.getImportSystemByTaskId(customerSpace, dataFeedTask.getUniqueId());
        }
        Set<String> systemIds = new HashSet<>();
        if (importSystem != null) {
            if (importSystem.getSystemType().getPrimaryAccount().equals(EntityTypeUtils.matchFeedType(feedType))) {
                if (StringUtils.isNotEmpty(importSystem.getAccountSystemId())) {
                    systemIds.add(importSystem.getAccountSystemId());
                }
            }
            if (importSystem.getSystemType().getPrimaryContact().equals(EntityTypeUtils.matchFeedType(feedType))) {
                if (StringUtils.isNotEmpty(importSystem.getContactSystemId())) {
                    systemIds.add(importSystem.getContactSystemId());
                }
            }
        }
        log.info("Get depending template based on id set: " + StringUtils.join(systemIds, ","));
        List<String> dependingTemplates = getDependingTemplate(customerSpace, dataFeedTask.getUniqueId(), systemIds);
        if (CollectionUtils.isNotEmpty(dependingTemplates)) {
            throw new LedpException(LedpCode.LEDP_40089, new String[]{StringUtils.join(dependingTemplates, ",")});
        }

        // 3. try delete active actions
        List<Long> actionPidList =
                actionService.findPidWithoutOwnerByTypeAndStatusAndConfig(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW,
                        ActionStatus.ACTIVE, dataFeedTask.getUniqueId());
        if (CollectionUtils.isNotEmpty(actionPidList)) {
            if (Boolean.TRUE.equals(forceDelete)) {
                actionPidList.forEach(actionPid -> actionService.delete(actionPid));
            } else {
                throw new LedpException(LedpCode.LEDP_40090);
            }
        }
        // remove cancelled as well
        actionPidList = actionService.findPidWithoutOwnerByTypeAndStatusAndConfig(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW,
                        ActionStatus.CANCELED, dataFeedTask.getUniqueId());
        if (CollectionUtils.isNotEmpty(actionPidList)) {
            actionPidList.forEach(actionPid -> actionService.delete(actionPid));
        }
        log.info(String.format("Delete template %s, template table name: %s", dataFeedTask.getUniqueId(),
                dataFeedTask.getImportTemplate().getName()));
        metadataProxy.deleteImportTable(customerSpace, dataFeedTask.getImportTemplate().getName());
        // last step: reset Import System MapToLattice flag:
        if (importSystem != null) {
            if (importSystem.getSystemType().getPrimaryAccount().equals(EntityTypeUtils.matchFeedType(feedType))) {
                importSystem.setMapToLatticeAccount(false);
                s3ImportSystemService.updateS3ImportSystem(customerSpace, importSystem);
            } else if (importSystem.getSystemType().getPrimaryContact().equals(EntityTypeUtils.matchFeedType(feedType))) {
                importSystem.setMapToLatticeContact(false);
                s3ImportSystemService.updateS3ImportSystem(customerSpace, importSystem);
            }
        }
        return true;
    }

    @Override
    public boolean hasPAConsumedImportAction(String customerSpace, String taskUniqueId) {
        return CollectionUtils.isNotEmpty(getPAConsumedActions(taskUniqueId));
    }

    @Override
    public boolean hasPAConsumedImportAction(String customerSpace, String source, String feedType) {
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, source, feedType);
        if (dataFeedTask != null) {
            return CollectionUtils.isNotEmpty(getPAConsumedActions(dataFeedTask.getUniqueId()));
        }
        return false;
    }

    @Override
    public List<String> getPAConsumedTemplates(String customerSpace) {
        List<ActionConfiguration> allConfigs = actionService.findConfigByTypeAndOwnerType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW,
                ProcessAnalyzeWorkflowConfiguration.WORKFLOW_NAME);
        Set<String> templateUUIDs = new HashSet<>();
        if (CollectionUtils.isNotEmpty(allConfigs)) {
            allConfigs.forEach(config -> {
                if (config instanceof ImportActionConfiguration) {
                    templateUUIDs.add(((ImportActionConfiguration) config).getDataFeedTaskId());
                }
            });
        }
        return new ArrayList<>(templateUUIDs);
    }

    @Override
    public void addAttributeLengthValidator(String customerSpace, String uniqueTaskId, String attrName, Integer length,
                                            boolean nullable) {
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, uniqueTaskId);
        if (dataFeedTask == null) {
            throw new IllegalArgumentException("Cannot find data feed task with uniqueId " + uniqueTaskId);
        }
        if (dataFeedTask.getDataFeedTaskConfig() == null) {
            DataFeedTaskConfig config = new DataFeedTaskConfig();
            config.addTemplateValidator(getAttributeLengthValidator(attrName, length, nullable));
            dataFeedTask.setDataFeedTaskConfig(config);

        } else {
            if (CollectionUtils.isNotEmpty(dataFeedTask.getDataFeedTaskConfig().getTemplateValidators())) {
                for (TemplateValidator validator : dataFeedTask.getDataFeedTaskConfig().getTemplateValidators()) {
                    if (validator instanceof AttributeLengthValidator) {
                        AttributeLengthValidator lengthValidator = (AttributeLengthValidator) validator;
                        if (lengthValidator.getAttributeName().equalsIgnoreCase(attrName)) {
                            throw new IllegalArgumentException("Already set length validator for attribute " + attrName);
                        }
                    }
                }
            }
            dataFeedTask.getDataFeedTaskConfig().addTemplateValidator(getAttributeLengthValidator(attrName, length, nullable));
        }
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask, true);
    }

    @Override
    public void updateAttributeLengthValidator(String customerSpace, String uniqueTaskId, String attrName, Integer length,
                                        boolean nullable) {
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, uniqueTaskId);
        if (dataFeedTask == null) {
            throw new IllegalArgumentException("Cannot find data feed task with uniqueId " + uniqueTaskId);
        }
        if (dataFeedTask.getDataFeedTaskConfig() == null) {
            DataFeedTaskConfig config = new DataFeedTaskConfig();
            config.addTemplateValidator(getAttributeLengthValidator(attrName, length, nullable));
            dataFeedTask.setDataFeedTaskConfig(config);

        } else {
            if (CollectionUtils.isNotEmpty(dataFeedTask.getDataFeedTaskConfig().getTemplateValidators())) {
                for (TemplateValidator validator : dataFeedTask.getDataFeedTaskConfig().getTemplateValidators()) {
                    if (validator instanceof AttributeLengthValidator) {
                        AttributeLengthValidator lengthValidator = (AttributeLengthValidator) validator;
                        if (lengthValidator.getAttributeName().equalsIgnoreCase(attrName)) {
                            lengthValidator.setLength(length);
                            lengthValidator.setNullable(nullable);
                            dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask, true);
                            return;
                        }
                    }
                }
            }
            dataFeedTask.getDataFeedTaskConfig().addTemplateValidator(getAttributeLengthValidator(attrName, length, nullable));
        }
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask, true);
    }

    @Override
    public void addSimpleValueFilter(String customerSpace, String uniqueTaskId, SimpleValueFilter simpleValueFilter) {
        Preconditions.checkNotNull(simpleValueFilter);
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace, uniqueTaskId);
        if (dataFeedTask == null) {
            throw new IllegalArgumentException("Cannot find data feed task with uniqueId " + uniqueTaskId);
        }
        if (dataFeedTask.getDataFeedTaskConfig() == null) {
            DataFeedTaskConfig config = new DataFeedTaskConfig();
            config.addTemplateValidator(simpleValueFilter);
            dataFeedTask.setDataFeedTaskConfig(config);
        } else {
            dataFeedTask.getDataFeedTaskConfig().addTemplateValidator(simpleValueFilter);
        }
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask, true);
    }

    private AttributeLengthValidator getAttributeLengthValidator(String attrName, Integer length, boolean nullable) {
        AttributeLengthValidator lengthValidator = new AttributeLengthValidator();
        lengthValidator.setAttributeName(attrName);
        lengthValidator.setLength(length);
        lengthValidator.setNullable(nullable);
        return lengthValidator;
    }

    private List<String> getDependingTemplate(String customerSpace, String uniqueTaskId, Set<String> systemIdSet) {
        if (CollectionUtils.isEmpty(systemIdSet)) {
            return Collections.emptyList();
        }
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
        if (dataFeed == null || CollectionUtils.isEmpty(dataFeed.getTasks())) {
            return Collections.emptyList();
        }
        List<String> dependingTemplate = new ArrayList<>();
        for (DataFeedTask task: dataFeed.getTasks()) {
            if (task.getUniqueId().equals(uniqueTaskId)) {
                continue;
            }
            Set<String> attrSet = task.getImportTemplate().getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());
            if (!Collections.disjoint(attrSet, systemIdSet)) {
                dependingTemplate.add(task.getTemplateDisplayName());
            }
        }
        return dependingTemplate;
    }

    private List<Long> getPAConsumedActions(String uniqueTaskId) {
        return actionService.findPidByTypeAndConfigAndOwnerType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW,
                ProcessAnalyzeWorkflowConfiguration.WORKFLOW_NAME, uniqueTaskId);
    }

    private DataFeedTask createOpportunityTemplateOnly(String customerSpace, String systemName, EntityType entityType,
                                                       SimpleTemplateMetadata simpleTemplateMetadata) {
        S3ImportSystem importSystem = s3ImportSystemService.getS3ImportSystem(customerSpace,
                systemName);
        if (importSystem == null) {
            throw new IllegalStateException(String.format("S3ImportSystem cannot be null, systemName is %s," +
                    " tenant %s.", systemName, customerSpace));
        }
        createDropFolder(customerSpace, systemName, entityType);
        ImportWorkflowSpec spec;
        try {
            String fileSystemType = "allsystem";
            spec = importWorkflowSpecService.loadSpecFromS3(fileSystemType, entityType.getDisplayName());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Could not create template for tenant %s, system type %s, and system object %s " +
                                    "because the Spec failed to load", customerSpace, importSystem.getSystemType().name(),
                            entityType.getDisplayName()), e);
        }
        log.info("entityType is {}", entityType);
        if (EntityType.Opportunity.equals(entityType)) {
            processMatchAccountId(importSystem, spec);
        }
        Table standardTable = importWorkflowSpecService.tableFromRecord(null, true, spec);

        return setupDataFeedTask(customerSpace, simpleTemplateMetadata, entityType, importSystem,
                standardTable);
    }

    private DataFeedTask createMarketingTemplateOnly(String customerSpace, String systemName, String systemType,
                                                     EntityType entityType,
                                                     SimpleTemplateMetadata simpleTemplateMetadata) {
        S3ImportSystem importSystem = s3ImportSystemService.getS3ImportSystem(customerSpace,
                systemName);
        if (importSystem == null) {
            throw new IllegalStateException(String.format("S3ImportSystem cannot be null, systemName is %s," +
                    " tenant %s.", systemName, customerSpace));
        }
        createDropFolder(customerSpace, systemName, entityType);
        ImportWorkflowSpec spec;
        try {
            String filesystemType = systemType;
            if (EntityType.MarketingActivityType.equals(entityType)) {
                filesystemType = "allsystem";
            }
            spec = importWorkflowSpecService.loadSpecFromS3(filesystemType, entityType.getDisplayName());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Could not create template for tenant %s, system type %s, and system object %s " +
                                    "because the Spec failed to load", customerSpace, importSystem.getSystemType().name(),
                            entityType.getDisplayName()), e);
        }
        log.info("entityType is {}", entityType);
        if (EntityType.MarketingActivity.equals(entityType)) {
            processMatchContactId(importSystem, spec);
        }
        Table standardTable = importWorkflowSpecService.tableFromRecord(null, true, spec);

        return setupDataFeedTask(customerSpace, simpleTemplateMetadata, entityType, importSystem,
                standardTable, systemType);
    }

    private DataFeedTask createDnbIntentDataTemplateOnly(String customerSpace, String systemName, EntityType entityType,
                                                       SimpleTemplateMetadata simpleTemplateMetadata) {
        S3ImportSystem importSystem = s3ImportSystemService.getS3ImportSystem(customerSpace,
                systemName);
        if (importSystem == null) {
            throw new IllegalStateException(String.format("S3ImportSystem cannot be null, systemName is %s," +
                    " tenant %s.", systemName, customerSpace));
        }
        createDropFolder(customerSpace, systemName, entityType);
        ImportWorkflowSpec spec;
        try {
            String fileSystemType = "allsystem";
            spec = importWorkflowSpecService.loadSpecFromS3(fileSystemType, entityType.getDisplayName());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Could not create template for tenant %s, system type %s, and system object %s " +
                                    "because the Spec failed to load", customerSpace, importSystem.getSystemType().name(),
                            entityType.getDisplayName()), e);
        }
        log.info("entityType is {}", entityType);
        Table standardTable = importWorkflowSpecService.tableFromRecord(null, true, spec);

        return setupDataFeedTask(customerSpace, simpleTemplateMetadata, entityType, importSystem,
                standardTable);
    }

    private void attachDimensionCatalog(@NotNull Tenant tenant, String dimensionName, Catalog catalog) {
        if (catalog == null) {
            return;
        }
        AtlasStream stream = streamEntityMgr.findByNameAndTenant(EntityType.WebVisit.name(), tenant);
        if (stream == null) {
            log.info("No WebVisit activity stream created for tenant {} yet, ignore attaching path pattern catalog",
                    tenant);
            return;
        }

        StreamDimension pathPatternDimension = dimensionEntityMgr
                .findByNameAndTenantAndStream(dimensionName, tenant, stream);
        Preconditions.checkNotNull(pathPatternDimension,
                String.format("Must have dimension %s created with WebVisit stream. Tenant=%s, Stream=%s",
                        dimensionName, tenant.getId(), stream.getPid()));

        pathPatternDimension.setCatalog(catalog);
        dimensionEntityMgr.update(pathPatternDimension);
        log.info("Attach {} catalog {} to WebVisit stream = {}, {} dimension = {}", dimensionName, catalog,
                stream.getPid(), dimensionName, pathPatternDimension.getPid());
    }

    private Table generateTemplate(Table standardTable, SimpleTemplateMetadata simpleTemplateMetadata) {
        Preconditions.checkNotNull(standardTable);
        if (simpleTemplateMetadata == null) {
            return standardTable;
        }
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getIgnoredStandardAttributes())) {
            standardTable.getAttributes()
                    .removeIf(attribute -> !Boolean.TRUE.equals(attribute.getRequired()) &&
                            simpleTemplateMetadata.getIgnoredStandardAttributes().contains(attribute.getName()));
        }
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getStandardAttributes())) {
            configStandardAttributes(standardTable, simpleTemplateMetadata);
        }
        standardTable.getAttributes().forEach(attribute -> {
            if (StringUtils.isEmpty(attribute.getDisplayName())) {
                attribute.setDisplayName(attribute.getName());
                attribute.setSourceAttrName(attribute.getName());
            }
        });
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getCustomerAttributes())) {
            standardTable.addAttributes(generateCustomerAttributes(simpleTemplateMetadata.getCustomerAttributes()));
        }
        return standardTable;
    }

    private void configStandardAttributes(Table standardTable, SimpleTemplateMetadata simpleTemplateMetadata) {
        Map<String, SimpleTemplateMetadata.SimpleTemplateAttribute> nameMapping =
                simpleTemplateMetadata.getStandardAttributes().stream()
                        .filter(simpleTemplateAttribute -> StringUtils.isNotBlank(simpleTemplateAttribute.getName()))
                        .collect(Collectors.toMap(SimpleTemplateMetadata.SimpleTemplateAttribute::getName,
                                simpleTemplateAttribute -> simpleTemplateAttribute));
        standardTable.getAttributes().forEach(attribute -> {
            if (nameMapping.containsKey(attribute.getName())) {
                attribute.setDisplayName(nameMapping.get(attribute.getName()).getDisplayName());
                attribute.setSourceAttrName(nameMapping.get(attribute.getName()).getDisplayName());
                if (LogicalDataType.Date.equals(attribute.getLogicalDataType())) {
                    if (StringUtils.isNotEmpty(nameMapping.get(attribute.getName()).getDateFormat())) {
                        attribute.setDateFormatString(nameMapping.get(attribute.getName()).getDateFormat());
                    }
                    if (StringUtils.isNotEmpty(nameMapping.get(attribute.getName()).getTimeFormat())) {
                        attribute.setTimeFormatString(nameMapping.get(attribute.getName()).getTimeFormat());
                    }
                    if (StringUtils.isNotEmpty(nameMapping.get(attribute.getName()).getTimezone())) {
                        attribute.setTimezone(nameMapping.get(attribute.getName()).getTimezone());
                    }
                }
            }
        });
    }

    private List<Attribute> generateCustomerAttributes(List<SimpleTemplateMetadata.SimpleTemplateAttribute> simpleCustomerAttrs) {
        List<Attribute> customerAttributes = new ArrayList<>();
        for (SimpleTemplateMetadata.SimpleTemplateAttribute simpleTemplateAttr : simpleCustomerAttrs) {
            Attribute attribute = new Attribute();
            attribute.setDisplayName(simpleTemplateAttr.getDisplayName());
            attribute.setNullable(Boolean.TRUE);
            attribute.setName(getCustomerAttributeName(simpleTemplateAttr.getName(), simpleTemplateAttr.getDisplayName()));
            attribute.setPhysicalDataType(simpleTemplateAttr.getPhysicalDataType().name());
            FundamentalType fundamentalType = getFundamentalType(simpleTemplateAttr.getFundamentalType(),
                    simpleTemplateAttr.getPhysicalDataType());
            attribute.setFundamentalType(fundamentalType);
            attribute.setLogicalDataType(simpleTemplateAttr.getLogicalDataType());
            if (LogicalDataType.Date.equals(attribute.getLogicalDataType())) {
                if (StringUtils.isNotEmpty(simpleTemplateAttr.getDateFormat())) {
                    attribute.setDateFormatString(simpleTemplateAttr.getDateFormat());
                }
                if (StringUtils.isNotEmpty(simpleTemplateAttr.getTimeFormat())) {
                    attribute.setTimeFormatString(simpleTemplateAttr.getTimeFormat());
                }
                if (StringUtils.isNotEmpty(simpleTemplateAttr.getTimezone())) {
                    attribute.setTimezone(simpleTemplateAttr.getTimezone());
                }
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
                // Ignore
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

    private S3ImportSystem createS3ImportSystem(String customerSpace, String systemName, String systemDisplayName,
                                                S3ImportSystem.SystemType systemType) {
        S3ImportSystem s3ImportSystem = new S3ImportSystem();
        s3ImportSystem.setSystemType(systemType);
        s3ImportSystem.setName(systemName);
        s3ImportSystem.setDisplayName(StringUtils.isBlank(systemDisplayName)? systemName : systemDisplayName);
        s3ImportSystem.setTenant(tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString()));
        s3ImportSystemService.createS3ImportSystem(customerSpace, s3ImportSystem);
        dropBoxService.createFolder(customerSpace, systemName, null, null);
        return s3ImportSystemService.getS3ImportSystem(customerSpace, systemName);
    }

    private void createOpportunityMetadata(String customerSpace, String opportunityAtlasStreamName,
                                        DataFeedTask opportunityDataFeedTask, DataFeedTask stageDataFeedTask) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        AtlasStream opportunityAtlasStream =
                new AtlasStream.Builder().withTenant(tenant).withDataFeedTask(opportunityDataFeedTask).withStreamType(AtlasStream.StreamType.Opportunity)
                        .withName(opportunityAtlasStreamName).withMatchEntities(Collections.singletonList(BusinessEntity.Account.name()))
                        .withAggrEntities(Collections.singletonList(BusinessEntity.Account.name())).withDateAttribute(InterfaceName.LastModifiedDate.name())
                        .withPeriods(Collections.singletonList(PeriodStrategy.Template.Week.name())).withRetentionDays(365).withReducer(prepareReducer()).build();
        opportunityAtlasStream.setStreamId(AtlasStream.generateId());
        streamEntityMgr.create(opportunityAtlasStream);
        log.info("opportunityAtlasStream is {}.", JsonUtils.serialize(opportunityAtlasStream));
        Catalog stageCatalog = createCatalog(tenant, opportunityAtlasStreamName, stageDataFeedTask);
        catalogEntityMgr.create(stageCatalog);
        log.info("stageCatalog is {}.", JsonUtils.serialize(stageCatalog));
        StreamDimension dimension = createStageDimension(opportunityAtlasStream, stageCatalog);
        dimensionEntityMgr.create(dimension);
        log.info("dimension is {}.", JsonUtils.serialize(dimension));
        ActivityMetricsGroup defaultGroup = activityMetricsGroupService.setUpDefaultOpportunityGroup(tenant.getId(),
                opportunityAtlasStream.getName());
        if (defaultGroup == null) {
            throw new IllegalStateException(String.format(
                    "Failed to setup Opportunity metric groups for tenant %s", customerSpace));
        }
    }

    private void createMarketingMetadata(String customerSpace, String marketingAtlasStreamName,
                                         DataFeedTask marketingDataFeedTask, DataFeedTask marketingTypeDataFeedTask) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        AtlasStream marketingAtlasStream =
                new AtlasStream.Builder().withTenant(tenant).withDataFeedTask(marketingDataFeedTask).withStreamType(AtlasStream.StreamType.MarketingActivity)
                        .withName(marketingAtlasStreamName).withMatchEntities(Collections.singletonList(BusinessEntity.Contact.name()))
                        .withAggrEntities(Arrays.asList(BusinessEntity.Contact.name(),
                                BusinessEntity.Account.name())).withDateAttribute(InterfaceName.ActivityDate.name())
                        .withPeriods(Collections.singletonList(PeriodStrategy.Template.Week.name())).withRetentionDays(365).build();
        marketingAtlasStream.setStreamId(AtlasStream.generateId());
        streamEntityMgr.create(marketingAtlasStream);
        log.info("marketingAtlasStream is {}.", JsonUtils.serialize(marketingAtlasStream));
        Catalog marketingTypeCatalog = createCatalog(tenant, marketingAtlasStreamName, marketingTypeDataFeedTask);
        catalogEntityMgr.create(marketingTypeCatalog);
        log.info("marketingTypeCatalog is {}.", JsonUtils.serialize(marketingTypeCatalog));
        StreamDimension dimension = createActivityTypeDimension(marketingAtlasStream, marketingTypeCatalog);
        dimensionEntityMgr.create(dimension);
        log.info("dimension is {}.", JsonUtils.serialize(dimension));
        List<ActivityMetricsGroup> defaultGroups =
                activityMetricsGroupService.setupDefaultMarketingGroups(tenant.getId(),
                marketingAtlasStream.getName());
        if (CollectionUtils.isEmpty(defaultGroups)) {
            throw new IllegalStateException(String.format(
                    "Failed to setup marketing metric groups for tenant %s", customerSpace));
        }
    }

    private void createDnbIntentMetadata(String customerSpace, String streamName, DataFeedTask intentDataTask) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        AtlasStream stream = new AtlasStream.Builder().withTenant(tenant).withDataFeedTask(intentDataTask).withStreamType(AtlasStream.StreamType.DnbIntentData)
                .withName(streamName).withMatchEntities(Collections.singletonList(BusinessEntity.Account.name()))
                .withAggrEntities(Collections.singletonList(BusinessEntity.Account.name())).withDateAttribute(InterfaceName.LastModifiedDate.name())
                .withPeriods(Collections.singletonList(PeriodStrategy.Template.Week.name())).withReducer(prepareBuyingScoreReducer()).withRetentionDays(365).build();
        stream.setStreamId(AtlasStream.generateId());
        streamEntityMgr.create(stream);
        log.info("BuyingScore stream config: {}.", JsonUtils.serialize(stream));
        StreamDimension modelDimension = createDnbIntentDataModelDimension(stream);
        dimensionEntityMgr.create(modelDimension);
        log.info("DnbIntentData ModelDimension is {}.", JsonUtils.serialize(modelDimension));
        List<ActivityMetricsGroup> defaultGroups = activityMetricsGroupService.setupDefaultDnbIntentGroups(customerSpace, stream.getName());
        if (CollectionUtils.isEmpty(defaultGroups)) {
            throw new IllegalStateException(String.format("Failed to setup buying score metrics for tenant %s", customerSpace));
        }
    }

    private void createDropFolder(String customerSpace, String systemName, EntityType entityType) {
        List<String> allSubFolder = dropBoxService.getDropFoldersFromSystem(customerSpace, systemName);
        if (CollectionUtils.isEmpty(allSubFolder)) {
            throw new IllegalArgumentException(String.format("no subFolder, customerSpace is %s.", customerSpace));
        }
        String folderName = S3PathBuilder.getFolderName(systemName, entityType.getDefaultFeedTypeName());
        log.info("customerSpace : {}, systemName {}, entityType {}, allSubFolder is {}, want create folderName is {}.",
                customerSpace, systemName, entityType, allSubFolder, folderName);
        if (!allSubFolder.contains(folderName)) {
            dropBoxService.createSubFolder(customerSpace, systemName, entityType.getDefaultFeedTypeName(), null);
            log.info("create folder {} success.", folderName);
        }
    }

    private void updateLatticeId(boolean isMappedToLatticeId, String fieldName, String columnName,
                                        FieldDefinitionsRecord record) {
        if (isMappedToLatticeId) {
            FieldDefinition latticeIdDefinition = record.getFieldDefinition(LATTICE_IDS_SECTION, fieldName);
            if (latticeIdDefinition == null) {
                latticeIdDefinition = new FieldDefinition();
                latticeIdDefinition.setFieldName(fieldName);
                latticeIdDefinition.setFieldType(UserDefinedType.TEXT);
                latticeIdDefinition.setColumnName(columnName);
                record.addFieldDefinition(LATTICE_IDS_SECTION, latticeIdDefinition, false);

                log.info("Creating new Lattice ID field {} for {}.", fieldName, columnName);
            } else {
                latticeIdDefinition.setColumnName(columnName);
                log.info("Updating old Lattice ID field {} which columnName {}.",fieldName , columnName);
            }
        }
    }

    /*
     * map to system Account UniqueId
     * for entitymatchGA tenant, under DefaultSystem, if no systemAccountId, map to AccountSystemId.
     */
    private void processMatchAccountId(S3ImportSystem importSystem, FieldDefinitionsRecord record) {
        List<FieldDefinition> fieldDefinitionList = record.getFieldDefinitionsRecords(MATCH_TO_ACCOUNT_ID_SECTION);
        log.info("fieldDefinitionList is {}.", JsonUtils.serialize(fieldDefinitionList));
        if (CollectionUtils.isEmpty(fieldDefinitionList)) {
            return;
        }
        for (FieldDefinition matchIdDefinition : fieldDefinitionList) {

            // Only set the field name if it is blank, indicating this is the first time it is being updated.
            if (StringUtils.isBlank(matchIdDefinition.getFieldName()) && ACCOUNT_FIELD_NAME.equals(matchIdDefinition.getColumnName())) {
                if (isDefaultSystemInGATenant(importSystem) && StringUtils.isEmpty(importSystem.getAccountSystemId())) {
                    matchIdDefinition.setFieldName(InterfaceName.CustomerAccountId.name());
                } else {
                    if (StringUtils.isBlank(importSystem.getAccountSystemId())) {
                        throw new IllegalStateException("Cannot assign column " + matchIdDefinition.getColumnName() +
                                " ID from system " + importSystem.getName() +
                                " as match ID in section " + MATCH_TO_ACCOUNT_ID_SECTION + " before that system has been set up");
                    }
                    matchIdDefinition.setFieldName(importSystem.getAccountSystemId());
                }
            }

            log.info("State|  section: {}  defSystem: {}  isMappedtoAccount:  {}  " +
                            "isMappedToContact: {}  columnName: {}  fieldName: {}", MATCH_TO_ACCOUNT_ID_SECTION, importSystem.getName(), importSystem.isMapToLatticeAccount(),
                    importSystem.isMapToLatticeContact(), matchIdDefinition.getColumnName(),
                    matchIdDefinition.getFieldName());
            // map to global id.
            updateLatticeId(importSystem.isMapToLatticeAccount(), InterfaceName.CustomerAccountId.name(),
                    matchIdDefinition.getColumnName(), record);
        }
    }

    /*
     * map to system Contact UniqueId
     * for entitymatchGA tenant, under DefaultSystem, if no systemContactId, map to ContactSystemId.
     */
    private void processMatchContactId(S3ImportSystem importSystem, FieldDefinitionsRecord record) {
        List<FieldDefinition> fieldDefinitionList = record.getFieldDefinitionsRecords(MATCH_TO_CONTACT_ID_SECTION);
        log.info("fieldDefinitionList is {}.", JsonUtils.serialize(fieldDefinitionList));
        if (CollectionUtils.isEmpty(fieldDefinitionList)) {
            return;
        }
        for (FieldDefinition matchIdDefinition : fieldDefinitionList) {

            // Only set the field name if it is blank, indicating this is the first time it is being updated.
            if (StringUtils.isBlank(matchIdDefinition.getFieldName()) && CONTACT_FIELD_NAME.contains(matchIdDefinition.getColumnName())) {
                if (isDefaultSystemInGATenant(importSystem) && StringUtils.isEmpty(importSystem.getContactSystemId())) {
                    matchIdDefinition.setFieldName(InterfaceName.CustomerContactId.name());
                } else {
                    if (StringUtils.isBlank(importSystem.getContactSystemId())) {
                        throw new IllegalStateException(String.format("Cannot assign column %s ID from system %s as " +
                                        "match ID in section %s before that system has been set up.", matchIdDefinition.getColumnName(),
                                importSystem.getName(), MATCH_TO_CONTACT_ID_SECTION));
                    }
                    matchIdDefinition.setFieldName(importSystem.getContactSystemId());
                }
            }

            log.info("State|  section: {}  defSystem: {}  isMappedtoAccount:  {}  " +
                            "isMappedToContact: {}  columnName: {}  fieldName: {}", MATCH_TO_CONTACT_ID_SECTION, importSystem.getName(),
                    importSystem.isMapToLatticeAccount(),
                    importSystem.isMapToLatticeContact(), matchIdDefinition.getColumnName(),
                    matchIdDefinition.getFieldName());
            // map to global id.
            updateLatticeId(importSystem.isMapToLatticeContact(), InterfaceName.CustomerContactId.name(),
                    matchIdDefinition.getColumnName(), record);
        }
    }

    private Catalog createCatalog(Tenant tenant, String catalogName, DataFeedTask dataFeedTask) {
        Catalog catalog = new Catalog();
        catalog.setTenant(tenant);
        catalog.setName(catalogName);
        catalog.setCatalogId(Catalog.generateId());
        catalog.setDataFeedTask(dataFeedTask);
        return catalog;
    }

    private StreamDimension createStageDimension(@NotNull AtlasStream stream, Catalog catalog) {
        StreamDimension dim = new StreamDimension();
        dim.setName(StageNameId.name());
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.addUsages(StreamDimension.Usage.Pivot);
        dim.setCatalog(catalog);

        // hash
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.Name.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        dim.setGenerator(generator);

        DimensionCalculatorRegexMode calculator = new DimensionCalculatorRegexMode();
        calculator.setName(InterfaceName.StageName.name());
        calculator.setAttribute(InterfaceName.StageName.name());
        calculator.setPatternAttribute(InterfaceName.StageName.name());
        calculator.setPatternFromCatalog(true);
        dim.setCalculator(calculator);
        return dim;
    }

    private StreamDimension createActivityTypeDimension(@NotNull AtlasStream stream, Catalog catalog) {
        StreamDimension dim = new StreamDimension();
        dim.setName(ActivityTypeId.name());
        dim.setDisplayName(dim.getName());
        dim.setTenant(stream.getTenant());
        dim.setStream(stream);
        dim.setCatalog(catalog);
        dim.addUsages(StreamDimension.Usage.Pivot);

        // standardize and hash ptn name for dimension
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.Name.name());
        generator.setFromCatalog(true);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        dim.setGenerator(generator);
        // use url attr in stream to determine whether it matches catalog pattern
        DimensionCalculatorRegexMode calculator = new DimensionCalculatorRegexMode();
        calculator.setName(InterfaceName.ActivityType.name());
        calculator.setAttribute(InterfaceName.ActivityType.name());
        calculator.setPatternAttribute(InterfaceName.ActivityType.name());
        calculator.setPatternFromCatalog(true);
        dim.setCalculator(calculator);
        return dim;
    }

    private StreamDimension createDnbIntentDataModelDimension(@NotNull AtlasStream stream) {
        StreamDimension modelDimension = new StreamDimension();
        modelDimension.setName(ModelNameId.name());
        modelDimension.setDisplayName(modelDimension.getName());
        modelDimension.setTenant(stream.getTenant());
        modelDimension.setStream(stream);
        modelDimension.addUsages(StreamDimension.Usage.Pivot);
        modelDimension.setShouldReplace(false);

        DimensionGenerator generator = new DimensionGenerator();
        generator.setFromCatalog(false);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.HASH);
        generator.setAttribute(InterfaceName.ModelName.name());
        modelDimension.setGenerator(generator);

        DimensionCalculator calculator = new DimensionCalculator();
        calculator.setAttribute(InterfaceName.ModelName.name());
        calculator.setName(InterfaceName.ModelName.name());
        modelDimension.setCalculator(calculator);
        return modelDimension;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(InterfaceName.OpportunityId.name()));
        reducer.setArguments(Collections.singletonList(InterfaceName.LastModifiedDate.name()));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private ActivityRowReducer prepareBuyingScoreReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Arrays.asList(AccountId.name(), ModelName.name()));
        reducer.setArguments(Collections.singletonList(LastModifiedDate.name()));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private boolean isDefaultSystemInGATenant(S3ImportSystem importSystem) {
        return batonService.onlyEntityMatchGAEnabled(MultiTenantContext.getCustomerSpace())
                 && DEFAULTSYSTEM.equals(importSystem.getName());
    }
}
