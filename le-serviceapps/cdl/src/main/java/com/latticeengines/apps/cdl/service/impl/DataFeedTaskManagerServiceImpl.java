package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.CDLConstants.DEFAULT_S3_USER;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.service.DLTenantMappingService;
import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.S3ImportFolderService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.util.DiagnoseTable;
import com.latticeengines.apps.cdl.workflow.CDLDataFeedImportWorkflowSubmitter;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.PrepareImportConfiguration;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("dataFeedTaskManagerService")
public class DataFeedTaskManagerServiceImpl implements DataFeedTaskManagerService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskManagerServiceImpl.class);

    private final TenantService tenantService;

    private final DLTenantMappingService dlTenantMappingService;

    private final CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter;

    private final CDLExternalSystemService cdlExternalSystemService;

    private final ActionService actionService;

    private final MetadataProxy metadataProxy;

    private final AttrConfigEntityMgr attrConfigEntityMgr;

    private final S3Service s3Service;

    private final S3ImportFolderService s3ImportFolderService;

    @Inject
    private DropBoxService dropBoxService;

    @Value("${cdl.dataloader.tenant.mapping.enabled:false}")
    private boolean dlTenantMappingEnabled;

    @Value("${common.microservice.url}")
    private String hostUrl;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private S3ImportService s3ImportService;

    @Inject
    private BatonService batonService;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    public DataFeedTaskManagerServiceImpl(CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter, TenantService tenantService,
                                          DLTenantMappingService dlTenantMappingService, CDLExternalSystemService cdlExternalSystemService,
                                          ActionService actionService, MetadataProxy metadataProxy, AttrConfigEntityMgr attrConfigEntityMgr,
                                          S3Service s3Service, S3ImportFolderService s3ImportFolderService) {
        this.cdlDataFeedImportWorkflowSubmitter = cdlDataFeedImportWorkflowSubmitter;
        this.tenantService = tenantService;
        this.dlTenantMappingService = dlTenantMappingService;
        this.cdlExternalSystemService = cdlExternalSystemService;
        this.actionService = actionService;
        this.metadataProxy = metadataProxy;
        this.attrConfigEntityMgr = attrConfigEntityMgr;
        this.s3Service = s3Service;
        this.s3ImportFolderService = s3ImportFolderService;
    }

    private String formatFolder(String folder) {
        if (StringUtils.isNotEmpty(folder)) {
            if (folder.startsWith("/")) {
                folder = folder.substring(1);
            }
            if (folder.endsWith("/")) {
                folder = folder.substring(0, folder.length() - 1);
            }
        }
        return folder;
    }

    @Override
    public synchronized String createDataFeedTask(String customerSpaceStr, String feedType, String entity,
                                                  String source, String subType, String templateDisplayName,
                                                  boolean sendEmail, String user, CDLImportConfig importConfig) {
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(source);
        CustomerSpace customerSpace = dataFeedMetadataService.getCustomerSpace(importConfig);
        if (dlTenantMappingEnabled) {
            log.info("DL tenant mapping is enabled");
            customerSpace = mapCustomerSpace(customerSpace);
        }
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        MultiTenantContext.setTenant(tenant);
        Pair<Table, List<AttrConfig>> metadataPair = dataFeedMetadataService.getMetadata(importConfig, entity);
        Table newMeta = metadataPair.getLeft();
        List<AttrConfig> attrConfigs = metadataPair.getRight();
        boolean withoutId = batonService.isEnabled(customerSpace, LatticeFeatureFlag.IMPORT_WITHOUT_ID);
        boolean enableEntityMatch = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH);
        boolean enableEntityMatchGA = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA);
        Table schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.valueOf(entity), true, withoutId,
                batonService.isEntityMatchEnabled(customerSpace));

        newMeta = dataFeedMetadataService.resolveMetadata(newMeta, schemaTable);
        setCategoryForTable(newMeta, entity);
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask != null) {
            dataFeedMetadataService.applyAttributePrefix(cdlExternalSystemService, customerSpace.toString(), newMeta,
                    schemaTable, dataFeedTask.getImportTemplate());
            crosscheckDataType(customerSpace, entity, source, newMeta, dataFeedTask.getUniqueId());
            Table originMeta = dataFeedTask.getImportTemplate();
            DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace.toString());
            if (!dataFeedMetadataService.compareMetadata(originMeta, newMeta,
                    !dataFeed.getStatus().equals(DataFeed.Status.Initing))) {
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);
                Table finalTemplate = mergeTable(originMeta, newMeta);
                if (!finalSchemaCheck(finalTemplate, entity, withoutId, batonService.isEntityMatchEnabled(customerSpace))) {
                    throw new RuntimeException("The final import template is invalid, please check import settings!");
                }
                dataFeedTask.setImportTemplate(finalTemplate);
                dataFeedTaskService.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
                updateAttrConfig(finalTemplate, attrConfigs, entity, customerSpace);
                if (sendEmail) {
                    sendS3TemplateChangeEmail(customerSpace.toString(), dataFeedTask, user, false);
                }
            }
            dataFeedMetadataService.autoSetCDLExternalSystem(cdlExternalSystemService, newMeta,
                    customerSpace.toString());
            return dataFeedTask.getUniqueId();
        } else {
            dataFeedMetadataService.applyAttributePrefix(cdlExternalSystemService, customerSpace.toString(), newMeta,
                    schemaTable, null);
            crosscheckDataType(customerSpace, entity, source, newMeta, "");
            if (!finalSchemaCheck(newMeta, entity, withoutId, batonService.isEntityMatchEnabled(customerSpace))) {
                throw new RuntimeException("The final import template is invalid, please check import settings!");
            }
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedTask.setImportTemplate(newMeta);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity);
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(source);
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTask.setSubType(subType);
            if (StringUtils.isNotBlank(templateDisplayName)) {
                dataFeedTask.setTemplateDisplayName(templateDisplayName);
            } else {
                dataFeedTask.setTemplateDisplayName(feedType);
            }
            dataFeedTaskService.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            if (StringUtils.isEmpty(S3PathBuilder.getSystemNameFromFeedType(feedType))) {
                String objectName = S3PathBuilder.getFolderNameFromFeedType(feedType);
                dropBoxService.createFolder(customerSpace.toString(), null, formatFolder(objectName), "");
            }
            updateAttrConfig(newMeta, attrConfigs, entity, customerSpace);
            if (dataFeedMetadataService.needUpdateDataFeedStatus()) {
                DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace.toString());
                if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
                    dataFeedService.updateDataFeed(customerSpace.toString(), "", DataFeed.Status.Initialized.getName());
                }
            }
            if (sendEmail) {
                sendS3TemplateChangeEmail(customerSpace.toString(), dataFeedTask, user, true);
            }
            dataFeedMetadataService.autoSetCDLExternalSystem(cdlExternalSystemService, newMeta,
                    customerSpace.toString());
            return dataFeedTask.getUniqueId();
        }
    }

    private void setCategoryForTable(Table table, String entity) {
        BusinessEntity businessEntity = BusinessEntity.valueOf(entity);
        String category;
        switch (businessEntity) {
        case Account:
            category = Category.ACCOUNT_ATTRIBUTES.name();
            break;
        case Contact:
            category = Category.CONTACT_ATTRIBUTES.name();
            break;
        // todo other entity
        default:
            category = Category.DEFAULT.getName();
        }
        for (Attribute attr : table.getAttributes()) {
            attr.setCategory(category);
        }
    }

    @Override
    public String submitImportJob(String customerSpaceStr, String taskIdentifier, CDLImportConfig importConfig) {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpaceStr);
        if (dlTenantMappingEnabled) {
            customerSpace = mapCustomerSpace(customerSpace);
        }
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        DataFeedTask dataFeedTask = getDataFeedTask(tenant, customerSpace, taskIdentifier);
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(dataFeedTask.getSource());
        String connectorConfig = dataFeedMetadataService.getConnectorConfig(importConfig, dataFeedTask.getUniqueId());
        CSVImportFileInfo csvImportFileInfo = dataFeedMetadataService.getImportFileInfo(importConfig);
        log.info(String.format("csvImportFileInfo=%s", csvImportFileInfo));
        if (csvImportFileInfo.isPartialFile()) {
            s3ImportService.saveImportMessage(csvImportFileInfo.getS3Bucket(),
                    csvImportFileInfo.getS3Path(), hostUrl);
            return null;
        } else {
            ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask, connectorConfig,
                    csvImportFileInfo, null, false, null, new WorkflowPidWrapper(-1L));
            return appId.toString();
        }
    }

    private DataFeedTask getDataFeedTask(Tenant tenant, CustomerSpace customerSpace, String taskIdentifier) {
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        MultiTenantContext.setTenant(tenant);
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace.toString(), taskIdentifier);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the data feed task!");
        }
        return dataFeedTask;
    }

    @Override
    public String submitDataOnlyImportJob(String customerSpaceStr, String taskIdentifier, CSVImportConfig importConfig) {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpaceStr);
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        DataFeedTask dataFeedTask = getDataFeedTask(tenant, customerSpace, taskIdentifier);
        S3ImportEmailInfo emailInfo = generateEmailInfo(customerSpace.toString(),
                importConfig.getCSVImportFileInfo().getReportFileDisplayName(), dataFeedTask, new Date());
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(dataFeedTask.getSource());
        String connectorConfig = dataFeedMetadataService.getConnectorConfig(importConfig, dataFeedTask.getUniqueId());
        CSVImportFileInfo csvImportFileInfo = dataFeedMetadataService.getImportFileInfo(importConfig);
        log.info(String.format("csvImportFileInfo=%s", csvImportFileInfo));
        ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask, connectorConfig,
                csvImportFileInfo, null, false, null, new WorkflowPidWrapper(-1L));
        TenantEmailNotificationLevel notificationLevel = tenant.getNotificationLevel();
        log.info("tenant " + tenant.getId() + " notification_level is: " + tenant.getNotificationLevel().name());
        if (notificationLevel.compareTo(TenantEmailNotificationLevel.INFO) >= 0) {
            sendS3ImportEmail(customerSpace.toString(), "In_Progress", emailInfo);
        }
        return appId.toString();
    }

    @Override
    public boolean resetImport(String customerSpaceStr, BusinessEntity entity) {
        List<DataFeedTask> dfTasks = getAllDataFeedTask(customerSpaceStr, entity);
        Set<String> taskIds = dfTasks.stream().map(DataFeedTask::getUniqueId).collect(Collectors.toSet());
        List<Action> importActions = actionService.findAll().stream()
                .filter(action -> action.getType().equals(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW))
                .collect(Collectors.toList());
        // delete action first
        try {
            if (CollectionUtils.isNotEmpty(importActions)) {
                for (Action action : importActions) {
                    if (action.getActionConfiguration() != null
                            && action.getActionConfiguration() instanceof ImportActionConfiguration) {
                        ImportActionConfiguration config = (ImportActionConfiguration) action.getActionConfiguration();
                        if (taskIds.contains(config.getDataFeedTaskId())) {
                            actionService.delete(action.getPid());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Cannot delete import action. " + e.getMessage());
            return false;
        }
        // delete import template.
        for (DataFeedTask dft : dfTasks) {
            metadataProxy.deleteImportTable(customerSpaceStr, dft.getImportTemplate().getName());
        }
        return true;
    }

    @Override
    public String submitS3ImportJob(String customerSpaceStr, S3FileToHdfsConfiguration importConfig) {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpaceStr);
        if (importConfig == null) {
            throw new IllegalArgumentException("S3 Import config cannot be null!");
        }
        if (StringUtils.isEmpty(importConfig.getFeedType())) {
            throw new IllegalArgumentException("Template name cannot be empty for S3 import!");
        }
        if (StringUtils.isEmpty(importConfig.getS3FilePath())) {
            throw new IllegalArgumentException("Template path cannot be empty for S3 import!");
        }
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpace.toString(), SourceType.FILE.getName(),
                importConfig.getFeedType());
        if (dataFeedTask == null || dataFeedTask.getImportTemplate() == null) {
            throw new RuntimeException("Cannot find the template for S3 file: " + importConfig.getS3FilePath());
        }

        // startImport to generate unique path to resolve file overwrite issue
        Pair<String, String> targetPair = s3ImportFolderService.startImport(customerSpace.getTenantId(),
                dataFeedTask.getEntity(), importConfig.getS3Bucket(), importConfig.getS3FilePath());
        PrepareImportConfiguration prepareImportConfig = new PrepareImportConfiguration();
        prepareImportConfig.setSourceBucket(importConfig.getS3Bucket());
        prepareImportConfig.setSourceKey(importConfig.getS3FilePath());
        prepareImportConfig.setDestBucket(s3ImportFolderService.getBucket());
        prepareImportConfig.setDestKey(targetPair.getLeft());
        prepareImportConfig.setBackupKey(targetPair.getRight());
        prepareImportConfig.setDataFeedTaskId(dataFeedTask.getUniqueId());

        String filePath = targetPair.getLeft();
        String backupPath = targetPair.getRight();
        importConfig.setS3FilePath(filePath);
        importConfig.setS3Bucket(s3ImportFolderService.getBucket());


        S3ImportEmailInfo emailInfo = generateEmailInfo(customerSpace.toString(), importConfig.getS3FileName(),
                dataFeedTask, new Date());
        importConfig.setJobIdentifier(dataFeedTask.getUniqueId());
        importConfig.setFileSource("S3");
        CSVImportFileInfo csvImportFileInfo = new CSVImportFileInfo();
        csvImportFileInfo.setFileUploadInitiator(DEFAULT_S3_USER);
        csvImportFileInfo.setReportFileName(importConfig.getS3FileName());
        csvImportFileInfo.setReportFileDisplayName(importConfig.getS3FileName());
        csvImportFileInfo.setReportFilePath(backupPath);
        prepareImportConfig.setEmailInfo(emailInfo);

        ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask,
                JsonUtils.serialize(importConfig), csvImportFileInfo, prepareImportConfig,true, emailInfo,
                new WorkflowPidWrapper(-1L));
        return appId.toString();
    }

    private S3ImportEmailInfo generateEmailInfo(String customerSpace, String fileName, DataFeedTask dataFeedTask,
                                                Date timeReceived) {
        S3ImportEmailInfo emailInfo = getS3ImportEmailInfo(dataFeedTask);
        emailInfo.setFileName(fileName);
        emailInfo.setTimeReceived(timeReceived);
        emailInfo.setTenantName(CustomerSpace.parse(customerSpace).getTenantId());
        return emailInfo;
    }

    private void sendS3ImportEmail(String customerSpace, String result, S3ImportEmailInfo emailInfo) {
        try {
            String tenantId = CustomerSpace.parse(customerSpace).toString();
            plsInternalProxy.sendS3ImportEmail(result, tenantId, emailInfo);
        } catch (Exception e) {
            log.error("Failed to send s3 import email: " + e.getMessage());
        }
    }

    private S3ImportEmailInfo getS3ImportEmailInfo(DataFeedTask dataFeedTask) {
        S3ImportEmailInfo emailInfo = new S3ImportEmailInfo();
        DropBoxSummary dropBoxSummary = dropBoxService.getDropBoxSummary();
        emailInfo.setDropFolder(S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(), dropBoxSummary.getDropBox(),
                dataFeedTask.getFeedType()));
        emailInfo.setEntityType(EntityType.fromDataFeedTask(dataFeedTask));
        String templateName = dataFeedTask.getTemplateDisplayName() == null ? dataFeedTask.getFeedType() :
                dataFeedTask.getTemplateDisplayName();
        emailInfo.setTemplateName(templateName);
        return emailInfo;
    }

    private void sendS3TemplateChangeEmail(String customerSpace, DataFeedTask dataFeedTask, String user, boolean isCreate) {
        try {
            if (dataFeedTask != null) {
                S3ImportEmailInfo emailInfo = getS3ImportEmailInfo(dataFeedTask);
                emailInfo.setUser(user);
                String tenantId = CustomerSpace.parse(customerSpace).toString();
                if (isCreate) {
                    plsInternalProxy.sendS3TemplateCreateEmail(tenantId, emailInfo);
                } else {
                    plsInternalProxy.sendS3TemplateUpdateEmail(tenantId, emailInfo);
                }
            }
        } catch (Exception e) {
            log.error("Failed to send s3 import email: " + e.getMessage());
        }
    }

    private List<DataFeedTask> getAllDataFeedTask(String customerSpaceStr, BusinessEntity entity) {
        List<DataFeedTask> allTasks = new ArrayList<>();
        List<BusinessEntity> entityList = new ArrayList<>();
        if (entity == null) {
            entityList.add(BusinessEntity.Account);
            entityList.add(BusinessEntity.Contact);
            entityList.add(BusinessEntity.Transaction);
            entityList.add(BusinessEntity.Product);
        } else {
            entityList.add(entity);
        }
        for (BusinessEntity businessEntity : entityList) {
            List<DataFeedTask> dataFeedTasks = dataFeedTaskService.getDataFeedTaskWithSameEntity(CustomerSpace.parse(customerSpaceStr).toString(),
                    businessEntity.name());
            if (CollectionUtils.isNotEmpty(dataFeedTasks)) {
                allTasks.addAll(dataFeedTasks);
            }
        }
        return allTasks;
    }

    private CustomerSpace mapCustomerSpace(CustomerSpace customerSpace) {
        CustomerSpace newCustomerSpace = customerSpace;
        String dlTenantId = customerSpace.getTenantId();
        DLTenantMapping dlTenantMapping = dlTenantMappingService.getDLTenantMapping(dlTenantId, "*");
        if (dlTenantMapping != null) {
            newCustomerSpace = CustomerSpace.parse(dlTenantMapping.getTenantId());
        }
        log.info(String.format("original tenant %s, new tenant %s", customerSpace.getTenantId(),
                newCustomerSpace.getTenantId()));
        return newCustomerSpace;
    }

    private void crosscheckDataType(CustomerSpace customerSpace, String entity, String source, Table metaTable,
            String dataFeedTaskUniqueId) {
        List<DataFeedTask> dataFeedTasks = dataFeedTaskService.getDataFeedTaskWithSameEntity(customerSpace.toString(),
                entity);
        if (dataFeedTasks == null || dataFeedTasks.size() == 0) {
            return;
        } else {
            boolean updatedAttrName = false;
            for (DataFeedTask dataFeedTask : dataFeedTasks) {
                if (!updatedAttrName) {
                    updateTableAttributeName(dataFeedTask.getImportTemplate(), metaTable);
                    updatedAttrName = true;
                }
                if (StringUtils.equals(dataFeedTask.getUniqueId(), dataFeedTaskUniqueId)) {
                    continue;
                }
                List<String> inconsistentAttrs = compareAttribute(dataFeedTask.getSource(),
                        dataFeedTask.getImportTemplate(), source, metaTable);
                if (inconsistentAttrs != null && inconsistentAttrs.size() > 0) {
                    throw new RuntimeException(String.format(
                            "The following field data type is not consistent with " + "the one that already exists: %s",
                            String.join(",", inconsistentAttrs)));
                }
            }
        }
    }

    private List<String> compareAttribute(String baseSource, Table baseTable, String targetSource, Table targetTable) {
        List<String> inconsistentAttrs = new ArrayList<>();
        DataFeedMetadataService baseService = DataFeedMetadataService.getService(baseSource);
        DataFeedMetadataService targetService = DataFeedMetadataService.getService(targetSource);
        Map<String, Attribute> baseAttrs = new HashMap<>();
        baseTable.getAttributes().forEach(attribute -> baseAttrs.put(attribute.getName().toLowerCase(), attribute));
        for (Attribute attr : targetTable.getAttributes()) {
            if (baseAttrs.containsKey(attr.getName().toLowerCase())) {
                Schema.Type baseType = baseService.getAvroType(baseAttrs.get(attr.getName().toLowerCase()));
                Schema.Type targetType = targetService.getAvroType(attr);
                if (baseType != targetType) {
                    inconsistentAttrs.add(attr.getName());
                }
            }
        }
        return inconsistentAttrs;
    }

    @VisibleForTesting
    void updateTableAttributeName(Table templateTable, Table metaTable) {
        Map<String, Attribute> templateAttrs = new HashMap<>();
        templateTable.getAttributes()
                .forEach(attribute -> templateAttrs.put(attribute.getName().toLowerCase(), attribute));
        for (Attribute attr : metaTable.getAttributes()) {
            if (templateAttrs.containsKey(attr.getName().toLowerCase())) {
                attr.setName(templateAttrs.get(attr.getName().toLowerCase()).getName());
            }
        }
    }

    @VisibleForTesting
    Table mergeTable(Table templateTable, Table metaTable) {
        Map<String, Attribute> templateAttrs = new HashMap<>();
        templateTable.getAttributes().forEach(attribute -> templateAttrs.put(attribute.getName(), attribute));
        for (Attribute attr : metaTable.getAttributes()) {
            if (!templateAttrs.containsKey(attr.getName())) {
                Attribute newAttr = new Attribute(attr.getName());
                AttributeUtils.copyPropertiesFromAttribute(attr, newAttr);
                templateTable.addAttribute(newAttr);
            } else {
                templateAttrs.get(attr.getName()).setDisplayName(attr.getDisplayName());
                if (attr.getSourceAttrName() != null) {
                    templateAttrs.get(attr.getName()).setSourceAttrName(attr.getSourceAttrName());
                }
                if (!StringUtils.equals(attr.getDateFormatString(),
                        templateAttrs.get(attr.getName()).getDateFormatString())) {
                    templateAttrs.get(attr.getName()).setDateFormatString(attr.getDateFormatString());
                }
                if (!StringUtils.equals(attr.getTimeFormatString(),
                        templateAttrs.get(attr.getName()).getTimeFormatString())) {
                    templateAttrs.get(attr.getName()).setTimeFormatString(attr.getTimeFormatString());
                }
                if (!StringUtils.equals(attr.getDateFormatString(),
                        templateAttrs.get(attr.getName()).getTimezone())) {
                    templateAttrs.get(attr.getName()).setTimezone(attr.getTimezone());
                }
            }
        }
        return templateTable;
    }

    @VisibleForTesting
    boolean finalSchemaCheck(Table finalTemplate, String entity, boolean withoutId, boolean enableEntityMatch) {
        if (finalTemplate == null) {
            log.error("Template cannot be null!");
            return false;
        }
        if (CollectionUtils.isEmpty(finalTemplate.getAttributes())) {
            log.error("Template has no attributes!");
            return false;
        }
        Map<String, Attribute> standardAttrs = new HashMap<>();
        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true, withoutId
                , enableEntityMatch);
        standardTable.getAttributes().forEach(attribute -> standardAttrs.put(attribute.getName(), attribute));
        Map<String, Attribute> templateAttrs = new HashMap<>();
        finalTemplate.getAttributes().forEach(attribute -> templateAttrs.put(attribute.getName(), attribute));
        for (Map.Entry<String, Attribute> attrEntry : standardAttrs.entrySet()) {
            if (attrEntry.getValue().getRequired() && attrEntry.getValue().getDefaultValueStr() == null) {
                if (!templateAttrs.containsKey(attrEntry.getKey())) {
                    log.error("Missing required field: " + attrEntry.getKey());
                    return false;
                }
            }
            if (templateAttrs.containsKey(attrEntry.getKey())) {
                if (!compareAttribute(attrEntry.getValue(), templateAttrs.get(attrEntry.getKey()))) {
                    return false;
                }
            }
        }
        return true;
    }

    @VisibleForTesting
    void updateAttrConfig(Table templateTable, List<AttrConfig> attrConfigs, String entity,
            CustomerSpace customerSpace) {
        try {
            if (CollectionUtils.isEmpty(attrConfigs) || templateTable == null
                    || CollectionUtils.isEmpty(templateTable.getAttributes())) {
                if (CollectionUtils.isEmpty(attrConfigs)) {
                    log.info(String.format("Attr config setting is empty for tenant %s", customerSpace.toString()));
                }
                if (templateTable == null) {
                    log.info(String.format("Template table is empty for tenant %s, entity %s", customerSpace.toString(),
                            entity));
                }
                if (CollectionUtils.isEmpty(templateTable.getAttributes())) {
                    log.info(String.format("Template table does not contain any attributes, tenant %s, entity %s",
                            customerSpace.toString(), entity));
                }
                return;
            }
            List<AttrConfig> originalAttrConfigs = attrConfigEntityMgr.findAllForEntity(customerSpace.getTenantId(),
                    BusinessEntity.getByName(entity));

            Map<String, Attribute> attributeMap = templateTable.getAttributes().stream()
                    .collect(Collectors.toMap(Attribute::getSourceAttrName, attr -> attr));
            attrConfigs.forEach(attrConfig -> {
                if (attributeMap.containsKey(attrConfig.getAttrName())) {
                    attrConfig.setAttrName(attributeMap.get(attrConfig.getAttrName()).getName());
                } else {
                    throw new RuntimeException(
                            "Template table doesn't contains source Attribute: " + attrConfig.getAttrName());
                }
            });

            Map<String, AttrConfig> originalAttrConfigMap = originalAttrConfigs.stream()
                    .collect(Collectors.toMap(AttrConfig::getAttrName, attrConfig -> attrConfig));

            // remove attr config that already has custom value.
            Iterator<AttrConfig> attrConfigIterator = attrConfigs.iterator();
            while (attrConfigIterator.hasNext()) {
                AttrConfig attrConfig = attrConfigIterator.next();
                if (originalAttrConfigMap.containsKey(attrConfig.getAttrName())) {
                    log.info(String.format("Remove attr config %s", attrConfig.getAttrName()));
                    attrConfigIterator.remove();
                }
            }
            log.info(String.format("Save AttrConfigs with size %d", attrConfigs.size()));
            List<AttrConfig> savedAttrConfigs = attrConfigEntityMgr.save(customerSpace.getTenantId(),
                    BusinessEntity.getByName(entity), attrConfigs);
            log.info(String.format("Saved AttrConfigs size %d", savedAttrConfigs.size()));
        } catch (Exception e) {
            log.error("We cannot auto set the AttrConfig for import, please set AttrConfig manually!");
        }
    }

    private boolean compareAttribute(Attribute attr1, Attribute attr2) {
        if (!attr1.getPhysicalDataType().equalsIgnoreCase(attr2.getPhysicalDataType())) {
            // A temp fix for schema update in maint_4.8.0.
            if (InterfaceName.Amount.equals(attr1.getInterfaceName())
                    || InterfaceName.Quantity.equals(attr1.getInterfaceName())
                    || InterfaceName.Cost.equals(attr1.getInterfaceName())) {
                if (!attr2.getPhysicalDataType().equalsIgnoreCase("int")
                        && !attr2.getPhysicalDataType().equalsIgnoreCase("double")) {
                    log.error(String.format("Attribute %s has wrong physicalDataType %s", attr2.getName(),
                            attr2.getPhysicalDataType()));
                    return false;
                }
            } else if (InterfaceName.CreatedDate.equals(attr1.getInterfaceName())
                        || InterfaceName.LastModifiedDate.equals(attr1.getInterfaceName())) {
                if (!attr2.getPhysicalDataType().equalsIgnoreCase("string")
                        && !attr2.getPhysicalDataType().equalsIgnoreCase("long")) {
                    log.error(String.format("Attribute %s has wrong physicalDataType %s", attr2.getName(),
                            attr2.getPhysicalDataType()));
                    return false;
                }

            } else {
                log.error("PhysicalDataType is not the same for attribute: " + attr1.getName());
                return false;
            }
        }
        if (!attr1.getRequired().equals(attr2.getRequired())) {
            log.error("Required flag is not the same for attribute: " + attr1.getName());
            return false;
        }
        if (attr1.getInterfaceName() == null || attr2.getInterfaceName() == null) {
            log.warn("Interface name is null for attribute : " + attr1.getName());
        } else if (!attr1.getInterfaceName().equals(attr2.getInterfaceName())) {
            log.error("Interface name is not the same for attribute: " + attr1.getName());
            return false;
        }
        return true;
    }

    @Override
    public ImportTemplateDiagnostic diagnostic(String customerSpaceStr, String taskIdentifier) {
        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(customerSpaceStr, taskIdentifier);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find datafeed task for id: " + taskIdentifier);
        }
        Table templateTable = dataFeedTask.getImportTemplate();
        if (templateTable == null) {
            throw new RuntimeException(String.format("The template for datafeed task %s is empty!", taskIdentifier));
        }
        BusinessEntity entity = BusinessEntity.getByName(dataFeedTask.getEntity());
        return DiagnoseTable.diagnostic(customerSpaceStr, dataFeedTask.getImportTemplate(), entity, batonService);
    }

}
