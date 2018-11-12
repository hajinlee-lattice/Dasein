package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.cdl.CDLConstants.DEFAULT_S3_USER;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
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
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.service.S3ImportFolderService;
import com.latticeengines.apps.cdl.util.ValidateFileHeaderUtils;
import com.latticeengines.apps.cdl.workflow.CDLDataFeedImportWorkflowSubmitter;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("dataFeedTaskManagerService")
public class DataFeedTaskManagerServiceImpl implements DataFeedTaskManagerService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskManagerServiceImpl.class);

    public static final int MAX_HEADER_LENGTH = 63;

    private final DataFeedProxy dataFeedProxy;

    private final TenantService tenantService;

    private final DLTenantMappingService dlTenantMappingService;

    private final CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter;

    private final CDLExternalSystemService cdlExternalSystemService;

    private final ActionService actionService;

    private final MetadataProxy metadataProxy;

    private final AttrConfigEntityMgr attrConfigEntityMgr;

    private final S3Service s3Service;

    private final S3ImportFolderService s3ImportFolderService;

    @Value("${cdl.dataloader.tenant.mapping.enabled:false}")
    private boolean dlTenantMappingEnabled;

    @Value("${common.pls.url}")
    private String hostPort;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    public DataFeedTaskManagerServiceImpl(CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter,
            DataFeedProxy dataFeedProxy, TenantService tenantService, DLTenantMappingService dlTenantMappingService,
            CDLExternalSystemService cdlExternalSystemService, ActionService actionService, MetadataProxy metadataProxy,
            AttrConfigEntityMgr attrConfigEntityMgr, S3Service s3Service, S3ImportFolderService s3ImportFolderService) {
        this.cdlDataFeedImportWorkflowSubmitter = cdlDataFeedImportWorkflowSubmitter;
        this.dataFeedProxy = dataFeedProxy;
        this.tenantService = tenantService;
        this.dlTenantMappingService = dlTenantMappingService;
        this.cdlExternalSystemService = cdlExternalSystemService;
        this.actionService = actionService;
        this.metadataProxy = metadataProxy;
        this.attrConfigEntityMgr = attrConfigEntityMgr;
        this.s3Service = s3Service;
        this.s3ImportFolderService = s3ImportFolderService;
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
        Table schemaTable = SchemaRepository.instance().getSchema(BusinessEntity.valueOf(entity), true);

        newMeta = dataFeedMetadataService.resolveMetadata(newMeta, schemaTable);
        setCategoryForTable(newMeta, entity);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask != null) {
            crosscheckDataType(customerSpace, entity, source, newMeta, dataFeedTask.getUniqueId());
            Table originMeta = dataFeedTask.getImportTemplate();
            DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
            if (!dataFeedMetadataService.compareMetadata(originMeta, newMeta,
                    !dataFeed.getStatus().equals(DataFeed.Status.Initing))) {
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);
                Table finalTemplate = mergeTable(originMeta, newMeta);
                if (!finalSchemaCheck(finalTemplate, entity)) {
                    throw new RuntimeException("The final import template is invalid, please check import settings!");
                }
                dataFeedTask.setImportTemplate(finalTemplate);
                dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
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
                    schemaTable);
            crosscheckDataType(customerSpace, entity, source, newMeta, "");
            if (!finalSchemaCheck(newMeta, entity)) {
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
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            dropBoxProxy.createTemplateFolder(customerSpace.toString(), feedType, "");
            updateAttrConfig(newMeta, attrConfigs, entity, customerSpace);
            if (dataFeedMetadataService.needUpdateDataFeedStatus()) {
                DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
                if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
                    dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), DataFeed.Status.Initialized.getName());
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
        if (businessEntity == null) {
            throw new RuntimeException(String.format("Cannot recognize entity: %s", entity));
        }
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
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        MultiTenantContext.setTenant(tenant);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), taskIdentifier);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the data feed task!");
        }
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(dataFeedTask.getSource());
        String connectorConfig = dataFeedMetadataService.getConnectorConfig(importConfig, dataFeedTask.getUniqueId());
        CSVImportFileInfo csvImportFileInfo = dataFeedMetadataService.getImportFileInfo(importConfig);
        log.info(String.format("csvImportFileInfo=%s", csvImportFileInfo));
        ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask, connectorConfig,
                csvImportFileInfo, false, null, new WorkflowPidWrapper(-1L));
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
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), SourceType.FILE.getName(),
                importConfig.getFeedType());
        if (dataFeedTask == null || dataFeedTask.getImportTemplate() == null) {
            throw new RuntimeException("Cannot find the template for S3 file: " + importConfig.getS3FilePath());
        }
        String filePath = importConfig.getS3FilePath();
        String newFilePath = s3ImportFolderService.startImport(customerSpace.getTenantId(),
                dataFeedTask.getEntity(), importConfig.getS3Bucket(), importConfig.getS3FilePath());
        importConfig.setS3FilePath(newFilePath);
        importConfig.setS3Bucket(s3ImportFolderService.getBucket());

        S3ImportEmailInfo emailInfo = generateEmailInfo(customerSpace.toString(), importConfig.getS3FileName(),
                dataFeedTask, new Date());
        // validate
        validateS3File(dataFeedTask, importConfig, customerSpace.toString(), emailInfo);
        importConfig.setJobIdentifier(dataFeedTask.getUniqueId());
        importConfig.setFileSource("S3");
        CSVImportFileInfo csvImportFileInfo = new CSVImportFileInfo();
        csvImportFileInfo.setFileUploadInitiator(DEFAULT_S3_USER);
        csvImportFileInfo.setReportFileName(importConfig.getS3FileName());
        csvImportFileInfo.setReportFileDisplayName(importConfig.getS3FileName());
        csvImportFileInfo.setReportFilePath(filePath);

        ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask,
                JsonUtils.serialize(importConfig), csvImportFileInfo, true, emailInfo, new WorkflowPidWrapper(-1L));
        return appId.toString();
    }

    private S3ImportEmailInfo generateEmailInfo(String customerSpace, String fileName, DataFeedTask dataFeedTask,
                                                Date timeReceived) {
        S3ImportEmailInfo emailInfo = new S3ImportEmailInfo();
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        emailInfo.setDropFolder(S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(), dropBoxSummary.getDropBox(),
                dataFeedTask.getFeedType()));
        emailInfo.setEntityType(EntityType.fromEntityAndSubType(BusinessEntity.getByName(dataFeedTask.getEntity()),
                dataFeedTask.getSubType()));
        String templateName = dataFeedTask.getTemplateDisplayName() == null ? dataFeedTask.getFeedType() :
                dataFeedTask.getTemplateDisplayName();
        emailInfo.setTemplateName(templateName);
        emailInfo.setFileName(fileName);
        emailInfo.setTimeReceived(timeReceived);
        emailInfo.setTenantName(CustomerSpace.parse(customerSpace).getTenantId());
        return emailInfo;
    }

    private void sendS3ImportEmail(String customerSpace, String result, S3ImportEmailInfo emailInfo) {
        try {
            InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);

            String tenantId = CustomerSpace.parse(customerSpace).toString();
            proxy.sendS3ImportEmail(result, tenantId, emailInfo);
        } catch (Exception e) {
            log.error("Failed to send s3 import email: " + e.getMessage());
        }
    }

    private void sendS3TemplateChangeEmail(String customerSpace, DataFeedTask dataFeedTask, String user,
                                           boolean isCreate) {
        try {
            InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
            if (dataFeedTask != null) {
                S3ImportEmailInfo emailInfo = new S3ImportEmailInfo();
                DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
                emailInfo.setDropFolder(S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(), dropBoxSummary.getDropBox(),
                        dataFeedTask.getFeedType()));
                emailInfo.setEntityType(EntityType.fromEntityAndSubType(BusinessEntity.getByName(dataFeedTask.getEntity()),
                        dataFeedTask.getSubType()));
                String templateName = dataFeedTask.getTemplateDisplayName() == null ? dataFeedTask.getFeedType() :
                        dataFeedTask.getTemplateDisplayName();
                emailInfo.setTemplateName(templateName);
                emailInfo.setUser(user);

                String tenantId = CustomerSpace.parse(customerSpace).toString();
                if (isCreate) {
                    proxy.sendS3TemplateCreateEmail(tenantId, emailInfo);
                } else {
                    proxy.sendS3TemplateUpdateEmail(tenantId, emailInfo);
                }
            }
        } catch (Exception e) {
            log.error("Failed to send s3 import email: " + e.getMessage());
        }
    }

    private void validateS3File(DataFeedTask dataFeedTask, S3FileToHdfsConfiguration importConfig,
                                String customerSpace, S3ImportEmailInfo emailInfo) {
        Table template = dataFeedTask.getImportTemplate();
        String s3Bucket = importConfig.getS3Bucket();
        String s3FilePath = importConfig.getS3FilePath();
        List<String> warnings = new ArrayList<>();
        try (InputStream fileStream = s3Service.readObjectAsStream(s3Bucket, s3FilePath)) {
            InputStreamReader reader = new InputStreamReader(
                    new BOMInputStream(fileStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                            ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                    StandardCharsets.UTF_8);

            CSVFormat format = LECSVFormat.format;
            CSVParser parser = new CSVParser(reader, format);
            Set<String> headerFields = parser.getHeaderMap().keySet();
            ValidateFileHeaderUtils.checkForCSVInjectionInFileNameAndHeaders(importConfig.getS3FileName(),
                    headerFields);
            for (String header : headerFields) {
                if (StringUtils.length(header) > MAX_HEADER_LENGTH) {
                    throw new LedpException(LedpCode.LEDP_18188,
                            new String[] { String.valueOf(MAX_HEADER_LENGTH), header });
                }
            }
            Map<String, List<Attribute>> displayNameMap = template.getAttributes().stream()
                    .collect(groupingBy(Attribute::getDisplayName));
            List<String> templateMissing = new ArrayList<>();
            List<String> csvMissing = new ArrayList<>();
            List<String> requiredMissing = new ArrayList<>();
            for (String header : headerFields) {
                if (!displayNameMap.containsKey(header)) {
                    templateMissing.add(header);
                }
            }
            for (Map.Entry<String, List<Attribute>> entry : displayNameMap.entrySet()) {
                if (!headerFields.contains(entry.getKey())) {
                    csvMissing.add(entry.getKey());
                    for (Attribute attr : entry.getValue()) {
                        if (attr.getRequired()) {
                            requiredMissing.add(entry.getKey());
                        }
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(templateMissing)) {
                log.warn(String.format("Template doesn't contains the following columns: %s",
                        String.join(",", templateMissing)));
            }
            if (CollectionUtils.isNotEmpty(csvMissing)) {
                log.warn(String.format("S3File doesn't contains the following columns: %s",
                        String.join(",", csvMissing)));
            }
            if (CollectionUtils.isNotEmpty(requiredMissing)) {
                throw new LedpException(LedpCode.LEDP_40043, new String[] { String.join(",", requiredMissing) });
            }
            parser.close();
            String message = CollectionUtils.isNotEmpty(warnings) ? String.join("\n", warnings) : null;
            emailInfo.setErrorMsg(message);
            sendS3ImportEmail(customerSpace, "In_Progress", emailInfo);
        } catch (LedpException e) {
            s3ImportFolderService.moveFromInProgressToFailed(s3FilePath);
            emailInfo.setErrorMsg(e.getMessage());
            sendS3ImportEmail(customerSpace, "Failed", emailInfo);
            throw e;
        } catch (IOException e) {
            log.error(e.getMessage());
        } catch (IllegalArgumentException e) {
            s3ImportFolderService.moveFromInProgressToFailed(s3FilePath);
            emailInfo.setErrorMsg(e.getMessage());
            sendS3ImportEmail(customerSpace, "Failed", emailInfo);
            log.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unknown Exception when validate S3 import! " + e.toString());
            s3ImportFolderService.moveFromInProgressToFailed(s3FilePath);
            emailInfo.setErrorMsg(e.getMessage());
            sendS3ImportEmail(customerSpace, "Failed", emailInfo);
            throw e;
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
            List<DataFeedTask> dataFeedTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpaceStr,
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
        List<DataFeedTask> dataFeedTasks = dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace.toString(),
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
            }
        }
        return templateTable;
    }

    @VisibleForTesting
    boolean finalSchemaCheck(Table finalTemplate, String entity) {
        if (finalTemplate == null) {
            log.error("Template cannot be null!");
            return false;
        }
        if (CollectionUtils.isEmpty(finalTemplate.getAttributes())) {
            log.error("Template has no attributes!");
            return false;
        }
        Map<String, Attribute> standardAttrs = new HashMap<>();
        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.getByName(entity), true);
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

    private void updateAttrConfig(Table templateTable, List<AttrConfig> attrConfigs, String entity,
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

}
