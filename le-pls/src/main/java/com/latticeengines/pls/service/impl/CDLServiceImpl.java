package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.app.exposed.download.TemplateFileHttpDownloader;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ImportFileInfo;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.Status;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.exception.UIActionCode;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.domain.exposed.exception.View;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskSummary;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.SourcefileConfig;
import com.latticeengines.domain.exposed.pls.frontend.FieldCategory;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeFieldCategory;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.query.StoreFilter;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.domain.exposed.util.WebVisitUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("cdlService")
public class CDLServiceImpl implements CDLService {

    private static final Logger log = LoggerFactory.getLogger(CDLServiceImpl.class);

    private static final String PA_JOB_TYPE = "processAnalyzeWorkflow";
    private static final String PATHNAME = "N/A";
    private static final String DELETE_SUCCESS_TITLE = "Success! Delete Action has been submitted.";
    private static final String DELETE_FAIL_TITLE = "Validation Error";
    private static final String DELETE_SUCCESSE_MSG = "<p>The delete action will be scheduled to process and analyze after validation. You can track the status from the <a ui-sref=\"home.jobs.data\">Data Processing Job page</a>.</p>";

    private static final String DEFAULT_WEBSITE_SYSTEM = "Default_Website_System";
    private static final String DEFAULT_SYSTEM = "DefaultSystem";

    private static final String ATTR_VALUE = "Record";

    private static final String SOURCE = "File";

    @Inject
    protected SourceFileService sourceFileService;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private FileUploadService fileUploadService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private BatonService batonService;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${pls.pa.max.concurrent.limit}")
    private int maxActivePA;

    private List<String> templateMappingHeaders = Arrays.asList("Field Type", "Category", "Your Field Name",
            "Standard Field Name", "Data Type");

    private static final String CUSTOM = "Custom";
    private static final String STANDARD = "Standard";
    private static final String UNMAPPED = "unmapped";

    @Override
    public ApplicationId processAnalyze(String customerSpace, ProcessAnalyzeRequest request) {
        boolean isActivityBasedPA = cdlProxy.isActivityBasedPA();
        log.info("Submitting PA request for tenant = {}, isActivityBasedPA = {}", customerSpace, isActivityBasedPA);
        if (isActivityBasedPA) {
            SchedulingStatus status = cdlProxy.getSchedulingStatus(customerSpace);
            if (status != null) {
                boolean hasScheduleNowQuota = MapUtils.emptyIfNull(status.getRemainingPaQuota())
                        .getOrDefault(SchedulerConstants.QUOTA_SCHEDULE_NOW, 0L) > 0;
                if (!hasScheduleNowQuota) {
                    log.info("No scheduling quota left for tenant {}, quota = {}", customerSpace,
                            status.getRemainingPaQuota());
                    UIAction action = UIActionUtils.generateUIError("Schedule Now Quota Exhausted", View.Banner,
                            UIActionCode.PA_SCHEDULER_001);
                    throw UIActionException.fromAction(action);
                }
            }
            return cdlProxy.scheduleProcessAnalyze(customerSpace, false, request);
        } else {
            checkPALimit(customerSpace, request);
            return cdlProxy.processAnalyze(customerSpace, request);
        }
    }

    @Override
    public ApplicationId submitCSVImport(String customerSpace, String templateFileName, String dataFileName,
            String source, String entity, String feedType) {
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("The email of the file upload initiator is %s", email));
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateFileName, dataFileName, email);
        // temp fix for file upload directly.
        String subType = "";
        if (feedType.contains("Bundle")) {
            subType = DataFeedTask.SubType.Bundle.name();
        } else if (feedType.contains("Hierarchy")) {
            subType = DataFeedTask.SubType.Hierarchy.name();
        }
        String taskId = cdlProxy.createDataFeedTask(customerSpace, source, entity, feedType, subType, "", metaData);
        if (StringUtils.isEmpty(taskId)) {
            throw new LedpException(LedpCode.LEDP_18162, new String[] { entity, source, feedType });
        }
        return cdlProxy.submitImportJob(customerSpace, taskId, metaData);
    }

    @Override
    public String createS3Template(String customerSpace, String templateFileName, String source, String entity,
            String feedType, String subType, String displayName) {
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("The email of the s3 file upload initiator is %s", email));
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateFileName, templateFileName, email);
        String taskId = cdlProxy.createDataFeedTask(customerSpace, source, entity, feedType, subType, displayName, true,
                email, metaData);
        addAttributeLengthValidatorToTask(customerSpace, templateFileName, taskId);
        return taskId;
    }

    private void addAttributeLengthValidatorToTask(String customerSpace, String templateFileName, String taskId) {
        SourceFile sourceFile = sourceFileService.findByName(templateFileName);
        if (sourceFile != null) {
            SourcefileConfig config = sourceFile.getSourcefileConfig();
            if (config != null && config.getUniqueIdentifierConfig() != null) {
                cdlProxy.updateAttributeLengthValidator(customerSpace, taskId,
                        config.getUniqueIdentifierConfig().getName(), config.getUniqueIdentifierConfig().getLength(),
                        !config.getUniqueIdentifierConfig().isRequired());
            }
        }
    }

    @Override
    public ApplicationId submitS3ImportWithTemplateData(String customerSpace, String taskId, String templateFileName) {
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("The email of the s3 file upload initiator is %s", email));
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateFileName, templateFileName, email);
        return cdlProxy.submitImportJob(customerSpace, taskId, metaData);
    }

    @Override
    public ApplicationId submitS3ImportOnlyData(String customerSpace, String taskId, String dataFileName) {
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("The email of the s3 file upload initiator is %s", email));
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskId);
        if (dataFeedTask == null || dataFeedTask.getImportTemplate() == null) {
            throw new IllegalArgumentException(
                    String.format("Cannot find DataFeedTask %s or template is null!", taskId));
        }
        CSVImportConfig metaData = generateDataOnlyImportConfig(customerSpace,
                dataFeedTask.getImportTemplate().getName(), dataFileName, email);
        return cdlProxy.submitImportJob(customerSpace, taskId, true, metaData);
    }

    @Override
    public void importFileToS3(String customerSpace, String templateFileName, String s3Path) {
        SourceFile sourceFile = getSourceFile(templateFileName);
        if (sourceFile == null) {
            throw new IllegalArgumentException("Cannot find source file: " + templateFileName);
        }
        dropBoxProxy.importS3file(customerSpace, s3Path, sourceFile.getPath(), sourceFile.getDisplayName());
    }

    @Override
    public UIAction delete(DeleteRequest deleteRequest) {
        log.info(deleteRequest.toString());
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        UIAction uiAction = new UIAction();
        if (!batonService.isEntityMatchEnabled(customerSpace)) {
            uiAction.setTitle(DELETE_FAIL_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Error);
            uiAction.setMessage(generateDeleteResultMsg( //
                    "<p>Cleaning up by entity ids requires entity match.</p>"));
            throw new UIActionException(uiAction, LedpCode.LEDP_18182);
        }
        try {
            String email = MultiTenantContext.getEmailAddress();
            deleteRequest.setUser(email);
            cdlProxy.registerDeleteData(customerSpace.toString(), deleteRequest);
        } catch (RuntimeException e) {
            uiAction.setTitle(DELETE_FAIL_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Error);
            uiAction.setMessage(generateDeleteResultMsg(e.getMessage()));
            throw new UIActionException(uiAction, LedpCode.LEDP_18182);
        }
        uiAction.setTitle(DELETE_SUCCESS_TITLE);
        uiAction.setView(View.Banner);
        uiAction.setStatus(Status.Success);
        uiAction.setMessage(generateDeleteResultMsg(DELETE_SUCCESSE_MSG));
        return uiAction;
    }

    @Override
    public UIAction cleanup(String customerSpace, String sourceFileName, SchemaInterpretation schemaInterpretation,
            CleanupOperationType cleanupOperationType) {
        BusinessEntity entity;
        UIAction uiAction = new UIAction();
        if (batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace))
                && !batonService.onlyEntityMatchGAEnabled(CustomerSpace.parse(customerSpace))) {
            uiAction.setTitle(DELETE_FAIL_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Error);
            uiAction.setMessage( //
                    generateDeleteResultMsg("<p>Cleanup operation does not support entity match tenant</p>"));
            throw new UIActionException(uiAction, LedpCode.LEDP_18182);
        }

        switch (schemaInterpretation) {
        case DeleteAccountTemplate:
            entity = BusinessEntity.Account;
            break;
        case DeleteContactTemplate:
            entity = BusinessEntity.Contact;
            break;
        case DeleteTransactionTemplate:
            entity = BusinessEntity.Transaction;
            break;
        default:
            uiAction.setTitle(DELETE_FAIL_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Error);
            uiAction.setMessage(generateDeleteResultMsg(String
                    .format("<p>Cleanup operation does not support schema: %s </p>", schemaInterpretation.name())));
            throw new UIActionException(uiAction, LedpCode.LEDP_18182);
        }
        SourceFile sourceFile = getSourceFile(sourceFileName);
        if (sourceFile == null) {
            uiAction.setTitle(DELETE_FAIL_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Error);
            uiAction.setMessage(generateDeleteResultMsg(
                    String.format("<p>Cannot find source file with name: %s </p>", sourceFileName)));
            throw new UIActionException(uiAction, LedpCode.LEDP_18182);
        }
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            uiAction.setTitle(DELETE_FAIL_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Error);
            uiAction.setMessage(generateDeleteResultMsg(
                    String.format("<p>Source file %s doesn't have a schema.</p>", sourceFileName)));
            throw new UIActionException(uiAction, LedpCode.LEDP_18182);
        }
        String email = MultiTenantContext.getEmailAddress();
        try {
            cdlProxy.legacyDeleteByUpload(customerSpace, sourceFile, entity, cleanupOperationType, email);
        } catch (RuntimeException e) {
            uiAction.setTitle(DELETE_FAIL_TITLE);
            uiAction.setView(View.Modal);
            uiAction.setStatus(Status.Error);
            uiAction.setMessage(generateDeleteResultMsg(e.getMessage()));
            throw new UIActionException(uiAction, LedpCode.LEDP_18182);
        }
        uiAction.setTitle(DELETE_SUCCESS_TITLE);
        uiAction.setView(View.Banner);
        uiAction.setStatus(Status.Success);
        uiAction.setMessage(generateDeleteResultMsg(DELETE_SUCCESSE_MSG));
        return uiAction;
    }

    @VisibleForTesting
    String generateDeleteResultMsg(String message) {
        StringBuilder html = new StringBuilder();
        html.append(message);
        return html.toString();
    }

    @Override
    public ApplicationId cleanupByTimeRange(String customerSpace, String startTime, String endTime,
            SchemaInterpretation schemaInterpretation) {
        BusinessEntity entity;
        switch (schemaInterpretation) {
        case Transaction:
            entity = BusinessEntity.Transaction;
            break;
        default:
            throw new RuntimeException("Cleanup operation does not support schema: " + schemaInterpretation.name());
        }
        String email = MultiTenantContext.getEmailAddress();
        try {
            return cdlProxy.cleanupByTimeRange(customerSpace, startTime, endTime, entity, email);
        } catch (Exception e) {
            throw new RuntimeException("Failed to cleanup by time range: " + e.toString());
        }
    }

    @Override
    public ApplicationId cleanupAllData(String customerSpace, SchemaInterpretation schemaInterpretation) {
        BusinessEntity entity;
        switch (schemaInterpretation) {
        case Account:
            entity = BusinessEntity.Account;
            break;
        case Contact:
            entity = BusinessEntity.Contact;
            break;
        case Transaction:
            entity = BusinessEntity.Transaction;
            break;
        default:
            throw new RuntimeException("Cleanup operation does not support schema: " + schemaInterpretation.name());
        }
        String email = MultiTenantContext.getEmailAddress();
        return cdlProxy.cleanupAllData(customerSpace, entity, email);
    }

    @Override
    public void replaceData(String customerSpace, SchemaInterpretation schemaInterpretation) {
        BusinessEntity entity;
        switch (schemaInterpretation) {
        case Account:
            entity = BusinessEntity.Account;
            break;
        case Contact:
            entity = BusinessEntity.Contact;
            break;
        case Transaction:
            entity = BusinessEntity.Transaction;
            break;
        default:
            throw new RuntimeException("Cleanup operation does not support schema: " + schemaInterpretation.name());
        }
        String email = MultiTenantContext.getEmailAddress();
        cdlProxy.cleanupAllByAction(customerSpace, entity, email);
    }

    @VisibleForTesting
    CSVImportConfig generateImportConfig(String customerSpace, String templateFileName, String dataFileName,
            String email) {
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        SourceFile templateSourceFile = getSourceFile(templateFileName);
        SourceFile dataSourceFile = getSourceFile(dataFileName);
        if (StringUtils.isEmpty(templateSourceFile.getTableName())) {
            throw new RuntimeException(
                    String.format("Source file %s doesn't have a table template!", templateFileName));
        }
        importConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        importConfig.setTemplateName(templateSourceFile.getTableName());
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileSource("HDFS");
        CSVImportFileInfo importFileInfo = new CSVImportFileInfo();
        importFileInfo.setFileUploadInitiator(email);
        importFileInfo.setReportFileDisplayName(dataSourceFile.getDisplayName());
        importFileInfo.setReportFileName(dataSourceFile.getName());
        importFileInfo.setPartialFile(dataSourceFile.isPartialFile());
        if (dataSourceFile.isPartialFile()) {
            importFileInfo.setS3Bucket(dataSourceFile.getS3Bucket());
            importFileInfo.setS3Path(dataSourceFile.getS3Path());
        }
        CSVImportConfig csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvImportConfig.setCSVImportFileInfo(importFileInfo);

        return csvImportConfig;
    }

    private CSVImportConfig generateDataOnlyImportConfig(String customerSpace, String templateTableName,
            String dataFileName, String email) {
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        SourceFile dataSourceFile = getSourceFile(dataFileName);
        if (StringUtils.isEmpty(templateTableName)) {
            throw new RuntimeException("Template table name cannot be empty!");
        }
        importConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        importConfig.setTemplateName(templateTableName);
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileSource("HDFS");
        CSVImportFileInfo importFileInfo = new CSVImportFileInfo();
        importFileInfo.setFileUploadInitiator(email);
        importFileInfo.setReportFileDisplayName(dataSourceFile.getDisplayName());
        importFileInfo.setReportFileName(dataSourceFile.getName());
        CSVImportConfig csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvImportConfig.setCSVImportFileInfo(importFileInfo);

        return csvImportConfig;
    }

    @VisibleForTesting
    SourceFile getSourceFile(String sourceFileName) {
        SourceFile sourceFile = sourceFileService.findByName(sourceFileName);
        if (sourceFile == null) {
            throw new RuntimeException(String.format("Could not locate source file with name %s", sourceFileName));
        }
        return sourceFile;
    }

    @Override
    public List<S3ImportTemplateDisplay> getS3ImportTemplate(String customerSpace, String sortBy,
            Set<EntityType> excludeTypes) {
        List<S3ImportTemplateDisplay> templates = new ArrayList<>();
        List<String> folderNames;
        try (PerformanceTimer timer = new PerformanceTimer("Get sub folders from S3", log)) {
            folderNames = dropBoxProxy.getAllSubFolders(customerSpace, null, null, null);
        }
        log.info("folderNames is : " + folderNames.toString());
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        if (dropBoxSummary == null) {
            throw new RuntimeException("Tenant " + customerSpace //
                    + " does not have a dropbox.");
        }
        if (CollectionUtils.isEmpty(folderNames)) {
            log.info(String.format("Empty path in s3 folders for tenant %s in", customerSpace));
        }
        Map<String, DataFeedTaskSummary> summaryMap = new HashMap<>();
        try (PerformanceTimer timer = new PerformanceTimer("Get Task Summaries", log)) {
            List<DataFeedTaskSummary> allTaskSummaries = dataFeedProxy.getDataFeedTaskSummaries(customerSpace, "File");
            if (CollectionUtils.isNotEmpty(allTaskSummaries)) {
                summaryMap = allTaskSummaries.stream()
                        .collect(Collectors.toMap(DataFeedTaskSummary::getFeedType, summary -> summary));
            }
        }

        Map<String, S3ImportSystem> systemMap = new HashMap<>();
        try (PerformanceTimer timer = new PerformanceTimer("Get System Map", log)) {
            List<S3ImportSystem> allSystems = cdlProxy.getS3ImportSystemList(customerSpace);
            if (CollectionUtils.isNotEmpty(allSystems)) {
                systemMap = allSystems.stream().collect(Collectors.toMap(S3ImportSystem::getName, system -> system));
            }
        }

        Set<String> consumedTemplates = new HashSet<>();
        try (PerformanceTimer timer = new PerformanceTimer("Get PA Consumed templates", log)) {
            List<String> allTemplates = cdlProxy.getPAConsumedTemplateIds(customerSpace);
            if (CollectionUtils.isNotEmpty(allTemplates)) {
                consumedTemplates = new HashSet<>(allTemplates);
            }
        }

        try (PerformanceTimer timer = new PerformanceTimer("Build S3ImportTemplateDisplay Object", log)) {
            for (String folderName : folderNames) {
                if (hideLegacyTemplate(folderName, summaryMap)) {
                    continue;
                }
                if (!summaryMap.containsKey(folderName)) {
                    EntityType entityType = EntityType
                            .fromFeedTypeName(S3PathBuilder.getFolderNameFromFeedType(folderName));
                    if (CollectionUtils.isNotEmpty(excludeTypes) && excludeTypes.contains(entityType)) {
                        continue;
                    }
                    if (entityType != null) {
                        S3ImportTemplateDisplay display = new S3ImportTemplateDisplay();
                        display.setPath(S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(),
                                dropBoxSummary.getDropBox(), folderName));
                        display.setExist(Boolean.FALSE);
                        display.setTemplateName(entityType.getDefaultFeedTypeName());
                        display.setEntity(entityType.getEntity());
                        display.setObject(entityType.getDisplayName());
                        display.setFeedType(folderName);
                        display.setS3ImportSystem(systemMap.get(S3PathBuilder.getSystemNameFromFeedType(folderName)));
                        display.setImportStatus(DataFeedTask.S3ImportStatus.Pause);
                        display.setDataLoaded(Boolean.FALSE);
                        templates.add(display);
                    }
                } else {
                    DataFeedTaskSummary taskSummary = summaryMap.get(folderName);
                    S3ImportTemplateDisplay display = new S3ImportTemplateDisplay();
                    display.setPath(S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(),
                            dropBoxSummary.getDropBox(), folderName));
                    display.setExist(Boolean.TRUE);
                    display.setLastEditedDate(taskSummary.getLastUpdated());
                    // get from data feed task
                    display.setTemplateName(taskSummary.getTemplateDisplayName());
                    EntityType entityType = EntityType.fromDataFeedTaskSummary(taskSummary);
                    if (CollectionUtils.isNotEmpty(excludeTypes) && excludeTypes.contains(entityType)) {
                        continue;
                    }
                    display.setObject(entityType.getDisplayName());
                    display.setFeedType(taskSummary.getFeedType());
                    display.setEntity(entityType.getEntity());
                    display.setS3ImportSystem(systemMap.get(S3PathBuilder.getSystemNameFromFeedType(folderName)));
                    display.setImportStatus(taskSummary.getS3ImportStatus() == null ? DataFeedTask.S3ImportStatus.Pause
                            : taskSummary.getS3ImportStatus());
                    display.setDataLoaded(consumedTemplates.contains(taskSummary.getUniqueId()));
                    display.setSpecType(taskSummary.getSpecType());
                    templates.add(display);
                }
            }
        }

        try (PerformanceTimer timer = new PerformanceTimer("Sort S3ImportTemplateDisplay Object", log)) {
            if (StringUtils.isNotEmpty(sortBy)) {
                Comparator<S3ImportTemplateDisplay> compareBySystemType = Comparator
                        .comparing((S3ImportTemplateDisplay s) -> s.getS3ImportSystem() == null ? ""
                                : s.getS3ImportSystem().getSystemType().name());
                Comparator<S3ImportTemplateDisplay> compareBySystemName = Comparator
                        .comparing((S3ImportTemplateDisplay s) -> s.getS3ImportSystem() == null ? ""
                                : s.getS3ImportSystem().getDisplayName() == null ? ""
                                        : s.getS3ImportSystem().getDisplayName());
                Comparator<S3ImportTemplateDisplay> compareBySystem = compareBySystemType
                        .thenComparing(compareBySystemName);

                Comparator<S3ImportTemplateDisplay> compareBySystemPriority = Comparator
                        .comparing((S3ImportTemplateDisplay s) -> s.getS3ImportSystem() == null ? -1
                                : s.getS3ImportSystem().getPriority());
                if (sortBy.equalsIgnoreCase("SystemDisplay")) {
                    templates.sort(compareBySystem);
                } else if (sortBy.equalsIgnoreCase("SystemPriority")) {
                    templates.sort(compareBySystemPriority);
                }
            }
        }
        return templates;
    }

    // For migrated tenant, user may create the drop folder again. Need to hide
    // these templates from UI.
    private boolean hideLegacyTemplate(String folderName, Map<String, DataFeedTaskSummary> summaryMap) {
        if (StringUtils.isEmpty(getSystemNameFromFeedType(folderName))) {
            if (!summaryMap.containsKey(folderName)) {
                return summaryMap.containsKey(DEFAULT_SYSTEM + "_" + folderName);
            } else {
                DataFeedTaskSummary currentSummary = summaryMap.get(folderName);
                Optional<DataFeedTaskSummary> summaryWithSystem = summaryMap.values().stream()
                        .filter(summary -> !summary.getFeedType().equals(folderName)
                                && summary.getEntity().equals(currentSummary.getEntity())
                                && S3PathBuilder.DEFAULT_SYSTEM
                                        .equals(S3PathBuilder.getSystemNameFromFeedType(summary.getFeedType())))
                        .findFirst();
                return summaryWithSystem.isPresent();
            }
        }
        return false;
    }

    @Override
    public List<FileProperty> getFileListForS3Path(String customerSpace, String s3Path, String filter) {
        if (StringUtils.isBlank(s3Path)) {
            throw new IllegalArgumentException("Cannot list files for empty path!");
        }
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        if (!s3Path.contains(dropBoxSummary.getDropBox())) {
            throw new IllegalArgumentException("Must provide path under tenant's root directory!");
        }
        // first occurrence of the dropbox
        String relativePath = s3Path
                .substring(s3Path.indexOf(dropBoxSummary.getDropBox()) + dropBoxSummary.getDropBox().length());
        return dropBoxProxy.getFileListForPath(customerSpace, relativePath, filter);
    }

    @Override
    public String createS3ImportSystem(String customerSpace, String systemDisplayName,
            S3ImportSystem.SystemType systemType, Boolean primary) {
        S3ImportSystem s3ImportSystem = new S3ImportSystem();
        String systemName = AvroUtils.getAvroFriendlyString(systemDisplayName);
        s3ImportSystem.setSystemType(systemType);
        s3ImportSystem.setName(systemName);
        s3ImportSystem.setDisplayName(systemDisplayName);
        s3ImportSystem.setTenant(MultiTenantContext.getTenant());
        if (Boolean.TRUE.equals(primary)) {
            s3ImportSystem.setPriority(1);
        }
        switch (systemType) {
            case Salesforce:
                 String accountSystemId = s3ImportSystem.generateAccountSystemId();
                 s3ImportSystem.setAccountSystemId(accountSystemId);
                break;
            case Marketo:
            case Eloqua:
            case Pardot:
                String contactSystemId = s3ImportSystem.generateContactSystemId();
                s3ImportSystem.setContactSystemId(contactSystemId);
                break;
            default:
                break;
        }
        cdlProxy.createS3ImportSystem(customerSpace, s3ImportSystem);
        dropBoxProxy.createTemplateFolder(customerSpace, systemName, null, null);
        return systemName;
    }

    @Override
    public S3ImportSystem getS3ImportSystem(String customerSpace, String systemName) {
        return cdlProxy.getS3ImportSystem(customerSpace, systemName);
    }

    @Override
    public S3ImportSystem getDefaultImportSystem(String customerSpace) {
        return cdlProxy.getS3ImportSystem(customerSpace, DEFAULT_SYSTEM);
    }

    @Override
    public List<S3ImportSystem> getAllS3ImportSystem(String customerSpace) {
        List<S3ImportSystem> allSystems = cdlProxy.getS3ImportSystemList(customerSpace);
        if (CollectionUtils.isNotEmpty(allSystems)) {
            allSystems.sort(Comparator.comparing(S3ImportSystem::getPriority));
        }
        return allSystems;
    }

    @Override
    public Set<String> getAllS3ImportSystemIdSet(String customerSpace) {
        List<String> systemIdList = cdlProxy.getS3ImportSystemIdList(customerSpace);
        if (CollectionUtils.isNotEmpty(systemIdList)) {
            return new HashSet<>(systemIdList);
        }
        return Collections.emptySet();
    }

    @Override
    public List<S3ImportSystem> getS3ImportSystemWithFilter(String customerSpace, boolean filterAccount,
            boolean filterContact, S3ImportTemplateDisplay templateDisplay) {
        List<S3ImportSystem> allSystems = getAllS3ImportSystem(customerSpace);
        if (filterAccount) {
            allSystems = allSystems.stream().filter(system -> system.getAccountSystemId() != null)
                    .collect(Collectors.toList());
            if (templateDisplay != null) {
                EntityType entityType = EntityTypeUtils.matchFeedType(templateDisplay.getFeedType());
                // There should be 2 use cases here:
                // 1. EntityType=Accounts, means filter out the system pointing to itself
                // 2. EntityType=Contacts/Leads, means return all systems that have
                // SystemAccountId.
                if (EntityType.Accounts.equals(entityType) && CollectionUtils.isNotEmpty(allSystems)) {
                    allSystems = allSystems.stream()
                            .filter(system -> !system.getName().equals(templateDisplay.getS3ImportSystem().getName()))
                            .collect(Collectors.toList());
                }
            }
        }
        if (filterContact) {
            allSystems = allSystems.stream().filter(system -> system.getContactSystemId() != null)
                    .collect(Collectors.toList());
            if (templateDisplay != null) {
                EntityType entityType = EntityTypeUtils.matchFeedType(templateDisplay.getFeedType());
                // There should be 2 use cases here:
                // 1. EntityType=Contacts, means filter out the system pointing to itself
                // 2. EntityType=Leads, means return all systems that have SystemContactId.
                if (EntityType.Contacts.equals(entityType) && CollectionUtils.isNotEmpty(allSystems)) {
                    allSystems = allSystems.stream()
                            .filter(system -> !system.getName().equals(templateDisplay.getS3ImportSystem().getName()))
                            .collect(Collectors.toList());
                }
            }
        }
        return allSystems;
    }

    @Override
    public List<TemplateFieldPreview> getTemplatePreview(String customerSpace, Table templateTable,
            Table standardTable, Set<String> matchingFieldNames) {
        List<TemplateFieldPreview> templatePreview = templateTable.getAttributes().stream()
                .map(this::getFieldPreviewFromAttribute).collect(Collectors.toList());
        List<TemplateFieldPreview> standardPreview = standardTable.getAttributes().stream()
                .map(this::getFieldPreviewFromAttribute).collect(Collectors.toList());
        Set<String> standardAttrNames = standardTable.getAttributes().stream().map(Attribute::getName)
                .collect(Collectors.toSet());
        for (TemplateFieldPreview fieldPreview : templatePreview) {
            if (standardAttrNames.contains(fieldPreview.getNameInTemplate())) {
                fieldPreview.setFieldCategory(FieldCategory.LatticeField);
                if (matchingFieldNames.contains(fieldPreview.getNameInTemplate())) {
                    fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.MatchField);
                } else {
                    fieldPreview.setLatticeFieldCategory(LatticeFieldCategory.Other);
                }
                standardPreview
                        .removeIf(preview -> preview.getNameInTemplate().equals(fieldPreview.getNameInTemplate()));
            } else {
                fieldPreview.setFieldCategory(FieldCategory.CustomField);
            }
        }
        standardPreview.forEach(preview -> {
            preview.setUnmapped(true);
            if (matchingFieldNames.contains(preview.getNameInTemplate())) {
                preview.setLatticeFieldCategory(LatticeFieldCategory.MatchField);
            } else {
                preview.setLatticeFieldCategory(LatticeFieldCategory.Other);
            }
            preview.setFieldCategory(FieldCategory.LatticeField);
        });
        templatePreview.addAll(standardPreview);
        return templatePreview;
    }

    @Override
    public boolean autoImport(String templateFileName) {
        SourceFile sourceFile = getSourceFile(templateFileName);
        return sourceFile != null && !sourceFile.isPartialFile();
    }

    private void appendTemplateMapptingValue(StringBuffer fileContent, String value) {
        fileContent.append(value);
        fileContent.append(",");
    }

    // append field type for the file
    private void appendFieldType(StringBuffer fileContent, TemplateFieldPreview preview) {
        UserDefinedType userDefinedType = preview.getFieldType();
        if (userDefinedType.equals(UserDefinedType.DATE)) {
            StringBuffer formatStr = new StringBuffer();
            if (!StringUtils.isBlank(preview.getDateFormatString())) {
                formatStr.append(preview.getDateFormatString());
                formatStr.append(" ");
            }
            if (!StringUtils.isBlank(preview.getTimeFormatString())) {
                formatStr.append(preview.getTimeFormatString());
                formatStr.append(" ");
            }
            if (!StringUtils.isBlank(preview.getTimezone())) {
                formatStr.append(preview.getTimezone());
            }
            if (StringUtils.isBlank(formatStr.toString())) {
                fileContent.append(userDefinedType);
            } else {
                fileContent.append(formatStr.toString().trim());
            }
        } else {
            fileContent.append(userDefinedType);
        }
    }

    // get both mapped and unmapped field mappings
    @Override
    public String getTemplateMappingContent(List<TemplateFieldPreview> previews) {
        StringBuffer fileContent = new StringBuffer();
        for (String templateMappingHeader : templateMappingHeaders) {
            appendTemplateMapptingValue(fileContent, templateMappingHeader);
        }
        fileContent.deleteCharAt(fileContent.length() - 1);
        fileContent.append("\n");
        for (TemplateFieldPreview preview : previews) {
            // field category
            if (FieldCategory.LatticeField.equals(preview.getFieldCategory())) {
                appendTemplateMapptingValue(fileContent, STANDARD);
            } else {
                appendTemplateMapptingValue(fileContent, CUSTOM);
            }
            // lattice field category, only lattice field has
            if (preview.getLatticeFieldCategory() != null) {
                appendTemplateMapptingValue(fileContent, preview.getLatticeFieldCategory().toString());
            } else {
                // empty string if no
                appendTemplateMapptingValue(fileContent, "");
            }
            // name from file
            if (preview.isUnmapped()) {
                appendTemplateMapptingValue(fileContent, UNMAPPED);
            } else {
                appendTemplateMapptingValue(fileContent, preview.getNameFromFile());
            }
            // name in template
            appendTemplateMapptingValue(fileContent, preview.getNameInTemplate());
            // data type
            appendFieldType(fileContent, preview);
            fileContent.append("\n");

        }
        fileContent.deleteCharAt(fileContent.length() - 1);
        return fileContent.toString();
    }

    @Override
    public String getSystemNameFromFeedType(String feedType) {
        if (StringUtils.isEmpty(feedType) || !feedType.contains("_")) {
            return null;
        }
        return feedType.substring(0, feedType.lastIndexOf("_"));
    }

    @Override
    public void updateS3ImportSystem(String customerSpace, S3ImportSystem importSystem) {
        cdlProxy.updateS3ImportSystem(customerSpace, importSystem);
    }

    @Override
    public void updateS3ImportSystemPriorityBasedOnSequence(String customerSpace, List<S3ImportSystem> systemList) {
        if (CollectionUtils.isEmpty(systemList)) {
            return;
        }
        for (int i = 0; i < systemList.size(); i++) {
            systemList.get(i).setPriority(i + 1);
        }
        cdlProxy.updateAllS3ImportSystemPriority(customerSpace, systemList);
    }

    @Override
    public boolean createWebVisitProfile(String customerSpace, EntityType entityType, InputStream inputStream) {
        if (!EntityType.WebVisit.equals(entityType) && !EntityType.WebVisitPathPattern.equals(entityType)) {
            throw new RuntimeException("Cannot create template for: " + entityType.getDisplayName());
        }
        S3ImportSystem websiteSystem = getS3ImportSystem(customerSpace, DEFAULT_WEBSITE_SYSTEM);
        if (websiteSystem != null) {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, "File",
                    EntityTypeUtils.generateFullFeedType(DEFAULT_WEBSITE_SYSTEM, entityType));
            if (dataFeedTask != null) {
                throw new RuntimeException("Already created template for: " + entityType.getDisplayName());
            }
        } else {
            createS3ImportSystem(customerSpace, DEFAULT_WEBSITE_SYSTEM, S3ImportSystem.SystemType.Website, false);
            websiteSystem = getS3ImportSystem(customerSpace, DEFAULT_WEBSITE_SYSTEM);
        }
        SourceFile templateFile;
        templateFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                entityType.getDefaultFeedTypeName() + "TemplateFile", inputStream);
        Table templateTable = SchemaRepository.instance().getSchema(websiteSystem.getSystemType(), entityType,
                batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)),
                batonService.onlyEntityMatchGAEnabled(CustomerSpace.parse(customerSpace)));
        MetadataResolver resolver = new MetadataResolver(templateFile.getPath(), yarnConfiguration, null, true, null);
        FieldMappingDocument document = resolver.getFieldMappingsDocumentBestEffort(templateTable);
        if (document != null && CollectionUtils.isNotEmpty(document.getFieldMappings())) {
            document.getFieldMappings().forEach(this::applyUserPrefix);
        }
        resolver = new MetadataResolver(templateFile.getPath(), yarnConfiguration, document, true, templateTable);
        resolver.calculateBasedOnFieldMappingDocument(templateTable);
        Table newTable = resolver.getMetadata();
        newTable.setName("SourceFile_" + templateFile.getName().replace(".", "_"));
        metadataProxy.createTable(customerSpace, newTable.getName(), newTable);
        templateFile.setTableName(newTable.getName());
        sourceFileService.update(templateFile);
        String feedType = EntityTypeUtils.generateFullFeedType(websiteSystem.getName(), entityType);
        String subType = entityType.getSubType() == null ? "" : entityType.getSubType().name();
        String taskId = createS3Template(customerSpace, templateFile.getName(), "File", entityType.getEntity().name(),
                feedType, subType, feedType);

        if (EntityType.WebVisitPathPattern == entityType) {
            // create ptn catalog
            Catalog catalog = activityStoreProxy.createCatalog(customerSpace, EntityType.WebVisitPathPattern.name(),
                    taskId, InterfaceName.PathPatternName.name());
            log.info("Create WebVisitPathPattern catalog for tenant {}, catalog={}, dataFeedTaskUniqueId={}",
                    customerSpace, catalog, taskId);

            // check if webvisit stream is created
            AtlasStream webVisitStream = activityStoreProxy.findStreamByName(customerSpace, EntityType.WebVisit.name(),
                    true);
            if (webVisitStream != null) {
                Preconditions.checkNotNull(webVisitStream.getDimensions());
                Preconditions.checkArgument(webVisitStream.getDimensions().size() > 1);

                // get path pattern dimension and attach catalog
                Optional<StreamDimension> pathPtnDimension = webVisitStream.getDimensions().stream() //
                        .filter(Objects::nonNull) //
                        .filter(dim -> InterfaceName.PathPatternId.name().equals(dim.getName())) //
                        .findFirst();
                Preconditions.checkArgument(pathPtnDimension.isPresent(),
                        "Path pattern dimension should be created with WebVisit stream");
                StreamDimension dimension = pathPtnDimension.get();
                dimension.setCatalog(catalog);
                log.info("Attach path pattern catalog {} to WebVisit stream = {}, PathPatternId dimension = {}",
                        catalog, webVisitStream.getPid(), dimension.getPid());
                activityStoreProxy.updateDimension(customerSpace, webVisitStream.getName(), dimension);
            } else {
                log.info("No WebVisit activity stream created for tenant {} yet, ignore attaching path pattern catalog",
                        customerSpace);
            }
        } else {
            Catalog pathPtnCatalog = activityStoreProxy.findCatalogByName(customerSpace,
                    EntityType.WebVisitPathPattern.name());
            // dummy task only to provide unique ID
            DataFeedTask task = new DataFeedTask();
            task.setUniqueId(taskId);
            AtlasStream webVisitStream = WebVisitUtils.newWebVisitStream(MultiTenantContext.getTenant(), task);
            List<StreamDimension> dimensions = WebVisitUtils.newWebVisitDimensions(webVisitStream, pathPtnCatalog,
                    null);
            webVisitStream.setDimensions(dimensions);
            webVisitStream = activityStoreProxy.createStream(customerSpace, webVisitStream);

            Preconditions.checkNotNull(webVisitStream);
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(webVisitStream.getDimensions()));
            Preconditions.checkArgument(webVisitStream.getDimensions().size() > 1);
            log.info("Create WebVisit activity stream for tenant {}. stream PID = {}, dataFeedTaskUniqueId = {}",
                    customerSpace, webVisitStream.getPid(), taskId);
            if (!activityMetricsProxy.setupDefaultWebVisitProfile(customerSpace, webVisitStream)) {
                throw new IllegalStateException(
                        String.format("Failed to create default web visit groups for tenant %s.", customerSpace));
            }
        }
        return true;
    }

    @Override
    public boolean createDefaultOpportunityTemplate(String customerSpace, String systemName) {
        return cdlProxy.createDefaultOpportunityTemplate(customerSpace, systemName);
    }

    @Override
    public boolean createDefaultMarketingTemplate(String customerSpace, String systemName, String systemType) {
        return cdlProxy.createDefaultMarketingTemplate(customerSpace, systemName, systemType);
    }

    @Override
    public boolean createDefaultDnbIntentDataTemplate(String customerSpace) {
        return cdlProxy.createDefaultDnbIntentDataTemplate(customerSpace);
    }

    private void applyUserPrefix(FieldMapping fieldMapping) {
        if (fieldMapping != null && StringUtils.isEmpty(fieldMapping.getMappedField())) {
            if (!fieldMapping.getUserField().startsWith(MetadataResolver.USER_PREFIX)) {
                fieldMapping.setMappedField(MetadataResolver.USER_PREFIX + fieldMapping.getUserField());
            }
        }
    }

    private TemplateFieldPreview getFieldPreviewFromAttribute(Attribute attribute) {
        TemplateFieldPreview fieldPreview = new TemplateFieldPreview();
        fieldPreview.setNameInTemplate(attribute.getName());
        fieldPreview.setNameFromFile(
                attribute.getSourceAttrName() == null ? attribute.getDisplayName() : attribute.getSourceAttrName());
        fieldPreview.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));
        if (UserDefinedType.DATE.equals(fieldPreview.getFieldType())) {
            fieldPreview.setDateFormatString(attribute.getDateFormatString());
            fieldPreview.setTimeFormatString(attribute.getTimeFormatString());
            fieldPreview.setTimezone(attribute.getTimezone());
        }
        return fieldPreview;
    }

    /*
     * Make sure the system is not busy
     */
    private void checkPALimit(String customerSpace, ProcessAnalyzeRequest request) {
        if (StringUtils.isBlank(customerSpace) || request == null || StringUtils.isBlank(request.getUserId())) {
            // don't fail anything if something is wrong, since this check is not critical
            log.warn("Invalid parameter in checkPALimit. CustomerSpace={}, ProcessAnalyzeRequest={}", customerSpace,
                    request);
            return;
        }
        if (Boolean.TRUE.equals(request.getForceRun())) {
            log.info("Skipping PA limit check when forceRun flag is set");
            return;
        }
        String userId = request.getUserId();
        if (!EmailUtils.isInternalUser(userId)) {
            log.debug("User({}) is not internal, skip PA limit check", userId);
            return;
        }

        // make sure currently not-terminated PA jobs do not exceed limit
        Integer nActivePA = workflowProxy.getNonTerminalJobCount(customerSpace, Collections.singletonList(PA_JOB_TYPE));
        Preconditions.checkNotNull(nActivePA);
        if (nActivePA >= maxActivePA) {
            log.info(
                    "There are {} non-terminal PA at the moment, cannot start another one. Limit = {}, internal user = {}",
                    nActivePA, maxActivePA, userId);
            throw new UIActionException(getSystemBusyUIAction(), LedpCode.LEDP_40054);
        }
    }

    private UIAction getSystemBusyUIAction() {
        UIAction action = new UIAction();
        action.setView(View.Banner);
        action.setTitle("");
        action.setMessage(LedpCode.LEDP_40054.getMessage());
        action.setStatus(Status.Error);
        return action;
    }

    public boolean checkBundleUpload(String customerSpace) {
        // get action without running PA
        List<Action> actionsWithoutPA = actionProxy.getActionsByOwnerId(customerSpace, null);
        if (CollectionUtils.isEmpty(actionsWithoutPA)) {
            return true;
        }

        List<Action> importActionsWithoutPA = actionsWithoutPA.stream()
                .filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())
                        && ActionStatus.ACTIVE.equals(action.getActionStatus()))
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(importActionsWithoutPA)) {
            return true;
        }

        for (Action action : importActionsWithoutPA) {
            ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action
                    .getActionConfiguration();
            String dataFeedTaskId = importActionConfiguration.getDataFeedTaskId();
            if (dataFeedTaskId == null) {
                continue;
            }
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, dataFeedTaskId);
            if (dataFeedTask == null) {
                continue;
            }
            if (BusinessEntity.Product.name().equals(dataFeedTask.getEntity())
                    && DataFeedTask.SubType.Bundle.equals(dataFeedTask.getSubType())) {
                Long workflowId = importActionConfiguration.getWorkflowId();
                Preconditions.checkNotNull(workflowId, "configuration is null for bundle");
                Job job = workflowProxy.getJobByWorkflowJobPid(customerSpace, workflowId);
                // forbidden user upload if status is completed, running, enqueued, pending
                if (JobStatus.COMPLETED.equals(job.getJobStatus()) || JobStatus.RUNNING.equals(job.getJobStatus())
                        || JobStatus.ENQUEUED.equals(job.getJobStatus())
                        || JobStatus.PENDING.equals(job.getJobStatus())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Map<String, String> getDecoratedDisplayNameMapping(String customerSpace, EntityType entityType) {
        if (entityType == null) {
            return Collections.emptyMap();
        }
        if (EntityType.isStandardEntityType(entityType)) {
            return servingStoreProxy
                    .getDecoratedMetadata(customerSpace, entityType.getEntity(), null, null, StoreFilter.NON_LDC)
                    .filter(clm -> StringUtils.isNotEmpty(clm.getAttrName())
                            && StringUtils.isNotEmpty(clm.getDisplayName()))
                    .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName).block();
        } else if (EntityType.isStreamEntityType(entityType)) {
            return servingStoreProxy
                    .getDecoratedMetadata(customerSpace, BusinessEntity.Account, null, null, StoreFilter.NON_LDC)
                    .filter(clm -> StringUtils.isNotEmpty(clm.getAttrName())
                            && StringUtils.isNotEmpty(clm.getDisplayName()))
                    .collectMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName).block();
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, List<Map<String, Object>>> getDimensionMetadataInStream(String customerSpace, String systemName,
            EntityType entityType) {
        log.info("customerSpace is {}, systemName is {}, entityType is {}.", customerSpace, systemName, entityType);
        String streamName;
        switch (entityType) {
        case WebVisit:
            streamName = entityType.name();
            break;
        case MarketingActivity:
        case Opportunity:
            streamName = systemName + "_" + entityType.name();
            break;
        default:
            throw new IllegalArgumentException(
                    String.format("getDimensionMetadata() Cannot support " + "entityType %s", entityType.name()));
        }
        try {
            Map<String, DimensionMetadata> metadataMap = activityStoreProxy.getDimensionMetadataInStream(customerSpace,
                    streamName, null);
            log.info("customerSpace is {}, streamName is {}, signature is {}.", customerSpace, streamName, null);
            Map<String, List<Map<String, Object>>> dimensionMetadataMap = new HashMap<>();
            log.info("dimensionMetadataMap is {}.", JsonUtils.serialize(dimensionMetadataMap));
            metadataMap.forEach((dimId, metadata) -> {
                dimensionMetadataMap.put(dimId, resetMetadataValue(metadata));
            });
            return dimensionMetadataMap;
        } catch (Exception e) {
            log.error("getDimensionMetadataInStream error. detail is {}.", e);
            return new HashMap<>();
        }
    }

    @Override
    public void downloadDimensionMetadataInStream(HttpServletRequest request, HttpServletResponse response,
            String mimeType, String fileName, String customerSpace, String systemName, EntityType entityType) {
        log.info("mimeType is {}, fileName is {}, customerSpace is {}, systemName is {}, entityType is {}, ", mimeType,
                fileName, customerSpace, systemName, entityType);
        List<Map<String, Object>> metadataValues = getMetadataValues(customerSpace, systemName, entityType);
        DataFeedTask datafeedTask = dataFeedProxy.getDataFeedTask(customerSpace, "File",
                S3PathBuilder.getFolderName(systemName, entityType.getDefaultFeedTypeName()));
        if (datafeedTask == null) {
            throw new RuntimeException(String.format("Cannot find template for entityType %s, tenant %s.",
                    entityType.name(), customerSpace));
        }

        TemplateFileHttpDownloader.TemplateFileHttpDownloaderBuilder builder = new TemplateFileHttpDownloader.TemplateFileHttpDownloaderBuilder();
        builder.setMimeType(mimeType).setFileName(fileName)
                .setFileContent(getCSVFromValues(datafeedTask.getImportTemplate(), metadataValues))
                .setBatonService(batonService);
        TemplateFileHttpDownloader downloader = new TemplateFileHttpDownloader(builder);
        downloader.downloadFile(request, response);
    }

    private List<Map<String, Object>> getMetadataValues(String customerSpace, String systemName,
            EntityType entityType) {
        String dimensionName;
        EntityType streamType;
        switch (entityType) {
        case WebVisitPathPattern:
            streamType = EntityType.WebVisit;
            dimensionName = InterfaceName.PathPatternId.name();
            break;
        case WebVisitSourceMedium:
            streamType = EntityType.WebVisit;
            dimensionName = InterfaceName.SourceMediumId.name();
            break;
        case OpportunityStageName:
            streamType = EntityType.Opportunity;
            dimensionName = InterfaceName.StageNameId.name();
            break;
        case MarketingActivityType:
            streamType = EntityType.MarketingActivity;
            dimensionName = InterfaceName.ActivityTypeId.name();
            break;
        default:
            throw new IllegalArgumentException(String
                    .format("this method cannot support this " + "entityType. entityType is %s.", entityType.name()));
        }

        Map<String, List<Map<String, Object>>> getAllDimensionMetadataValues = getDimensionMetadataInStream(
                customerSpace, systemName, streamType);
        if (!getAllDimensionMetadataValues.containsKey(dimensionName)) {
            throw new IllegalArgumentException(String.format(
                    "CustomerSpace %s, cannot find dimensionName %s in " + "systemName %s, entityType is %s.",
                    customerSpace, dimensionName, systemName, entityType));
        }
        return getAllDimensionMetadataValues.get(dimensionName);
    }

    private List<Map<String, Object>> resetMetadataValue(DimensionMetadata item) {
        if (item == null) {
            return null;
        }
        List<String> nlFields = Arrays.asList(InterfaceName.PathPatternId.name(), InterfaceName.SourceMediumId.name(),
                InterfaceName.StageNameId.name(), InterfaceName.City.name());
        List<Map<String, Object>> metadataValue = item.getDimensionValues();
        for (Map<String, Object> simpleMetadataValue : metadataValue) {
            for (String needDeleteField : nlFields) {
                simpleMetadataValue.remove(needDeleteField);
            }
        }
        log.info("after reset MetadataValue, metadataValue is {}.", JsonUtils.serialize(metadataValue));
        return metadataValue;
    }

    private String getCSVFromValues(Table templateTable, List<Map<String, Object>> metadataValues) {
        StringBuffer fileContent = new StringBuffer();
        Map<String, Attribute> templateAttrNameMap = templateTable.getNameAttributeMap();
        List<String> attrNameList = new ArrayList<>();
        if (MapUtils.isNotEmpty(templateAttrNameMap)) {
            String name = InterfaceName.Name.name();
            if (templateAttrNameMap.containsKey(name)) {// make column "Name" in the first position.
                Attribute attribute = templateAttrNameMap.get(name);
                String displayName = attribute.getSourceAttrName() == null ? attribute.getDisplayName()
                        : attribute.getSourceAttrName();
                appendTemplateMapptingValue(fileContent, displayName);
                attrNameList.add(name);
            }
            for (Attribute attribute : templateAttrNameMap.values()) {
                String attrName = attribute.getName();
                // FIXME use other util to check whether this is a custom attribute
                if (StringUtils.isBlank(attrName) || attrName.equals(name) || attrName.startsWith("user_")) {
                    // skip Name & custom attributes
                    continue;
                }
                String displayName = attribute.getSourceAttrName() == null ? attribute.getDisplayName()
                        : attribute.getSourceAttrName();
                appendTemplateMapptingValue(fileContent, displayName);
                attrNameList.add(attribute.getName());
            }
        }
        fileContent.deleteCharAt(fileContent.length() - 1);
        fileContent.append("\n");

        if (CollectionUtils.isEmpty(metadataValues)) {
            return fileContent.toString();
        }

        for (Map<String, Object> metadataValue : metadataValues) {
            for (String attrName : attrNameList) {
                if (metadataValue.containsKey(attrName)) {
                    appendTemplateMapptingValue(fileContent, metadataValue.get(attrName).toString());
                } else {
                    appendTemplateMapptingValue(fileContent, " ");
                }
            }
            fileContent.deleteCharAt(fileContent.length() - 1);
            fileContent.append("\n");
        }
        fileContent.deleteCharAt(fileContent.length() - 1);
        return fileContent.toString();
    }

    private String modifyStringForCSV(String value) {
        if (value == null) {
            return "";
        }
        return "\"" + value.replaceAll("\"", "\"\"") + "\"";
    }

    @Override
    public List<ImportFileInfo> getAllImportFiles(String customerSpace) {
        List<Action> importActions = actionProxy.getActions(customerSpace).stream()
                .filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType()))
                .collect(Collectors.toList());
        return importActions.stream().map(action -> getImportFile(customerSpace, action)).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public void resetTemplate(String customerSpace, String feedType, Boolean forceReset) {
        if (!cdlProxy.resetTemplate(customerSpace, "File", feedType, forceReset)) {
            // could not locate template.
            throw new RuntimeException("Cannot reset template!");
        }
    }

    @Override
    public boolean validateAndUpdateS3ImportSystemPriority(String customerSpace, List<S3ImportSystem> systemList) {
        if (CollectionUtils.isEmpty(systemList)) {
            return false;
        }
        for (int i = 0; i < systemList.size(); i++) {
            systemList.get(i).setPriority(i + 1);
        }
        return cdlProxy.validateAndUpdateSystemPriority(customerSpace, systemList);
    }

    @Override
    public ApplicationId generateIntentAlert(String customerSpace) {
        return cdlProxy.generateIntentAlert(customerSpace);
    }

    private ImportFileInfo getImportFile(String customerSpace, Action action) {
        ImportFileInfo importFileInfo = new ImportFileInfo();
        ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action
                .getActionConfiguration();
        String dataFeedTaskId = importActionConfiguration.getDataFeedTaskId();
        if (dataFeedTaskId == null) {
            return null;
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, dataFeedTaskId);
        if (dataFeedTask == null) {
            return null;
        }
        importFileInfo.setFeedType(dataFeedTask.getFeedType());
        if (dataFeedTask.getImportSystemName() == null) {
            importFileInfo.setSystemName(S3PathBuilder.getSystemNameFromFeedType(dataFeedTask.getFeedType()));
        } else {
            importFileInfo.setSystemName(dataFeedTask.getImportSystemName());
        }

        importFileInfo.setTemplateName(dataFeedProxy.getTemplateName(customerSpace, dataFeedTaskId));
        Long workflowId = importActionConfiguration.getWorkflowId();
        if (workflowId == null) {
            return null;
        }
        Job job = workflowProxy.getJobByWorkflowJobPid(customerSpace, workflowId);
        Map<String, String> inputs = job.getInputs();
        String filePath = inputs.get(WorkflowContextConstants.Inputs.SOURCE_FILE_PATH);
        if (StringUtils.isNotEmpty(filePath)) {
            importFileInfo.setFilePath(filePath);
        } else {
            String fileName = inputs.get(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME);
            if (fileName == null) {
                return null;
            }
            SourceFile source = sourceFileService.findByName(fileName);
            importFileInfo.setFilePath(source.getPath());
        }

        return importFileInfo;
    }
}
