package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("cdlService")
public class CDLServiceImpl implements CDLService {

    private static final Logger log = LoggerFactory.getLogger(CDLServiceImpl.class);

    private static final String PA_JOB_TYPE = "processAnalyzeWorkflow";
    private static final String LATTICE_ENGINES_COM = "LATTICE-ENGINES.COM";
    private static final String PATHNAME = "N/A";
    private static final String DELETE_SUCCESS_TITLE = "Success! Delete Action has been submitted.";
    private static final String DELETE_FAIL_TITLE = "Validation Error";
    private static final String DELETE_SUCCESSE_MSG = "<p>The delete action will be scheduled to process and analyze after validation. You can track the status from the <a ui-sref=\"home.jobs\">Data Processing Job page</a>.</p>";

    @Inject
    protected SourceFileService sourceFileService;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Value("${pls.pa.max.concurrent.limit}")
    private int maxActivePA;

    @Override
    public ApplicationId processAnalyze(String customerSpace, ProcessAnalyzeRequest request) {
        checkPALimit(customerSpace, request);
        return cdlProxy.processAnalyze(customerSpace, request);
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
        return cdlProxy.createDataFeedTask(customerSpace, source, entity, feedType, subType, displayName, true, email,
                metaData);
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
            throw new IllegalArgumentException(String.format("Cannot find DataFeedTask %s or template is null!", taskId));
        }
        CSVImportConfig metaData = generateDataOnlyImportConfig(customerSpace, dataFeedTask.getImportTemplate().getName(),
                dataFileName, email);
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
    public UIAction cleanup(String customerSpace, String sourceFileName, SchemaInterpretation schemaInterpretation,
            CleanupOperationType cleanupOperationType) {
        BusinessEntity entity;
        UIAction uiAction = new UIAction();
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
            cdlProxy.cleanupByUpload(customerSpace, sourceFile, entity, cleanupOperationType, email);
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

    /*
     * logic provide 5 display by default if drop folder is empty then generate
     * template according to data feed task
     */
    @Override
    public List<S3ImportTemplateDisplay> getS3ImportTemplate(String customerSpace) {
        List<S3ImportTemplateDisplay> templates = new ArrayList<>();
        List<String> folderNames = dropBoxProxy.getAllSubFolders(customerSpace, null, null);
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        if (dropBoxSummary == null) {
            throw new RuntimeException("Tenant " + customerSpace //
                    + " does not have a dropbox.");
        }

        S3ImportTemplateDisplay display = null;
        if (CollectionUtils.isEmpty(folderNames)) {
            log.info(String.format("Empty path in s3 folders for tenant %s in", customerSpace));
        }
        for (String folderName : folderNames) {
            display = new S3ImportTemplateDisplay();
            DataFeedTask task = dataFeedProxy.getDataFeedTask(customerSpace, "File", folderName);
            if (task == null) {
                log.warn(String.format("Empty data feed task for tenant %s with feedtype %s", customerSpace,
                        folderName));
            } else {
                display.setPath(S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(), dropBoxSummary.getDropBox(),
                        folderName));
                display.setExist(Boolean.TRUE);
                display.setLastEditedDate(task.getLastUpdated());
                // get from data feed task
                display.setTemplateName(task.getTemplateDisplayName());
                EntityType entityType = EntityType.fromEntityAndSubType(BusinessEntity.getByName(task.getEntity()),
                        task.getSubType());
                display.setObject(entityType.getDisplayName());
                display.setFeedType(task.getFeedType());
                templates.add(display);
            }
        }
        // ensure there exists 5 templates at least in the returned list
        populateDefaultTemplate(templates);
        return templates;
    }

    private void populateDefaultTemplate(List<S3ImportTemplateDisplay> templates) {
        Set<String> existingObjects = templates.stream().map(entry -> entry.getObject()).collect(Collectors.toSet());
        for (EntityType object : EntityType.values()) {
            if (!existingObjects.contains(object.getDisplayName())) {
                S3ImportTemplateDisplay display = new S3ImportTemplateDisplay();
                display.setPath(PATHNAME);
                display.setExist(Boolean.FALSE);
                display.setObject(object.getDisplayName());
                display.setTemplateName(object.getDisplayName());
                templates.add(display);
            }
        }
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
        if (!isInternalUser(userId)) {
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

    private boolean isInternalUser(String email) {
        if (StringUtils.isBlank(email)) {
            return false;
        }

        return email.trim().toUpperCase().endsWith(LATTICE_ENGINES_COM);
    }
}
