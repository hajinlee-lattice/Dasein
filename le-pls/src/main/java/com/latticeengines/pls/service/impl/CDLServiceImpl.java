package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
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
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropFolderProxy;
import com.latticeengines.db.exposed.util.MultiTenantContext;

@Component("cdlService")
public class CDLServiceImpl implements CDLService {

    private static final Logger log = LoggerFactory.getLogger(CDLServiceImpl.class);

    @Inject
    protected SourceFileService sourceFileService;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DropFolderProxy dropFolderProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;
    private static final String PREFIX = "/templates/";
    private static final String TEMPLATENAME = "N/A";
    private static final String PATHNAME = "N/A";
    private static final String DELETE_SUCCESS_TITLE = "Success! Delete Action has been submitted.";
    private static final String DELETE_FAIL_TITLE = "Validation Error";
    public static final String DELETE_SUCCESSE_MSG = "<p>The delete action will be scheduled to process and analyze after validation. You can track the status from the <a ui-sref=\"home.jobs\">Data Processing Job page</a>.</p>";

    @Override
    public ApplicationId processAnalyze(String customerSpace, ProcessAnalyzeRequest request) {
        return cdlProxy.processAnalyze(customerSpace, request);
    }

    @Override
    public ApplicationId submitCSVImport(String customerSpace, String templateFileName, String dataFileName,
                                         String source, String entity, String feedType) {
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("The email of the file upload initiator is %s", email));
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateFileName, dataFileName,
                email);
        // temp fix for file upload directly.
        String subType = "";
        if (feedType.contains("Bundle")) {
            subType = "Bundle";
        } else if (feedType.contains("Hierarchy")) {
            subType = "Hierarchy";
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
        String taskId = cdlProxy.createDataFeedTask(customerSpace, source, entity, feedType, subType, displayName,
                metaData);
        return taskId;
    }

    @Override
    public ApplicationId submitS3ImportWithTemplateData(String customerSpace, String taskId, String templateFileName) {
        String email = MultiTenantContext.getEmailAddress();
        log.info(String.format("The email of the s3 file upload initiator is %s", email));
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateFileName, templateFileName, email);
        return cdlProxy.submitImportJob(customerSpace, taskId, metaData);
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
            uiAction.setMessage(generateDeleteResultMsg(
                    String.format("<p>Cleanup operation does not support schema: %s </p>",
                            schemaInterpretation.name())));
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
            uiAction.setMessage(
                    generateDeleteResultMsg(
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
        List<String> folderNames = dropFolderProxy.getAllSubFolders(customerSpace, null, null);
        S3ImportTemplateDisplay display = null;
        if (CollectionUtils.isEmpty(folderNames)) {
            log.info("Empty path in s3 folders for tenant %s in", customerSpace);
        }
        for (String folderName : folderNames) {
            display = new S3ImportTemplateDisplay();
            DataFeedTask task = dataFeedProxy.getDataFeedTask(customerSpace, "File", folderName);
            if (task == null) {
                log.warn("Empty data feed task for tenant %s in %s with feedtype %s", customerSpace, folderName);
            } else {
                display.setPath(PREFIX + folderName);
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
        for (String object : EntityType.getNames()) {
            if (!existingObjects.contains(object)) {
                S3ImportTemplateDisplay display = new S3ImportTemplateDisplay();
                display.setPath(PATHNAME);
                display.setExist(Boolean.FALSE);
                display.setObject(object);
                display.setTemplateName(TEMPLATENAME);
                templates.add(display);
            }
        }
    }


}
