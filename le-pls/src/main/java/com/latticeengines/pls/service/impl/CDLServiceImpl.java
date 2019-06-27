package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldCategory;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.pls.metadata.resolution.MetadataResolver;
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

    @Value("${cdl.activity.based.pa}")
    boolean isActivityBasedPA;

    private List<String> templateMappingHeaders = Arrays.asList("Field Type", "Your Field Name", "Lattice Field Name", "Data Type");

    private static final String CUSTOM = "Custom";
    private static final String STANDARD = "Standard";
    private static final String UNMAPPED = "unmapped";

    @Override
    public ApplicationId processAnalyze(String customerSpace, ProcessAnalyzeRequest request) {
        checkPALimit(customerSpace, request);
        if (isActivityBasedPA) {
            return cdlProxy.scheduleProcessAnalyze(customerSpace, false, request);
        } else {
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
            throw new LedpException(LedpCode.LEDP_18162, new String[]{entity, source, feedType});
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
        importFileInfo.setPartialFile(dataSourceFile.isPartialFile());
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
        List<String> folderNames = dropBoxProxy.getAllSubFolders(customerSpace, null, null, null);
        log.info("folderNames is : " + folderNames.toString());
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        if (dropBoxSummary == null) {
            throw new RuntimeException("Tenant " + customerSpace //
                    + " does not have a dropbox.");
        }
        if (CollectionUtils.isEmpty(folderNames)) {
            log.info(String.format("Empty path in s3 folders for tenant %s in", customerSpace));
        }
        for (String folderName : folderNames) {
            DataFeedTask task = dataFeedProxy.getDataFeedTask(customerSpace, "File", folderName);
            if (task == null) {
                EntityType entityType =
                        EntityType.fromFeedTypeName(S3PathBuilder.getFolderNameFromFeedType(folderName));
                if (entityType != null) {
                    S3ImportTemplateDisplay display = new S3ImportTemplateDisplay();
                    display.setPath(S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(), dropBoxSummary.getDropBox(),
                            folderName));
                    display.setExist(Boolean.FALSE);
                    display.setTemplateName(entityType.getDefaultFeedTypeName());
                    display.setEntity(entityType.getEntity());
                    display.setObject(entityType.getDisplayName());
                    display.setFeedType(folderName);
                    display.setS3ImportSystem(getS3ImportSystem(customerSpace,
                            S3PathBuilder.getSystemNameFromFeedType(folderName)));
                    display.setImportStatus(DataFeedTask.S3ImportStatus.Pause);
                    templates.add(display);
                }
            } else {
                S3ImportTemplateDisplay display = new S3ImportTemplateDisplay();
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
                display.setEntity(entityType.getEntity());
                display.setS3ImportSystem(getS3ImportSystem(customerSpace,
                        S3PathBuilder.getSystemNameFromFeedType(folderName)));
                display.setImportStatus(task.getS3ImportStatus() == null ?
                        DataFeedTask.S3ImportStatus.Pause : task.getS3ImportStatus());
                templates.add(display);
            }
        }
        // ensure there exists 5 templates at least in the returned list
        populateDefaultTemplate(templates);
        return templates;
    }

    @Override
    public List<FileProperty> getFileListForS3Path(String customerSpace, String s3Path, String filter) {
        return dropBoxProxy.getFileListForPath(customerSpace, s3Path, filter);
    }

    @Override
    public void createS3ImportSystem(String customerSpace, String systemDisplayName,
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
        cdlProxy.createS3ImportSystem(customerSpace, s3ImportSystem);
        dropBoxProxy.createTemplateFolder(customerSpace, systemName, null, null);
    }

    @Override
    public S3ImportSystem getS3ImportSystem(String customerSpace, String systemName) {
        return cdlProxy.getS3ImportSystem(customerSpace, systemName);
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
    public List<TemplateFieldPreview> getTemplatePreview(String customerSpace, Table templateTable, Table standardTable) {
        List<TemplateFieldPreview> templatePreview =
                templateTable.getAttributes().stream().map(this::getFieldPreviewFromAttribute).collect(Collectors.toList());
        List<TemplateFieldPreview> standardPreview =
                standardTable.getAttributes().stream().map(this::getFieldPreviewFromAttribute).collect(Collectors.toList());
        Set<String> standardAttrNames = standardTable.getAttributes().stream().map(Attribute::getName).collect(Collectors.toSet());
        for (TemplateFieldPreview fieldPreview : templatePreview) {
            if (standardAttrNames.contains(fieldPreview.getNameInTemplate())) {
                fieldPreview.setFieldCategory(FieldCategory.LatticeField);
                standardPreview.removeIf(preview -> preview.getNameInTemplate().equals(fieldPreview.getNameInTemplate()));
            } else {
                fieldPreview.setFieldCategory(FieldCategory.CustomField);
            }
        }
        standardPreview.forEach(preview -> {
            preview.setUnmapped(true);
            preview.setFieldCategory(FieldCategory.LatticeField);
        });
        templatePreview.addAll(standardPreview);
        return templatePreview;
    }

    @Override
    public boolean autoImport(String templateFileName) {
        SourceFile sourceFile = getSourceFile(templateFileName);
        if (sourceFile != null && !sourceFile.isPartialFile()) {
            return true;
        }
        return false;
    }

    private void appendTemplateMapptingValue(StringBuffer fileContent, String value) {
        fileContent.append(value);
        fileContent.append(",");
    }

    // append field type for the file
    private void appendFieldType(StringBuffer fileContent, Attribute attribute) {
        UserDefinedType userDefinedType =
                MetadataResolver.getFieldTypeFromPhysicalType(attribute.getPhysicalDataType());
        if (userDefinedType.equals(UserDefinedType.DATE)) {
            StringBuffer formatStr = new StringBuffer();
            if (!StringUtils.isBlank(attribute.getDateFormatString())) {
                formatStr.append(attribute.getDateFormatString());
                formatStr.append(" ");
            }
            if (!StringUtils.isBlank(attribute.getTimeFormatString())) {
                formatStr.append(attribute.getTimeFormatString());
                formatStr.append(" ");
            }
            if (!StringUtils.isBlank(attribute.getTimezone())) {
                formatStr.append(attribute.getTimezone());
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
    public String getTemplateMappingContent(Table templateTable, Table standardTable) {
        StringBuffer fileContent = new StringBuffer();
        for (String templateMappingHeader : templateMappingHeaders) {
            appendTemplateMapptingValue(fileContent, templateMappingHeader);
        }
        fileContent.deleteCharAt(fileContent.length() - 1);
        fileContent.append("\n");
        Map<String, Attribute> standardAttrMap =
                standardTable.getAttributes().stream().collect(Collectors.toMap(Attribute::getName, Attribute -> Attribute));
        for (Attribute attribute : templateTable.getAttributes()) {
            if (standardAttrMap.containsKey(attribute.getName())) {
                standardAttrMap.remove(attribute.getName());
                appendTemplateMapptingValue(fileContent, STANDARD);
            } else {
                appendTemplateMapptingValue(fileContent, CUSTOM);
            }
            appendTemplateMapptingValue(fileContent, attribute.getDisplayName());
            appendTemplateMapptingValue(fileContent, attribute.getName());
            appendFieldType(fileContent, attribute);
            fileContent.append("\n");
        }
        Set<Map.Entry<String, Attribute>> unmappedAttributes = standardAttrMap.entrySet();
        for (Map.Entry<String, Attribute> entry : unmappedAttributes) {
            Attribute attribute = entry.getValue();
            appendTemplateMapptingValue(fileContent, STANDARD);
            appendTemplateMapptingValue(fileContent, UNMAPPED);
            appendTemplateMapptingValue(fileContent, attribute.getName());
            appendFieldType(fileContent, attribute);
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

    private TemplateFieldPreview getFieldPreviewFromAttribute(Attribute attribute) {
        TemplateFieldPreview fieldPreview = new TemplateFieldPreview();
        fieldPreview.setNameInTemplate(attribute.getName());
        fieldPreview.setNameFromFile(attribute.getDisplayName());
        fieldPreview.setFieldType(MetadataResolver.getFieldTypeFromPhysicalType(attribute.getPhysicalDataType()));
        if (UserDefinedType.DATE.equals(fieldPreview.getFieldType())) {
            fieldPreview.setDateFormatString(attribute.getDateFormatString());
            fieldPreview.setTimeFormatString(attribute.getTimeFormatString());
            fieldPreview.setTimezone(attribute.getTimezone());
        }
        return fieldPreview;
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
                display.setImportStatus(DataFeedTask.S3ImportStatus.Pause);
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

}
