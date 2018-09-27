package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
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
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropFolderProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
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
    private DataFeedProxy detaFeedProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    private static final String SLASH = "/";
    private static final String PERIOD = ".";
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
        CSVImportConfig metaData = generateImportConfig(customerSpace.toString(), templateFileName, dataFileName,
                email);
        String taskId = cdlProxy.createDataFeedTask(customerSpace.toString(), source, entity, feedType, metaData);
        if (StringUtils.isEmpty(taskId)) {
            throw new LedpException(LedpCode.LEDP_18162, new String[] { entity, source, feedType });
        }
        return cdlProxy.submitImportJob(customerSpace.toString(), taskId, metaData);
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

    @Override
    public List<S3ImportTemplateDisplay> getS3ImportTemplate(String customerSpace) {
        List<S3ImportTemplateDisplay> templates = new ArrayList<>();
        DataFeed feed = detaFeedProxy.getDataFeed(customerSpace);
        List<DataFeedTask> dataFeedTasks = feed.getTasks();
        Set<String> entities = dataFeedTasks.stream().map(DataFeedTask::getEntity)
                .collect(Collectors.toSet());
        Map<String, List<String>> pathMap = new HashMap<>();
        for (String entity : entities) {
            List<String> paths = new ArrayList<>();
            generateDropFolderPath(customerSpace, entity, null, paths);
            // the last part in path always contains period is valid, filter out
            // invalidate path and return path without file name, maybe change
            // the logic later
            paths = paths.stream()
                    .filter(entry -> entry.lastIndexOf(SLASH) != -1
                            && entry.substring(entry.lastIndexOf(SLASH)).contains(PERIOD))
                    .map(entry -> entry.substring(0, entry.lastIndexOf(SLASH))).collect(Collectors.toList());
            pathMap.put(entity, paths);
        }
        for (DataFeedTask task : dataFeedTasks) {
            String type = task.getEntity();
            Date lastImported = task.getLastImported();
            String templateName = task.getFeedType();
            S3ImportTemplateDisplay display = new S3ImportTemplateDisplay();
            List<String> paths = pathMap.get(type);
            if (CollectionUtils.isEmpty(paths)) {
                log.info(String.format("Empty path in s3 folders for tenant %s in %s", customerSpace, type));
                continue;
            }
            String path = paths.stream().filter(entry -> entry.endsWith(templateName)).findAny().orElse(null);
            if (path == null) {
                log.info(String.format("Cannot find valid path for tenant %s", customerSpace));
                continue;
            }
            display.setPath(path);
            display.setType(type);
            display.setLastImported(lastImported);
            display.setPath(path);
            display.setTemplateName(templateName);
            List<Action> actions = actionProxy.getActions(customerSpace);
            if (CollectionUtils.isEmpty(actions)) {
                log.info(String.format("Empty actions in Databases for tenant %s.", customerSpace));
                continue;
            }
            //look for the work flow which is corresponding to a data feed task by medium action
            List<Action> dataFeedImportActions = actions.stream()
                    // tracking Pid is equivalent to work flow job Pid
                    .filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())
                            && action.getTrackingPid() != null)
                    .collect(Collectors.toList());

            Job job = null;
            for(Action action : dataFeedImportActions) {
                if (action.getActionConfiguration() instanceof ImportActionConfiguration) {
                    ImportActionConfiguration importConfiguration = (ImportActionConfiguration) action
                            .getActionConfiguration();
                    // unique id in data feed task is equivalent to data feed
                    // task id in import action configuration
                    if (task.getUniqueId().equals(importConfiguration.getDataFeedTaskId())) {
                        List<Job> importJobs = workflowProxy.getWorkflowExecutionsByJobPids(
                                Arrays.asList(action.getTrackingPid().toString()), customerSpace);
                        if (CollectionUtils.isNotEmpty(importJobs)) {
                            job = importJobs.get(0);
                            break;
                        }
                    }
                }
            }
            if (job == null) {
                log.info(String.format("Empty job in db for tenant %s with task unique id %s.", customerSpace,
                        task.getUniqueId()));
                continue;

            }
            display.setStatus(job.getJobStatus());
            templates.add(display);
        }
        return templates;
    }

    private void generateDropFolderPath(String customerSpace, String type, String path, List<String> paths) {
        List<String> nodes = dropFolderProxy.getAllSubFolders(customerSpace, type, path);
        if (path == null) { // root path
            path = "";
        }
        if (CollectionUtils.isEmpty(nodes)) {
            paths.add(path);
        }
        for (String node : nodes) {
            String nodePath = path + SLASH + node;
            generateDropFolderPath(customerSpace, type, nodePath, paths);
        }
    }
}
