package com.latticeengines.cdl.workflow.listeners;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("dataFeedTaskImportListener")
public class DataFeedTaskImportListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskImportListener.class);

    private static final String ERROR_FILE = "error.csv";

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataLoaderService dataLoaderService;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String applicationId = job.getOutputContextValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        String importJobIdentifier = job
                .getInputContextValue(WorkflowContextConstants.Inputs.DATAFEEDTASK_IMPORT_IDENTIFIER);
        Boolean sendS3ImportEmail = Boolean.valueOf(
                job.getInputContextValue(WorkflowContextConstants.Inputs.S3_IMPORT_EMAIL_FLAG));
        String customerSpace = job.getTenant().getId();
        TenantEmailNotificationLevel notificationLevel = job.getTenant().getNotificationLevel();
        log.info("tenant " + job.getTenant().getId() + " notification_level is: " + job.getTenant().getNotificationLevel().name());
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (eaiImportJobDetail == null) {
            log.warn(String.format("Cannot find the job detail for %s", applicationId));
            return;
        }

        String emailInfoStr = job.getInputContextValue(WorkflowContextConstants.Inputs.S3_IMPORT_EMAIL_INFO);
        S3ImportEmailInfo emailInfo = null;
        if (StringUtils.isEmpty(emailInfoStr)) {
            log.error("Cannot get email info for import job: " + applicationId);
        } else {
            emailInfo = JsonUtils.deserialize(emailInfoStr, S3ImportEmailInfo.class);
        }

        VdbLoadTableStatus vdbLoadTableStatus = null;
        String statusUrl = eaiImportJobDetail.getReportURL();
        String queryHandle = eaiImportJobDetail.getQueryHandle();
        if (statusUrl != null && !statusUrl.isEmpty() && queryHandle != null && !queryHandle.isEmpty()) {
            vdbLoadTableStatus = new VdbLoadTableStatus();
            vdbLoadTableStatus.setVdbQueryHandle(queryHandle);
        }

        if (jobExecution.getStatus().isUnsuccessful()) {
            String result = "Failed";
            if (sendS3ImportEmail && notificationLevel.compareTo(TenantEmailNotificationLevel.ERROR) >= 0) {
                String message = "Failed to import s3 file, please contact Lattice admin.";
                sendS3ImportEmail(customerSpace, result,
                        importJobIdentifier, emailInfo, message);
            }
            List<String> pathList = eaiImportJobDetail.getPathDetail();
            if (CollectionUtils.isNotEmpty(pathList)) {
                setErrorFileContext(pathList, job);
                workflowJobEntityMgr.updateWorkflowJob(job);
            }
            updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            try {
                ImportActionConfiguration importActionConfiguration = new ImportActionConfiguration();
                importActionConfiguration.setWorkflowId(job.getPid());
                importActionConfiguration.setDataFeedTaskId(importJobIdentifier);
                importActionConfiguration.setImportCount(0L);
                List<String> registeredTables;
                String templateName = eaiImportJobDetail.getTemplateName();
                List<String> pathList = eaiImportJobDetail.getPathDetail();
                List<String> processedRecordsList = eaiImportJobDetail.getPRDetail();
                if (pathList == null || processedRecordsList == null
                        || pathList.size() != processedRecordsList.size()) {
                    log.error("Error in extract info, skip register extract.");
                    updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);

                    if (vdbLoadTableStatus != null) {
                        vdbLoadTableStatus.setJobStatus("Succeed");
                        vdbLoadTableStatus.setMessage("Skip register extract.");
                        dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
                    }
                    updateImportActionConfiguration(job, importActionConfiguration);
                    return;
                }
                setErrorFileContext(pathList, job);
                List<Extract> extracts = new ArrayList<>();
                for (int i = 0; i < pathList.size(); i++) {
                    log.info(
                            String.format("Extract %s have %s records.", pathList.get(i), processedRecordsList.get(i)));
                    long records = Long.parseLong(processedRecordsList.get(i));
                    if (records > 0) {
                        extracts.add(createExtract(pathList.get(i), records));
                    }
                }
                if (extracts.size() > 0) {
                    Long totalRecords = 0L;
                    if (extracts.size() == 1) {
                        log.info(String.format("Register single extract: %s", extracts.get(0).getName()));
                        totalRecords = extracts.get(0).getProcessedRecords();
                        registeredTables = dataFeedProxy.registerExtract(customerSpace, importJobIdentifier,
                                templateName, extracts.get(0));
                    } else {
                        log.info(String.format("Register %d extracts.", extracts.size()));
                        for (Extract extract : extracts) {
                            totalRecords += extract.getProcessedRecords();
                        }
                        registeredTables = dataFeedProxy.registerExtracts(customerSpace, importJobIdentifier,
                                templateName, extracts);
                    }
                    importActionConfiguration.setImportCount(totalRecords);
                    importActionConfiguration.setRegisteredTables(registeredTables);
                    log.info(String.format("Registered Tables are: %s", String.join(",", registeredTables)));
                }
                workflowJobEntityMgr.updateWorkflowJob(job);
                updateImportActionConfiguration(job, importActionConfiguration);
                updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.SUCCESS);
                updateDataFeedStatus(customerSpace);

                if (vdbLoadTableStatus != null) {
                    vdbLoadTableStatus.setJobStatus("Succeed");
                    vdbLoadTableStatus.setMessage("Load table complete!");
                    dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
                }
                String result = "Success";
                if (sendS3ImportEmail && notificationLevel.compareTo(TenantEmailNotificationLevel.INFO) >= 0) {
                    sendS3ImportEmail(customerSpace, result,
                            importJobIdentifier, emailInfo, null);
                }
            } catch (Exception e) {
                updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);

                if (vdbLoadTableStatus != null) {
                    vdbLoadTableStatus.setJobStatus("Failed");
                    vdbLoadTableStatus.setMessage(String.format("Load table failed with exception: %s", e.toString()));
                    dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
                }
                String result = "Failed";
                if (sendS3ImportEmail && notificationLevel.compareTo(TenantEmailNotificationLevel.ERROR) >= 0) {
                    sendS3ImportEmail(customerSpace, result,
                            importJobIdentifier, emailInfo, e.getMessage());
                }
            }

        } else {
            log.error(String.format("DataFeedTask import job ends in unknown status: %s",
                    jobExecution.getStatus().name()));
            updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);

            if (vdbLoadTableStatus != null) {
                vdbLoadTableStatus.setJobStatus("Failed");
                vdbLoadTableStatus.setMessage(String.format("DataFeedTask import job ends in unknown status: %s",
                        jobExecution.getStatus().name()));
                dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
            }
        }
    }

    private void sendS3ImportEmail(String customerSpace, String result,
                                   String taskId, S3ImportEmailInfo emailInfo, String message) {
        try {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskId);
            if (dataFeedTask != null && emailInfo != null) {
                if (StringUtils.isNotEmpty(message)) {
                    emailInfo.setErrorMsg(message);
                }
                String tenantId = CustomerSpace.parse(customerSpace).toString();
                plsInternalProxy.sendS3ImportEmail(result, tenantId, emailInfo);
            }
        } catch (Exception e) {
            log.error("Failed to send s3 import email: " + e.getMessage());
        }
    }

    private void setErrorFileContext(List<String> pathList, WorkflowJob job) {
        List<String> errorFiles = new ArrayList<>();

        pathList.forEach(path -> {
            try {
                String dirPath = path.substring(0, path.lastIndexOf("*.avro"));
                log.info("Diagnostic file path: " + dirPath);
                if (HdfsUtils.fileExists(yarnConfiguration, dirPath + ERROR_FILE)) {
                    Matcher matcher = Pattern.compile("^hdfs://(?<cluster>[^/]+)/Pods/(?<tail>.*)").matcher(dirPath);
                    if (matcher.matches()) {
                        errorFiles.add("/Pods/" + matcher.group("tail") + ERROR_FILE);
                    } else {
                        errorFiles.add(dirPath + ERROR_FILE);
                    }
                }
            } catch (IOException e) {
                log.error("Check error file existence error.");
            }
        });

        if (CollectionUtils.isNotEmpty(errorFiles)) {
            job.setOutputContextValue(WorkflowContextConstants.Outputs.DATAFEEDTASK_IMPORT_ERROR_FILES,
                    JsonUtils.serialize(errorFiles));
        } else {
            log.info("Error file list empty.");
        }
    }

    private void updateImportActionConfiguration(WorkflowJob job, ImportActionConfiguration importActionConfiguration) {
        String ActionPidStr = job.getInputContextValue(WorkflowContextConstants.Inputs.ACTION_ID);
        if (ActionPidStr != null) {
            Long pid = Long.parseLong(ActionPidStr);
            Action action = actionProxy.getActionsByPids(job.getTenant().getId(), Collections.singletonList(pid))
                    .get(0);
            if (action != null) {
                action.setActionConfiguration(importActionConfiguration);
                actionProxy.updateAction(job.getTenant().getId(), action);
            } else {
                log.warn(String.format("Action with pid=%d cannot be found", pid));
            }
        } else {
            log.warn(String.format("ActionPid is not correctly registered by workflow job=%d", job.getWorkflowId()));
        }
    }

    private void updateDataFeedStatus(String customerSpace) {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace);
        if (dataFeed.getStatus() == DataFeed.Status.Initialized) {
            dataFeedProxy.updateDataFeedStatus(customerSpace, DataFeed.Status.InitialLoaded.getName());
        }
    }

    private void updateEaiImportJobDetail(EaiImportJobDetail eaiImportJobDetail, ImportStatus importStatus) {
        eaiImportJobDetail.setStatus(importStatus);
        eaiJobDetailProxy.updateImportJobDetail(eaiImportJobDetail);
    }

    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }
}
