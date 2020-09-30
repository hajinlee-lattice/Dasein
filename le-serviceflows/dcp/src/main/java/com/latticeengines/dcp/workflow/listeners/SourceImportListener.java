package com.latticeengines.dcp.workflow.listeners;

import static com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration.ANALYSIS_PERCENTAGE;
import static com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration.INGESTION_PERCENTAGE;
import static com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration.MATCH_PERCENTAGE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.matchapi.UsageProxy;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("sourceImportListener")
public class SourceImportListener extends LEJobListener {

    public static final Logger log = LoggerFactory.getLogger(SourceImportListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private EmailProxy emailProxy;

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Inject
    private DataReportProxy reportProxy;

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private UsageProxy usageProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        log.info("Finish Source Import!");
        triggerReportWorkflow(jobExecution);
        sendEmail(jobExecution);
    }

    private void sendEmail(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantId=" + tenantId);
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        if (job == null) {
            log.error("Cannot locate workflow job with id {}", jobExecution.getId());
            throw new IllegalArgumentException("Cannot locate workflow job with id " + jobExecution.getId());
        }

        String uploadId = job.getInputContextValue(DCPSourceImportWorkflowConfiguration.UPLOAD_ID);
        String projectId = job.getInputContextValue(DCPSourceImportWorkflowConfiguration.PROJECT_ID);
        String sourceId = job.getInputContextValue(DCPSourceImportWorkflowConfiguration.SOURCE_ID);

        ProjectDetails project = projectProxy.getDCPProjectByProjectId(tenantId, projectId, Boolean.FALSE, null);
        UploadDetails upload = uploadProxy.getUploadByUploadId(tenantId, uploadId, Boolean.FALSE);
        Source source = sourceProxy.getSource(tenantId, sourceId);

        BatchStatus jobStatus = jobExecution.getStatus();
        UploadDiagnostics uploadDiagnostics = new UploadDiagnostics();
        uploadDiagnostics.setApplicationId(job.getApplicationId());
        if (BatchStatus.COMPLETED.equals(jobStatus)) {
            uploadProxy.updateUploadStatus(tenantId, uploadId, Upload.Status.FINISHED, uploadDiagnostics);
            uploadProxy.updateProgressPercentage(tenantId, uploadId, ANALYSIS_PERCENTAGE);

            UploadConfig uploadConfig = upload.getUploadConfig();
            String usageReportFilePath = copyUsageReportToS3(uploadConfig.getUsageReportFilePath(), tenantId, uploadId);
            uploadConfig.setUsageReportFilePath(usageReportFilePath);
            uploadProxy.updateUploadConfig(tenantId, uploadId, upload.getUploadConfig());
        } else {
            if (jobStatus.isUnsuccessful()) {
                log.info("SourceImport workflow job {} failed with status {}", jobExecution.getId(), jobStatus);
            } else {
                log.error("SourceImport workflow job {} failed with unknown status {}", jobExecution.getId(), jobStatus);
            }
            List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
            String processPercentage = "0";
            if (exceptions.size() > 0) {
                Throwable exception = exceptions.get(0);

                ErrorDetails details;
                if (exception instanceof LedpException) {
                    LedpException casted = (LedpException) exception;
                    details = casted.getErrorDetails();
                } else {
                    details = new ErrorDetails(LedpCode.LEDP_00002, exception.getMessage(),
                            ExceptionUtils.getStackTrace(exception));
                }
                uploadDiagnostics.setLastErrorMessage(JsonUtils.serialize(details));
                String lastStepName = getLastStepName(jobExecution);
                switch (lastStepName) {
                    case "startImportSource":
                    case "importSource":
                    case "getStartTime":
                        uploadDiagnostics.setLastErrorStep("Ingestion");
                        processPercentage = INGESTION_PERCENTAGE;
                        break;
                    case "matchImport":
                        uploadDiagnostics.setLastErrorStep("Match");
                        processPercentage = MATCH_PERCENTAGE;
                        break;
                    case "splitImportMatchResult":
                    case "finishImportSource":
                        uploadDiagnostics.setLastErrorStep("Analysis");
                        processPercentage = ANALYSIS_PERCENTAGE;
                        break;
                    default:
                        break;
                }
            }
            uploadProxy.updateProgressPercentage(tenantId, uploadId, processPercentage);
            uploadProxy.updateUploadStatus(tenantId, uploadId, Upload.Status.ERROR, uploadDiagnostics);
        }

        UploadEmailInfo uploadEmailInfo = new UploadEmailInfo();
        uploadEmailInfo.setProjectId(projectId);
        uploadEmailInfo.setSourceId(sourceId);
        uploadEmailInfo.setUploadId(uploadId);
        uploadEmailInfo.setProjectDisplayName(project.getProjectDisplayName());
        uploadEmailInfo.setSourceDisplayName(source.getSourceDisplayName());
        uploadEmailInfo.setUploadDisplayName(upload.getDisplayName());
        uploadEmailInfo.setRecipientList(project.getRecipientList());
        uploadEmailInfo.setJobStatus(jobStatus.name());
        log.info("Send SourceImport workflow status email {}", JsonUtils.serialize(uploadEmailInfo));
        emailProxy.sendUploadEmail(uploadEmailInfo);
    }

    private String copyUsageReportToS3(String reportFilePath, String tenantId, String uploadId) {
        SubmitBatchReportRequest batchReport = new SubmitBatchReportRequest();
        batchReport.setBatchRef(tenantId + "_" + uploadId);
        VboBatchUsageReport vboBatchUsageReport = usageProxy.submitBatchReport(batchReport);

        String s3PathDir = vboBatchUsageReport.getS3Prefix();
        log.info("Copy from " + reportFilePath + " to " + s3PathDir);
        if(!s3Service.objectExist(vboBatchUsageReport.getS3Bucket(), s3PathDir)) {
            s3Service.createFolder(vboBatchUsageReport.getS3Bucket(), s3PathDir);
        }
        try {
            List<String> csvFiles = HdfsUtils.getFilesForDir(yarnConfiguration, reportFilePath,
                    (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv"));
            for (String csvFile: csvFiles){
                Long fileSize = HdfsUtils.getFileSize(yarnConfiguration, csvFile);
                String filename = FilenameUtils.getName(csvFile);
                String dstPath = Paths.get(s3PathDir, filename).toString();
                RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                        Collections.singleton(AmazonS3Exception.class), null);
                retry.execute(context -> {
                    if (context.getRetryCount() > 0) {
                        log.info(String.format("(Attempt=%d) Retry copying file from hdfs://%s to s3://%s/%s", //
                                context.getRetryCount() + 1, reportFilePath, vboBatchUsageReport.getS3Bucket(), dstPath));
                    }
                    try (InputStream stream = HdfsUtils.getInputStream(yarnConfiguration, reportFilePath)) {
                        s3Service.uploadInputStreamMultiPart(vboBatchUsageReport.getS3Bucket(), dstPath, stream, fileSize);

                    }
                    return true;
                });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return s3PathDir;
    }

    private String getLastStepName(JobExecution jobExecution) {
        StepExecution lastStepExecution = null;
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            if(lastStepExecution == null){
                lastStepExecution = stepExecution;
            } else {
                if(stepExecution.getStartTime().after(lastStepExecution.getStartTime())){
                    lastStepExecution = stepExecution;
                }
            }
        }
        return lastStepExecution.getStepName();
    }

    private void triggerReportWorkflow(JobExecution jobExecution) {
        if (BatchStatus.COMPLETED.equals(jobExecution.getStatus())) {
            String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
            log.info("tenantId=" + tenantId);
            String rootId = CustomerSpace.parse(tenantId).toString();
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
            if (job == null) {
                log.error("Cannot locate workflow job with id {}", jobExecution.getId());
                throw new IllegalArgumentException("Cannot locate workflow job with id " + jobExecution.getId());
            }
            String uploadId = job.getInputContextValue(DCPSourceImportWorkflowConfiguration.UPLOAD_ID);

            dataReportProxy.copyDataReportToParent(tenantId, DataReportRecord.Level.Upload, uploadId);
            // update the report to be ready for rollup
            reportProxy.updateDataReport(tenantId, DataReportRecord.Level.Upload, uploadId);
            // comment out the data report logic for now
//            Boolean hasUnterminalUploads = uploadProxy.hasUnterminalUploads(tenantId, uploadId);
//            DataReport report = reportProxy.getDataReport(tenantId, DataReportRecord.Level.Tenant, rootId);
//            long refreshTime = report.getRefreshTimestamp() == null ? 0L : report.getRefreshTimestamp();
//            long now = Instant.now().toEpochMilli();
//            boolean moreThan4HoursSinceRefresh = now - refreshTime > TimeUnit.HOURS.toMillis(4);
//            boolean shouldTriggerRollup = moreThan4HoursSinceRefresh && Boolean.FALSE.equals(hasUnterminalUploads);
//            log.info("last refresh time is {}, current time is {}, " + //
//                            "moreThan4HoursSinceRefresh={}, hasUnterminalUploads={}: shouldTriggerRollup={}", //
//                    refreshTime, now, moreThan4HoursSinceRefresh, hasUnterminalUploads, shouldTriggerRollup);
//            if (shouldTriggerRollup) {
//                DCPReportRequest request = new DCPReportRequest();
//                request.setMode(DataReportMode.UPDATE);
//                request.setLevel(DataReportRecord.Level.Tenant);
//                request.setRootId(rootId);
//                log.info("Sending request to rollup data report.");
//                reportProxy.rollupDataReport(tenantId, request);
//            }
        }
    }
}
