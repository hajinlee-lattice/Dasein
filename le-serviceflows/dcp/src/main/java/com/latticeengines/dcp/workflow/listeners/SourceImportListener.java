package com.latticeengines.dcp.workflow.listeners;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("sourceImportListener")
public class SourceImportListener extends LEJobListener {

    public static final Logger log = LoggerFactory.getLogger(SourceImportListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private UploadProxy uploadProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        log.info("Finish Source Import!");
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

        ProjectDetails project = projectProxy.getDCPProjectByProjectId(tenantId, projectId);

        BatchStatus jobStatus = jobExecution.getStatus();
        UploadDiagnostics uploadDiagnostics = new UploadDiagnostics();
        uploadDiagnostics.setApplicationId(job.getApplicationId());
        if (BatchStatus.COMPLETED.equals(jobStatus)) {
            uploadProxy.updateUploadStatus(tenantId, uploadId, Upload.Status.FINISHED, uploadDiagnostics);
        } else {
            if (jobStatus.isUnsuccessful()) {
                log.info("SourceImport workflow job {} failed with status {}", jobExecution.getId(), jobStatus);
            } else {
                log.error("SourceImport workflow job {} failed with unknown status {}", jobExecution.getId(), jobStatus);
            }
            List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
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
            }
            uploadProxy.updateUploadStatus(tenantId, uploadId, Upload.Status.ERROR, uploadDiagnostics);
        }

        UploadEmailInfo uploadEmailInfo = new UploadEmailInfo();
        uploadEmailInfo.setProjectId(projectId);
        uploadEmailInfo.setSourceId(sourceId);
        uploadEmailInfo.setUploadId(uploadId);
        uploadEmailInfo.setRecipientList(project.getRecipientList());
        uploadEmailInfo.setJobStatus(jobStatus.name());
        log.info("Send SourceImport workflow status email {}", JsonUtils.serialize(uploadEmailInfo));
        plsInternalProxy.sendUploadEmail(uploadEmailInfo);
    }
}
