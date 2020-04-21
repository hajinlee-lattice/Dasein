package com.latticeengines.dcp.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("sourceImportListener")
public class SourceImportListener extends LEJobListener {

    public static final Logger log = LoggerFactory.getLogger(SourceImportListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private ProjectProxy projectProxy;

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
        if (job != null) {
            String uploadId = job.getInputContextValue(DCPSourceImportWorkflowConfiguration.UPLOAD_ID);
            String projectId = job.getInputContextValue(DCPSourceImportWorkflowConfiguration.PROJECT_ID);
            String sourceId = job.getInputContextValue(DCPSourceImportWorkflowConfiguration.SOURCE_ID);
            ;
            ProjectDetails project = projectProxy.getDCPProjectByProjectId(tenantId, projectId);

            log.info("tenantId=" + tenantId);
            BatchStatus jobStatus = jobExecution.getStatus();

            UploadEmailInfo uploadEmailInfo = new UploadEmailInfo();
            uploadEmailInfo.setProjectId(projectId);
            uploadEmailInfo.setSourceId(sourceId);
            uploadEmailInfo.setUploadId(uploadId);
            uploadEmailInfo.setRecipientList(project.getRecipientList());

            if (jobStatus == BatchStatus.COMPLETED) {
                uploadProxy.sendUploadCompletedEmail(tenantId, uploadEmailInfo);
            } else if (jobStatus == BatchStatus.FAILED) {
                uploadProxy.sendUploadCompletedEmail();
            }
        }
    }
}