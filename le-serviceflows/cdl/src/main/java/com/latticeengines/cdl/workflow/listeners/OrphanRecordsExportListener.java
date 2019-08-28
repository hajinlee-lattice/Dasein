package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.serviceflows.cdl.OrphanRecordsExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("orphanRecordsExportListener")
public class OrphanRecordsExportListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(OrphanRecordsExportListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        sendEmail(jobExecution);
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        sendEmail(jobExecution);
    }

    private void sendEmail(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantId=" + tenantId);
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        if (job != null) {
            String exportId = job.getInputContextValue(OrphanRecordsExportWorkflowConfiguration.EXPORT_ID);
            String orphanType = job.getInputContextValue(OrphanRecordsExportWorkflowConfiguration.ARTIFACT_TYPE);
            String createdBy = job.getInputContextValue(OrphanRecordsExportWorkflowConfiguration.CREATED_BY);
            try {
                log.info(String.format("userId=%s; exportId=%s", CustomerSpace.parse(tenantId).toString(), exportId));
                BatchStatus jobStatus = jobExecution.getStatus();

                OrphanRecordsExportRequest request = new OrphanRecordsExportRequest();
                request.setExportId(exportId);
                request.setOrphanRecordsType(OrphanRecordsType.valueOf(orphanType));
                request.setCreatedBy(createdBy);

                if (jobStatus == BatchStatus.COMPLETED) {
                    plsInternalProxy.sendOrphanRecordsExportEmail(
                            DataCollectionArtifact.Status.READY.name(), tenantId, request);
                } else if (jobStatus.isRunning()) {
                    plsInternalProxy.sendOrphanRecordsExportEmail(
                            DataCollectionArtifact.Status.GENERATING.name(), tenantId, request);
                }
            } catch (Exception e) {
                log.error("Can not send email: " + e.getMessage());
            }
        }
    }

}
