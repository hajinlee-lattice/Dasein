package com.latticeengines.cdl.workflow.listeners;


import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component
public class EntityExportWorkflowListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(EntityExportWorkflowListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        sendEmail(jobExecution, false);
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        sendEmail(jobExecution, true);
    }

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private AtlasExportProxy atlasExportProxy;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    private void sendEmail(JobExecution jobExecution, boolean updateExportStatus) {
        try {
            String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
            log.info("Tenant id: " + tenantId);
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
            if (job != null) {
                String exportId = job.getInputContextValue(SegmentExportWorkflowConfiguration.SEGMENT_EXPORT_ID);
                AtlasExport atlasExport = atlasExportProxy.findAtlasExportById(tenantId, exportId);
                log.info(String.format("userId: %s; segmentExportId: %s", atlasExport.getCreatedBy(),
                        atlasExport.getUuid()));
                String jobStatus = jobExecution.getStatus().name();
                if (updateExportStatus) {
                    if (atlasExport.getStatus() == MetadataSegmentExport.Status.FAILED) {
                        jobStatus = BatchStatus.FAILED.name();
                        atlasExportProxy.updateAtlasExportStatus(tenantId, atlasExport.getUuid(),
                                MetadataSegmentExport.Status.FAILED);
                    } else {
                        atlasExportProxy.updateAtlasExportStatus(tenantId, atlasExport.getUuid(),
                                MetadataSegmentExport.Status.COMPLETED);
                    }
                }
                plsInternalProxy.sendAtlasExportEmail(jobStatus, tenantId, atlasExport);
            }
        } catch (Exception e) {
            log.error("Can not send email: " + e.getMessage());
        }
    }

}
