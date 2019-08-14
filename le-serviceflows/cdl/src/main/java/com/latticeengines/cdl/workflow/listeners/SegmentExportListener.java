package com.latticeengines.cdl.workflow.listeners;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("segmentExportListener")
public class SegmentExportListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

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
        log.info("tenantid: " + tenantId);
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        if (job != null) {
            String exportId = job.getInputContextValue(SegmentExportWorkflowConfiguration.SEGMENT_EXPORT_ID);
            try {
                MetadataSegmentExport metadataSegmentExport = internalResourceRestApiProxy
                        .getMetadataSegmentExport(CustomerSpace.parse(tenantId), exportId);
                log.info(String.format("userId: %s; segmentExportId: %s", metadataSegmentExport.getCreatedBy(),
                        metadataSegmentExport.getExportId()));

                com.latticeengines.workflow.exposed.build.InternalResourceRestApiProxy emailProxy //
                        = new com.latticeengines.workflow.exposed.build.InternalResourceRestApiProxy(
                                internalResourceHostPort);
                String jobStatus = jobExecution.getStatus().name();
                if (metadataSegmentExport.getStatus() == Status.FAILED) {
                    jobStatus = BatchStatus.FAILED.name();
                }
                emailProxy.sendMetadataSegmentExportEmail(jobStatus, tenantId, metadataSegmentExport);
            } catch (Exception e) {
                log.error("Can not send email: " + e.getMessage());
            }
        }
    }

}
