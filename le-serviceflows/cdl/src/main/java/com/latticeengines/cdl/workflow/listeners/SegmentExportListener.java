package com.latticeengines.cdl.workflow.listeners;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("segmentExportListener")
public class SegmentExportListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportListener.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Value("${common.pls.url}")
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

                com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy emailProxy //
                        = new com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy(
                                internalResourceHostPort);
                emailProxy.sendMetadataSegmentExportEmail(jobExecution.getStatus().name(), tenantId,
                        metadataSegmentExport);
            } catch (Exception e) {
                log.error("Can not send email: " + e.getMessage());
            }
        }
    }

}
