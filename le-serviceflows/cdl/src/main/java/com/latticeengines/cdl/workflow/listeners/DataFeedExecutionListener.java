package com.latticeengines.cdl.workflow.listeners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("datafeedExecutionListener")
public class DataFeedExecutionListener extends LEJobListener {

    private static final Log log = LogFactory.getLog(DataFeedExecutionListener.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String datafeedName = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_NAME);
        String customerSpace = jobExecution.getJobParameters().getString("CustomerSpace");

        metadataProxy.updateExecutionWorkflowId(customerSpace, datafeedName, jobExecution.getId());
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String datafeedName = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_NAME);
        String customerSpace = jobExecution.getJobParameters().getString("CustomerSpace");
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            DataFeedExecution execution = metadataProxy.finishExecution(customerSpace, datafeedName);
            if (execution.getStatus() != Status.Consolidated) {
                throw new RuntimeException("Can't finish execution");
            }
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.error("workflow failed!");
            DataFeedExecution execution = metadataProxy.failExecution(customerSpace, datafeedName);
            if (execution.getStatus() != Status.Failed) {
                throw new RuntimeException("Can't fail execution");
            }
        }
    }

}
