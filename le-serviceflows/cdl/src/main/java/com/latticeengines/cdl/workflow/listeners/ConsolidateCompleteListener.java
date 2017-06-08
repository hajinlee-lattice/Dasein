package com.latticeengines.cdl.workflow.listeners;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("consolidateCompleteListener")
public class ConsolidateCompleteListener extends LEJobListener {

    @Autowired
    private DataFeedProxy datafeedProxy;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String datafeedName = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_NAME);
        String customerSpace = jobExecution.getJobParameters().getString("CustomerSpace");
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            DataFeedExecution execution = datafeedProxy.finishExecution(customerSpace, datafeedName);
            if (execution.getStatus() != Status.Consolidated) {
                throw new RuntimeException("Can't finish execution");
            }
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {

        }
    }

}
