package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("registerDeleteDataListener")
public class RegisterDeleteDataListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(RegisterDeleteDataListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private ActionProxy actionProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String customerSpace = job.getTenant().getId();

    }
}
