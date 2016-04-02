package com.latticeengines.pls.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.pls.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("workflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {

    private static final Log log = LogFactory.getLog(WorkflowJobService.class);

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void create(WorkflowJob workflowJob) {
        workflowJobEntityMgr.create(workflowJob);
    }

    @Override
    public List<WorkflowJob> getAll() {
        return workflowJobEntityMgr.findAll();
    }

    @Override
    public ApplicationId submit(WorkflowConfiguration configuration) {
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);

        log.info(String.format("Submitted %s with application id %s", configuration.getWorkflowName(), applicationId));

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setYarnAppId(applicationId);
        workflowJobEntityMgr.create(workflowJob);

        return ConverterUtils.toApplicationId(applicationId);
    }

    @Override
    public WorkflowStatus getWorkflowStatusFromApplicationId(String appId) {
        return workflowProxy.getWorkflowStatusFromApplicationId(appId);
    }

}
