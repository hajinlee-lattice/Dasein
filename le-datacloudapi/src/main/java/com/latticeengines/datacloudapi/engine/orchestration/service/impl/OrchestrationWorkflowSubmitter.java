package com.latticeengines.datacloudapi.engine.orchestration.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.OrchestrationWorkflowConfig;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class OrchestrationWorkflowSubmitter {
    private OrchestrationWorkflowConfig.Builder builder = new OrchestrationWorkflowConfig.Builder();
    private WorkflowProxy workflowProxy;

    public OrchestrationWorkflowSubmitter orchestrationProgress(OrchestrationProgress progress) {
        builder = builder.orchestrationProgress(progress);
        return this;
    }

    public OrchestrationWorkflowSubmitter workflowProxy(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
        return this;
    }

    public OrchestrationWorkflowSubmitter orchestration(Orchestration orch) {
        builder = builder.orchestration(orch);
        return this;
    }

    public OrchestrationWorkflowSubmitter orchestrationConfig(OrchestrationConfig config) {
        builder = builder.orchestrationConfig(config);
        return this;
    }

    public ApplicationId submit() {
        OrchestrationWorkflowConfig config = builder.build();
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(config);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }
}
