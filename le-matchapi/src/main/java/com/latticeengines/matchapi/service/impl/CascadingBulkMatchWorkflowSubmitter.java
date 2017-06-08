package com.latticeengines.matchapi.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CascadingBulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class CascadingBulkMatchWorkflowSubmitter {

    protected WorkflowProxy workflowProxy;

    public ApplicationId submit(CascadingBulkMatchWorkflowConfiguration configuration) {
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }

    public void setWorkflowProxy(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
    }
}
