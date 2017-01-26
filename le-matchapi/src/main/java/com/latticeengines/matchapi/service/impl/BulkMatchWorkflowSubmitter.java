package com.latticeengines.matchapi.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.datacloud.workflow.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class BulkMatchWorkflowSubmitter {

    private BulkMatchWorkflowConfiguration.Builder builder = new BulkMatchWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;

    public BulkMatchWorkflowSubmitter matchInput(MatchInput matchInput) {
        builder = builder.matchInput(matchInput);
        return this;
    }

    public BulkMatchWorkflowSubmitter hdfsPodId(String hdfsPod) {
        builder = builder.hdfsPodId(hdfsPod);
        return this;
    }

    public BulkMatchWorkflowSubmitter rootOperationUid(String rootOperationUid) {
        builder = builder.rootOperationUid(rootOperationUid);
        return this;
    }

    public BulkMatchWorkflowSubmitter workflowProxy(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
        return this;
    }

    public BulkMatchWorkflowSubmitter microserviceHostport(String hostport) {
        builder = builder.microserviceHostPort(hostport);
        return this;
    }

    public BulkMatchWorkflowSubmitter averageBlockSize(Integer blockSize) {
        builder = builder.averageBlockSize(blockSize);
        return this;
    }

    public BulkMatchWorkflowSubmitter inputProperties() {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "bulkMatchWorkflow");
        builder = builder.inputProperties(inputProperties);
        return this;
    }

    public ApplicationId submit() {
        BulkMatchWorkflowConfiguration configuration = builder.build();
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }

}
