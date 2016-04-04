package com.latticeengines.propdata.match.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.workflow.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class BulkMatchWorkflowSubmitter {

    private BulkMatchWorkflowConfiguration.Builder builder = new BulkMatchWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;

    public BulkMatchWorkflowSubmitter matchInput(MatchInput matchInput) {
        builder = builder.matchInput(matchInput);
        return this;
    }

    public BulkMatchWorkflowSubmitter groupSize(Integer groupSize) {
        builder = builder.groupSize(groupSize);
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

    public BulkMatchWorkflowSubmitter inputDir(String inputDir) {
        builder = builder.inputDir(inputDir);
        return this;
    }

    public BulkMatchWorkflowSubmitter microserviceHostport(String hostport) {
        builder = builder.microserviceHostPort(hostport);
        return this;
    }

    public ApplicationId submit() {
        BulkMatchWorkflowConfiguration configuration = builder.build();
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }

}
