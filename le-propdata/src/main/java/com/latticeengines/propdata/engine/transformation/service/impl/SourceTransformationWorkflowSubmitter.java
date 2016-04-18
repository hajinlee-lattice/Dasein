package com.latticeengines.propdata.engine.transformation.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.propdata.workflow.engine.TransformationWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class SourceTransformationWorkflowSubmitter {

    private TransformationWorkflowConfiguration.Builder builder = new TransformationWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;

    public SourceTransformationWorkflowSubmitter hdfsPodId(String hdfsPod) {
        builder = builder.hdfsPodId(hdfsPod);
        return this;
    }

    public SourceTransformationWorkflowSubmitter workflowProxy(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
        return this;
    }

    public ApplicationId submit() {
        TransformationWorkflowConfiguration configuration = builder.build();
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }

}
