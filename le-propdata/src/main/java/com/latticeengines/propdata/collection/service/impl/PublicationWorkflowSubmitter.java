package com.latticeengines.propdata.collection.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.propdata.publication.PublicationConfiguration;
import com.latticeengines.propdata.workflow.collection.PublicationWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class PublicationWorkflowSubmitter {

    private PublicationWorkflowConfiguration.Builder builder = new PublicationWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;

    public PublicationWorkflowSubmitter hdfsPodId(String hdfsPod) {
        builder = builder.hdfsPodId(hdfsPod);
        return this;
    }
    public PublicationWorkflowSubmitter workflowProxy(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
        return this;
    }

    public PublicationWorkflowSubmitter publicationConfig(PublicationConfiguration publicationConfig) {
        builder = builder.publicationConfig(publicationConfig);
        return this;
    }

    public ApplicationId submit() {
        PublicationWorkflowConfiguration configuration = builder.build();
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }

}
