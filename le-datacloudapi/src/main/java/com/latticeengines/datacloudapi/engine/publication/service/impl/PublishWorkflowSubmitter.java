package com.latticeengines.datacloudapi.engine.publication.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.PublishWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class PublishWorkflowSubmitter {

    private PublishWorkflowConfiguration.Builder builder = new PublishWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;

    public PublishWorkflowSubmitter hdfsPodId(String hdfsPod) {
        builder = builder.hdfsPodId(hdfsPod);
        return this;
    }
    public PublishWorkflowSubmitter workflowProxy(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
        return this;
    }

    public PublishWorkflowSubmitter publication(Publication publication) {
        builder = builder.publication(publication);
        return this;
    }

    public PublishWorkflowSubmitter progress(PublicationProgress progress) {
        builder = builder.progress(progress);
        return this;
    }

    public ApplicationId submit() {
        PublishWorkflowConfiguration configuration = builder.build();
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }

}
