package com.latticeengines.datacloudapi.engine.ingestion.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.IngestionWorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class IngestionWorkflowSubmitter {
    private IngestionWorkflowConfiguration.Builder builder = new IngestionWorkflowConfiguration.Builder();
    private WorkflowProxy workflowProxy;

    public IngestionWorkflowSubmitter ingestionProgress(IngestionProgress progress) {
        builder = builder.ingestionProgress(progress);
        return this;
    }

    public IngestionWorkflowSubmitter workflowProxy(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
        return this;
    }

    public IngestionWorkflowSubmitter ingestion(Ingestion ingestion) {
        builder = builder.ingestion(ingestion);
        return this;
    }

    public IngestionWorkflowSubmitter providerConfiguration(
            ProviderConfiguration providerConfiguration) {
        builder = builder.providerConfiguration(providerConfiguration);
        return this;
    }

    public ApplicationId submit() {
        IngestionWorkflowConfiguration config = builder.build();
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(config);
        return ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
    }
}
