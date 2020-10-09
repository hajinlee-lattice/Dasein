package com.latticeengines.matchapi.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PublishEntityMatchStagingConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Lazy
@Component("publishEntityMatchStagingWorkflowSubmitter")
public class PublishEntityMatchStagingWorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(PublishEntityMatchStagingWorkflowSubmitter.class);

    @Inject
    protected WorkflowProxy workflowProxy;

    public AppSubmission submit(@NotNull PublishEntityMatchStagingConfiguration configuration) {
        log.info("Submitting publish entity match staging job, config = {}", JsonUtils.serialize(configuration));
        validate(configuration);

        // generate config
        BaseDataCloudWorkflowConfiguration wfConfig = new BaseDataCloudWorkflowConfiguration();
        wfConfig.setContainerConfiguration("publishEntityMatchStagingWorkflow",
                CustomerSpace.parse(configuration.getCustomerSpace()), wfConfig.getClass().getSimpleName());
        wfConfig.add(configuration);

        // submit workflow
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(wfConfig);
        log.info("Publish entity match staging job submitted, submission = {}", JsonUtils.serialize(appSubmission));
        return appSubmission;
    }

    private void validate(@NotNull PublishEntityMatchStagingConfiguration configuration) {
        Preconditions.checkNotNull(configuration, "publish entity match staging step config should not be null");
        Preconditions.checkNotNull(configuration.getCustomerSpace(), "missing tenant");
        Preconditions.checkNotNull(configuration.getEntities(), "missing entities set to publish");
    }

}
