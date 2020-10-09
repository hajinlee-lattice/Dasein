package com.latticeengines.datacloud.workflow.match;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.steps.PublishEntityMatchStaging;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.PublishEntityMatchStagingWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Lazy
@Component("publishEntityMatchStagingWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishEntityMatchStagingWorkflowImpl extends AbstractWorkflow<BaseDataCloudWorkflowConfiguration>
        implements PublishEntityMatchStagingWorkflow {

    @Inject
    private PublishEntityMatchStaging publishEntityMatchStaging;

    @Override
    public Workflow defineWorkflow(BaseDataCloudWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(publishEntityMatchStaging) //
                .build();
    }
}
