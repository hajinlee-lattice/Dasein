package com.latticeengines.serviceflows.workflow.match;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("matchDataCloudWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchDataCloudWorkflow extends AbstractWorkflow<MatchDataCloudWorkflowConfiguration> {

    @Inject
    private PrepareMatchConfig preMatchConfigStep;

    @Inject
    private PrepareMatchDataStep prepareMatchData;

    @Inject
    private BulkMatchWorkflow bulkMatchWorkflow;

    @Inject
    private ProcessMatchResult processMatchResult;

    @Override
    public Workflow defineWorkflow(MatchDataCloudWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(prepareMatchData) //
                .next(preMatchConfigStep) //
                .next(bulkMatchWorkflow) //
                .next(processMatchResult) //
                .build();
    }
}
