package com.latticeengines.serviceflows.workflow.match;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
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

    @Inject
    private ProcessMatchResultCascading processMatchResultCascading;

    @Value("${workflowapi.use.spark}")
    private boolean useSpark;

    @Override
    public Workflow defineWorkflow(MatchDataCloudWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config) //
                .next(prepareMatchData) //
                .next(preMatchConfigStep) //
                .next(bulkMatchWorkflow);
        if (useSpark) {
            builder = builder.next(processMatchResult);
        } else {
            builder = builder.next(processMatchResultCascading);
        }
        return builder.build();
    }
}
