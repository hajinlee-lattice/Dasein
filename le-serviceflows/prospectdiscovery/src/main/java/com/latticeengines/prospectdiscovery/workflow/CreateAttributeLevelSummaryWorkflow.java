package com.latticeengines.prospectdiscovery.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.prospectdiscovery.workflow.steps.RunScoreTableDataFlow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("createAttributeLevelSummaryWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateAttributeLevelSummaryWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private RunScoreTableDataFlow runScoreTableDataFlow;

    @Autowired
    private RunAttributeLevelSummaryDataFlows runAttributeLevelSummaryDataFlows;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(runScoreTableDataFlow) //
                .next(runAttributeLevelSummaryDataFlows) //
                .next(runAttributeLevelSummaryDataFlows) //
                .build();
    }
}
