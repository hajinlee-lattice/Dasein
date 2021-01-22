package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.integration.BrokerDataInitialLoad;
import com.latticeengines.domain.exposed.serviceflows.cdl.BrokerInitialLoadWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("brokerInitialLoadWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BrokerInitialLoadWorkflow extends AbstractWorkflow<BrokerInitialLoadWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BrokerInitialLoadWorkflow.class);

    @Inject
    private BrokerDataInitialLoad brokerDataInitialLoad;

    @Override
    public Workflow defineWorkflow(BrokerInitialLoadWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(brokerDataInitialLoad)
                .build();
    }
}
