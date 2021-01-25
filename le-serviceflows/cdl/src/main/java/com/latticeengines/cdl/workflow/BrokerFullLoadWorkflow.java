package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.integration.BrokerDataFullLoad;
import com.latticeengines.domain.exposed.serviceflows.cdl.BrokerFullLoadWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("brokerFullLoadWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BrokerFullLoadWorkflow extends AbstractWorkflow<BrokerFullLoadWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BrokerFullLoadWorkflow.class);

    @Inject
    private BrokerDataFullLoad brokerDataFullLoad;

    @Override
    public Workflow defineWorkflow(BrokerFullLoadWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(brokerDataFullLoad)
                .build();
    }
}
