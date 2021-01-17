package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.MockBrokerAggregationListener;
import com.latticeengines.cdl.workflow.steps.integration.AggregateMockBrokerInstanceFile;
import com.latticeengines.domain.exposed.serviceflows.cdl.MockBrokerAggregationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("mockBrokerAggregationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MockBrokerAggregationWorkflow extends AbstractWorkflow<MockBrokerAggregationWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MockBrokerAggregationWorkflow.class);

    @Inject
    private AggregateMockBrokerInstanceFile aggregateMockBrokerInstanceFile;

    @Inject
    private MockBrokerAggregationListener mockBrokerAggregationListener;

    @Override
    public Workflow defineWorkflow(MockBrokerAggregationWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(aggregateMockBrokerInstanceFile)
                .listener(mockBrokerAggregationListener)
                .build();
    }
}
