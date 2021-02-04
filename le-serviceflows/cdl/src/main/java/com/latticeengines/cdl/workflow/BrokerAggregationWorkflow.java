package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.BrokerAggregationListener;
import com.latticeengines.cdl.workflow.steps.integration.AggregateBrokerFile;
import com.latticeengines.domain.exposed.serviceflows.cdl.BrokerAggregationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("brokerAggregationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BrokerAggregationWorkflow extends AbstractWorkflow<BrokerAggregationWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BrokerAggregationWorkflow.class);

    @Inject
    private AggregateBrokerFile aggregateBrokerFile;

    @Inject
    private BrokerAggregationListener brokerAggregationListener;

    @Override
    public Workflow defineWorkflow(BrokerAggregationWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(aggregateBrokerFile)
                .listener(brokerAggregationListener)
                .build();
    }
}
