package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startExecution")
public class StartExecution extends BaseWorkflowStep<StartExecutionConfiguration> {

    @Autowired
    private DataFeedProxy datafeedProxy;

    @Override
    public void execute() {
        datafeedProxy.startExecution(configuration.getCustomerSpace().toString(), configuration.getDataFeedName());
    }

}