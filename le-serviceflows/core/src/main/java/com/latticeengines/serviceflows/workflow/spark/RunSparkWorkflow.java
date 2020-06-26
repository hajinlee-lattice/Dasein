package com.latticeengines.serviceflows.workflow.spark;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.spark.RunSparkWorkflowConfig;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Lazy
@Component("runSparkWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RunSparkWorkflow extends AbstractWorkflow<RunSparkWorkflowConfig> {

    @Inject
    private RunSparkWorkflowStep runSpark;

    @Override
    public Workflow defineWorkflow(RunSparkWorkflowConfig config) {
        return new WorkflowBuilder(name(), config).next(runSpark).build();
    }

}
