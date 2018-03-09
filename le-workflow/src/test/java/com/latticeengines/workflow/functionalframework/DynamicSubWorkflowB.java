package com.latticeengines.workflow.functionalframework;

import javax.annotation.Resource;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dynamicSubWorkflowB")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DynamicSubWorkflowB extends AbstractWorkflow<WorkflowConfiguration> {

    @Resource(name = "stepC")
    private NamedStep stepC;

    @Resource(name = "stepD")
    private NamedStep stepD;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(stepC) //
                .next(stepD) //
                .build();
    }

}
