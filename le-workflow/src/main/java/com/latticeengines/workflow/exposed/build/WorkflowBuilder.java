package com.latticeengines.workflow.exposed.build;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();

    public WorkflowBuilder next(AbstractStep<? extends BaseStepConfiguration> step) {
        workflow.step(step);
        return this;
    }

    public WorkflowBuilder next(AbstractWorkflow<?> nextWorkflow) {
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow().getSteps()) {
            workflow.step(step);
        }

        return this;
    }

    public WorkflowBuilder next(WorkflowInterface<?> nextWorkflow) {
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow().getSteps()) {
            workflow.step(step);
        }

        return this;
    }

    public WorkflowBuilder enableDryRun() {
        workflow.setDryRun(true);
        return this;
    }

    public WorkflowBuilder listener(LEJobListener listener) {
        workflow.listener(listener);
        return this;
    }

    public Workflow build() {
        return workflow;
    }
}
