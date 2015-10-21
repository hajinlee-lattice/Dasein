package com.latticeengines.workflow.build;

import com.latticeengines.workflow.core.Workflow;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();

    public WorkflowBuilder next(AbstractStep step) {
        workflow.step(step);
        return this;
    }

    public WorkflowBuilder next(AbstractWorkflow nextWorkflow) {
        for (AbstractStep step : nextWorkflow.defineWorkflow().getSteps()) {
            workflow.step(step);
        }

        return this;
    }

    public WorkflowBuilder enableDryRun() {
        workflow.setDryRun(true);
        return this;
    }

    public Workflow build() {
        return workflow;
    }
}
