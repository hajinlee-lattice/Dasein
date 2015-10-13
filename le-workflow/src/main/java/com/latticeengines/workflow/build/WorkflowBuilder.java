package com.latticeengines.workflow.build;

import com.latticeengines.workflow.core.Workflow;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();

    public WorkflowBuilder next(WorkflowStep step) {
        workflow.step(step);
        return this;
    }

    public WorkflowBuilder next(Workflow otherWorkflow) {
        for (WorkflowStep step : otherWorkflow.getSteps()) {
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
