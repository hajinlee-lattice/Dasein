package com.latticeengines.workflow.exposed.build;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();

    public WorkflowBuilder next(AbstractStep<? extends BaseStepConfiguration> step) {
        workflow.step(step, new ArrayList<>());
        return this;
    }

    public WorkflowBuilder next(AbstractWorkflow<?> nextWorkflow) {
        int idx = 0;
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow().getSteps()) {
            List<String> namespace = new ArrayList<>();
            namespace.add(nextWorkflow.name());
            namespace.addAll(nextWorkflow.defineWorkflow().getStepNamespaces().get(idx));
            workflow.step(step, namespace);
            idx++;
        }

        return this;
    }

    public WorkflowBuilder next(WorkflowInterface<?> nextWorkflow) {
        int idx = 0;
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow().getSteps()) {
            List<String> namespace = new ArrayList<>();
            namespace.add(nextWorkflow.name());
            namespace.addAll(nextWorkflow.defineWorkflow().getStepNamespaces().get(idx));
            workflow.step(step, namespace);
            idx++;
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

    public WorkflowBuilder choreographer(Choreographer choreographer) {
        workflow.setChoreographer(choreographer);
        return this;
    }

    public Workflow build() {
        return workflow;
    }
}
