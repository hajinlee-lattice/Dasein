package com.latticeengines.workflow.exposed.build;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();

    public WorkflowBuilder next(AbstractStep<? extends BaseStepConfiguration> step) {
        workflow.step(step, null);
        return this;
    }

    public WorkflowBuilder next(AbstractWorkflow<?> nextWorkflow) {
        int idx = 0;
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow().getSteps()) {
            String namespace = nextWorkflow.defineWorkflow().getStepNamespaces().get(idx);
            namespace = StringUtils.isBlank(namespace) ? nextWorkflow.name() : nextWorkflow.name() + "." + namespace;
            workflow.step(step, namespace);
            idx++;
        }

        return this;
    }

    public WorkflowBuilder next(WorkflowInterface<?> nextWorkflow) {
        int idx = 0;
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow().getSteps()) {
            String namespace = nextWorkflow.defineWorkflow().getStepNamespaces().get(idx);
            namespace = StringUtils.isBlank(namespace) ? nextWorkflow.name() : nextWorkflow.name() + "." + namespace;
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
