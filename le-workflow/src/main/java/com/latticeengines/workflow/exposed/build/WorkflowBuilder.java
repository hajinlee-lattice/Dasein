package com.latticeengines.workflow.exposed.build;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();

    private String root;

    public WorkflowBuilder(String root) {
        this.root = root;
    }

    public WorkflowBuilder next(AbstractStep<? extends BaseStepConfiguration> step) {
        workflow.step(step, root + "." + step.getConfigurationClass().getSimpleName());
        return this;
    }

    public <T extends WorkflowConfiguration> WorkflowBuilder next(AbstractWorkflow<T> nextWorkflow, T config) {
        Workflow subWorkflow = nextWorkflow.defineWorkflow(config);
        Set<String> set = new HashSet<>();
        for (AbstractStep<? extends BaseStepConfiguration> step : subWorkflow.getSteps()) {
            String namespace = step.getNamespace();
            if (set.add(namespace)) {
                namespace = root + "." + namespace;
            }
            workflow.step(step, namespace);
        }
        return this;
    }

    public <T> WorkflowBuilder next(WorkflowInterface<T> nextWorkflow, T config) {
        Workflow subWorkflow = nextWorkflow.defineWorkflow(config);
        for (AbstractStep<? extends BaseStepConfiguration> step : subWorkflow.getSteps()) {
            String namespace = step.getNamespace();
            namespace = root + "." + namespace;
            workflow.step(step, namespace);
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
