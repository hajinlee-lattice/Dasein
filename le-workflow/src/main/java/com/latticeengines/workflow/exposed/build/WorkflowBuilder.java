package com.latticeengines.workflow.exposed.build;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();

    public WorkflowBuilder next(AbstractStep<? extends BaseStepConfiguration> step) {
        workflow.step(step, null);
        return this;
    }

    public <T extends WorkflowConfiguration> WorkflowBuilder next(AbstractWorkflow<T> nextWorkflow, T config) {
        int idx = 0;
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow(config).getSteps()) {
            String namespace = nextWorkflow.defineWorkflow(config).getStepNamespaces().get(idx);
            namespace = StringUtils.isBlank(namespace) ? nextWorkflow.name() : nextWorkflow.name() + "." + namespace;
            workflow.step(step, namespace);
            idx++;
        }

        return this;
    }

    public <S extends BaseWrapperStepConfiguration, W extends WorkflowConfiguration, R extends BaseWrapperStep<S, W>, I extends WorkflowInterface<W>> WorkflowBuilder next(
            WorkflowWrapper<S, W, R, I> nextWorkflow, TransformationConfiguration config) {
        int idx = 0;
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow(config).getSteps()) {
            String namespace = nextWorkflow.defineWorkflow(config).getStepNamespaces().get(idx);
            namespace = StringUtils.isBlank(namespace) ? nextWorkflow.name() : nextWorkflow.name() + "." + namespace;
            workflow.step(step, namespace);
            idx++;
        }

        return this;
    }

    public <T> WorkflowBuilder next(WorkflowInterface<T> nextWorkflow, T config) {
        int idx = 0;
        for (AbstractStep<? extends BaseStepConfiguration> step : nextWorkflow.defineWorkflow(config).getSteps()) {
            String namespace = nextWorkflow.defineWorkflow(config).getStepNamespaces().get(idx);
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
