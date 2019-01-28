package com.latticeengines.workflow.exposed.build;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class WorkflowBuilder {

    private Workflow workflow = new Workflow();
    private String root;
    private WorkflowConfiguration workflowConfig;

    public WorkflowBuilder(String root, WorkflowConfiguration workflowConfig) {
        this.root = root;
        this.workflowConfig = workflowConfig;
        if (workflowConfig != null && workflowConfig.getFailingStep() != null) {
            workflow.setFailingStep(workflowConfig.getFailingStep());
        }
    }

    public WorkflowBuilder next(AbstractStep<? extends BaseStepConfiguration> step) {
        workflow.step(step, StringUtils.isNotEmpty(root) ? root + "." + step.getConfigurationClass().getSimpleName()
                : step.getConfigurationClass().getSimpleName());
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T extends WorkflowConfiguration> WorkflowBuilder next(AbstractWorkflow<T> nextWorkflow) {
        T config = null;
        if (workflowConfig != null) {
            WorkflowConfiguration obj = workflowConfig.getSubWorkflowConfiguration(nextWorkflow.name());
            if (obj != null) {
                config = (T) obj;
            }
        }
        Workflow subWorkflow = nextWorkflow.defineWorkflow(config);
        Set<AbstractStep<? extends BaseStepConfiguration>> set = new HashSet<>();
        for (AbstractStep<? extends BaseStepConfiguration> step : subWorkflow.getSteps()) {
            String namespace = step.getNamespace();
            // in case we need to repeatedly use exactly the same steps under one namespace
            if (set.add(step) && StringUtils.isNotEmpty(root)) {
                namespace = root + "." + namespace;
            }
            workflow.step(step, namespace);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T extends WorkflowConfiguration> WorkflowBuilder next(WorkflowInterface<T> nextWorkflow) {
        T config = null;
        if (workflowConfig != null) {
            WorkflowConfiguration obj = workflowConfig.getSubWorkflowConfiguration(nextWorkflow.name());
            if (obj != null) {
                config = (T) obj;
            }
        }
        Workflow subWorkflow = nextWorkflow.defineWorkflow(config);
        Set<AbstractStep<? extends BaseStepConfiguration>> set = new HashSet<>();
        for (AbstractStep<? extends BaseStepConfiguration> step : subWorkflow.getSteps()) {
            String namespace = step.getNamespace();
            if (set.add(step) && StringUtils.isNotEmpty(root)) {
                namespace = root + "." + namespace;
            }
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
