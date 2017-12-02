package com.latticeengines.workflow.exposed.build;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class Workflow {

    private boolean dryRun = false;
    private List<AbstractStep<? extends BaseStepConfiguration>> steps = new ArrayList<>();
    private List<String> stepNamespaces = new ArrayList<>();
    private List<LEJobListener> listeners = new ArrayList<>();
    private Choreographer choreographer = Choreographer.DEFAULT_CHOREOGRAPHER;

    public List<AbstractStep<? extends BaseStepConfiguration>> getSteps() {
        return steps;
    }

    public void step(AbstractStep<? extends BaseStepConfiguration> step, String stepPath) {
        steps.add(step);
        stepNamespaces.add(stepPath);
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public List<LEJobListener> getListeners() {
        return listeners;
    }

    public void listener(LEJobListener listener) {
        this.listeners.add(listener);
    }

    public List<String> getStepNamespaces() {
        return stepNamespaces;
    }

    public void setStepNamespaces(List<String> stepNamespaces) {
        this.stepNamespaces = stepNamespaces;
    }

    public Choreographer getChoreographer() {
        return choreographer;
    }

    public void setChoreographer(Choreographer choreographer) {
        this.choreographer = choreographer;
    }
}
