package com.latticeengines.workflow.exposed.build;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class Workflow {

    private boolean dryRun = false;
    private List<AbstractStep<? extends BaseStepConfiguration>> steps = new ArrayList<>();
    private List<String> stepDAG = new ArrayList<>();
    private List<LEJobListener> listeners = new ArrayList<>();
    private Choreographer choreographer = Choreographer.DEFAULT_CHOREOGRAPHER;

    public List<AbstractStep<? extends BaseStepConfiguration>> getSteps() {
        return steps;
    }

    public void step(AbstractStep<? extends BaseStepConfiguration> step, String stepPath) {
        steps.add(step);
        stepDAG.add(stepPath);
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

    public List<String> getStepDAG() {
        return stepDAG;
    }

    public void setStepDAG(List<String> stepDAG) {
        this.stepDAG = stepDAG;
    }

    public Choreographer getChoreographer() {
        return choreographer;
    }

    public void setChoreographer(Choreographer choreographer) {
        this.choreographer = choreographer;
    }
}
