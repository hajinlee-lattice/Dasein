package com.latticeengines.workflow.exposed.build;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

public class Workflow {

    private boolean dryRun = false;
    private List<AbstractStep<? extends BaseStepConfiguration>> steps = new ArrayList<>();
    private List<LEJobListener> listeners = new ArrayList<>();

    public List<AbstractStep<? extends BaseStepConfiguration>> getSteps() {
        return steps;
    }

    public void step(AbstractStep<? extends BaseStepConfiguration> step) {
        steps.add(step);
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

}
