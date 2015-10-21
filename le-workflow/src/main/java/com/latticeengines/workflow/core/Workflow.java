package com.latticeengines.workflow.core;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.workflow.build.AbstractStep;

public class Workflow {

    private boolean dryRun = false;
    private List<AbstractStep> steps = new ArrayList<>();

    public List<AbstractStep> getSteps() {
        return steps;
    }

    public void step(AbstractStep step) {
        steps.add(step);
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

}
