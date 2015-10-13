package com.latticeengines.workflow.core;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.workflow.build.WorkflowStep;

public class Workflow {

    private boolean dryRun = false;
    private List<WorkflowStep> steps = new ArrayList<>();

    public List<WorkflowStep> getSteps() {
        return steps;
    }

    public void step(WorkflowStep step) {
        steps.add(step);
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

}
