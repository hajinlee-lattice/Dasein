package com.latticeengines.workflow.build;

import org.springframework.beans.factory.BeanNameAware;

public abstract class WorkflowStep implements BeanNameAware {

    private String name;
    private boolean dryRun = false;

    public abstract void execute();

    @Override
    public void setBeanName(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }
}
