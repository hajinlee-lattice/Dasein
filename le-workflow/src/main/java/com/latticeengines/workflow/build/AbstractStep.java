package com.latticeengines.workflow.build;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.BeanNameAware;

public abstract class AbstractStep implements BeanNameAware {

    private String name;
    private boolean dryRun = false;
    private boolean runAgainWhenComplete = false;

    public abstract void execute();

    /**
     * Override this to include any Step initialization logic.
     */
    @PostConstruct
    public void initialize() {
    }

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

    public boolean isRunAgainWhenComplete() {
        return runAgainWhenComplete;
    }

    public void setRunAgainWhenComplete(boolean runAgainWhenComplete) {
        this.runAgainWhenComplete = runAgainWhenComplete;
    }

}
