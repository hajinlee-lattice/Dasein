package com.latticeengines.workflow.build;

import javax.annotation.PostConstruct;

public abstract class AbstractStep extends AbstractNameAwareBean {

    private boolean dryRun = false;
    private boolean runAgainWhenComplete = false;

    public abstract void execute();

    /**
     * Override this to include any Step initialization logic.
     */
    @PostConstruct
    public void initialize() {
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
