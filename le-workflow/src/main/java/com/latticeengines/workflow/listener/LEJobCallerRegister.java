package com.latticeengines.workflow.listener;

public interface LEJobCallerRegister {
    /**
     * Register will wait for
     * {@link LEJobCallerRegister#register(Thread, LEJobCaller)} to be invoked
     */
    void enableWaitForCaller();

    void register(Thread callerThread, LEJobCaller caller);
}
