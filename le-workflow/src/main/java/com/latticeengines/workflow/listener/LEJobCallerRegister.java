package com.latticeengines.workflow.listener;

public interface LEJobCallerRegister {
    void register(Thread callerThread, LEJobCaller caller);
}
