package com.latticeengines.workflow.listener;

public interface LEJobCallerRegister {
    void regisger(Thread callerThread, LEJobCaller caller);
}
