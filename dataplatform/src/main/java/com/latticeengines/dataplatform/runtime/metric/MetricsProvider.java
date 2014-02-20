package com.latticeengines.dataplatform.runtime.metric;

public interface MetricsProvider {

    long getContainerWaitTime();
    
    int getNumberPreemptions();
    
    String getAppAttemptId();
    
    String getContainerId();
    
    String getPriority();
}
