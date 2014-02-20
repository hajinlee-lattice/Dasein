package com.latticeengines.dataplatform.runtime.metric;

public interface MetricProvider {

    long getContainerWaitTime();
    
    int getNumberPreemptions();
    
    String getAppAttemptId();
    
    String getContainerId();
    
    String getPriority();
}
