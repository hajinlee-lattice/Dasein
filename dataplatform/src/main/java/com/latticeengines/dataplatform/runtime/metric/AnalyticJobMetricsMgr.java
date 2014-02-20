package com.latticeengines.dataplatform.runtime.metric;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AMRunningToContainerLaunchWaitTime;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.NumberOfContainerPreemptions;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.latticeengines.dataplatform.runtime.metric.impl.ContainerLaunchWaitTimeMetric;
import com.latticeengines.dataplatform.runtime.metric.impl.NumberPreemptionsMetric;

public class AnalyticJobMetricsMgr implements MetricsProvider {

    private long appStartTime;
    private long containerLaunchTime;
    private final String appAttemptId;
    private String priority;
    private String containerId;
    private int numberPreemptions;
    private static AnalyticJobMetricsMgr instance;
    private MetricsSystem ms;
    
    private AnalyticJobMetricsMgr(String appAttemptId) {
        this.appAttemptId = appAttemptId;
        ms = DefaultMetricsSystem.instance();
    }
    
    public static AnalyticJobMetricsMgr getInstance(String appAttemptId) {
        if (instance == null) {
            synchronized (AnalyticJobMetricsMgr.class) {
                if (instance == null) {
                    instance = new AnalyticJobMetricsMgr(appAttemptId);
                }
            }
        }

        return instance;
    }
    
    public void initialize() {
        ContainerLaunchWaitTimeMetric containerWaitTimeMetric = new ContainerLaunchWaitTimeMetric(this);
        NumberPreemptionsMetric numPreemptionsMetric = new NumberPreemptionsMetric(this);
        ms.register(AMRunningToContainerLaunchWaitTime.name(),
                AMRunningToContainerLaunchWaitTime.description(), containerWaitTimeMetric);
        ms.register(NumberOfContainerPreemptions.name(),
                NumberOfContainerPreemptions.description(), numPreemptionsMetric);
        ms.init("ledpjob");
    }
    
    public void finalize() {
        ms.publishMetricsNow();
    }
    
    @Override
    public long getContainerWaitTime() {
        return getContainerLaunchTime() - getAppStartTime();
    }

    @Override
    public String getAppAttemptId() {
        return appAttemptId;
    }

    public long getAppStartTime() {
        return appStartTime;
    }

    public void setAppStartTime(long appStartTime) {
        this.appStartTime = appStartTime;
    }

    public long getContainerLaunchTime() {
        return containerLaunchTime;
    }

    public void setContainerLaunchTime(long containerLaunchTime) {
        this.containerLaunchTime = containerLaunchTime;
    }

    @Override
    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    @Override
    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    @Override
    public int getNumberPreemptions() {
        return numberPreemptions;
    }

    public void incrementNumberPreemptions() {
        numberPreemptions++;
    }
    
    

}
