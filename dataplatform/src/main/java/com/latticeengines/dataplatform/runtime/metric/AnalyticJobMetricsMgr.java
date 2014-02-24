package com.latticeengines.dataplatform.runtime.metric;

import java.util.Map;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.latticeengines.dataplatform.runtime.metric.impl.ApplicationElapsedTimeMetric;
import com.latticeengines.dataplatform.runtime.metric.impl.ContainerLaunchWaitTimeMetric;
import com.latticeengines.dataplatform.runtime.metric.impl.NumberPreemptionsMetric;

public class AnalyticJobMetricsMgr implements MetricsProvider {

    private long appStartTime;
    private long appEndTime;
    private long containerLaunchTime;
    private final String appAttemptId;
    private String priority;
    private String containerId;
    private String queue;
    private int numberPreemptions;
    private static AnalyticJobMetricsMgr instance;
    private MetricsSystem ms;
    private volatile boolean completed = false;
    
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
        new ContainerLaunchWaitTimeMetric(this);
        new NumberPreemptionsMetric(this);
        new ApplicationElapsedTimeMetric(this);
        
        Map<String, AnalyticJobBaseMetric> map = AnalyticJobBaseMetric.getRegistry();
        
        for (AnalyticJobBaseMetric metric : map.values()) {
            ms.register(metric.getName(), metric.getDescription(), metric);
        }
        
        ms.init("ledpjob");
    }
    
    public void setChanged(String name) {
        AnalyticJobBaseMetric.getRegistry().get(name).setChanged(true);
    }
    
    public void finalize() {
        completed = true;
        ms.publishMetricsNow();
    }
    
    @Override
    public long getContainerWaitTime() {
        if (completed) {
            return 0;
        }
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

    @Override
    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public long getAppEndTime() {
        return appEndTime;
    }

    public void setAppEndTime(long appEndTime) {
        this.appEndTime = appEndTime;
    }

    @Override
    public long getApplicationElapsedTime() {
        if (completed) {
            return 0;
        }
        return getAppEndTime() - getAppStartTime();
    }
    
    

}
