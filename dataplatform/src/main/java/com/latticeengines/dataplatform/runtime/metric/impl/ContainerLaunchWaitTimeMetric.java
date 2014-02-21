package com.latticeengines.dataplatform.runtime.metric.impl;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AMRunningToContainerLaunchWaitTime;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import com.latticeengines.dataplatform.runtime.metric.AnalyticJobBaseMetric;
import com.latticeengines.dataplatform.runtime.metric.MetricsProvider;

public class ContainerLaunchWaitTimeMetric extends AnalyticJobBaseMetric {
    
    public ContainerLaunchWaitTimeMetric(MetricsProvider provider) {
        super(provider);
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        MetricsProvider provider = getProvider();
        
        long waitTime = provider.getContainerWaitTime();
        
        if (waitTime >= 0) {
            for (MetricsRecordBuilder rb : getMetricsBuilders(provider, collector, all)) {
                rb.addGauge(AMRunningToContainerLaunchWaitTime, waitTime);
            }
            
        }
    }

}
