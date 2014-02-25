package com.latticeengines.dataplatform.runtime.metric.impl;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AMSubmissionToRunningWaitTime;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import com.latticeengines.dataplatform.runtime.metric.AnalyticJobBaseMetric;
import com.latticeengines.dataplatform.runtime.metric.MetricsProvider;

public class ApplicationLaunchWaitTimeMetric extends AnalyticJobBaseMetric {
    
    public ApplicationLaunchWaitTimeMetric(MetricsProvider provider) {
        super(provider, AMSubmissionToRunningWaitTime);
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        if (!isChanged()) {
            return;
        }
        MetricsProvider provider = getProvider();
        
        long waitTime = provider.getApplicationWaitTime();
        
        if (waitTime >= 0) {
            log.info("Application launch wait time = " + waitTime);
            for (MetricsRecordBuilder rb : getMetricsBuilders(provider, collector, all)) {
                rb.addGauge(AMSubmissionToRunningWaitTime, waitTime);
            }
            setChanged(false);
        }
    }

}
