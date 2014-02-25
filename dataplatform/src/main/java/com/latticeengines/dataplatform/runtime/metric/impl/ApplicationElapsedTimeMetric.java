package com.latticeengines.dataplatform.runtime.metric.impl;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AMElapsedTime;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import com.latticeengines.dataplatform.runtime.metric.AnalyticJobBaseMetric;
import com.latticeengines.dataplatform.runtime.metric.MetricsProvider;

public class ApplicationElapsedTimeMetric extends AnalyticJobBaseMetric {
    
    public ApplicationElapsedTimeMetric(MetricsProvider provider) {
        super(provider, AMElapsedTime);
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        if (!isChanged()) {
            return;
        }
        MetricsProvider provider = getProvider();
        
        long elapsedTime = provider.getApplicationElapsedTime();
        
        if (elapsedTime >= 0) {
            log.info("Application elapsed time = " + elapsedTime);
            for (MetricsRecordBuilder rb : getMetricsBuilders(provider, collector, all)) {
                rb.addGauge(AMElapsedTime, elapsedTime);
            }
            setChanged(false);
        }
    }

}
