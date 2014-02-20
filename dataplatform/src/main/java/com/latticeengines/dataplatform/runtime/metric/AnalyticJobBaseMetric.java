package com.latticeengines.dataplatform.runtime.metric;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AnalyticJobMetrics;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AppId;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.ContainerId;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.Priority;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;

public abstract class AnalyticJobBaseMetric implements MetricsSource {
    
    private MetricsProvider metricsProvider;
    
    public AnalyticJobBaseMetric(MetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }
    
    protected MetricsProvider getProvider() {
        return metricsProvider;
    }
    
    protected MetricsRecordBuilder getMetricBuilder(MetricsProvider provider, MetricsCollector collector, boolean all) {
        MetricsRecordBuilder rb = collector.addRecord(AnalyticJobMetrics) //
                .setContext("ledpjob") //
                .tag(AppId, provider.getAppAttemptId()) //
                .tag(ContainerId, provider.getContainerId()) //
                .tag(Priority, provider.getPriority());
        return rb;
    }
}
