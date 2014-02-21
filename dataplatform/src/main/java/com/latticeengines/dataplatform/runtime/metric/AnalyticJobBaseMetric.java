package com.latticeengines.dataplatform.runtime.metric;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AnalyticJobMetrics;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.Priority;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.Queue;

import java.util.ArrayList;
import java.util.List;

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
    
    protected List<MetricsRecordBuilder> getMetricsBuilders(MetricsProvider provider, MetricsCollector collector, boolean all) {
        List<MetricsRecordBuilder> builderList = new ArrayList<MetricsRecordBuilder>();
        builderList.add(collector.addRecord(AnalyticJobMetrics) //
                .setContext("ledpjob") //
                .tag(Queue, provider.getQueue()));
        builderList.add(collector.addRecord(AnalyticJobMetrics) //
                .setContext("ledpjob") //
                .tag(Priority, provider.getPriority()));
        return builderList;
    }
}
