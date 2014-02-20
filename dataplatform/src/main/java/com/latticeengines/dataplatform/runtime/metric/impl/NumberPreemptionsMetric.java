package com.latticeengines.dataplatform.runtime.metric.impl;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AnalyticJobMetrics;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AppId;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.ContainerId;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.NumberOfContainerPreemptions;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.Priority;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;

import com.latticeengines.dataplatform.runtime.metric.MetricProvider;

public class NumberPreemptionsMetric implements MetricsSource {

    private MetricProvider provider;

    public NumberPreemptionsMetric(MetricProvider provider) {
        this.provider = provider;
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        long numPreemptions = provider.getNumberPreemptions();

        MetricsRecordBuilder rb = collector.addRecord(AnalyticJobMetrics) //
                .setContext("ledpjob") //
                .tag(AppId, provider.getAppAttemptId()) //
                .tag(ContainerId, provider.getContainerId()) //
                .tag(Priority, provider.getPriority());
        rb.addCounter(NumberOfContainerPreemptions, numPreemptions);
    }

}
