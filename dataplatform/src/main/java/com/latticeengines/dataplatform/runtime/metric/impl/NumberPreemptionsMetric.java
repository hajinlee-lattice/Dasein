package com.latticeengines.dataplatform.runtime.metric.impl;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.NumberOfContainerPreemptions;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import com.latticeengines.dataplatform.runtime.metric.AnalyticJobBaseMetric;
import com.latticeengines.dataplatform.runtime.metric.MetricsProvider;

public class NumberPreemptionsMetric extends AnalyticJobBaseMetric {

    public NumberPreemptionsMetric(MetricsProvider provider) {
        super(provider);
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        MetricsProvider provider = getProvider();
        long numPreemptions = provider.getNumberPreemptions();

        MetricsRecordBuilder rb = getMetricBuilder(provider, collector, all);
        rb.addCounter(NumberOfContainerPreemptions, numPreemptions);
    }

}
