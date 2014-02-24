package com.latticeengines.dataplatform.runtime.metric.impl;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.NumberOfContainerPreemptions;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import com.latticeengines.dataplatform.runtime.metric.AnalyticJobBaseMetric;
import com.latticeengines.dataplatform.runtime.metric.MetricsProvider;

public class NumberPreemptionsMetric extends AnalyticJobBaseMetric {

    public NumberPreemptionsMetric(MetricsProvider provider) {
        super(provider, NumberOfContainerPreemptions);
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        if (!isChanged()) {
            return;
        }
        MetricsProvider provider = getProvider();
        long numPreemptions = provider.getNumberPreemptions();

        for (MetricsRecordBuilder rb : getMetricsBuilders(provider, collector, all)) {
            rb.addCounter(NumberOfContainerPreemptions, numPreemptions);
        }
        setChanged(false);
    }

}
