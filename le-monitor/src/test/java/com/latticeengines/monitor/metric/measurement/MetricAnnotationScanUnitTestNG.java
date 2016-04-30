package com.latticeengines.monitor.metric.measurement;

import org.reflections.Reflections;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.util.MetricUtils;

public class MetricAnnotationScanUnitTestNG {

    @SuppressWarnings("unchecked")
    @Test(groups = "unit")
    public void testMetricDimension() {
        Reflections reflections = new Reflections("com.latticeengines.monitor.metric.measurement");

        for (Class<?> clz: reflections.getSubTypesOf(Dimension.class)) {
            MetricUtils.scanTags(clz);
        }

        for (Class<?> clz: reflections.getSubTypesOf(Fact.class)) {
            MetricUtils.scanFields(clz);
        }

        for (Class<?> clz: reflections.getSubTypesOf(Measurement.class)) {
            MetricUtils.scan((Class<Measurement<?, ?>>) clz);
        }
    }

}
