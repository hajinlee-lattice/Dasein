package com.latticeengines.domain.exposed.monitor.metric;

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
        Reflections reflections = new Reflections("com.latticeengines.domain.exposed");

        for (Class<?> clz: reflections.getSubTypesOf(Dimension.class)) {
            System.out.println("Scanning tags for dimension class " + clz.getSimpleName());
            MetricUtils.scanTags(clz);
        }

        for (Class<?> clz: reflections.getSubTypesOf(Fact.class)) {
            System.out.println("Scanning fields for fact class " + clz.getSimpleName());
            MetricUtils.scanFields(clz);
        }

        for (Class<?> clz: reflections.getSubTypesOf(Measurement.class)) {
            System.out.println("Scanning tags and fields for measurement class " + clz.getSimpleName());
            MetricUtils.scan((Class<Measurement<?, ?>>) clz);
        }
    }

}
