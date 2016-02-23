package com.latticeengines.propdata.match.metric;

import org.reflections.Reflections;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.util.MetricUtils;

public class MetricAnnotationScanUnitTestNG {

    @SuppressWarnings("unchecked")
    @Test(groups = "unit")
    public void scanMetricClasses() {
        Reflections reflections = new Reflections("com.latticeengines.propdata");

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
