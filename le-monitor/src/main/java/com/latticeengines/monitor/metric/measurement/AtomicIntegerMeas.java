package com.latticeengines.monitor.metric.measurement;

import java.util.concurrent.atomic.AtomicInteger;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class AtomicIntegerMeas extends BaseMeasurement<AtomicIntegerMeas.CollectionSizeFact, InspectionDimension>
        implements Measurement<AtomicIntegerMeas.CollectionSizeFact, InspectionDimension> {

    private final InspectionDimension dimension;
    private final CollectionSizeFact fact;

    public AtomicIntegerMeas(String collectionName, AtomicInteger maxSize, AtomicInteger minSize, AtomicInteger avgSize) {
        this.fact = new CollectionSizeFact(maxSize, minSize, avgSize);
        this.dimension = new InspectionDimension(collectionName);
    }

    @Override
    public CollectionSizeFact getFact() {
        return fact;
    }

    @Override
    public InspectionDimension getDimension() {
        return dimension;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_WEEK;
    }


    public static class CollectionSizeFact implements Fact {

        private final AtomicInteger maxSize;
        private final AtomicInteger minSize;
        private final AtomicInteger avgSize;


        CollectionSizeFact(AtomicInteger maxSize, AtomicInteger minSize, AtomicInteger avgSize) {
            this.maxSize = maxSize;
            this.minSize = minSize;
            this.avgSize = avgSize;
        }

        @MetricField(name = "Max", fieldType = MetricField.FieldType.INTEGER)
        private Integer getMax() {
            return maxSize.get();
        }

        @MetricField(name = "Min", fieldType = MetricField.FieldType.INTEGER)
        private Integer getMin() {
            return minSize.get();
        }

        @MetricField(name = "Avg", fieldType = MetricField.FieldType.INTEGER)
        private Integer getAvg() {
            return avgSize.get();
        }

    }

}
