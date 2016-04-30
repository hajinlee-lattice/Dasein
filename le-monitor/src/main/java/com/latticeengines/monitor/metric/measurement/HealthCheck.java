package com.latticeengines.monitor.metric.measurement;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class HealthCheck extends BaseMeasurement<HealthCheck.HealthFact, InspectionDimension>
        implements Measurement<HealthCheck.HealthFact, InspectionDimension> {

    private final InspectionDimension dimension;
    private final HealthFact fact = new HealthFact();

    public HealthCheck(String component) {
        this.dimension = new InspectionDimension(component);
    }

    @Override
    public HealthFact getFact() {
        return fact;
    }

    @Override
    public InspectionDimension getDimension() {
        return dimension;
    }


    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_HOUR;
    }

    public static class HealthFact implements Fact {

        @MetricField(name = "Alive", fieldType = MetricField.FieldType.INTEGER)
        private Integer isAlive() {
            return 1;
        }

    }

}
