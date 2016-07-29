package com.latticeengines.modelquality.metrics;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class ModelingMeasurement extends BaseMeasurement<ModelQualityMetrics, ModelQualityMetrics> implements
        Measurement<ModelQualityMetrics, ModelQualityMetrics> {

    private ModelQualityMetrics modelQualityMetrics;

    public ModelingMeasurement(ModelQualityMetrics modelQualityMetrics) {
        this.modelQualityMetrics = modelQualityMetrics;
    }

    public ModelQualityMetrics getModelQualityMetrics() {
        return modelQualityMetrics;
    }

    @Override
    public ModelQualityMetrics getDimension() {
        return getModelQualityMetrics();
    }

    @Override
    public ModelQualityMetrics getFact() {
        return getModelQualityMetrics();
    }
}
