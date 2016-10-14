package com.latticeengines.modelquality.metrics;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public class ModelQualityMetrics implements Fact, Dimension {

    private ModelSummary modelSummary;

    private SelectedConfig selectedConfig;

    private ModelRunEntityNames modelRunEntityNames;

    public ModelQualityMetrics(ModelSummary modelSummary, SelectedConfig selectedConfig,
            ModelRunEntityNames modelRunEntityNames) {
        this.modelSummary = modelSummary;
        this.selectedConfig = selectedConfig;
        this.modelRunEntityNames = modelRunEntityNames;
    }

    @MetricFieldGroup
    @MetricTagGroup
    public ModelSummary getModelSummary() {
        return modelSummary;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    @MetricTagGroup
    public SelectedConfig getSelectedConfig() {
        return selectedConfig;
    }

    public void setSelectedConfig(SelectedConfig selectedConfig) {
        this.selectedConfig = selectedConfig;
    }

    @MetricTagGroup
    public ModelRunEntityNames getModelRunEntityNames() {
        return modelRunEntityNames;
    }

    public void setModelRunEntityNames(ModelRunEntityNames modelRunEntityNames) {
        this.modelRunEntityNames = modelRunEntityNames;
    }

}
