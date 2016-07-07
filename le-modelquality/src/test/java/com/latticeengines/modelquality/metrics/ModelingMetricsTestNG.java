package com.latticeengines.modelquality.metrics;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

public class ModelingMetricsTestNG extends ModelQualityDeploymentTestNGBase {

    @Autowired
    private MetricService metricService;

    @Test(groups = "deployment")
    public void run() {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setId("Id1");
        modelSummary.setRocScore(0.875);
        modelSummary.setTop10PercentLift(0.5);
        modelSummary.setTop20PercentLift(0.7);
        modelSummary.setTop30PercentLift(0.85);

        SelectedConfig config = getSelectedConfig(AlgorithmFactory.ALGORITHM_NAME_RF);

        ModelQualityMetrics metrics = new ModelQualityMetrics(modelSummary, config);
        ModelingMesurement measurement = new ModelingMesurement(metrics);
        metricService.write(MetricDB.MODEL_QUALITY, measurement);
        try {
            Thread.sleep(5000L);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
