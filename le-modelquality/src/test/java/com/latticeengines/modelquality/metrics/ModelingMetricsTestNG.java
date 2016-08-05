package com.latticeengines.modelquality.metrics;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

public class ModelingMetricsTestNG extends ModelQualityDeploymentTestNGBase {

    @Autowired
    private MetricService metricService;

    @Test(groups = "deployment")
    public void run() throws Exception {
        String mStr = FileUtils.readFileToString(new File( // 
                ClassLoader.getSystemResource("com/latticeengines/modelquality/metrics/modelsummary.json").getFile()));
        String cStr = FileUtils.readFileToString(new File( // 
                ClassLoader.getSystemResource("com/latticeengines/modelquality/metrics/selectedconfig.json").getFile()));
        ModelSummary m = JsonUtils.deserialize(mStr, ModelSummary.class);
        SelectedConfig s = JsonUtils.deserialize(cStr, SelectedConfig.class);

        ModelQualityMetrics metrics = new ModelQualityMetrics(m, s);
        ModelingMeasurement measurement = new ModelingMeasurement(metrics);
        metricService.write(MetricDB.MODEL_QUALITY, measurement);
        
        try {
            Thread.sleep(5000L);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
