package com.latticeengines.modelquality.metrics;

import java.io.File;
import java.nio.charset.Charset;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.modelquality.functionalframework.ModelQualityTestNGBase;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
public class ModelingMetricsDeploymentTestNG extends ModelQualityTestNGBase {

    @Inject
    private MetricService metricService;

    @Test(groups = "deployment")
    public void run() throws Exception {
        String mStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/metrics/modelsummary.json").getFile()),
                Charset.defaultCharset());
        String cStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/metrics/selectedconfig.json")
                        .getFile()),
                Charset.defaultCharset());
        String nStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/metrics/modelrunentitynames.json")
                        .getFile()),
                Charset.defaultCharset());
        SelectedConfig s = JsonUtils.deserialize(cStr, SelectedConfig.class);
        ModelSummary m = JsonUtils.deserialize(mStr, ModelSummary.class);
        ModelRunEntityNames n = JsonUtils.deserialize(nStr, ModelRunEntityNames.class);

        ModelQualityMetrics metrics = new ModelQualityMetrics(m, s, n);
        ModelingMeasurement measurement = new ModelingMeasurement(metrics);
        metricService.write(MetricDB.MODEL_QUALITY, measurement);
        SleepUtils.sleep(5000l);
    }
}
