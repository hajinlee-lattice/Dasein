package com.latticeengines.domain.exposed.modeling.factory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyDef;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyValue;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;

public class SamplingFactory extends ModelFactory {

    private static final Log log = LogFactory.getLog(SamplingFactory.class);

    public static final String MODEL_SAMPLING_RATE_KEY = "model.sampling.rate";
    public static final String MODEL_SAMPLING_TRAINING_PERCENTAGE_KEY = "model.sampling.training.percentage";
    public static final String MODEL_SAMPLING_TEST_PERCENTAGE_KEY = "model.sampling.test.percentage";

    public static void configSampling(SamplingConfiguration samplingConfig, Map<String, String> runTimeParams) {

        log.info("Check and Config sampling.");

        com.latticeengines.domain.exposed.modelquality.Sampling sampling = getModelSampling(runTimeParams);
        if (sampling == null) {
            return;
        }
        samplingConfig.setParallelEnabled(sampling.isParallelEnabled());
        Map<String, String> paramMap = getParamMap(sampling);
        try {
            if (paramMap.containsKey(MODEL_SAMPLING_RATE_KEY)) {
                samplingConfig.setSamplingRate(Integer.parseInt(paramMap.get(MODEL_SAMPLING_RATE_KEY)));
            }
            if (paramMap.containsKey(MODEL_SAMPLING_TRAINING_PERCENTAGE_KEY)) {
                samplingConfig.setTrainingPercentage(Integer.parseInt(paramMap
                        .get(MODEL_SAMPLING_TRAINING_PERCENTAGE_KEY)));
            }
            if (paramMap.containsKey(MODEL_SAMPLING_TEST_PERCENTAGE_KEY)) {
                samplingConfig.setTestPercentage(Integer.parseInt(paramMap.get(MODEL_SAMPLING_TEST_PERCENTAGE_KEY)));
            }

        } catch (Exception ex) {
            log.warn("Failed to config sampling!", ex);
        }
        log.info("Successfully configured the Sampling");

    }

    private static Map<String, String> getParamMap(Sampling sampling) {
        Map<String, String> paramMap = new HashMap<>();
        List<SamplingPropertyDef> defs = sampling.getSamplingPropertyDefs();
        if (defs != null) {
            for (SamplingPropertyDef def : defs) {
                List<SamplingPropertyValue> values = def.getSamplingPropertyValues();
                if (values.size() > 0 && StringUtils.isNotEmpty(values.get(0).getValue())) {
                    paramMap.put(def.getName(), values.get(0).getValue());
                }
            }
        }
        return paramMap;
    }

    private static com.latticeengines.domain.exposed.modelquality.Sampling getModelSampling(
            Map<String, String> runTimeParams) {
        SelectedConfig selectedConfig = getModelConfig(runTimeParams);
        if (selectedConfig != null) {
            return selectedConfig.getSampling();
        }
        return null;
    }

}
