package com.latticeengines.domain.exposed.modeling.factory;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;

public class ModelFactory {

    private static final Logger log = LoggerFactory.getLogger(ModelFactory.class);

    public static final String MODEL_CONFIG = "modelConfig";

    protected static SelectedConfig getModelConfig(Map<String, String> runTimeParams) {
        if (runTimeParams == null || !runTimeParams.containsKey(MODEL_CONFIG)) {
            log.info("There's no model config!");
            return null;
        }
        try {
            log.info("Model Config=" + runTimeParams.get(MODEL_CONFIG));
            SelectedConfig selectedConfig = JsonUtils.deserialize(runTimeParams.get(MODEL_CONFIG), SelectedConfig.class);
            return selectedConfig;
        } catch (Exception ex) {
            log.warn("Failed to get model config!");
        }

        return null;
    }
}
