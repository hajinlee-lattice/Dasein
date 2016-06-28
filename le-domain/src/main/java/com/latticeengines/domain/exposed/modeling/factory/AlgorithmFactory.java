package com.latticeengines.domain.exposed.modeling.factory;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyDef;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyValue;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;

public class AlgorithmFactory {

    private static final Log log = LogFactory.getLog(AlgorithmFactory.class);

    public static final String MODEL_CONFIG = "modelConfig";

    public static final String ALGORITHM_NAME_RF = "RF";
    public static final String ALGORITHM_NAME_LR = "LR";
    public static final String ALGORITHM_NAME_DT = "DT";

    private static final String SAMPLE_NAME = "all";

    public static Algorithm createAlgorithm(Map<String, String> runTimeParams) {

        log.info("Check and Create new algorithm.");

        com.latticeengines.domain.exposed.modelquality.Algorithm modelAlgo = getModelAlgorithm(runTimeParams);
        if (modelAlgo == null) {
            return null;
        }
        Algorithm algorithm = null;
        switch (modelAlgo.getName()) {
        case ALGORITHM_NAME_RF:
            algorithm = createRF(modelAlgo);
            break;
        case ALGORITHM_NAME_LR:
            algorithm = createLR(modelAlgo);
            break;
        case ALGORITHM_NAME_DT:
            algorithm = createDT(modelAlgo);
            break;
        }
        log.info("Successfully created the Algorithm=" + algorithm.getName() + " algorithm properties="
                + algorithm.getAlgorithmProperties());
        return algorithm;
    }

    private static com.latticeengines.domain.exposed.modelquality.Algorithm getModelAlgorithm(
            Map<String, String> runTimeParams) {

        if (!runTimeParams.containsKey(MODEL_CONFIG)) {
            log.info("There's no model config!");
            return null;
        }
        try {
            log.info("Model Config=" + runTimeParams.get(MODEL_CONFIG));
            SelectedConfig selectedConfig = JsonUtils.deserialize(runTimeParams.get(MODEL_CONFIG),
                    com.latticeengines.domain.exposed.modelquality.SelectedConfig.class);
            return selectedConfig.getAlgorithm();

        } catch (Exception ex) {
            log.warn("Failed to create Algorithm!", ex);
        }
        return null;
    }

    private static Algorithm createDT(com.latticeengines.domain.exposed.modelquality.Algorithm modelAlgo) {
        AlgorithmBase algo = new DecisionTreeAlgorithm();
        configAlgorithm(algo, modelAlgo);
        return algo;
    }

    private static Algorithm createLR(com.latticeengines.domain.exposed.modelquality.Algorithm modelAlgo) {
        AlgorithmBase algo = new LogisticRegressionAlgorithm();
        configAlgorithm(algo, modelAlgo);
        return algo;
    }

    private static Algorithm createRF(com.latticeengines.domain.exposed.modelquality.Algorithm modelAlgo) {
        AlgorithmBase algo = new RandomForestAlgorithm();
        configAlgorithm(algo, modelAlgo);
        return algo;
    }

    private static void configAlgorithm(AlgorithmBase algo,
            com.latticeengines.domain.exposed.modelquality.Algorithm modelAlgo) {

        algo.setName(modelAlgo.getName());
        algo.setSampleName(SAMPLE_NAME);
        if (StringUtils.isNotEmpty(modelAlgo.getScript())) {
            algo.setScript(modelAlgo.getScript());
        }
        List<AlgorithmPropertyDef> defs = modelAlgo.getAlgorithmPropertyDefs();
        if (CollectionUtils.isNotEmpty(defs)) {
            StringBuilder builder = new StringBuilder();
            for (AlgorithmPropertyDef def : defs) {
                if (StringUtils.isEmpty(def.getName())) {
                    continue;
                }
                List<AlgorithmPropertyValue> values = def.getAlgorithmPropertyValues();
                if (CollectionUtils.isNotEmpty(values)) {
                    AlgorithmPropertyValue value = values.get(0);
                    if (value != null && StringUtils.isNotEmpty(value.getValue())) {
                        builder.append(def.getName()).append("=").append(value.getValue()).append(" ");
                    }
                }
            }
            if (builder.length() > 0) {
                builder.trimToSize();
                algo.setAlgorithmProperties(builder.toString());
            }
        }
    }

}
