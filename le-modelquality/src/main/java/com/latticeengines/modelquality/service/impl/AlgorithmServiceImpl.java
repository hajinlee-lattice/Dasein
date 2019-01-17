package com.latticeengines.modelquality.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyDef;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyValue;
import com.latticeengines.domain.exposed.modelquality.AlgorithmType;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.service.AlgorithmService;

@Component("algorithmService")
public class AlgorithmServiceImpl extends BaseServiceImpl implements AlgorithmService {

    @Autowired
    private AlgorithmEntityMgr algorithmEntityMgr;

    @Override
    public Algorithm createLatestProductionAlgorithm() {
        String version = getLedsVersion();
        String algorithmName = "PRODUCTION-" + version.replace('/', '_');
        Algorithm algorithm = algorithmEntityMgr.findByName(algorithmName);
        if (algorithm != null) {
            return algorithm;
        }

        algorithm = new Algorithm();
        algorithm.setName(algorithmName);
        algorithm.setType(AlgorithmType.RANDOMFOREST);
        String algorithmScript = String.format("/datascience/%s/dataplatform/scripts/algorithm/rf_train.py", version);
        algorithm.setScript(algorithmScript);

        AlgorithmPropertyDef def = new AlgorithmPropertyDef("n_estimators");
        AlgorithmPropertyValue value = new AlgorithmPropertyValue("200");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("random_state");
        value = new AlgorithmPropertyValue("123456");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("criterion");
        value = new AlgorithmPropertyValue("gini");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("n_jobs");
        value = new AlgorithmPropertyValue("5");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("min_samples_split");
        value = new AlgorithmPropertyValue("25");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("min_samples_leaf");
        value = new AlgorithmPropertyValue("20");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("max_depth");
        value = new AlgorithmPropertyValue("6");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("bootstrap");
        value = new AlgorithmPropertyValue("True");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("calibration_width");
        value = new AlgorithmPropertyValue("4");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("cross_validation");
        value = new AlgorithmPropertyValue("5");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        Algorithm previousLatest = algorithmEntityMgr.getLatestProductionVersion();
        int versionNo = 1;
        if (previousLatest != null) {
            versionNo = previousLatest.getVersion() + 1;
        }
        algorithm.setVersion(versionNo);

        algorithmEntityMgr.create(algorithm);
        return algorithm;
    }

}
