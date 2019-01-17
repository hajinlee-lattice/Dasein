package com.latticeengines.modelquality.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modeling.factory.SamplingFactory;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyDef;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyValue;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.service.SamplingService;

@Component("samplingService")
public class SamplingServiceImpl extends BaseServiceImpl implements SamplingService {

    @Autowired
    private SamplingEntityMgr samplingEntityMgr;

    @Override
    public Sampling createLatestProductionSamplingConfig() {
        String version = getLedsVersion();
        String samplingName = "PRODUCTION-" + version.replace('/', '_');

        Sampling sampling = samplingEntityMgr.findByName(samplingName);

        if (sampling != null) {
            return sampling;
        }

        sampling = new Sampling();
        sampling.setName(samplingName);

        SamplingPropertyDef defSeed = new SamplingPropertyDef();
        defSeed.setName(SamplingFactory.MODEL_SAMPLING_SEED_KEY);
        SamplingPropertyValue valueSeed = new SamplingPropertyValue("123456");
        defSeed.addSamplingPropertyValue(valueSeed);
        sampling.addSamplingPropertyDef(defSeed);

        SamplingPropertyDef defRate = new SamplingPropertyDef();
        defRate.setName(SamplingFactory.MODEL_SAMPLING_RATE_KEY);
        SamplingPropertyValue valueRate = new SamplingPropertyValue("100");
        defRate.addSamplingPropertyValue(valueRate);
        sampling.addSamplingPropertyDef(defRate);

        SamplingPropertyDef defTraining = new SamplingPropertyDef();
        defTraining.setName(SamplingFactory.MODEL_SAMPLING_TRAINING_PERCENTAGE_KEY);
        SamplingPropertyValue valueTraining = new SamplingPropertyValue("80");
        defTraining.addSamplingPropertyValue(valueTraining);
        sampling.addSamplingPropertyDef(defTraining);

        SamplingPropertyDef defTesting = new SamplingPropertyDef();
        defTesting.setName(SamplingFactory.MODEL_SAMPLING_TEST_PERCENTAGE_KEY);
        SamplingPropertyValue valueTesting = new SamplingPropertyValue("20");
        defTesting.addSamplingPropertyValue(valueTesting);
        sampling.addSamplingPropertyDef(defTesting);

        Sampling previousLatest = samplingEntityMgr.getLatestProductionVersion();
        int versionNo = 1;
        if (previousLatest != null) {
            versionNo = previousLatest.getVersion() + 1;
        }
        sampling.setVersion(versionNo);

        samplingEntityMgr.create(sampling);

        return sampling;
    }

}
