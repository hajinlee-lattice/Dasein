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
        String version = getVersion();
        String samplingName = "PRODUCTION-" + version.replace('/', '_');
        
        Sampling sampling = samplingEntityMgr.findByName(samplingName);
        
        if(sampling != null)
        {
            return sampling;
        }
        
        sampling =new Sampling();
        sampling.setName(samplingName);

        SamplingPropertyDef def = new SamplingPropertyDef();
        def.setName(SamplingFactory.MODEL_SAMPLING_RATE_KEY);
        SamplingPropertyValue value = new SamplingPropertyValue("100");
        def.addSamplingPropertyValue(value);
        sampling.addSamplingPropertyDef(def);

        def.setName(SamplingFactory.MODEL_SAMPLING_TRAINING_PERCENTAGE_KEY);
        value = new SamplingPropertyValue("80");
        def.addSamplingPropertyValue(value);
        sampling.addSamplingPropertyDef(def);

        def.setName(SamplingFactory.MODEL_SAMPLING_TEST_PERCENTAGE_KEY);
        value = new SamplingPropertyValue("20");
        def.addSamplingPropertyValue(value);
        sampling.addSamplingPropertyDef(def);
        
        samplingEntityMgr.create(sampling);

        return sampling;
    }

}
