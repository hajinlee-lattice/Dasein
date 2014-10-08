package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;

public class ModelingServiceImplUnitTestNG {

    private ModelingServiceImpl modelingService = null;
    private Algorithm algorithm = null;
    
    @BeforeClass(groups = "unit")
    public void setup() {
        modelingService = new ModelingServiceImpl();
        algorithm = new AlgorithmBase();
    }
    
    @Test(groups = "unit")
    public void doThrottlingNullConfig() {
        assertFalse(modelingService.doThrottling(null, algorithm, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingDisabledConfig() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setEnabled(false);
        assertFalse(modelingService.doThrottling(config, algorithm, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingEnabledConfigCutoff2() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setJobRankCutoff(2);
        assertFalse(modelingService.doThrottling(config, algorithm, 1));
    }

    @Test(groups = "unit")
    public void doThrottlingEnabledConfigCutoff1() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setJobRankCutoff(1);
        assertTrue(modelingService.doThrottling(config, algorithm, 1));
    }
}
