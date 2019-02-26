package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;

public class ProcessAccountWithAdvancedMatchDeploymentTestNG  extends ProcessAccountDeploymentTestNG {
    private static final Logger log = LoggerFactory.getLogger(ProcessAccountWithAdvancedMatchDeploymentTestNG.class);


    @BeforeClass(groups = { "end2end" })
    @Override
    public void setup() throws Exception {
        log.error("$JAW$ Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        log.error("$JAW$ Setup Complete!");
    }


    //@Test(groups = "end2end")
    @Test(groups = "end2end", enabled = false)
    @Override
    public void runTest() throws Exception {
        super.runTest();
    }
}
