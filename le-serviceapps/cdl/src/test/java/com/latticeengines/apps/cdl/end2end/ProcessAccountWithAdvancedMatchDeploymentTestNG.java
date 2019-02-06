package com.latticeengines.apps.cdl.end2end;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;
import java.util.Map;

public class ProcessAccountWithAdvancedMatchDeploymentTestNG  extends ProcessAccountDeploymentTestNG {
    private static final Logger log = LoggerFactory.getLogger(ProcessAccountWithAdvancedMatchDeploymentTestNG.class);


    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
        log.error("$JAW$ Running setup with ENABLE_ENTITY_MATCH!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        log.error("$JAW$ Setup Complete!");
    }

}
