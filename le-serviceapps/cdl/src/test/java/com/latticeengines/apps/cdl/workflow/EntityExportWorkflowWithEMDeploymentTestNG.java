package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.end2end.ProcessAccountWithAdvancedMatchDeploymentTestNG;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

/**
 * dpltc deploy -a admin,pls,lp,cdl,metadata,matchapi,workflowapi
 */
public class EntityExportWorkflowWithEMDeploymentTestNG extends EntityExportWorkflowDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(EntityExportWorkflowWithEMDeploymentTestNG.class);

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH_GA enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        checkpointService.resumeCheckpoint(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT, CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
        log.info("Setup Complete!");
        configExportAttrs();
        saveCsvToLocal = false;
    }

    protected void verifyCsvGzHeder(Map<String, Integer> headerMap) {
        // make sure header map contains customer account and contact id
        Assert.assertTrue(headerMap.containsKey("CustomerContactId"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("CustomerAccountId"), "Header map: " + JsonUtils.serialize(headerMap));
        // make sure no account and contact id
        Assert.assertFalse(headerMap.containsKey("Atlas Account ID"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertFalse(headerMap.containsKey("Atlas Contact ID"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("CEO Name"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("Test Date"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("Has Oracle Commerce"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey(InterfaceName.AtlasExportTime.name()), "Header map: " + JsonUtils.serialize(headerMap));
    }

}
