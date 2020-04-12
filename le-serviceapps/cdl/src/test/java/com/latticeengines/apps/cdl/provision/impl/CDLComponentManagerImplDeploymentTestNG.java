package com.latticeengines.apps.cdl.provision.impl;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class CDLComponentManagerImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentManagerImplDeploymentTestNG.class);

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private BatonService batonService;

    @BeforeClass(groups = "deployment")
    public void setup() {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupTestEnvironmentWithFeatureFlags(featureFlagMap);
        log.info("Setup ENABLE_ENTITY_MATCH tenant complete!");
    }

    @Test(groups = "deployment")
    public void testSystemInstallation() {
        log.info("Feature flag in test");
        log.info(JsonUtils.serialize(batonService.getFeatureFlags(CustomerSpace.parse(mainCustomerSpace))));
        Assert.assertTrue(CollectionUtils.isEmpty(cdlProxy.getS3ImportSystemList(mainCustomerSpace)));
    }
}
