package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchVersion;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

public class EntityMatchRematchDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log =
            LoggerFactory.getLogger(EntityMatchRematchDeploymentTestNG.class);

    @Inject
    private MatchProxy matchProxy;

    private String customerSpace;

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        log.info("Setup Complete!");
    }

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT);

        importData();

        EntityMatchVersion entityMatchVersion = matchProxy.getEntityMatchVersion(customerSpace,
                EntityMatchEnvironment.SERVING, false);
        log.info("entityMatchVersion is {}.", entityMatchVersion);
        runTestWithRetry(getCandidateFailingSteps(), true);
        EntityMatchVersion entityMatchVersionAfterPA = matchProxy.getEntityMatchVersion(customerSpace,
                EntityMatchEnvironment.SERVING, false);
        Assert.assertEquals(entityMatchVersion.getNextVersion(), entityMatchVersionAfterPA.getCurrentVersion());
    }

    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Account, ADVANCED_MATCH_SUFFIX, 1, "DefaultSystem_AccountData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, ADVANCED_MATCH_SUFFIX, 1, "DefaultSystem_ContactData");
        Thread.sleep(2000);
    }

    private List<String> getCandidateFailingSteps() {
        return Arrays.asList(
                "matchAccount",
                "mergeAccount", //
                "mergeContact");
    }
}
