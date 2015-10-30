package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class TargetMarketEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    TargetMarketEntityMgr targetMarketEntityMgr;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        TARGET_MARKET.setName(TEST_TARGET_MARKET_NAME);
        TARGET_MARKET.setCreationDate(CREATION_DATE);
        TARGET_MARKET.setDescription(DESCRIPTION);
        TARGET_MARKET.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED);
        TARGET_MARKET.setNumDaysBetweenIntentProspectResends(NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS);
        TARGET_MARKET.setIntentScoreThreshold(INTENT_SCORE_THRESHOLD);
        TARGET_MARKET.setFitScoreThreshold(FIT_SCORE_THRESHOLD);
        TARGET_MARKET.setModelId(MODEL_ID);
        TARGET_MARKET.setEventColumnName(EVENT_COLUMN_NAME);
        TARGET_MARKET.setDeliverProspectsFromExistingAccounts(DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS);;
        TARGET_MARKET.setAccountFilter(ACCOUNT_FILTER);
        TARGET_MARKET.setContactFilter(CONTACT_FILTER);

        setUpMarketoEloquaTestEnvironment();
        cleanupTargetMarketDB();
    }

    @Test(groups = { "functional" })
    public void create_calledWithParameters_assertTargetMarketIsCreated() {
        setupSecurityContext(mainTestingTenant);
        assertNull(this.targetMarketEntityMgr
                .findTargetMarketByName(TEST_TARGET_MARKET_NAME));

        this.targetMarketEntityMgr.create(TARGET_MARKET);

        TargetMarket targetMarket = this.targetMarketEntityMgr
                .findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getName(), TEST_TARGET_MARKET_NAME);
        assertEquals(targetMarket.getAccountFilter(), JsonUtils.serialize(ACCOUNT_FILTER));
        assertEquals(targetMarket.getContactFilter(), JsonUtils.serialize(CONTACT_FILTER));
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "create_calledWithParameters_assertTargetMarketIsCreated" })
    public void targetMarketCreatedInOneTenant_switchToAlternativeTenant_assertTargetMarketCannnotBeFound() {
        setupSecurityContext(ALTERNATIVE_TESTING_TENANT);
        
        TargetMarket targetMarket = this.targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        
        assertNull(targetMarket);
    }
    
    @Test(groups = { "functional" }, dependsOnMethods = { "targetMarketCreatedInOneTenant_switchToAlternativeTenant_assertTargetMarketCannnotBeFound" })
    public void update_calledWithParameters_assertTargetMarketIsUpdated() {
        setupSecurityContext(mainTestingTenant);
        
        TARGET_MARKET.setPid(null);
        TARGET_MARKET.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED_1);
        this.targetMarketEntityMgr.updateTargetMarketByName(TARGET_MARKET, TEST_TARGET_MARKET_NAME);

        TargetMarket targetMarket = this.targetMarketEntityMgr
                .findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED_1);
    }
    
    @Test(groups = { "functional" }, dependsOnMethods = { "update_calledWithParameters_assertTargetMarketIsUpdated" })
    public void delete_calledWithParameters_assertTargetMarketIsDeleted() {
        setupSecurityContext(mainTestingTenant);
        TargetMarket targetMarket = this.targetMarketEntityMgr
                .findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);

        this.targetMarketEntityMgr
                .deleteTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNull(this.targetMarketEntityMgr
                .findTargetMarketByName(TEST_TARGET_MARKET_NAME));
    }

}
