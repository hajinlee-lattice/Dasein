package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class TargetMarketResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_TARGETMARKET_URL = "pls/targetmarket/";

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
        TARGET_MARKET.setDeliverProspectsFromExistingAccounts(DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS);
        TARGET_MARKET.setAccountFilter(ACCOUNT_FILTER);
        TARGET_MARKET.setContactFilter(CONTACT_FILTER);
        TARGET_MARKET.setIsDefault(IS_DEFAULT);
        TARGET_MARKET.setOffset(OFFSET);
        TARGET_MARKET.setIntentSort(SORT);

        setupUsers();
        cleanupTargetMarketDB();
    }

    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        // using admin session by default
        switchToSuperAdmin();
    }

    @Test(groups = { "functional" })
    public void postTargetMarket_calledWithTargetMarket_assertAllAttributesArePersisted() {
        restTemplate.postForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL, TARGET_MARKET, TargetMarket.class);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getName(), TEST_TARGET_MARKET_NAME);
        assertEquals(targetMarket.getDescription(), DESCRIPTION);
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED);
        assertEquals(targetMarket.getNumDaysBetweenIntentProspectResends(), NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS);
        assertEquals(targetMarket.getIntentScoreThreshold(), INTENT_SCORE_THRESHOLD);
        assertEquals(targetMarket.getFitScoreThreshold(), FIT_SCORE_THRESHOLD);
        assertEquals(targetMarket.getModelId(), MODEL_ID);
        assertEquals(targetMarket.getEventColumnName(), EVENT_COLUMN_NAME);
        assertEquals(targetMarket.isDeliverProspectsFromExistingAccounts(), DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS);
        assertEquals(targetMarket.getIsDefault(), IS_DEFAULT);
        assertEquals(targetMarket.getOffset(), OFFSET);
        assertEquals(targetMarket.getAccountFilterString(), JsonUtils.serialize(ACCOUNT_FILTER));
        assertEquals(targetMarket.getContactFilterString(), JsonUtils.serialize(CONTACT_FILTER));
        assertEquals(targetMarket.getIntentSortString(), JsonUtils.serialize(SORT));
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "postTargetMarket_calledWithTargetMarket_assertAllAttributesArePersisted" })
    public void updateTargetMarket_calledWithParameters_assertTargetMarketUpdated() {
        TARGET_MARKET.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED_1);

        restTemplate.put(getRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME, TARGET_MARKET);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getName(), TEST_TARGET_MARKET_NAME);
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED_1);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "updateTargetMarket_calledWithParameters_assertTargetMarketUpdated" })
    public void deleteTargetMarketPosted_calledWithTargetMarketName_assertTargetMarketDeleted() {
        restTemplate.delete(getRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertNull(targetMarket);
    }

}
