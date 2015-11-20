package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowConfiguration;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOptionName;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class TargetMarketResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_TARGETMARKET_URL = "pls/targetmarkets/";

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        TARGET_MARKET.setName(TEST_TARGET_MARKET_NAME);
        TARGET_MARKET.setCreationTimestampObject(CREATION_DATE);
        TARGET_MARKET.setDescription(DESCRIPTION);
        TARGET_MARKET.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED);
        TARGET_MARKET.setModelId(MODEL_ID);
        TARGET_MARKET.setEventColumnName(EVENT_COLUMN_NAME);
        TARGET_MARKET.setAccountFilter(ACCOUNT_FILTER);
        TARGET_MARKET.setContactFilter(CONTACT_FILTER);
        TARGET_MARKET.setIsDefault(IS_DEFAULT);
        TARGET_MARKET.setOffset(OFFSET);
        TARGET_MARKET.setIntentSort(SORT);

        TargetMarketDataFlowConfiguration configuration = TARGET_MARKET.getDataFlowConfiguration();
        configuration.setInt(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends,
                NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS);
        configuration.setString(TargetMarketDataFlowOptionName.IntentScoreThreshold, INTENT_SCORE_THRESHOLD.toString());
        configuration.setDouble(TargetMarketDataFlowOptionName.FitScoreThreshold, FIT_SCORE_THRESHOLD);
        configuration.setBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts,
                DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS);
        configuration.setInt(TargetMarketDataFlowOptionName.MaxProspectsPerAccount, MAX_PROSPECTS_PER_ACCOUNT);

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
        assertEquals(targetMarket.getCreationTimestampObject().getDayOfYear(), CREATION_DATE.getDayOfYear());
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED);
        assertEquals(targetMarket.getModelId(), MODEL_ID);
        assertEquals(targetMarket.getEventColumnName(), EVENT_COLUMN_NAME);
        assertEquals(targetMarket.getIsDefault(), IS_DEFAULT);
        assertEquals(targetMarket.getOffset(), OFFSET);
        assertEquals(targetMarket.getAccountFilterString(), JsonUtils.serialize(ACCOUNT_FILTER));
        assertEquals(targetMarket.getContactFilterString(), JsonUtils.serialize(CONTACT_FILTER));

        TargetMarketDataFlowConfiguration configuration = targetMarket.getDataFlowConfiguration();
        assertEquals(configuration.getInt(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends),
                NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS.intValue());
        assertEquals(configuration.getString(TargetMarketDataFlowOptionName.IntentScoreThreshold),
                INTENT_SCORE_THRESHOLD.toString());
        assertEquals(configuration.getDouble(TargetMarketDataFlowOptionName.FitScoreThreshold),
                FIT_SCORE_THRESHOLD.doubleValue());
        assertEquals(configuration.getBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts),
                DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS.booleanValue());
        assertEquals(configuration.getInt(TargetMarketDataFlowOptionName.MaxProspectsPerAccount),
                MAX_PROSPECTS_PER_ACCOUNT.intValue());
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
