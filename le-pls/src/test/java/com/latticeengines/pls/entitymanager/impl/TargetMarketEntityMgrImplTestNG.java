package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.joda.time.DateTimeZone;
import org.python.google.common.base.Predicate;
import org.python.google.common.collect.Iterables;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowConfiguration;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOptionName;
import com.latticeengines.domain.exposed.pls.TargetMarketReportMap;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class TargetMarketEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    TargetMarketEntityMgr targetMarketEntityMgr;

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
        TARGET_MARKET.setSelectedIntent(SELECTED_INTENT);
        TARGET_MARKET.setReports(TARGET_MARKET_REPORTS);

        TargetMarketDataFlowConfiguration configuration = TARGET_MARKET.getDataFlowConfiguration();
        configuration.setInt(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends,
                NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS);
        configuration.setString(TargetMarketDataFlowOptionName.IntentScoreThreshold, INTENT_SCORE_THRESHOLD.toString());
        configuration.setDouble(TargetMarketDataFlowOptionName.FitScoreThreshold, FIT_SCORE_THRESHOLD);
        configuration.setBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts,
                DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS);
        configuration.setInt(TargetMarketDataFlowOptionName.MaxProspectsPerAccount, MAX_PROSPECTS_PER_ACCOUNT);

        TARGET_MARKET_STATISTICS.setExpectedLift(EXPECTED_LIFT);
        TARGET_MARKET_STATISTICS.setIsOutOfDate(IS_OUT_OF_DATE);
        TARGET_MARKET_STATISTICS.setMarketRevenue(MARKET_REVENUE);
        TARGET_MARKET_STATISTICS.setNumAccounts(NUM_ACCOUNTS);
        TARGET_MARKET_STATISTICS.setNumCompanies(NUM_COMPANIES);
        TARGET_MARKET_STATISTICS.setNumCustomers(NUM_CUSTOMERS);
        TARGET_MARKET_STATISTICS.setRevenue(REVENUE);
        TARGET_MARKET.setTargetMarketStatistics(TARGET_MARKET_STATISTICS);

        setupUsers();
        cleanupTargetMarketDB();
    }

    @Test(groups = { "functional" })
    public void create_calledWithParameters_assertAllAttributesArePersisted() {
        setupSecurityContext(mainTestingTenant);
        assertNull(targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME));

        targetMarketEntityMgr.create(TARGET_MARKET);

        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getName(), TEST_TARGET_MARKET_NAME);
        assertEquals(targetMarket.getDescription(), DESCRIPTION);
        assertEquals(targetMarket.getCreationTimestampObject().getDayOfYear(),
                CREATION_DATE.toDateTime(DateTimeZone.UTC).getDayOfYear());
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED);
        assertEquals(targetMarket.getModelId(), MODEL_ID);
        assertEquals(targetMarket.getEventColumnName(), EVENT_COLUMN_NAME);
        assertEquals(targetMarket.getIsDefault(), IS_DEFAULT);
        assertEquals(targetMarket.getOffset(), OFFSET);
        assertEquals(targetMarket.getAccountFilterString(), JsonUtils.serialize(ACCOUNT_FILTER));
        assertEquals(targetMarket.getContactFilterString(), JsonUtils.serialize(CONTACT_FILTER));
        assertEquals(targetMarket.getTargetMarketStatistics(), TARGET_MARKET_STATISTICS);
        assertEquivalent(targetMarket.getReports(), TARGET_MARKET_REPORTS);

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

    @Test(groups = { "functional" }, dependsOnMethods = { "create_calledWithParameters_assertAllAttributesArePersisted" })
    public void targetMarketCreatedInOneTenant_switchToAlternativeTenant_assertTargetMarketCannnotBeFound() {
        setupSecurityContext(ALTERNATIVE_TESTING_TENANT);

        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);

        assertNull(targetMarket);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "targetMarketCreatedInOneTenant_switchToAlternativeTenant_assertTargetMarketCannnotBeFound" })
    public void update_calledWithParameters_assertTargetMarketIsUpdated() {
        setupSecurityContext(mainTestingTenant);

        TARGET_MARKET.setPid(null);
        TARGET_MARKET.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED_1);
        targetMarketEntityMgr.updateTargetMarketByName(TARGET_MARKET, TEST_TARGET_MARKET_NAME);

        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED_1);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "update_calledWithParameters_assertTargetMarketIsUpdated" })
    public void delete_calledWithParameters_assertTargetMarketIsDeleted() {
        setupSecurityContext(mainTestingTenant);
        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);

        targetMarketEntityMgr.deleteTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNull(targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME));
    }

    private void assertEquivalent(final List<TargetMarketReportMap> actual, final List<TargetMarketReportMap> expected) {
        assertEquals(actual.size(), expected.size());
        assertTrue(Iterables.all(actual, new Predicate<TargetMarketReportMap>() {
            @Override
            public boolean apply(final TargetMarketReportMap actualReportMap) {
                return Iterables.any(expected, new Predicate<TargetMarketReportMap>() {

                    @Override
                    public boolean apply(TargetMarketReportMap expectedReportMap) {
                        String expectedReportName = expectedReportMap.getReportName();
                        String actualReportName = actualReportMap.getReportName();
                        return expectedReportName != null && actualReportName != null
                                && expectedReportName.equals(actualReportName);
                    }
                });
            }
        }));
    }
}
