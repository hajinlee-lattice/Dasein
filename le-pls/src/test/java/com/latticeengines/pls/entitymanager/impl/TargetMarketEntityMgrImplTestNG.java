package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.python.google.common.base.Predicate;
import org.python.google.common.collect.Iterables;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowConfiguration;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOptionName;
import com.latticeengines.domain.exposed.pls.TargetMarketReportMap;
import com.latticeengines.workflow.exposed.entitymgr.KeyValueEntityMgr;
import com.latticeengines.workflow.exposed.entitymgr.ReportEntityMgr;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TargetMarketService;

public class TargetMarketEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    private static final String REPORT_PAYLOAD = "{ \"foo\":\"bar\" }";
    private static final ReportPurpose REPORT_PURPOSE = ReportPurpose.IMPORT_SUMMARY;

    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;

    @Autowired
    private TargetMarketService targetMarketService;

    @Autowired
    private ReportEntityMgr reportEntityMgr;

    @Autowired
    private KeyValueEntityMgr keyValueEntityMgr;

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

        setupUsers();
        cleanupTargetMarketDB();
    }

    @Test(groups = { "functional" })
    public void testCreate() {
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

    @Test(groups = { "functional" }, dependsOnMethods = { "testCreate" })
    public void testNotVisibleInAlternateTenant() {
        setupSecurityContext(ALTERNATIVE_TESTING_TENANT);

        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);

        assertNull(targetMarket);
    }
    
    @Test(groups = { "functional" }, dependsOnMethods = { "testNotVisibleInAlternateTenant" })
    public void testFindAllInAlternativeTenantReturnsEmpty() {
        setupSecurityContext(ALTERNATIVE_TESTING_TENANT);

        List<TargetMarket> targetMarkets = targetMarketEntityMgr.findAllTargetMarkets();

        assertTrue(targetMarkets.isEmpty());
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "testFindAllInAlternativeTenantReturnsEmpty" })
    public void testUpdate() {
        setupSecurityContext(mainTestingTenant);

        TARGET_MARKET.setPid(null);
        TARGET_MARKET.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED_1);
        targetMarketEntityMgr.updateTargetMarketByName(TARGET_MARKET, TEST_TARGET_MARKET_NAME);

        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED_1);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "testUpdate" })
    public void testRegisterReport() {
        Report report = new Report();
        report.setIsOutOfDate(false);
        report.setPurpose(REPORT_PURPOSE);
        KeyValue kv = new KeyValue();
        kv.setPayload(REPORT_PAYLOAD);
        report.setJson(kv);

        targetMarketService.registerReport(TEST_TARGET_MARKET_NAME, report);

        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);

        assertEquals(targetMarket.getReports().size(), 1);
        String reportName = targetMarket.getReports().get(0).getReportName();
        assertNotNull(reportName);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "testRegisterReport" })
    public void testDelete() {
        setupSecurityContext(mainTestingTenant);
        TargetMarket targetMarket = targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNotNull(targetMarket);

        final List<Long> reportPids = new ArrayList<>();
        final List<Long> keyValuePids = new ArrayList<>();

        for (TargetMarketReportMap reportMap : targetMarket.getReports()) {
            reportPids.add(reportMap.getReport().getPid());
            keyValuePids.add(reportMap.getReport().getJson().getPid());
        }

        targetMarketEntityMgr.deleteTargetMarketByName(TEST_TARGET_MARKET_NAME);
        assertNull(targetMarketEntityMgr.findTargetMarketByName(TEST_TARGET_MARKET_NAME));

        // Ensure that the delete cascaded
        List<Report> allReports = reportEntityMgr.findAll();
        assertFalse(Iterables.any(allReports, new Predicate<Report>() {
            @Override
            public boolean apply(Report report) {
                return reportPids.contains(report.getPid());
            }
        }));

        List<KeyValue> allKeyValues = keyValueEntityMgr.findByTenantId(mainTestingTenant.getPid());
        assertFalse(Iterables.any(allKeyValues, new Predicate<KeyValue>() {
            @Override
            public boolean apply(KeyValue keyValue) {
                return keyValuePids.contains(keyValue.getPid());
            }
        }));

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
