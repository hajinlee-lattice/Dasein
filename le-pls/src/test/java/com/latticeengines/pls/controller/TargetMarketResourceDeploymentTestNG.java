package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOptionName;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowProperty;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class TargetMarketResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String PLS_TARGETMARKET_URL = "/pls/targetmarkets/";

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${common.test.microservice.url}")
    private String microServiceHostPort;

    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @BeforeClass(groups = "deployment.pd")
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

        TargetMarketDataFlowProperty configuration = TARGET_MARKET.getDataFlowConfiguration();
        configuration.setInt(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends,
                NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS);
        configuration.setString(TargetMarketDataFlowOptionName.IntentScoreThreshold, INTENT_SCORE_THRESHOLD.toString());
        configuration.setDouble(TargetMarketDataFlowOptionName.FitScoreThreshold, FIT_SCORE_THRESHOLD);
        configuration.setBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts,
                DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS);
        configuration.setInt(TargetMarketDataFlowOptionName.MaxProspectsPerAccount, MAX_PROSPECTS_PER_ACCOUNT);

        System.out.println("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.PD);
        cleanupTargetMarketDB();
        System.out.println("Test environment setup finished.");
    }

    @BeforeMethod(groups = "deployment.pd")
    public void beforeMethod() {
        // using external admin session by default
        switchToExternalAdmin();
    }

    @Test(groups = "deployment.pd")
    public void testCreateDefault() {
        TargetMarket targetMarket = restTemplate.postForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + "default", null, TargetMarket.class);
        assertTrue(targetMarket.getIsDefault());
        targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL + "default",
                TargetMarket.class);
        assertTrue(targetMarket.getIsDefault());
    }

    @Test(groups = "deployment.pd", dependsOnMethods = "testCreateDefault")
    public void testResetDefaultTargetMarket() throws Exception {
        String tenantId = mainTestTenant.getId();
        CustomerSpace space = CustomerSpace.parse(tenantId);

        if (metadataProxy.getImportTables(space.toString()).size() == 0) {
            provisionMetadataTables(mainTestTenant);
        }

        List<Table> importTables = metadataProxy.getImportTables(space.toString());
        assertEquals(importTables.size(), 5);
        assertEquals(metadataProxy.getTables(space.toString()).size(), 0);

        List<Table> tables = new ArrayList<>();
        String dataTablesHdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
        for (Table importTable : importTables) {
            importTable.setTableType(TableType.DATATABLE);
            DateTime date = new DateTime();
            importTable.getLastModifiedKey().setLastModifiedTimestamp(date.getMillis());
            tables.add(importTable);
            String dataTableHdfsPath = String.format("%1$s/SFDC/%2$s/Extracts/sometimestamp/%2$s-timestamp.json",
                    dataTablesHdfsPath, importTable.getName());
            HdfsUtils.writeToFile(yarnConfiguration, dataTableHdfsPath, "somecontent");
            assertTrue(HdfsUtils.fileExists(yarnConfiguration, dataTableHdfsPath));
        }
        updateTables(space.toString(), tables);
        assertEquals(metadataProxy.getTables(space.toString()).size(), 5);

        importTables = metadataProxy.getImportTables(space.toString());
        resetDefaultMarketId(space.toString());
        assertEquals(metadataProxy.getTables(space.toString()).size(), 0);
        List<Table> newImportTables = metadataProxy.getImportTables(space.toString());
        assertEquals(importTables.size(), newImportTables.size());
        for (int i = 0; i < importTables.size(); i++) {
            Table importTable = importTables.get(i);
            for (int j = 0; j < newImportTables.size(); j++) {
                Table newImportTable = newImportTables.get(j);
                if (newImportTable.getName().equals(importTable.getName())) {
                    assertEquals(importTable.getAttributes().size(), newImportTable.getAttributes().size());
                    assertEquals(importTable.getPrimaryKey().getAttributesAsStr(), newImportTable.getPrimaryKey()
                            .getAttributesAsStr());
                    assertEquals(importTable.getLastModifiedKey().getAttributesAsStr(), newImportTable
                            .getLastModifiedKey().getAttributesAsStr());
                    assertTrue(importTable.getLastModifiedKey().getLastModifiedTimestamp() < newImportTable
                            .getLastModifiedKey().getLastModifiedTimestamp());
                    String dataTableHdfsPath = String.format(
                            "%1$s/SFDC/%2$s/Extracts/sometimestamp/%2$s-timestamp.json", dataTablesHdfsPath,
                            importTable.getName());
                    assertFalse(HdfsUtils.fileExists(yarnConfiguration, dataTableHdfsPath));
                }
            }
            metadataProxy.deleteImportTable(space.toString(), importTable.getName());
        }
    }

    private void provisionMetadataTables(Tenant tenant) {
        Boolean success = magicRestTemplate.postForObject(microServiceHostPort + "/metadata/admin/provision", tenant,
                Boolean.class);
        if (!success) {
            throw new RuntimeException("Failed to provision metadata component");
        }
    }

    private void updateTables(String customerSpace, List<Table> tables) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);

        for (Table table : tables) {
            uriVariables.put("tableName", table.getName());
            magicRestTemplate.put(
                    String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, customerSpace,
                            table.getName()), table);
        }
    }

    private void resetDefaultMarketId(String customerSpace) {
        Boolean success = restTemplate.postForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + "default/reset", null, Boolean.class);
        assertTrue(success);
    }

    @Test(groups = "deployment.pd")
    public void testCreate() {
        restTemplate.postForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL, TARGET_MARKET,
                TargetMarket.class);

        TargetMarket targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
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

        TargetMarketDataFlowProperty configuration = targetMarket.getDataFlowConfiguration();
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

    @Test(groups = "deployment.pd", dependsOnMethods = "testCreate")
    public void testUpdate() {
        TargetMarket targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        targetMarket.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED_1);
        targetMarket.getReports().clear();

        restTemplate.put(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME, targetMarket);

        TargetMarket received = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertNotNull(received);
        assertEquals(received.getName(), TEST_TARGET_MARKET_NAME);
        assertEquals(received.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED_1);
    }

    @Test(groups = "deployment.pd", dependsOnMethods = "testUpdate")
    public void testRegisterReport() {
        Report report = createReport(ReportPurpose.IMPORT_SUMMARY, "{ \"foo\":\"bar\" }", false);
        restTemplate.postForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME
                + "/reports", report, Void.class);

        TargetMarket targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);

        assertEquals(targetMarket.getReports().size(), 1);
        String reportName = targetMarket.getReports().get(0).getReportName();
        assertNotNull(reportName);

        Report received = restTemplate.getForObject(getDeployedRestAPIHostPort() + "/pls/reports/" + reportName,
                Report.class);
        assertEquals(received.getPurpose(), report.getPurpose());
        assertEquals(received.getIsOutOfDate(), report.getIsOutOfDate());
        assertEquals(received.getJson().getPayload(), report.getJson().getPayload());
    }

    @Test(groups = "deployment.pd", dependsOnMethods = "testRegisterReport")
    public void testReplaceReport() {
        Report report = createReport(ReportPurpose.IMPORT_SUMMARY, "{ \"baz\":\"qux\" }", false);

        restTemplate.postForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME
                + "/reports", report, Void.class);

        TargetMarket targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);

        assertEquals(targetMarket.getReports().size(), 1);
        String reportName = targetMarket.getReports().get(0).getReportName();
        assertNotNull(reportName);

        Report received = restTemplate.getForObject(getDeployedRestAPIHostPort() + "/pls/reports/" + reportName,
                Report.class);
        assertEquals(received.getPurpose(), report.getPurpose());
        assertEquals(received.getIsOutOfDate(), report.getIsOutOfDate());
        assertEquals(received.getJson().getPayload(), report.getJson().getPayload());
    }

    @Test(groups = "deployment.pd", dependsOnMethods = "testReplaceReport")
    public void testAddReport() {
        Report report = createReport(ReportPurpose.MODEL_SUMMARY, "{ \"baz\":\"qux\" }", false);

        restTemplate.postForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME
                + "/reports", report, Void.class);

        TargetMarket targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertEquals(targetMarket.getReports().size(), 2);
    }

    private Report createReport(ReportPurpose purpose, String json, boolean outOfDate) {
        Report report = new Report();
        report.setPurpose(purpose);
        report.setIsOutOfDate(outOfDate);
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        report.setJson(kv);
        return report;
    }

    @Test(groups = "deployment.pd", dependsOnMethods = "testAddReport")
    public void testUpdateRetainsReports() {
        TargetMarket targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        targetMarket.setModelId("model_12345");
        restTemplate.put(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME, targetMarket);

        TargetMarket received = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);

        assertFalse(received.getReports().isEmpty());
        assertEquals(received.getReports().size(), targetMarket.getReports().size());
    }

    @Test(groups = "deployment.pd", dependsOnMethods = "testUpdateRetainsReports")
    public void testDelete() {
        restTemplate.delete(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME);

        TargetMarket targetMarket = restTemplate.getForObject(getDeployedRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertNull(targetMarket);
    }

    protected void cleanupTargetMarketDB() {
        setupSecurityContext(mainTestTenant);
        List<TargetMarket> targetMarkets = targetMarketEntityMgr.findAllTargetMarkets();
        for (TargetMarket targetMarket : targetMarkets) {
            if (targetMarket.getName().startsWith("TEST") || targetMarket.getIsDefault()) {
                targetMarketEntityMgr.deleteTargetMarketByName(targetMarket.getName());
            }
        }
    }
}
