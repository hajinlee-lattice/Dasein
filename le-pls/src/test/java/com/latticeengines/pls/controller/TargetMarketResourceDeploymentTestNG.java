package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowConfiguration;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOptionName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.entitymanager.impl.microservice.RestApiProxy;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.Constants;

public class TargetMarketResourceDeploymentTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_TARGETMARKET_URL = "pls/targetmarkets/";

    @Autowired
    private RestApiProxy proxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microServiceHostPort;

    @Autowired
    private MetadataProxy metadataProxy;

    private RestTemplate microServiceRestTemplate = new RestTemplate();

    @BeforeClass(groups = "deployment")
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

    @BeforeMethod(groups = "deployment")
    public void beforeMethod() {
        // using external admin session by default
        switchToExternalAdmin();
    }

    @Test(groups = "deployment", timeOut = 360000, enabled = false)
    public void create() {
        restTemplate.postForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL, TARGET_MARKET, TargetMarket.class);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
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
        assertEquals(targetMarket.getTargetMarketStatistics(), TARGET_MARKET_STATISTICS);

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

        WorkflowStatus status = waitForWorkflowCompletion(targetMarket.getApplicationId());
        assertEquals(status.getStatus(), BatchStatus.COMPLETED);
    }

    @Test(groups = "deployment", dependsOnMethods = "create", enabled = false)
    public void update() {
        TARGET_MARKET.setNumProspectsDesired(NUM_PROPSPECTS_DESIRED_1);

        restTemplate.put(getRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME, TARGET_MARKET);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertNotNull(targetMarket);
        assertEquals(targetMarket.getName(), TEST_TARGET_MARKET_NAME);
        assertEquals(targetMarket.getNumProspectsDesired(), NUM_PROPSPECTS_DESIRED_1);
    }

    @Test(groups = "deployment", dependsOnMethods = "update", enabled = false)
    public void delete() {
        restTemplate.delete(getRestAPIHostPort() + PLS_TARGETMARKET_URL + TEST_TARGET_MARKET_NAME);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TEST_TARGET_MARKET_NAME, TargetMarket.class);
        assertNull(targetMarket);
    }

    @Test(groups = "deployment", timeOut = 360000, enabled = false)
    public void createDefault() {
        restTemplate.postForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL + "default", null, Void.class);

        TargetMarket targetMarket = restTemplate.getForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL
                + TargetMarket.DEFAULT_NAME, TargetMarket.class);
        assertTrue(targetMarket.getIsDefault());

        waitForWorkflowCompletion(targetMarket.getApplicationId());
    }

    @Test(groups = "deployment", enabled = true)
    public void resetDefaultTargetMarket() throws Exception {
        String tenantId = mainTestingTenant.getId();
        CustomerSpace space = CustomerSpace.parse(tenantId);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        microServiceRestTemplate.getInterceptors().add(addMagicAuthHeader);

        assertEquals(metadataProxy.getImportTables(space.toString()).size(), 0);
        assertEquals(metadataProxy.getTables(space.toString()).size(), 0);

        provisionMetadataTables(mainTestingTenant);

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
        resetDefaultTargetMarket(space.toString());
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
        Boolean success = microServiceRestTemplate.postForObject(microServiceHostPort + "/metadata/admin/provision",
                tenant, Boolean.class);
        if (!success) {
            throw new RuntimeException("Failed to provision metadata component");
        }
    }

    private void updateTables(String customerSpace, List<Table> tables) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);

        for (Table table : tables) {
            uriVariables.put("tableName", table.getName());
            microServiceRestTemplate.put(
                    String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, customerSpace,
                            table.getName()), table);
        }
    }

    private void resetDefaultTargetMarket(String customerSpace) {
        Boolean success = restTemplate.postForObject(getRestAPIHostPort() + PLS_TARGETMARKET_URL + "default/reset",
                null, Boolean.class);
        assertTrue(success);
    }

    private WorkflowStatus waitForWorkflowCompletion(String applicationId) {
        while (true) {
            WorkflowStatus status = proxy.getWorkflowStatusFromApplicationId(applicationId);
            if (!status.getStatus().isRunning()) {
                return status;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
