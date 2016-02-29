package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.prospectdiscovery.workflow.steps.CreateAttributeLevelSummaryWorkflow;
import com.latticeengines.prospectdiscovery.workflow.steps.CreateAttributeLevelSummaryWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class CreateAttributeLevelSummaryWorkflowDeploymentTestNG extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${security.test.pls.api.hostport}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    private TargetMarket defaultTargetMarket;

    @Autowired
    private CreateAttributeLevelSummaryWorkflow createAttributeLevelSummaryWorkflow;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForAttributeLevelSummary();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflow() throws Exception {
        CreateAttributeLevelSummaryWorkflowConfiguration workflowConfig = generateWorkflowConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(createAttributeLevelSummaryWorkflow.name(),
                workflowConfig);

        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    private void setupForAttributeLevelSummary() throws Exception {
        restTemplate.setInterceptors(getAddMagicAuthHeaders());
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        setupUsers(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);
        createTablesInMetadataStore(DEMO_CUSTOMERSPACE, tenant);

        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        defaultTargetMarket = internalResourceProxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME,
                DEMO_CUSTOMERSPACE.toString());
        if (defaultTargetMarket != null) {
            internalResourceProxy.deleteTargetMarketByName(TargetMarket.DEFAULT_NAME, DEMO_CUSTOMERSPACE.toString());
        }
        internalResourceProxy.createDefaultTargetMarket(DEMO_CUSTOMERSPACE.toString());
        defaultTargetMarket = internalResourceProxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME,
                DEMO_CUSTOMERSPACE.toString());
    }

    private CreateAttributeLevelSummaryWorkflowConfiguration generateWorkflowConfiguration() {
        CreateAttributeLevelSummaryWorkflowConfiguration workflowConfig = new CreateAttributeLevelSummaryWorkflowConfiguration.Builder()
                .customer(DEMO_CUSTOMERSPACE) //
                .microServiceHostPort(microServiceHostPort) //
                .targetMarket(defaultTargetMarket) //
                .internalResourceHostPort(internalResourceHostPort) //
                .accountMasterNameAndPath(
                        new String[] { "AccountMaster", //
                                "/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/ScoredEventTable" }) //
                .scoreResult("ScoreResult") //
                .uniqueKeyColumn("LatticeAccountID") //
                .attributes(
                        Arrays.asList(new String[] { "BusinessIndustry", "BusinessRevenueRange",
                                "BusinessEmployeesRange" })) //
                .eventColumnName("Event_IsWon") //
                .eventTableName("MatchedTable") //
                .build();

        return workflowConfig;
    }

    private void createTablesInMetadataStore(CustomerSpace customerSpace, Tenant tenant) throws Exception {
        URL scoredEventTable = getClass().getClassLoader().getResource(
                "com/latticeengines/workflowapi/flows/prospectdiscovery/ScoredEventTable");
        URL scoreResultTable = getClass().getClassLoader().getResource(
                "com/latticeengines/workflowapi/flows/prospectdiscovery/ScoreResult");
        URL matchedTable = getClass().getClassLoader().getResource(
                "com/latticeengines/workflowapi/flows/prospectdiscovery/MatchedTable");
        File scoredEventTableDir = new File(scoredEventTable.getFile());
        File scoreResultTableDir = new File(scoreResultTable.getFile());
        File matchedTableDir = new File(matchedTable.getFile());
        File[] scoredEventTableFiles = scoredEventTableDir.listFiles();
        File[] scoreResultTableFiles = scoreResultTableDir.listFiles();
        File[] matchedTableFiles = matchedTableDir.listFiles();

        String scoredEventTableHdfsDir = "/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/ScoredEventTable";
        createTable("ScoredEventTable", scoredEventTableHdfsDir, scoredEventTableFiles, false);

        String scoreResultTableHdfsDir = "/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/ScoreResult";
        createTable("ScoreResult", scoreResultTableHdfsDir, scoreResultTableFiles, true);

        String matchedTableHdfsDir = "/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/MatchedTable";
        createTable("MatchedTable", matchedTableHdfsDir, matchedTableFiles, true);
    }

    private void createTable(String tableName, String path, File[] files, boolean register) throws Exception {
        HdfsUtils.mkdir(yarnConfiguration, path);
        for (File file : files) {
            if (file.isDirectory()) {
                continue;
            }
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, file.getAbsolutePath(), path);
        }

        if (register) {
            Table table = MetadataConverter.getTable(yarnConfiguration, path);
            table.setName(tableName);
            Map<String, String> urlVariables = new HashMap<>();
            urlVariables.put("customerSpace", DEMO_CUSTOMERSPACE.toString());
            urlVariables.put("tableName", table.getName());

            restTemplate.postForObject(microServiceHostPort
                    + "/metadata/customerspaces/{customerSpace}/tables/{tableName}", table, String.class, urlVariables);
        }
    }

}
