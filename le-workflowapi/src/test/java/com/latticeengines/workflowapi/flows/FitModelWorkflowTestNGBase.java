package com.latticeengines.workflowapi.flows;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class FitModelWorkflowTestNGBase extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Value("${workflowapi.test.sfdc.user.name}")
    private String salesforceUserName;

    @Value("${workflowapi.test.sfdc.passwd.encrypted}")
    private String salesforcePasswd;

    @Value("${workflowapi.test.sfdc.securitytoken}")
    private String salesforceSecurityToken;

    private TargetMarket defaultTargetMarket;

    protected void setupForFitModel() throws Exception {
        restTemplate.setInterceptors(getAddMagicAuthHeaders());
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        setupUsers(DEMO_CUSTOMERSPACE);
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);
        installServiceFlow();
        createImportTablesInMetadataStore(DEMO_CUSTOMERSPACE, tenant);
        copyStopListToHdfs();

        defaultTargetMarket = internalResourceProxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME,
                DEMO_CUSTOMERSPACE.toString());
        if (defaultTargetMarket != null) {
            internalResourceProxy.deleteTargetMarketByName(TargetMarket.DEFAULT_NAME, DEMO_CUSTOMERSPACE.toString());
        }
        internalResourceProxy.createDefaultTargetMarket(DEMO_CUSTOMERSPACE.toString());
        defaultTargetMarket = internalResourceProxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME,
                DEMO_CUSTOMERSPACE.toString());
    }

    protected FitModelWorkflowConfiguration generateFitModelWorkflowConfiguration() {
        List<String> eventCols = new ArrayList<>();
        eventCols.add("Event_IsWon");
        eventCols.add("Event_StageIsClosedWon");
        eventCols.add("Event_IsClosed");
        eventCols.add("Event_OpportunityCreated");

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("PublicDomain", "/tmp/Stoplist/*.avro");

        FitModelWorkflowConfiguration workflowConfig = new FitModelWorkflowConfiguration.Builder()
                .customer(DEMO_CUSTOMERSPACE) //
                .microServiceHostPort(microServiceHostPort) //
                .sourceType(SourceType.SALESFORCE) //
                .extraSources(extraSources) //
                .matchDbUrl(
                        "jdbc:sqlserver://10.51.15.130:1433;databaseName=PropDataMatchDB;user=DLTransfer;password=free&NSE") //
                .matchDbUser("DLTransfer") //
                .matchDbPasswordEncrypted(CipherUtils.encrypt("free&NSE")) //
                .matchDestTables("DerivedColumns") //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchClient("PD130") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .eventColumns(eventCols) //
                .targetMarket(defaultTargetMarket) //
                .internalResourceHostPort(internalResourceHostPort) //
                .uniqueKeyColumn("LatticeAccountID") //
                .directoryToScore("/tmp/AccountMaster") //
                .registerScoredTable(true) //
                .attributes(Arrays.asList(new String[] { "BusinessIndustry", "BusinessRevenueRange", "BusinessEmployeesRange" })) //
                .build();

        return workflowConfig;
    }

    private void setupCamille(CustomerSpace customerSpace) throws Exception {
        BatonService baton = new BatonServiceImpl();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo();
        spaceInfo.properties = new CustomerSpaceProperties();
        spaceInfo.properties.displayName = "";
        spaceInfo.properties.description = "";
        spaceInfo.featureFlags = "";
        baton.createTenant(customerSpace.getContractId(), //
                customerSpace.getTenantId(), //
                customerSpace.getSpaceId(), //
                spaceInfo);
        crmCredentialZKService.removeCredentials("sfdc", customerSpace.toString(), true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredential.setSecurityToken(salesforceSecurityToken);
        crmCredential.setUrl("https://login.salesforce.com");
        crmCredentialZKService.writeToZooKeeper("sfdc", customerSpace.toString(), true, crmCredential, true);

        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customerSpace.toString()), "Eai");
        Path connectTimeoutDocPath = docPath.append("SalesforceEndpointConfig") //
                .append("HttpClient").append("ConnectTimeout");
        camille.create(connectTimeoutDocPath, new Document("60000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        Path importTimeoutDocPath = docPath.append("SalesforceEndpointConfig").append("HttpClient")
                .append("ImportTimeout");
        camille.create(importTimeoutDocPath, new Document("3600000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    private void createImportTablesInMetadataStore(CustomerSpace customerSpace, Tenant tenant) throws IOException {
        URL url = getClass().getClassLoader().getResource("Tables");
        File tablesDir = new File(url.getFile());
        File[] files = tablesDir.listFiles();

        for (File file : files) {
            if (file.isDirectory()) {
                continue;
            }
            String str = FileUtils.readFileToString(file);
            Table table = JsonUtils.deserialize(str, Table.class);

            Map<String, String> urlVariables = new HashMap<>();
            urlVariables.put("customerSpace", customerSpace.toString());
            urlVariables.put("tableName", table.getName());

            table.setTenant(tenant);
            table.setTableType(TableType.IMPORTTABLE);

            DateTime date = new DateTime();
            table.getLastModifiedKey().setLastModifiedTimestamp(date.minusYears(2).getMillis());

            restTemplate.postForObject(microServiceHostPort
                    + "/metadata/customerspaces/{customerSpace}/importtables/{tableName}", table, String.class,
                    urlVariables);
        }
    }

    private void copyStopListToHdfs() {
        // add the stop list to HDFS
        String stoplist = ClassLoader.getSystemResource(
                "com/latticeengines/workflowapi/flows/prospectdiscovery/Stoplist/Stoplist.avro").getPath();
        try {
            HdfsUtils.mkdir(yarnConfiguration, "/tmp/Stoplist");
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00000, e, new String[] { "/tmp/Stoplist" });
        }
        try {
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, stoplist, "/tmp/Stoplist");
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_27001, e, new String[] { stoplist, "/tmp/Stoplist" });
        }
    }

}
