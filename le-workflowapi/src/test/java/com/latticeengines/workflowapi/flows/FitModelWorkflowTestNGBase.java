package com.latticeengines.workflowapi.flows;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class FitModelWorkflowTestNGBase extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");

    @Value("${workflowapi.test.accountmaster.path}")
    private String accountMasterPath;
    
    @Autowired
    private Configuration yarnConfiguration;

    private TargetMarket defaultTargetMarket;

    protected void setupForFitModel() throws Exception {
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        setupUsers(DEMO_CUSTOMERSPACE);
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs(DEMO_CUSTOMERSPACE);
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
                .targetMarket(defaultTargetMarket) //
                .internalResourceHostPort(internalResourceHostPort) //
                .uniqueKeyColumn("LatticeAccountID") //
                .directoryToScore(accountMasterPath) //
                .registerScoredTable(true) //
                .attributes(
                        Arrays.asList(new String[] { "BusinessIndustry", "BusinessRevenueRange",
                                "BusinessEmployeesRange" })) //
                .prematchFlowTableName("PrematchFlow") //
                .modelName("Default Model") //
                .build();

        return workflowConfig;
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
