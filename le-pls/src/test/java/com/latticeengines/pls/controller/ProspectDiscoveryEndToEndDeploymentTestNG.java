package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.functionalframework.SalesforceExtractAndImportUtil;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;


public class ProspectDiscoveryEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String CUSTOMERSPACE = "DemoContract.DemoTenant.Production";
    private CustomerSpace customerSpace;
    //private String salesforceUserName = "rgonzalez2@lattice-engines.com";
    //private String salesforcePasswd = "Welcome123JYJz57SnYag3YWQ5caQxIP04b";
    private String salesforceUserName = "apeters-widgettech@lattice-engines.com";
    private String salesforcePasswd = "Happy2010oIogZVEFGbL3n0qiAp6F66TC";
    private RestTemplate restTemplate = new RestTemplate();
    
    @Value("${pls.modelingservice.rest.endpoint.hostport}")
    private String modelingServiceHostPort;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microServiceHostPort;
    
    

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private SoftwareLibraryService softwareLibraryService;
    
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        restTemplate.setInterceptors(addMagicAuthHeaders);
        customerSpace = CustomerSpace.parse(CUSTOMERSPACE);
        setupTenant();
        setupCamille();
        setupHdfs();
        installServiceFlow();
    }
    
    
    @Test(groups = "deployment", enabled = false)
    public void runPipeline() throws Exception {
        importData();
        runDataFlow();
        AbstractMap.SimpleEntry<Table, DbCreds> preMatchEventTableAndCreds = loadHdfsTableToPDServer();
        Long commandId = match(preMatchEventTableAndCreds.getKey());
        Table eventTable = createEventTableFromMatchResult(commandId, preMatchEventTableAndCreds);
        
    }
    
    private Table createEventTableFromMatchResult(Long commandId, //
            AbstractMap.SimpleEntry<Table, DbCreds> preMatchEventTableAndCreds) throws Exception {
        Table table = preMatchEventTableAndCreds.getKey();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ItemID");
        for (Attribute attr : table.getAttributes()) {
           sb.append(", Source_" + attr.getName() + " AS " + attr.getName()).append("\n");
        }
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String matchTableName = "RunMatchWithLEUniverse_" + commandId + "_DerivedColumns";
        try (Connection conn = DriverManager.getConnection(preMatchEventTableAndCreds.getValue().getJdbcUrl())) {
            String query = "SELECT DISTINCT s.name FROM SYSOBJECTS, SYSCOLUMNS s, " + matchTableName + "_Metadata r" +
                    " WHERE SYSOBJECTS.id = s.id AND " + 
                    " SYSOBJECTS.xtype = 'u' AND " +
                    " SYSOBJECTS.name = '" + matchTableName + "'" +
                    " AND r.InternalColumnName = s.name";

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                ResultSet rset = pstmt.executeQuery();
                
                while (rset.next()) {
                    String column = rset.getString(1);
                    sb.append(", ").append(column).append("\n");
                }
            }
            sb.append(" FROM " + matchTableName + " WHERE $CONDITIONS");
        }
        String hdfsTargetPath = getTargetPath() + "/Data/Tables/" + matchTableName;
        String url = String.format("%s/modeling/dataloads", microServiceHostPort);
        LoadConfiguration config = new LoadConfiguration();
        config.setCreds(preMatchEventTableAndCreds.getValue());
        config.setQuery(sb.toString());
        config.setCustomer(CUSTOMERSPACE);
        config.setKeyCols(Arrays.<String>asList(new String[] { "ItemID" }));
        config.setTargetHdfsDir(hdfsTargetPath);
        
        AppSubmission submission = restTemplate.postForObject(url, config, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString());
        List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsTargetPath, new HdfsUtils.HdfsFilenameFilter() {
            
            @Override
            public boolean accept(String fileName) {
                return fileName.endsWith(".avro");
            }
        });
        
        Table eventTable = MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, avroFiles.get(0), null, null);
        eventTable.setName(matchTableName);
        
        addMetadata(eventTable, preMatchEventTableAndCreds.getValue());
        url = String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, CUSTOMERSPACE, eventTable.getName());
        restTemplate.postForLocation(url, eventTable);
        return eventTable;
    }
    
    private void addMetadata(Table table, DbCreds creds) throws Exception {
        try (Connection conn = DriverManager.getConnection(creds.getJdbcUrl())) {
            String query = "SELECT InternalColumnName, MetaDataName, MetaDataValue FROM " +
                    table.getName() + "_MetaData ORDER BY InternalColumnName";
            Map<String, Attribute> map = table.getNameAttributeMap();
            Class<?> attrClass = Class.forName(Attribute.class.getName());
            Map<String, Method> methodMap = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                ResultSet rset = pstmt.executeQuery();
                
                while (rset.next()) {
                    String column = rset.getString(1);
                    String metadataName = rset.getString(2);
                    String metadataValue = rset.getString(3);
                    
                    Attribute attr = map.get(column);
                    
                    if (attr == null) {
                        continue;
                    }
                    String methodName = "set" + metadataName;
                    Method m = methodMap.get(methodName);
                    
                    if (m == null) {
                        try {
                            m = attrClass.getMethod(methodName, String.class);
                        } catch (Exception e) {
                            // no method, skip
                            continue;
                        }
                        methodMap.put(methodName, m);
                    }
                    
                    if (m != null) {
                        m.invoke(attr, metadataValue);
                    }
                }
            }
        }
    }
    
    private String getTargetPath() {
        CustomerSpace space = CustomerSpace.parse(CUSTOMERSPACE);
        return PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
    }
    
    private Long match(Table preMatchEventTable) throws Exception {
        CreateCommandRequest matchCommand = new CreateCommandRequest();
        matchCommand.setSourceTable(preMatchEventTable.getName());
        matchCommand.setCommandType(MatchCommandType.MATCH_WITH_UNIVERSE);
        matchCommand.setContractExternalID(CUSTOMERSPACE);
        matchCommand.setDestTables("DerivedColumns");
        
        String url = String.format("%s/propdata/matchcommands", microServiceHostPort);
        Commands response = restTemplate.postForObject(url, matchCommand, Commands.class);
        
        waitForMatchCommand(response);
        
        return response.getPid(); 
    }
    
    private AbstractMap.SimpleEntry<Table, DbCreds> loadHdfsTableToPDServer() throws Exception {
        ExportConfiguration config = new ExportConfiguration();
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, CUSTOMERSPACE, "PrematchFlow");
        Table prematchFlowTable = restTemplate.getForObject(url, Table.class);
        DbCreds.Builder credsBuilder = new DbCreds.Builder(). //
                dbType("SQLServer"). //
                jdbcUrl("jdbc:sqlserver://10.51.15.130:1433;databaseName=PropDataMatchDB;user=DLTransfer;password=free&NSE"). //
                user("DLTransfer"). //
                password("free&NSE");
        DbCreds creds = new DbCreds(credsBuilder);
        
        createTable(prematchFlowTable, creds);

        config.setTable(prematchFlowTable.getName());
        config.setCustomer(CUSTOMERSPACE);
        config.setHdfsDirPath(prematchFlowTable.getExtracts().get(0).getPath());
        config.setCreds(creds);
        
        url = String.format("%s/modeling/dataexports", microServiceHostPort);
        AppSubmission submission = restTemplate.postForObject(url, config, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString());
        return new AbstractMap.SimpleEntry<Table, DbCreds>(prematchFlowTable, creds);
    }
    
    private void createTable(Table table, DbCreds creds) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s (\n", table.getName()));
        int size = table.getAttributes().size();
        int i = 1;
        for (Attribute attr : table.getAttributes()) {
            sb.append(String.format("  %s %s%s\n", attr.getName(),
                    getSQLServerType(attr.getPhysicalDataType()), i == size ? ")" : ","));
            i++;
        }
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        try (Connection conn = DriverManager.getConnection(creds.getJdbcUrl())) {
            try (PreparedStatement dropTablePstmt = conn.prepareStatement("DROP TABLE " + table.getName())) {
                try {
                    dropTablePstmt.executeUpdate();
                } catch (Exception e) {
                    // ignore
                }
            }
            
            try (PreparedStatement createTablePstmt = conn.prepareStatement(sb.toString())) {
                createTablePstmt.executeUpdate();
            }
            
        }
    }
    
    private String getSQLServerType(String type) {
        switch (type) {
        case "double":
            return "FLOAT";
        case "float":
            return "FLOAT";
        case "string":
            return "VARCHAR(255)";
        case "long":
            return "BIGINT";
        case "boolean":
            return "BIT";

        default:
            throw new RuntimeException("Unknown SQL Server type for avro type " + type);
        }

    }
    
    private void installServiceFlow() throws Exception {
        String jarFile = ClassLoader.getSystemResource(
                "com/latticeengines/pls/controller/le-serviceflows-prospectdiscovery.jar").getPath();
        HdfsUtils.rmdir(yarnConfiguration, //
                String.format("%s/%s", SoftwareLibraryService.TOPLEVELPATH, "dataflowapi"));
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflowapi");
        pkg.setGroupId("com.latticeengines");
        pkg.setArtifactId("le-serviceflows-prospectdiscovery");
        pkg.setVersion("2.0.13-SNAPSHOT");
        pkg.setInitializerClass("com.latticeengines.prospectdiscovery.Initializer");
        softwareLibraryService.installPackage(pkg, new File(jarFile));
    }
    
    private void importData() throws Exception {
        ImportConfiguration importConfig = setupSalesforceImportConfig();
        String url = microServiceHostPort + "/eai/importjobs";
        AppSubmission submission = restTemplate.postForObject(url, importConfig, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString());
    }
    
    private void runDataFlow() throws Exception {
        DataFlowConfiguration dataFlowConfig = setupPreMatchTableDataFlow();
        String url = microServiceHostPort + "/dataflowapi/dataflows/";

        AppSubmission submission = restTemplate.postForObject(url, dataFlowConfig, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString());
    }
    
    private DataFlowConfiguration setupPreMatchTableDataFlow() throws Exception {
        DataFlowConfiguration dataFlowConfig = new DataFlowConfiguration();
        dataFlowConfig.setName("PrematchFlow");
        dataFlowConfig.setCustomerSpace(CustomerSpace.parse(CUSTOMERSPACE));
        dataFlowConfig.setDataFlowBeanName("createEventTable");
        dataFlowConfig.setDataSources(createDataFlowSources());
        dataFlowConfig.setTargetPath("/PrematchFlowRun");
        return dataFlowConfig;
    }
    
    private List<DataFlowSource> createDataFlowSources() throws Exception {
        List<Table> tables = retrieveRegisteredTablesAndExtraSources();
        List<DataFlowSource> sources = new ArrayList<>();
        for (Table table : tables) {
            DataFlowSource source = new DataFlowSource();
            source.setName(table.getName());
            sources.add(source);
        }
        return sources;
    }
    
    @SuppressWarnings("unchecked")
    private List<Table> retrieveRegisteredTablesAndExtraSources() throws Exception {
        String url = String.format("%s/metadata/customerspaces/%s/tables", microServiceHostPort, CUSTOMERSPACE);
        List<String> tableList = restTemplate.getForObject(url, List.class);
        List<Table> tables = new ArrayList<>();
        
        for (String tableName : tableList) {
            Table t = new Table();
            t.setName(tableName);
            tables.add(t);
        }
        // add the stop list to HDFS
        String stoplist = ClassLoader.getSystemResource("com/latticeengines/pls/controller/Stoplist/Stoplist.avro").getPath();
        HdfsUtils.mkdir(yarnConfiguration, "/tmp/Stoplist");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, stoplist, "/tmp/Stoplist");
        Table stopList = MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, "/tmp/Stoplist/Stoplist.avro", null, null);
        stopList.getExtracts().get(0).setPath("/tmp/Stoplist/*.avro");
        // register the stop list table
        url = String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, CUSTOMERSPACE, stopList.getName());
        restTemplate.postForLocation(url, stopList);
        tables.add(stopList);
        return tables;
    }
    
    private void setupHdfs() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/Pods");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/Stoplist");
    }
    
    private void setupTenant() throws Exception {
        super.deleteTenantByRestCall(CUSTOMERSPACE);
        Tenant tenant = new Tenant();
        tenant.setId(CUSTOMERSPACE);
        tenant.setName(CUSTOMERSPACE);
        super.createTenantByRestCall(tenant);
    }
    
    private void setupCamille() throws Exception {
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
        crmCredentialZKService.removeCredentials("sfdc", CUSTOMERSPACE, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredentialZKService.writeToZooKeeper("sfdc", CUSTOMERSPACE, true, crmCredential, true);
    }
    
    protected ImportConfiguration setupSalesforceImportConfig() {
        List<Table> tables = new ArrayList<>();
        Table lead = SalesforceExtractAndImportUtil.createLead();
        Table account = SalesforceExtractAndImportUtil.createAccount();
        Table opportunity = SalesforceExtractAndImportUtil.createOpportunity();
        Table contact = SalesforceExtractAndImportUtil.createContact();
        Table contactRole = SalesforceExtractAndImportUtil.createOpportunityContactRole();
        tables.add(lead);
        tables.add(account);
        tables.add(opportunity);
        tables.add(contact);
        tables.add(contactRole);

        ImportConfiguration importConfig = new ImportConfiguration();
        SourceImportConfiguration salesforceConfig = new SourceImportConfiguration();
        salesforceConfig.setSourceType(SourceType.SALESFORCE);
        salesforceConfig.setTables(tables);

        importConfig.addSourceConfiguration(salesforceConfig);
        importConfig.setCustomer(CUSTOMERSPACE);
        return importConfig;
    }
    
    @SuppressWarnings("unchecked")
    private void waitForMatchCommand(Commands commands) throws Exception {
        Map<String, String> status = new HashMap<>();
        int maxTries = 1000;
        int i = 0;
        do {
            String url = String.format(microServiceHostPort + "/propdata/matchcommands/%s?matchClient=PD130", commands.getPid());
            status = restTemplate.getForObject(url, Map.class);
            System.out.println("Status = " + status.get("Status"));
            Thread.sleep(10000L);
            i++;
            
            if (i == maxTries) {
                break;
            }
        } while (status != null && !status.get("Status").equals("COMPLETE"));
    }
    
    private void waitForAppId(String appId) throws Exception {
        JobStatus status;
        int maxTries = 1000;
        int i = 0;
        do {
            String url = String.format(microServiceHostPort + "/modeling/jobs/%s", appId);
            status = restTemplate.getForObject(url, JobStatus.class);
            Thread.sleep(10000L);
            i++;
            
            if (i == maxTries) {
                break;
            }
        } while (status.getStatus() != FinalApplicationStatus.SUCCEEDED
                && status.getStatus() != FinalApplicationStatus.FAILED);
        
        assertEquals(status.getStatus(), FinalApplicationStatus.SUCCEEDED);
    }


}
