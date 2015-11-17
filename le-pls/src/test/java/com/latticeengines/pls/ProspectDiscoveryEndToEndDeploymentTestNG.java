package com.latticeengines.pls;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
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
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.pls.functionalframework.ModelingServiceExecutor;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class ProspectDiscoveryEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String CUSTOMERSPACE = "DemoContract.DemoTenant.Production";
    private CustomerSpace customerSpace;
    
    @Value("${pls.test.sfdc.user.name}")
    private String salesforceUserName;
    
    @Value("${pls.test.sfdc.passwd.encrypted}")
    private String salesforcePasswd;
    
    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microServiceHostPort;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    private Tenant tenant;

    private List<String> modelGuids = new ArrayList<>();

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        restTemplate.setInterceptors(addMagicAuthHeaders);
        customerSpace = CustomerSpace.parse(CUSTOMERSPACE);
        setupTenant();
        setupUsers();
        setupCamille();
        setupHdfs();
        installServiceFlow();
        createImportTablesInMetadataStore();
    }

    @Test(groups = "deployment", enabled = true)
    public void runPipeline() throws Exception {
        importData();
        runDataFlow();
        AbstractMap.SimpleEntry<Table, DbCreds> preMatchEventTableAndCreds = loadHdfsTableToPDServer();
        Long commandId = match(preMatchEventTableAndCreds.getKey());
        Table eventTable = createEventTableFromMatchResult(commandId, preMatchEventTableAndCreds);
        ModelingServiceExecutor.Builder bldr = sample(eventTable);
        profileAndModel(eventTable, bldr);
        //scoring(eventTable);
    }

    private void setupUsers() throws Exception {
        createAdminUserByRestCall(CUSTOMERSPACE, //
                "rgonzalez@lattice-engines.com", //
                "rgonzalez@lattice-engines.com", //
                "PD Super", //
                "User", //
                adminPasswordHash);
    }

    private ModelingServiceExecutor.Builder sample(Table eventTable) throws Exception {
        String metadataContents = JsonUtils.serialize(eventTable.getModelingMetadata());
        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.sampleSubmissionUrl("/modeling/samples") //
                .profileSubmissionUrl("/modeling/profiles") //
                .modelSubmissionUrl("/modeling/models") //
                .retrieveFeaturesUrl("/modeling/features") //
                .retrieveJobStatusUrl("/modeling/jobs/%s") //
                .retrieveModelingJobStatusUrl("/modeling/modelingjobs/%s") //
                .modelingServiceHostPort(microServiceHostPort) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .customer(CUSTOMERSPACE) //
                .metadataContents(metadataContents) //
                .yarnConfiguration(yarnConfiguration) //
                .hdfsDirToSample(eventTable.getExtracts().get(0).getPath()) //
                .table(eventTable.getName());

        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        modelExecutor.sample();
        return bldr;
    }

    private void profileAndModel(Table eventTable, ModelingServiceExecutor.Builder bldr) throws Exception {
        String[] eventCols = new String[] { //
                "Event_IsWon", //
                "Event_StageIsClosedWon", //
                "Event_IsClosed", //
                "Event_OpportunityCreated" //
        };
        List<String> excludedColumns = new ArrayList<>();

        for (String eventCol : eventCols) {
            excludedColumns.add(eventCol);
        }

        for (Attribute attr : eventTable.getAttributes()) {
            if (attr.getApprovedUsage() == null || attr.getApprovedUsage().get(0).equals("None")) {
                excludedColumns.add(attr.getName());
            }
        }

        String[] excludeList = new String[excludedColumns.size()];
        excludedColumns.toArray(excludeList);
        bldr = bldr.profileExcludeList(excludeList);

        for (String eventCol : eventCols) {
            bldr = bldr.targets(eventCol) //
                    .metadataTable("EventTable-" + eventCol) //
                    .keyColumn("Id") //
                    .modelName("Model-" + eventCol);
            ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
            modelExecutor.writeMetadataFile();
            modelExecutor.profile();
            String uuid = modelExecutor.model();
            modelGuids.add("ms__" + uuid + "-PLS_model");
        }
    }

    private void scoring(Table eventTable) throws Exception {
        String dataPath = String.format("%s%s/data/%s/samples", modelingServiceHdfsBaseDir, CUSTOMERSPACE, eventTable.getName());
        System.out.println(dataPath);
        String scorePath = modelingServiceHdfsBaseDir + CUSTOMERSPACE + "/scoring/" + UUID.randomUUID() + "/scores";
        System.out.println(scorePath);
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(CUSTOMERSPACE);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(modelGuids);
        System.out.println(modelGuids);
        scoringConfig.setUniqueKeyColumn("Id");
        System.out.println(scoringConfig);
        AppSubmission submission = restTemplate.postForObject(microServiceHostPort + "/scoring/scoringJobs",
                scoringConfig, AppSubmission.class, new Object[] {});
        String appId = submission.getApplicationIds().get(0);
        waitForAppId(appId);
    }

    private Table createEventTableFromMatchResult(Long commandId, //
            AbstractMap.SimpleEntry<Table, DbCreds> preMatchEventTableAndCreds) throws Exception {
        Table table = preMatchEventTableAndCreds.getKey();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT Source_Id");
        for (Attribute attr : table.getAttributes()) {
            sb.append(", Source_" + attr.getName() + " AS " + attr.getName()).append("\n");
        }

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String matchTableName = "RunMatchWithLEUniverse_" + commandId + "_DerivedColumns";
        try (Connection conn = DriverManager.getConnection(preMatchEventTableAndCreds.getValue().getJdbcUrl())) {
            String query = "SELECT DISTINCT s.name FROM SYSOBJECTS, SYSCOLUMNS s, " + matchTableName + "_Metadata r"
                    + " WHERE SYSOBJECTS.id = s.id AND " + " SYSOBJECTS.xtype = 'u' AND " + " SYSOBJECTS.name = '"
                    + matchTableName + "'" + " AND r.InternalColumnName = s.name";

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                ResultSet rset = pstmt.executeQuery();

                while (rset.next()) {
                    String column = rset.getString(1);
                    sb.append(", ").append(column).append("\n");
                }
            }
            sb.append(" FROM " + matchTableName + " WHERE $CONDITIONS");
        }
        String hdfsTargetPath = getTargetPath() + "/" + matchTableName;
        String url = String.format("%s/modeling/dataloads", microServiceHostPort);
        LoadConfiguration config = new LoadConfiguration();
        config.setCreds(preMatchEventTableAndCreds.getValue());
        config.setQuery(sb.toString());
        config.setCustomer(CUSTOMERSPACE);
        config.setKeyCols(Arrays.<String> asList(new String[] { "Source_Id" }));
        config.setTargetHdfsDir(hdfsTargetPath);

        AppSubmission submission = restTemplate.postForObject(url, config, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString());
        List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsTargetPath,
                new HdfsUtils.HdfsFilenameFilter() {

                    @Override
                    public boolean accept(String fileName) {
                        return fileName.endsWith(".avro");
                    }
                });

        Table eventTable = MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, avroFiles.get(0), null, null);
        eventTable.setName(matchTableName);

        addMetadata(eventTable, preMatchEventTableAndCreds.getValue());
        String extractPath = eventTable.getExtracts().get(0).getPath();
        extractPath = extractPath.substring(0, extractPath.lastIndexOf("/"));
        eventTable.getExtracts().get(0).setPath(extractPath);
        url = String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, CUSTOMERSPACE,
                eventTable.getName());
        restTemplate.postForLocation(url, eventTable);
        return eventTable;
    }

    private void addMetadata(Table table, DbCreds creds) throws Exception {
        try (Connection conn = DriverManager.getConnection(creds.getJdbcUrl())) {
            String query = "SELECT InternalColumnName, MetaDataName, MetaDataValue FROM " + table.getName()
                    + "_MetaData ORDER BY InternalColumnName";
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
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, CUSTOMERSPACE,
                "PrematchFlow");
        Table prematchFlowTable = restTemplate.getForObject(url, Table.class);
        DbCreds.Builder credsBuilder = new DbCreds.Builder()
                . //
                dbType("SQLServer")
                . //
                jdbcUrl("jdbc:sqlserver://10.51.15.130:1433;databaseName=PropDataMatchDB;user=DLTransfer;password=free&NSE")
                . //
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
            sb.append(String.format("  %s %s%s\n", attr.getName(), getSQLServerType(attr.getPhysicalDataType()),
                    i == size ? ")" : ","));
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
        case "int":
            return "INT";
        default:
            throw new RuntimeException("Unknown SQL Server type for avro type " + type);
        }

    }
    
    private String getVersionFromPomXmlFile() throws Exception {
        Collection<File> files = FileUtils.listFiles(new File("."), new IOFileFilter() {

            @Override
            public boolean accept(File file) {
                return file.getName().equals("pom.xml");
            }

            @Override
            public boolean accept(File dir, String name) {
                return name.equals("le-pls");
            }
            
        }, null);
        
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLStreamReader reader = factory.createXMLStreamReader(new FileInputStream(files.iterator().next()));
        StringBuilder content = null;
        String version = null;
        while (reader.hasNext() && version == null) {
            int event = reader.next();

            switch (event) {
            case XMLStreamConstants.START_ELEMENT:
                if ("version".equalsIgnoreCase(reader.getLocalName())) {
                    content = new StringBuilder();
                }
                break;

            case XMLStreamConstants.CHARACTERS:
                if (content != null) {
                    content.append(reader.getText().trim());
                }
                break;

            case XMLStreamConstants.END_ELEMENT:
                if (content != null) {
                    version = content.toString();
                }
                content = null;
                break;

            case XMLStreamConstants.START_DOCUMENT:
                break;
            }
        }
        
        return version;
    }

    private void installServiceFlow() throws Exception {
        String mavenHome = System.getProperty("MVN_HOME");
        String version = getVersionFromPomXmlFile();
        // Retrieve the service flow jar file from the maven repository
        String command = "%s/bin/mvn -DgroupId=com.latticeengines " + "-DartifactId=le-serviceflows-prospectdiscovery "
                + "-Dversion=%s -Dclassifier=shaded -Ddest=%s.jar dependency:get";
        String jarFileName = "le-serviceflows-prospectdiscovery-" + System.currentTimeMillis();
        command = String.format(command, mavenHome, version, jarFileName);

        CommandLine cmdLine = CommandLine.parse(command);
        DefaultExecutor executor = new DefaultExecutor();
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        PumpStreamHandler psh = new PumpStreamHandler(stdout);
        executor.setStreamHandler(psh);
        executor.execute(cmdLine);
        
        System.out.println(new String(stdout.toByteArray()));

        HdfsUtils.rmdir(yarnConfiguration, //
                String.format("%s/%s", SoftwareLibraryService.TOPLEVELPATH, "dataflowapi"));
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflowapi");
        pkg.setGroupId("com.latticeengines");
        pkg.setArtifactId("le-serviceflows-prospectdiscovery");
        pkg.setVersion("2.0.13-SNAPSHOT");
        pkg.setInitializerClass("com.latticeengines.prospectdiscovery.Initializer");
        File localFile = new File(jarFileName + ".jar");
        try {
            softwareLibraryService.installPackage(pkg, localFile);
        } finally {
            FileUtils.deleteQuietly(localFile);
        }

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
        dataFlowConfig.setDataFlowBeanName("preMatchEventTableFlow");
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
        String stoplist = ClassLoader.getSystemResource("com/latticeengines/pls/controller/Stoplist/Stoplist.avro")
                .getPath();
        HdfsUtils.mkdir(yarnConfiguration, "/tmp/Stoplist");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, stoplist, "/tmp/Stoplist");
        Table stopList = MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, "/tmp/Stoplist/Stoplist.avro",
                null, null);
        stopList.getExtracts().get(0).setPath("/tmp/Stoplist/*.avro");
        // register the stop list table
        url = String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort, CUSTOMERSPACE,
                stopList.getName());
        restTemplate.postForLocation(url, stopList);
        tables.add(stopList);
        return tables;
    }

    private void setupHdfs() throws Exception {
        String podId = CamilleEnvironment.getPodId();
        HdfsUtils.rmdir(yarnConfiguration, "/Pods/" + podId + "/Contracts/DemoContract");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/Stoplist");
        HdfsUtils.rmdir(yarnConfiguration, "/user/s-analytics/customers/DemoContract.DemoTenant.Production");
    }

    private void setupTenant() throws Exception {
        super.deleteTenantByRestCall(CUSTOMERSPACE);
        tenant = new Tenant();
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
        crmCredential.setUrl("https://login.salesforce.com");
        crmCredentialZKService.writeToZooKeeper("sfdc", CUSTOMERSPACE, true, crmCredential, true);

        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(CUSTOMERSPACE), "Eai");
        Path connectTimeoutDocPath = docPath.append("SalesforceEndpointConfig") //
                .append("HttpClient").append("ConnectTimeout");
        camille.create(connectTimeoutDocPath, new Document("60000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        Path importTimeoutDocPath = docPath.append("SalesforceEndpointConfig").append("HttpClient")
                .append("ImportTimeout");
        camille.create(importTimeoutDocPath, new Document("3600000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    private void createImportTablesInMetadataStore() throws IOException {
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

    protected ImportConfiguration setupSalesforceImportConfig() {
        ImportConfiguration importConfig = new ImportConfiguration();
        SourceImportConfiguration salesforceConfig = new SourceImportConfiguration();
        salesforceConfig.setSourceType(SourceType.SALESFORCE);

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
            String url = String.format(microServiceHostPort + "/propdata/matchcommands/%s?matchClient=PD131",
                    commands.getPid());
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
    
    static class CollectingLogOutputStream extends LogOutputStream {
        private final List<String> lines = new LinkedList<String>();
        
        @Override 
        protected void processLine(String line, int level) {
            lines.add(line);
        }   
        public List<String> getLines() {
            return lines;
        }
    }

}
