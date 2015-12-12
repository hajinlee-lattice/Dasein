package com.latticeengines.workflowapi.flows;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
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
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class FitModelWorkflowTestNGBase extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @Value("${workflowapi.test.sfdc.user.name}")
    private String salesforceUserName;

    @Value("${workflowapi.test.sfdc.passwd.encrypted}")
    private String salesforcePasswd;

    @Value("${workflowapi.test.sfdc.securitytoken}")
    private String salesforceSecurityToken;

    @Value("${security.test.pls.api.hostport}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    private TargetMarket defaultTargetMarket;

    protected void setupForFitModel() throws Exception {
        restTemplate.setInterceptors(getAddMagicAuthHeaders());
        Tenant tenant = setupTenant(DEMO_CUSTOMERSPACE);
        setupUsers();
        setupCamille(DEMO_CUSTOMERSPACE);
        setupHdfs();
        installServiceFlow();
        createImportTablesInMetadataStore(DEMO_CUSTOMERSPACE, tenant);
        copyStopListToHdfs();

        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        defaultTargetMarket = internalResourceProxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME, DEMO_CUSTOMERSPACE.toString());
        if (defaultTargetMarket != null) {
            internalResourceProxy.deleteTargetMarketByName(TargetMarket.DEFAULT_NAME, DEMO_CUSTOMERSPACE.toString());
        }
        internalResourceProxy.createDefaultTargetMarket(DEMO_CUSTOMERSPACE.toString());
        defaultTargetMarket = internalResourceProxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME, DEMO_CUSTOMERSPACE.toString());
    }

    protected FitModelWorkflowConfiguration generateFitModelWorkflowConfiguration() {
        List<String> eventCols = new ArrayList<>();
        //eventCols.add("Event_IsWon");
        eventCols.add("Event_StageIsClosedWon");
        //eventCols.add("Event_IsClosed");
        //eventCols.add("Event_OpportunityCreated");

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("PublicDomain", "/tmp/Stoplist/*.avro");

        FitModelWorkflowConfiguration workflowConfig = new FitModelWorkflowConfiguration.Builder()
                .customer(DEMO_CUSTOMERSPACE) //
                .microServiceHostPort(microServiceHostPort) //
                .sourceType(SourceType.SALESFORCE) //
                .extraSources(extraSources) //
                .targetPath("/PrematchFlowRun") //
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
                .sourceDir("/tmp/AccountMaster") //
                .registerScoredTable(true) //
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

    private void setupUsers() throws Exception {
        createAdminUserByRestCall(DEMO_CUSTOMERSPACE.toString(), //
                "rgonzalez@lattice-engines.com", //
                "rgonzalez@lattice-engines.com", //
                "PD Super", //
                "User", //
                SecurityFunctionalTestNGBase.adminPasswordHash);
    }

    private void setupHdfs() throws Exception {
        String podId = CamilleEnvironment.getPodId();
        HdfsUtils.rmdir(yarnConfiguration, "/Pods/" + podId + "/Contracts/DemoContract");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/Stoplist");
        HdfsUtils.rmdir(yarnConfiguration, "/user/s-analytics/customers/DemoContract.DemoTenant.Production");
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
        pkg.setVersion(version);
        pkg.setInitializerClass("com.latticeengines.prospectdiscovery.Initializer");
        File localFile = new File(jarFileName + ".jar");
        try {
            softwareLibraryService.installPackage(pkg, localFile);
        } finally {
            FileUtils.deleteQuietly(localFile);
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
