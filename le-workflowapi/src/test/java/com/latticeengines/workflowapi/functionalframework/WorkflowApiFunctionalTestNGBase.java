package com.latticeengines.workflowapi.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.rest.URLUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-workflowapi-context.xml" })
public class WorkflowApiFunctionalTestNGBase extends WorkflowFunctionalTestNGBase {

    protected static final CustomerSpace WFAPITEST_CUSTOMERSPACE = CustomerSpace.parse("WFAPITests.WFAPITests.WFAPITests");
    protected static final long WORKFLOW_WAIT_TIME_IN_MILLIS = 1000L * 60 * 60;

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(WorkflowApiFunctionalTestNGBase.class);

    @Value("${workflowapi.microservice.rest.endpoint.hostport}")
    protected String microServiceHostPort;

    @Value("${workflowapi.modelingservice.basedir}")
    protected String modelingServiceHdfsBaseDir;

    @Value("${security.test.pls.api.hostport}")
    protected String internalResourceHostPort;

    protected InternalResourceRestApiProxy internalResourceProxy;

    protected RestTemplate restTemplate = new RestTemplate();
    protected DataPlatformFunctionalTestNGBase platformTestBase;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
        Tenant t = tenantEntityMgr.findByTenantId(WFAPITEST_CUSTOMERSPACE.toString());
        if (t != null) {
            tenantEntityMgr.delete(t);
        }
        t = new Tenant();
        t.setId(WFAPITEST_CUSTOMERSPACE.toString());
        t.setName(WFAPITEST_CUSTOMERSPACE.toString());
        tenantEntityMgr.create(t);

        com.latticeengines.domain.exposed.camille.Path path = //
        PathBuilder.buildCustomerSpacePath("Production", WFAPITEST_CUSTOMERSPACE);
        HdfsUtils.rmdir(yarnConfiguration, path.toString());
        HdfsUtils.mkdir(yarnConfiguration, path.toString());
    }

    protected AppSubmission submitWorkflow(WorkflowConfiguration configuration) {
        String url = String.format("%s/workflowapi/workflows/", URLUtils.getRestAPIHostPort(microServiceHostPort));
        try {
            AppSubmission submission = restTemplate.postForObject(url, configuration, AppSubmission.class);
            return submission;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void submitWorkflowAndAssertSuccessfulCompletion(WorkflowConfiguration workflowConfig) throws Exception {
        AppSubmission submission = submitWorkflow(workflowConfig);
        assertNotNull(submission);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, WORKFLOW_WAIT_TIME_IN_MILLIS, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String url = String.format("%s/workflowapi/workflows/yarnapps/status/%s", URLUtils.getRestAPIHostPort(microServiceHostPort), appId);
        WorkflowStatus workflowStatus = restTemplate.getForObject(url, WorkflowStatus.class);
        assertEquals(workflowStatus.getStatus(), BatchStatus.COMPLETED);
    }

    protected void installServiceFlow() throws Exception {
        String mavenHome = System.getProperty("MVN_HOME", "/usr");
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

    protected void setupUsers(CustomerSpace customerSpace) throws Exception {
        createAdminUserByRestCall(customerSpace.toString(), //
                "rgonzalez@lattice-engines.com", //
                "rgonzalez@lattice-engines.com", //
                "PD Super", //
                "User", //
                SecurityFunctionalTestNGBase.adminPasswordHash);
    }

    protected void setupHdfs(CustomerSpace customerSpace) throws Exception {
        String podId = CamilleEnvironment.getPodId();
        HdfsUtils.rmdir(yarnConfiguration, "/Pods/" + podId + "/Contracts/" + customerSpace.getContractId());
        HdfsUtils.rmdir(yarnConfiguration, "/user/s-analytics/customers/ " + customerSpace.toString());
    }

    
}
