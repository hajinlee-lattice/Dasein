package com.latticeengines.workflowapi.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

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

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.rest.URLUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
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

    protected RestTemplate restTemplate = new RestTemplate();
    protected DataPlatformFunctionalTestNGBase platformTestBase;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
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

        String url = String.format("%s/workflowapi/workflows/yarnapps/id/%s", URLUtils.getRestAPIHostPort(microServiceHostPort), appId);
        String workflowId = restTemplate.getForObject(url, String.class);

        url = String.format("%s/workflowapi/workflows/status/%s", URLUtils.getRestAPIHostPort(microServiceHostPort), workflowId);
        WorkflowStatus workflowStatus = restTemplate.getForObject(url, WorkflowStatus.class);
        assertEquals(workflowStatus.getStatus(), BatchStatus.COMPLETED);
    }

}
