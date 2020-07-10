package com.latticeengines.workflowapi.functionalframework;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.app.LatticeInsightsInternalProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowJobService;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-workflowapi-context.xml" })
public class WorkflowApiFunctionalTestNGBase extends WorkflowTestNGBase {

    protected static final CustomerSpace WFAPITEST_CUSTOMERSPACE = CustomerSpace
            .parse("WFAPITests.WFAPITests.WFAPITests");
    protected static final long WORKFLOW_WAIT_TIME_IN_MILLIS = 1000L * 60 * 90;

    private static final Logger log = LoggerFactory.getLogger(WorkflowApiFunctionalTestNGBase.class);

    @Value("${common.test.pls.url}")
    protected String internalResourceHostPort;

    @Value("${common.test.microservice.url}")
    protected String microServiceHostPort;

    @Value("${workflowapi.modelingservice.basedir}")
    protected String modelingServiceHdfsBaseDir;

    @Inject
    protected WorkflowJobService workflowJobService;

    @Value("${workflowapi.test.sfdc.user.name}")
    private String salesforceUserName;

    @Value("${workflowapi.test.sfdc.passwd.encrypted}")
    private String salesforcePasswd;

    @Value("${workflowapi.test.sfdc.securitytoken}")
    private String salesforceSecurityToken;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Inject
    protected LatticeInsightsInternalProxy latticeInsightsInternalProxy;

    @Inject
    protected SourceFileProxy sourceFileProxy;

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    protected YarnFunctionalTestNGBase platformTestBase;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private YarnClient defaultYarnClient;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    protected Tenant tenant;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        restTemplate.setInterceptors(getAddMagicAuthHeaders());
        setupYarnPlatform();

        tenant = tenantEntityMgr.findByTenantId(WFAPITEST_CUSTOMERSPACE.toString());
        if (tenant != null) {
            tenantEntityMgr.delete(tenant);
        }
        tenant = new Tenant();
        tenant.setId(WFAPITEST_CUSTOMERSPACE.toString());
        tenant.setName(WFAPITEST_CUSTOMERSPACE.toString());
        tenantEntityMgr.create(tenant);
        MultiTenantContext.setTenant(tenant);

        com.latticeengines.domain.exposed.camille.Path path = //
                PathBuilder.buildCustomerSpacePath("Production", WFAPITEST_CUSTOMERSPACE);
        HdfsUtils.rmdir(yarnConfiguration, path.toString());
        HdfsUtils.mkdir(yarnConfiguration, path.toString());
    }

    protected void setupYarnPlatform() {
        platformTestBase = new YarnFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

    protected void waitForCompletion(WorkflowExecutionId workflowId) {
        log.info("Workflow id = " + workflowId.getId());
        JobStatus status = workflowService.sleepForCompletionWithStatus(workflowId);
        assertEquals(status, JobStatus.COMPLETED);
    }

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        int retryOnException = 4;
        Job job;
        while (true) {
            try {
                job = workflowJobService.getJobByApplicationId(tenant.getId(), applicationId, false);
            } catch (Exception e) {
                log.error(String.format("Workflow job exception: %s", e.getMessage()), e);

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                if (job.getJobStatus() == JobStatus.FAILED || job.getJobStatus() == JobStatus.PENDING_RETRY) {
                    log.error(applicationId + " Failed with ErrorCode " + job.getErrorCode() + ". \n"
                            + job.getErrorMsg());
                }
                return job.getJobStatus();
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
