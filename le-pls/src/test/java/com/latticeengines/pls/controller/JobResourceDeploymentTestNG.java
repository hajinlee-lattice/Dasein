package com.latticeengines.pls.controller;

import java.util.HashMap;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class JobResourceDeploymentTestNG  extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(JobResourceDeploymentTestNG.class);

    @Inject
    private WorkflowProxy workflowProxy;

    private static final String ACTION_INITIATOR = "jxiao@lattice-engines.com";
    private static final String ERRORCATEGORY = "Test Error";

    private Long jobPid;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        Tenant tenant = deploymentTestBed.getMainTestTenant();
        deploymentTestBed.loginAndAttach(TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN),
                TestFrameworkUtils.GENERAL_PASSWORD, tenant);
        MultiTenantContext.setTenant(tenant);
        restTemplate = deploymentTestBed.getRestTemplate();
    }

    @Test(groups = "deployment")
    public void testCreate() {
        Job failedJob = new Job();
        failedJob.setJobType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getName());
        failedJob.setUser(ACTION_INITIATOR);
        failedJob.setErrorCode(LedpCode.LEDP_00002);
        failedJob.setErrorMsg("ac");
        failedJob.setInputs(new HashMap<>());
        jobPid = workflowProxy.createFailedWorkflowJob(MultiTenantContext.getShortTenantId(), failedJob);
        List<Job> jobs = JsonUtils.convertList(restTemplate.getForObject(getDeployedRestAPIHostPort() + "/pls/jobs/",
                List.class), Job.class);
        log.info("jobs is " + jobs.toString());
        Assert.assertFalse(jobs.isEmpty());
        Job job = jobs.get(0);
        Assert.assertEquals(job.getPid(), jobPid);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testSetErrorCategory() {
        log.info("resttemplate:"+restTemplate.getInterceptors());
        restTemplate.getForObject(getDeployedRestAPIHostPort() + "/pls/jobs/" + jobPid + "/setErrorCategory" +
                        "?errorCategory=" + ERRORCATEGORY, Void.class);
        List<Job> jobs = JsonUtils.convertList(restTemplate.getForObject(getDeployedRestAPIHostPort() + "/pls/jobs/", List.class), Job.class);
        log.info("jobs is " + jobs.toString());
        Assert.assertFalse(jobs.isEmpty());
        Job job = jobs.get(0);
        Assert.assertEquals(job.getErrorCategory(), ERRORCATEGORY);
    }

}
