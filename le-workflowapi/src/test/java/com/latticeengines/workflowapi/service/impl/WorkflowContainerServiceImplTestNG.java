package com.latticeengines.workflowapi.service.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Date;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSBatchConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowContainerService;

public class WorkflowContainerServiceImplTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private WorkflowContainerService workflowContainerService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Test(groups = "functional")
    public void getJobStatusForJobWithoutWorkflowIdFromDB() {
        Tenant t = tenantEntityMgr.findByTenantId(WFAPITEST_CUSTOMERSPACE.toString());

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(t);
        workflowJob.setStatus(FinalApplicationStatus.FAILED);
        workflowJob.setStartTimeInMillis(100000000L);
        workflowJobEntityMgr.create(workflowJob);

        Job job = workflowContainerService.getJobFromWorkflowJobAndYarn(workflowJob);

        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(job.getStartTimestamp(), new Date(workflowJob.getStartTimeInMillis()));
        workflowJobEntityMgr.delete(workflowJob);
    }

    @Test(groups = "functional", enabled = false)
    public void getJobStatusForJobWithoutWorkflowIdFromYarn() {
        Tenant t = tenantEntityMgr.findByTenantId(WFAPITEST_CUSTOMERSPACE.toString());

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(t);
        workflowJob.setApplicationId("applicationid_0001");
        workflowJobEntityMgr.create(workflowJob);

        JobProxy jobProxy = mock(JobProxy.class);
        when(jobProxy.getJobStatus(any(String.class))).thenAnswer(invocation -> {
            com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus = new com.latticeengines.domain.exposed.dataplatform.JobStatus();
            jobStatus.setState(YarnApplicationState.FINISHED);
            jobStatus.setStatus(FinalApplicationStatus.FAILED);
            jobStatus.setStartTime(100000001L);
            return jobStatus;
        });

        ((WorkflowContainerServiceImpl) workflowContainerService).setJobProxy(jobProxy);
        Job job = workflowContainerService.getJobFromWorkflowJobAndYarn(workflowJob);

        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(job.getStartTimestamp(), new Date(workflowJob.getStartTimeInMillis()));

        workflowJob = workflowJobEntityMgr.findByApplicationId("applicationid_0001");
        assertEquals(workflowJob.getStatus(), FinalApplicationStatus.FAILED);
        assertEquals(workflowJob.getStartTimeInMillis().longValue(), 100000001L);

    }

    @Test(groups = "functional", enabled = true)
    public void getJobStatusForJobCantFindInYarn() {
        Tenant t = tenantEntityMgr.findByTenantId(WFAPITEST_CUSTOMERSPACE.toString());

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(t);
        workflowJob.setApplicationId("applicationid_0001");
        workflowJobEntityMgr.create(workflowJob);

        JobProxy jobProxy = mock(JobProxy.class);
        when(jobProxy.getJobStatus(any(String.class))).thenAnswer(invocation -> {
            throw new RemoteLedpException(LedpCode.LEDP_00002);
        });

        ((WorkflowContainerServiceImpl) workflowContainerService).setJobProxy(jobProxy);
        Job job = workflowContainerService.getJobFromWorkflowJobAndYarn(workflowJob);

        assertEquals(job.getJobStatus(), JobStatus.FAILED);

        workflowJob = workflowJobEntityMgr.findByApplicationId("applicationid_0001");
        assertEquals(workflowJob.getStatus(), FinalApplicationStatus.FAILED);
    }

    @Test(groups = "functional", enabled = false)
    public void submitAwsWorkFlow() {
        WorkflowConfiguration workflowConfig = new WorkflowConfiguration();
        workflowConfig.setWorkflowName("dummyWorkflow");
        workflowConfig.setCustomerSpace(WFAPITEST_CUSTOMERSPACE);

        AWSBatchConfiguration awsConfig = new AWSBatchConfiguration();
        awsConfig.setCustomerSpace(WFAPITEST_CUSTOMERSPACE);
        awsConfig.setMicroServiceHostPort("https://localhost");
        awsConfig.setRunInAws(true);
        workflowConfig.add(awsConfig);
        BaseStepConfiguration baseConfig = new BaseStepConfiguration();
        workflowConfig.add(baseConfig);

        String jobId = workflowContainerService.submitAwsWorkFlow(workflowConfig);
        Assert.assertNotNull(jobId);
    }

}
