package com.latticeengines.workflowapi.service.impl;

import java.util.Date;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.security.Tenant;
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

        Job job = workflowContainerService.getJobStatusFromWorkflowJobAndYarn(workflowJob);

        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(job.getStartTimestamp(), new Date(workflowJob.getStartTimeInMillis()));
        workflowJobEntityMgr.delete(workflowJob);
    }

    @Test(groups = "functional")
    public void getJobStatusForJobWithoutWorkflowIdFromYarn() {
        Tenant t = tenantEntityMgr.findByTenantId(WFAPITEST_CUSTOMERSPACE.toString());

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(t);
        workflowJob.setApplicationId("applicationid_0001");
        workflowJobEntityMgr.create(workflowJob);

        JobProxy jobProxy = mock(JobProxy.class);
        when(jobProxy.getJobStatus(any(String.class))).thenAnswer(
                new Answer<com.latticeengines.domain.exposed.dataplatform.JobStatus>() {

                    @Override
                    public com.latticeengines.domain.exposed.dataplatform.JobStatus answer(InvocationOnMock invocation)
                            throws Throwable {
                        com.latticeengines.domain.exposed.dataplatform.JobStatus jobStatus = new com.latticeengines.domain.exposed.dataplatform.JobStatus();
                        jobStatus.setState(YarnApplicationState.FINISHED);
                        jobStatus.setStartTime(100000001L);
                        return jobStatus;
                    }

                });

        ((WorkflowContainerServiceImpl) workflowContainerService).setJobProxy(jobProxy);
        Job job = workflowContainerService.getJobStatusFromWorkflowJobAndYarn(workflowJob);

        assertEquals(job.getJobStatus(), JobStatus.FAILED);
        assertEquals(job.getStartTimestamp(), new Date(workflowJob.getStartTimeInMillis()));

        workflowJob = workflowJobEntityMgr.findByApplicationId("applicationid_0001");
        assertEquals(workflowJob.getStatus(), FinalApplicationStatus.FAILED);
        assertEquals(workflowJob.getStartTimeInMillis().longValue(), 100000001L);

    }

}
