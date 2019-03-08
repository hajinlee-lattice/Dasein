package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyList;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class CDLJobServiceImplTestNG {

    @Mock
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    @Mock
    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Mock
    private WorkflowProxy workflowProxy;

    @Mock
    private DataFeedProxy dataFeedProxy;

    @InjectMocks
    private CDLJobServiceImpl cdlJobService;

    private String USERID = "Auto Scheduled";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);

        Map<String, String> activeStack = new TreeMap<>();
        activeStack.put("EMRClusterId", "EMRClusterId");
        when(internalResourceRestApiProxy.getActiveStack()).thenReturn(activeStack);

        CDLJobDetail cdlJobDetail = new CDLJobDetail();
        when(cdlJobDetailEntityMgr.listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE)).thenReturn(Collections.singletonList(cdlJobDetail));

        try {
            Field field1 = cdlJobService.getClass().getDeclaredField("concurrentProcessAnalyzeJobs");
            field1.setAccessible(true);
            field1.setInt(cdlJobService, 6);

            Field field2 = cdlJobService.getClass().getDeclaredField("minimumScheduledJobCount");
            field2.setAccessible(true);
            field2.setInt(cdlJobService, 2);

            Field field3 = cdlJobService.getClass().getDeclaredField("maximumScheduledJobCount");
            field3.setAccessible(true);
            field3.setInt(cdlJobService, 4);
        } catch (Exception e) {}
    }

    @Test(groups = "unit")
    public void testSubmitJob() {
        List<WorkflowJob> runningPAJobs = new ArrayList<>();
        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(runningPAJobs);

        when(dataFeedProxy.getAllSimpleDataFeedsByTenantStatus(TenantStatus.ACTIVE)).thenReturn(null);

        Assert.assertThrows(
                NullPointerException.class,
                () -> cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null));
    }

    @Test(groups = "unit")
    public void testSubmitJob1() {
        when(dataFeedProxy.getAllSimpleDataFeedsByTenantStatus(TenantStatus.ACTIVE)).thenReturn(null);

        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs1());
        Assert.assertThrows(
                NullPointerException.class,
                () -> cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null));

        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs2());
        Assert.assertThrows(
                NullPointerException.class,
                () -> cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null));

        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs3());
        Assert.assertThrows(
                NullPointerException.class,
                () -> cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null));

        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs4());
        Assert.assertThrows(
                NullPointerException.class,
                () -> cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null));

        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs5());
        Assert.assertThrows(
                NullPointerException.class,
                () -> cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null));

        Exception e = null;
        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs6());
        try {
            cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertNull(e);
    }

    private List<WorkflowJob> geTesttWorkflowJobs1() {
        List<WorkflowJob> runningPAJobs = new ArrayList<>();
        runningPAJobs.add(getWorkflowJob(USERID, "tenant1"));
        runningPAJobs.add(getWorkflowJob("user2", "tenant2"));

        return runningPAJobs;
    }

    private List<WorkflowJob> geTesttWorkflowJobs2() {
        List<WorkflowJob> runningPAJobs = new ArrayList<>();
        runningPAJobs.add(getWorkflowJob(USERID, "tenant1"));
        runningPAJobs.add(getWorkflowJob(USERID, "tenant2"));
        runningPAJobs.add(getWorkflowJob("user3", "tenant3"));

        return runningPAJobs;
    }

    private List<WorkflowJob> geTesttWorkflowJobs3() {
        List<WorkflowJob> runningPAJobs = new ArrayList<>();
        runningPAJobs.add(getWorkflowJob(USERID, "tenant1"));
        runningPAJobs.add(getWorkflowJob(USERID, "tenant2"));
        runningPAJobs.add(getWorkflowJob(USERID, "tenant3"));

        return runningPAJobs;
    }

    private List<WorkflowJob> geTesttWorkflowJobs4() {
        List<WorkflowJob> runningPAJobs = new ArrayList<>();
        runningPAJobs.add(getWorkflowJob(USERID, "tenant1"));
        runningPAJobs.add(getWorkflowJob(USERID, "tenant2"));
        runningPAJobs.add(getWorkflowJob(USERID, "tenant3"));
        runningPAJobs.add(getWorkflowJob("user4", "tenant4"));

        return runningPAJobs;
    }

    private List<WorkflowJob> geTesttWorkflowJobs5() {
        List<WorkflowJob> runningPAJobs = new ArrayList<>();
        runningPAJobs.add(getWorkflowJob(USERID, "tenant1"));
        runningPAJobs.add(getWorkflowJob("user2", "tenant2"));
        runningPAJobs.add(getWorkflowJob("user3", "tenant3"));
        runningPAJobs.add(getWorkflowJob("user4", "tenant4"));
        runningPAJobs.add(getWorkflowJob("user5", "tenant5"));
        runningPAJobs.add(getWorkflowJob("user6", "tenant6"));
        runningPAJobs.add(getWorkflowJob("user7", "tenant7"));

        return runningPAJobs;
    }

    private List<WorkflowJob> geTesttWorkflowJobs6() {
        List<WorkflowJob> runningPAJobs = new ArrayList<>();
        runningPAJobs.add(getWorkflowJob(USERID, "tenant1"));
        runningPAJobs.add(getWorkflowJob(USERID, "tenant2"));
        runningPAJobs.add(getWorkflowJob(USERID, "tenant3"));
        runningPAJobs.add(getWorkflowJob("user4", "tenant4"));
        runningPAJobs.add(getWorkflowJob("user5", "tenant5"));
        runningPAJobs.add(getWorkflowJob("user6", "tenant5"));

        return runningPAJobs;
    }

    private WorkflowJob getWorkflowJob(String userId, String tenantId) {
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setUserId(userId);
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        workflowJob.setTenant(tenant);

        return workflowJob;
    }
}
