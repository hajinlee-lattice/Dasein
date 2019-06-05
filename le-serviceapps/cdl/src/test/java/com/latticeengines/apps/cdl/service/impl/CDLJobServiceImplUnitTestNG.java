package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.CDLJobDetailEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.aws.AwsApplicationId;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class CDLJobServiceImplUnitTestNG {

    @Mock
    private ColumnMetadataProxy columnMetadataProxy;

    @Mock
    private CDLJobDetailEntityMgr cdlJobDetailEntityMgr;

    @Mock
    private WorkflowProxy workflowProxy;

    @Mock
    private DataFeedService dataFeedService;

    @Mock
    private ActionService actionService;

    @Mock
    private BatonService batonService;

    @Mock
    private CDLProxy cdlProxy;

    @InjectMocks
    @Spy
    private CDLJobServiceImpl cdlJobService;

    private String USERID = "Auto Scheduled";

    private static final String HIGH_PRIORITY_RUNNING_JOB_MAP = "HIGH_PRIORITY_RUNNING_JOB_MAP";
    private static final String LOW_PRIORITY_RUNNING_JOB_MAP = "LOW_PRIORITY_RUNNING_JOB_MAP";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        CDLJobServiceImpl.CACHE_KEY_PREFIX = "CDLScheduledJobCache";
        CDLJobServiceImpl.HIGH_PRIORITY_RUNNING_JOB_MAP = "HIGH_PRIORITY_RUNNING_JOB_MAP";
        CDLJobServiceImpl.LOW_PRIORITY_RUNNING_JOB_MAP = "LOW_PRIORITY_RUNNING_JOB_MAP";
    }

    @Test(groups = "unit")
    public void test_orchestrateJob() {
        CDLJobServiceImpl.appIdMap = new LinkedHashMap<>();
        cdlJobService.concurrentProcessAnalyzeJobs = 8;
        cdlJobService.minimumScheduledJobCount = 3;
        cdlJobService.maximumScheduledJobCount = 5;
        cdlJobService.isActivityBasedPA = false;

        CDLJobDetail cdlJobDetail = new CDLJobDetail();
        when(cdlJobDetailEntityMgr.listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE)).thenReturn(Collections.singletonList(cdlJobDetail));
        when(cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE)).thenReturn(cdlJobDetail);

        Tenant tenant1 = new Tenant();
        tenant1.setPid(1L);
        tenant1.setId("testTenant1");
        SimpleDataFeed simpleDataFeed1 = new SimpleDataFeed();
        simpleDataFeed1.setStatus(DataFeed.Status.Active);
        simpleDataFeed1.setTenant(tenant1);

        Tenant tenant2 = new Tenant();
        tenant2.setPid(2L);
        tenant2.setId("testTenant2");
        SimpleDataFeed simpleDataFeed2 = new SimpleDataFeed();
        simpleDataFeed2.setStatus(DataFeed.Status.ProcessAnalyzing);
        simpleDataFeed2.setTenant(tenant2);

        List<SimpleDataFeed> simpleDataFeeds = new ArrayList<>();
        simpleDataFeeds.add(simpleDataFeed1);
        simpleDataFeeds.add(simpleDataFeed2);
        when(dataFeedService.getSimpleDataFeeds(TenantStatus.ACTIVE, "4.0")).thenReturn(simpleDataFeeds);

        long currentTimeMillis = System.currentTimeMillis();
        Date currentTime = new Date(currentTimeMillis - 1000);
        doReturn(currentTime).when(cdlJobService).getNextInvokeTime(any(CustomerSpace.class), any(Tenant.class), any(CDLJobDetail.class));

        doNothing().when(dataFeedService).updateDataFeedNextInvokeTime(anyString(), any(Date.class));

        doReturn(true).when(cdlJobService).submitProcessAnalyzeJob(any(Tenant.class), any(CDLJobDetail.class));
        doReturn(false).when(cdlJobService).systemCheck();
        test_ClusterIDIsNotEmpty();
        test_ClusterIDIsEmpty();
    }

    @Test(groups = "unit")
    public void test_orchestrateJob_New() {
        cdlJobService.maximumHighPriorityScheduledJobCount = 2;
        cdlJobService.minimumHighPriorityScheduledJobCount = 1;
        cdlJobService.maximumLowPriorityScheduledJobCount = 4;
        cdlJobService.minimumLowPriorityScheduledJobCount = 1;
        cdlJobService.isActivityBasedPA = true;

        doReturn(false).when(cdlJobService).systemCheck();
        when(columnMetadataProxy.latestBuildNumber()).thenReturn("2.0.18.0");

        Tenant tenant1 = new Tenant();
        tenant1.setPid(1L);
        tenant1.setId("testTenant1");
        tenant1.setName("testTenant1");
        SimpleDataFeed simpleDataFeed1 = new SimpleDataFeed();
        simpleDataFeed1.setStatus(DataFeed.Status.Active);
        simpleDataFeed1.setTenant(tenant1);

        List<SimpleDataFeed> simpleDataFeeds = new ArrayList<>();
        simpleDataFeeds.add(simpleDataFeed1);

        when(dataFeedService.getSimpleDataFeeds(TenantStatus.ACTIVE, "4.0")).thenReturn(simpleDataFeeds);

        CDLJobDetail cdlJobDetail =  new CDLJobDetail();
        when(cdlJobDetailEntityMgr.findLatestJobByJobType(CDLJobType.PROCESSANALYZE)).thenReturn(cdlJobDetail);

        List<Action> actions = new ArrayList<>();
        Action action = new Action();
        action.setPid(1l);
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setCreated(new Date(new Date().getTime() - (60 * 1000 * 15)));
        action.setUpdated(new Date(new Date().getTime() - (60 * 1000 * 15)));
        action.setTenant(tenant1);
        actions.add(action);
        when(actionService.findByOwnerIdAndActionStatus(null, ActionStatus.ACTIVE)).thenReturn(actions);

        when(batonService.isEnabled(any(CustomerSpace.class), eq(LatticeFeatureFlag.ENABLE_DATA_CLOUD_REFRESH_ACTIVITY))).thenReturn(true);
        doReturn(true).when(cdlJobService).checkDataCloudChange(anyString(), anyString());

        long currentTimeMillis = System.currentTimeMillis();
        Date currentTime = new Date(currentTimeMillis - 1000);
        doReturn(currentTime).when(cdlJobService).getNextInvokeTime(any(CustomerSpace.class), any(Tenant.class), any(CDLJobDetail.class));

        doNothing().when(dataFeedService).updateDataFeedNextInvokeTime(anyString(), any(Date.class));

        when(cdlJobDetailEntityMgr.listAllRunningJobByJobType(CDLJobType.PROCESSANALYZE)).thenReturn(Collections.singletonList(cdlJobDetail));

        Map<String, List<Object>> highMap = new HashMap<>();
        Map<String, List<Object>> lowMap = new HashMap<>();
        doReturn(highMap).when(cdlJobService).getMap(eq(HIGH_PRIORITY_RUNNING_JOB_MAP));
        doReturn(lowMap).when(cdlJobService).getMap(eq(LOW_PRIORITY_RUNNING_JOB_MAP));

        doReturn(false).when(cdlJobService).retryProcessAnalyze(any(Tenant.class), any(CDLJobDetail.class));
        when(cdlJobDetailEntityMgr.createJobDetail(eq(CDLJobType.PROCESSANALYZE), any(Tenant.class))).thenReturn(cdlJobDetail);

        AwsApplicationId applicationId = new AwsApplicationId();
        applicationId.setJobId(UUID.randomUUID().toString());
        when(cdlProxy.processAnalyze(anyString(), any(ProcessAnalyzeRequest.class))).thenReturn(applicationId);
        doNothing().when(cdlJobDetailEntityMgr).updateJobDetail(any(CDLJobDetail.class));

        doNothing().when(cdlJobService).updateRedisTemplate(anyString(), any(Map.class));

        test_ClusterIDIsNotEmpty();
        test_ClusterIDIsEmpty();
    }

    private void test_ClusterIDIsNotEmpty() {
        doReturn("abc").when(cdlJobService).getCurrentClusterID();

        Exception e = null;

        when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(new ArrayList<>());
        try {
            cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertNull(e);

        if (!cdlJobService.isActivityBasedPA) {
            when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs1());
            try {
                cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
            } catch (Exception ex) {
                e = ex;
            }
            Assert.assertNull(e);

            when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs2());
            try {
                cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
            } catch (Exception ex) {
                e = ex;
            }
            Assert.assertNull(e);

            when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs3());
            try {
                cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
            } catch (Exception ex) {
                e = ex;
            }
            Assert.assertNull(e);

            when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs4());
            try {
                cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
            } catch (Exception ex) {
                e = ex;
            }
            Assert.assertNull(e);

            when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs5());
            try {
                cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
            } catch (Exception ex) {
                e = ex;
            }
            Assert.assertNull(e);

            when(workflowProxy.queryByClusterIDAndTypesAndStatuses(anyString(), anyList(), anyList())).thenReturn(geTesttWorkflowJobs6());
            try {
                cdlJobService.submitJob(CDLJobType.PROCESSANALYZE, null);
            } catch (Exception ex) {
                e = ex;
            }
            Assert.assertNull(e);
        }
    }

    private void test_ClusterIDIsEmpty() {
        doReturn(null).when(cdlJobService).getCurrentClusterID();

        Exception e = null;

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
