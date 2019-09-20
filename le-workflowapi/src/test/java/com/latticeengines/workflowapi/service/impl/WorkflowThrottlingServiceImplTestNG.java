package com.latticeengines.workflowapi.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.zookeeper.ZooDefs;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.ThrottlingResult;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowThrottlingService;

public class WorkflowThrottlingServiceImplTestNG extends WorkflowApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(WorkflowThrottlingServiceImplTestNG.class);

    private static final String GLOBAL = WorkflowThrottlingUtils.GLOBAL;
    private static final String DEFAULT = WorkflowThrottlingUtils.DEFAULT;
    private static final String GLOBALCONFIG = WorkflowThrottlingUtils.GLOBALCONFIG;
    private static final String TENANTCONFIG = WorkflowThrottlingUtils.TENANTCONFIG;

    private static String TEST_PODID = "ThrottlingTestPod";
    private static final Path TEST_POD_PATH = PathBuilder.buildPodPath(TEST_PODID);

    private static final String TEST_DIV1 = "throttlingServiceTestDivisionName1";
    private static final String TEST_DIV2 = "throttlingServiceTestDivisionName2";
    private static final String TEST_WF_TYPE = "TestWorkflowType";
    private static final String GENERIC_WF_TYPE = "genericWorkflowType";
    private static final String EMR_CLUSTER_ID = "fakedId";

    private static List<WorkflowJob> FAKE_WF_FOR_STATUS1;
    private static List<WorkflowJob> FAKE_WF_FOR_STATUS2;
    private static List<WorkflowJob> REACHED_QUEUE_LIMIT;
    private static List<WorkflowJob> NOT_REACHED_QUEUE_LIMIT;
    private static List<WorkflowJob> FAKE_WORKFLOW_FOR_RESULT1;
    private static List<WorkflowJob> FAKE_WORKFLOW_FOR_RESULT2;

    private static String TEST_TENANT_ID;
    private static final String TENANT_ID_EMPTY_CONFIG = "abcde.abcde.Production";

    private static WorkflowThrottlingConfiguration populated;

    private static String originMasterConfigStr;

    private static Long WORKFLOW_PID = 0L;

    @Inject
    @InjectMocks
    private WorkflowThrottlingService workflowThrottlingService;

    @Mock
    private EMRCacheService emrCacheService;

    @Mock
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @BeforeClass(groups = "functional")
    public void setupData() {
        MockitoAnnotations.initMocks(this);

        Camille c = CamilleEnvironment.getCamille();

        // setup flags
        Path flagPath1 = PathBuilder.buildWorkflowThrottlingFlagPath(TEST_PODID, TEST_DIV1);
        Path flagPath2 = PathBuilder.buildWorkflowThrottlingFlagPath(TEST_PODID, TEST_DIV2);
        Path workflowFlagPath = PathBuilder.buildSingleWorkflowThrottlingFlagPath(TEST_PODID, TEST_DIV1, TEST_WF_TYPE);
        try {
            if (c.exists(TEST_POD_PATH)) {
                c.delete(TEST_POD_PATH);
            }
            c.create(flagPath1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            c.create(flagPath2, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            c.create(workflowFlagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            c.set(flagPath1, new Document("false"));
            c.set(flagPath2, new Document("true"));
            c.set(workflowFlagPath, new Document("true"));
        } catch (Exception e) {
            log.error("Error encountered while setting up zk data.", e);
        }
        TEST_TENANT_ID = CustomerSpace.parse(tenant.getId()).toString();
        populateFakeWorkflows();
        populateTestConfigToZK();
        String testMasterConfigStr = null;
        try {
            testMasterConfigStr = c.get(PathBuilder.buildWorkflowThrottlingMasterConfigPath()).getData();
        } catch (Exception e) {
            log.error("Error encountered while reading test master config string from zk.", e);
        }
        ReflectionTestUtils.setField(workflowThrottlingService, "masterConfigStr", testMasterConfigStr);
    }

    @AfterClass(groups = "functional")
    public void deleteZKData() {
        Camille c = CamilleEnvironment.getCamille();
        try {
            c.delete(PathBuilder.buildPodPath(TEST_PODID));
            c.delete(PathBuilder.buildWorkflowThrottlingMasterConfigPath());
        } catch (Exception e) {
            log.error("Error encountered while tearing down zk data.", e);
        }
    }

    @Test(groups = "functional")
    public void testGetConfig() {
        WorkflowThrottlingConfiguration retrieved = workflowThrottlingService.getThrottlingConfig(TEST_PODID, TEST_DIV1,
                new HashSet<String>() {
                    {
                        add(TEST_TENANT_ID);
                        add(TENANT_ID_EMPTY_CONFIG);
                    }
                });

        Assert.assertNotNull(retrieved);
        Assert.assertNotNull(retrieved.getGlobalLimit());
        Assert.assertNotNull(retrieved.getTenantLimit());
        Assert.assertNotNull(retrieved.getTenantLimit().get(DEFAULT));
        Assert.assertNotNull(retrieved.getTenantLimit().get(DEFAULT).get(DEFAULT));
        Assert.assertNotNull(retrieved.getTenantLimit().get(DEFAULT).get(TEST_WF_TYPE));
        Assert.assertNotNull(retrieved.getTenantLimit().get(TEST_TENANT_ID));
        Assert.assertNotNull(retrieved.getTenantLimit().get(TEST_TENANT_ID).get(TEST_WF_TYPE));
        Assert.assertNull(retrieved.getTenantLimit().get(TEST_TENANT_ID).get(GENERIC_WF_TYPE));
    }

    @Test(groups = "functional", dependsOnMethods = "testGetConfig", dataProvider = "testGetStatusProvider")
    public void testGetStatus(String podid, String div, List<WorkflowJob> fakedWorkflowJobs) {
        List<WorkflowJob> runningWorkflowJobInCurrentcluster = fakedWorkflowJobs.stream()
                .filter(o -> EMR_CLUSTER_ID.equals(o.getEmrClusterId()))
                .filter(o -> JobStatus.RUNNING.name().equals(o.getStatus())
                        || JobStatus.PENDING.name().equals(o.getStatus()))
                .collect(Collectors.toList());
        List<WorkflowJob> enqueuedWorkflowJobInCurrentStack = fakedWorkflowJobs.stream()
                .filter(o -> JobStatus.ENQUEUED.name().equals(o.getStatus())).filter(o -> div.equals(o.getStack()))
                .collect(Collectors.toList());
        when(emrCacheService.getClusterId()).thenReturn(EMR_CLUSTER_ID);
        when(workflowJobEntityMgr.findByStatusesAndClusterId(emrCacheService.getClusterId(),
                Arrays.asList(JobStatus.RUNNING.name(), JobStatus.PENDING.name())))
                        .thenReturn(runningWorkflowJobInCurrentcluster);
        when(workflowJobEntityMgr.findByStatuses(Collections.singletonList(JobStatus.ENQUEUED.name())))
                .thenReturn(fakedWorkflowJobs.stream().filter(o -> JobStatus.ENQUEUED.name().equals(o.getStatus()))
                        .collect(Collectors.toList()));
        WorkflowThrottlingSystemStatus status = workflowThrottlingService.constructSystemStatus(podid, div);
        Assert.assertNotNull(status);

        assertTotalExistingMatch(status.getRunningWorkflowInEnv(), runningWorkflowJobInCurrentcluster, div);
        assertTotalExistingMatch(status.getEnqueuedWorkflowInEnv(), enqueuedWorkflowJobInCurrentStack, div);

        assertTenantExistingMatch(status.getTenantRunningWorkflow(), runningWorkflowJobInCurrentcluster,
                JobStatus.RUNNING);
        assertTenantExistingMatch(status.getTenantEnqueuedWorkflow(), enqueuedWorkflowJobInCurrentStack,
                JobStatus.ENQUEUED);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetConfig", dataProvider = "testBackPressureProvider")
    public void testQueueLimit(String podid, String div, String customerSpace, String workflowType,
            List<WorkflowJob> fakedWorkflowJobs, boolean expectedBackPressure) {
        when(workflowJobEntityMgr.findByStatuses(any())).thenReturn(fakedWorkflowJobs);

        Assert.assertEquals(workflowThrottlingService.queueLimitReached(customerSpace, workflowType, podid, div),
                expectedBackPressure);
    }

    @Test(groups = "functional", dataProvider = "flagProvider")
    public void testGetFlag(String podid, String division, Boolean expectedFlag) {
        Assert.assertEquals(workflowThrottlingService.isWorkflowThrottlingEnabled(podid, division),
                (boolean) expectedFlag);
    }

    @Test(groups = "functional", dataProvider = "singleWorkflowFlagProvider")
    public void testSingleWorkflowFlag(String podid, String division, String workflowType, boolean rolledout) {
        Assert.assertEquals(workflowThrottlingService.isWorkflowThrottlingRolledOut(podid, division, workflowType),
                rolledout);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetStatus", dataProvider = "throttlingResultProvider")
    public void testGetThrottlingResult(String podid, String div, List<WorkflowJob> fakedWorkflowJobs,
            int submittedCount, int enqueuedCount) {
        when(emrCacheService.getClusterId()).thenReturn(EMR_CLUSTER_ID);
        when(workflowJobEntityMgr.findByStatusesAndClusterId(emrCacheService.getClusterId(),
                Arrays.asList(JobStatus.RUNNING.name(), JobStatus.PENDING.name()))).thenReturn(Collections.emptyList());
        when(workflowJobEntityMgr.findByStatuses(Collections.singletonList(JobStatus.ENQUEUED.name())))
                .thenReturn(fakedWorkflowJobs.stream().filter(o -> JobStatus.ENQUEUED.name().equals(o.getStatus()))
                        .collect(Collectors.toList()));

        ThrottlingResult result = workflowThrottlingService.getThrottlingResult(podid, div);

        // count canSubmit
        Assert.assertEquals(result.getCanSubmit().getOrDefault(TEST_TENANT_ID, Collections.emptyList()).size(),
                submittedCount);
        Assert.assertEquals(result.getStillEnqueued().getOrDefault(TEST_TENANT_ID, Collections.emptyList()).size(),
                enqueuedCount);
    }

    @DataProvider(name = "throttlingResultProvider")
    public Object[][] throttlingResultProvider() {
        // podid, division, faked workflowJobs, expect submittedCount, expect
        // enqueuedCount
        return new Object[][] { { TEST_PODID, TEST_DIV1, FAKE_WORKFLOW_FOR_RESULT1, 4, 0 },
                { TEST_PODID, TEST_DIV2, FAKE_WORKFLOW_FOR_RESULT2, 0, 0 } };
    }

    @DataProvider(name = "flagProvider")
    public Object[][] flagProvider() {
        // podId, divId, expectedFlag
        return new Object[][] { { TEST_PODID, TEST_DIV1, false }, { TEST_PODID, TEST_DIV2, true },
                { TEST_PODID, "unknownDivision", false }, { "unknownPod", TEST_DIV1, false } };
    }

    @DataProvider(name = "singleWorkflowFlagProvider")
    public Object[][] singleWorkflowflagProvider() {
        // podId, divId, expectedFlag
        return new Object[][] { { TEST_PODID, TEST_DIV1, TEST_WF_TYPE, true },
                { TEST_PODID, TEST_DIV1, GENERIC_WF_TYPE, false }, { TEST_PODID, TEST_DIV2, TEST_WF_TYPE, false },
                { TEST_PODID, TEST_DIV2, GENERIC_WF_TYPE, false } };
    }

    @DataProvider(name = "testGetStatusProvider")
    public Object[][] testGetStatusProvider() {
        // podId, divId, fake wfj
        return new Object[][] { { TEST_PODID, TEST_DIV1, FAKE_WF_FOR_STATUS1 },
                { TEST_PODID, TEST_DIV2, FAKE_WF_FOR_STATUS2 } };
    }

    @DataProvider(name = "testBackPressureProvider")
    public Object[][] testBackPressureProvider() {
        // podid, divId, customerSpace workflowConfig, faked wfj, expectBackPressure
        return new Object[][] { { TEST_PODID, TEST_DIV1, TEST_TENANT_ID, GENERIC_WF_TYPE, REACHED_QUEUE_LIMIT, true },
                { TEST_PODID, TEST_DIV2, TEST_TENANT_ID, TEST_WF_TYPE, NOT_REACHED_QUEUE_LIMIT, false } };
    }

    private void populateFakeWorkflows() {
        FAKE_WF_FOR_STATUS1 = new ArrayList<WorkflowJob>() {
            {
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV1, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV1, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
            }
        };

        FAKE_WF_FOR_STATUS2 = new ArrayList<WorkflowJob>() {
            {
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
            }
        };

        REACHED_QUEUE_LIMIT = new ArrayList<WorkflowJob>() {
            {
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
            }
        };

        NOT_REACHED_QUEUE_LIMIT = new ArrayList<WorkflowJob>() {
            {
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
            }
        };

        FAKE_WORKFLOW_FOR_RESULT1 = new ArrayList<WorkflowJob>() {
            {
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
            }
        };
        FAKE_WORKFLOW_FOR_RESULT2 = new ArrayList<WorkflowJob>() {
            {
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, GENERIC_WF_TYPE, null, tenant));
                add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, GENERIC_WF_TYPE, null, tenant));
            }
        };
    }

    private WorkflowJob workflowJobBuilder(JobStatus jobStatus, String division, String type, String emrClusterId,
            Tenant tenant) {
        WorkflowJob wfj = new WorkflowJob();
        wfj.setPid(WORKFLOW_PID++);
        wfj.setStatus(jobStatus.name());
        wfj.setStack(division);
        wfj.setType(type);
        wfj.setEmrClusterId(emrClusterId);
        wfj.setTenant(tenant);
        return wfj;
    }

    private void populateTestConfigToZK() {
        Camille c = CamilleEnvironment.getCamille();

        Map<String, Map<String, Map<JobStatus, Integer>>> masterPropertyFile = new HashMap<String, Map<String, Map<JobStatus, Integer>>>() {
            {
                put(GLOBALCONFIG, new HashMap<String, Map<JobStatus, Integer>>() {
                    {
                        put(DEFAULT, new HashMap<JobStatus, Integer>() {
                            {
                                put(JobStatus.RUNNING, 3);
                                put(JobStatus.ENQUEUED, 5);
                            }
                        });
                        put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {
                            {
                                put(JobStatus.RUNNING, 2);
                                put(JobStatus.ENQUEUED, 10);
                            }
                        });
                    }
                });
                put(TENANTCONFIG, new HashMap<String, Map<JobStatus, Integer>>() {
                    {
                        put(DEFAULT, new HashMap<JobStatus, Integer>() {
                            {
                                put(JobStatus.RUNNING, 2);
                                put(JobStatus.ENQUEUED, 3);
                            }
                        });
                        put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {
                            {
                                put(JobStatus.RUNNING, 1);
                                put(JobStatus.ENQUEUED, 2);
                            }
                        });
                    }
                });
            }
        };
        Map<String, Map<JobStatus, Integer>> tenantOverwrite = new HashMap<String, Map<JobStatus, Integer>>() {
            {
                put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {
                    {
                        put(JobStatus.RUNNING, 20);
                        put(JobStatus.ENQUEUED, 30);
                    }
                });
            }
        };
        try {
            Path masterConfigPath = PathBuilder.buildWorkflowThrottlingMasterConfigPath();
            String masterPropertyFileStr = JsonUtils.serialize(masterPropertyFile);
            if (c.exists(masterConfigPath)) {
                c.delete(masterConfigPath);
            }
            c.create(masterConfigPath, new Document(masterPropertyFileStr), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            String tenantOverwriteStr = JsonUtils.serialize(tenantOverwrite);
            c.create(
                    PathBuilder.buildWorkflowThrottlingTenantConfigPath(TEST_PODID,
                            CustomerSpace.parse(TEST_TENANT_ID)),
                    new Document(tenantOverwriteStr), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            log.error("Failed to create test config in zk", e);
        }
    }

    private void assertTotalExistingMatch(Map<String, Integer> existingWorkflows, List<WorkflowJob> fakeWorkflows,
            String division) {
        Assert.assertEquals((int) existingWorkflows.get(GLOBAL), fakeWorkflows.size());
        Assert.assertEquals((int) existingWorkflows.getOrDefault(TEST_WF_TYPE, 0), fakeWorkflows.stream()
                .filter(o -> TEST_WF_TYPE.equals(o.getType())).collect(Collectors.toList()).size());
    }

    private void assertTenantExistingMatch(Map<String, Map<String, Integer>> existingWorkflows,
            List<WorkflowJob> fakeWorkflows, JobStatus jobStatus) {
        List<JobStatus> desiredStatus = jobStatus.equals(JobStatus.RUNNING)
                ? Arrays.asList(JobStatus.RUNNING, JobStatus.PENDING)
                : Collections.singletonList(JobStatus.ENQUEUED);
        Assert.assertEquals(
                (int) existingWorkflows.get(
                        tenant.getId()).get(
                                GLOBAL),
                fakeWorkflows.stream().filter(wfj -> wfj.getTenant().getId().equals(tenant.getId())
                        && desiredStatus.contains(JobStatus.fromString(wfj.getStatus()))).count());
        Assert.assertEquals((int) existingWorkflows.get(tenant.getId()).getOrDefault(TEST_WF_TYPE, 0),
                fakeWorkflows.stream()
                        .filter(wfj -> wfj.getTenant().getId().equals(tenant.getId())
                                && desiredStatus.contains(JobStatus.fromString(wfj.getStatus()))
                                && wfj.getType().equals(TEST_WF_TYPE))
                        .count());
    }
}
