package com.latticeengines.workflowapi.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.zookeeper.ZooDefs;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.service.WorkflowThrottlingService;

public class WorkflowThrottlingServiceImplTestNG extends WorkflowApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(WorkflowThrottlingServiceImplTestNG.class);

    private static String TEST_PODID = "ThrottlingTestPod";
    private static final Path TEST_POD_PATH = PathBuilder.buildPodPath(TEST_PODID);

    private static final String TEST_DIV1 = "throttlingServiceTestDivisionName1";
    private static final String TEST_DIV2 = "throttlingServiceTestDivisionName2";
    private static final String TEST_WF_TYPE = "TestWorkflowType";
    private static final String GENERIC_WF_TYPE = "genericWorkflowType";
    private static final String EMR_CLUSTER_ID = "fakedId";

    private static List<WorkflowJob> FAKE_WORKFLOW_DIV1;
    private static List<WorkflowJob> FAKE_WORKFLOW_DIV2;

    private static List<WorkflowJob> FAKE_WORKFLOW_FOR_BP;

    private static String TEST_TENANT_ID;

    private static final String GLOBAL = "global";

    private static WorkflowThrottlingConfiguration populated;

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
        try {
            if (c.exists(TEST_POD_PATH)) {
                c.delete(TEST_POD_PATH);
            }
            c.create(flagPath1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            c.create(flagPath2, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            c.set(flagPath1, new Document("false"));
            c.set(flagPath2, new Document("true"));
        } catch (Exception e) {
            log.error("Error encountered while setting up zk data.", e);
        }
        TEST_TENANT_ID = CustomerSpace.parse(tenant.getId()).toString();
        populateFakeWorkflows();
    }

    @AfterClass(groups = "functional")
    public void deleteZKData() {
        Camille c = CamilleEnvironment.getCamille();
        try {
            c.delete(PathBuilder.buildPodPath(TEST_PODID));
        } catch (Exception e) {
            log.error("Error encountered while tearing down zk data.", e);
        }
    }

    @Test(groups = "functional")
    public void testGetConfig() {
        populated = populateTestConfig();
        WorkflowThrottlingConfiguration retrieved = workflowThrottlingService.getThrottlingConfig(TEST_PODID, TEST_DIV1, new HashSet<String>() {{
            add(TEST_TENANT_ID);
        }});

        assertConfigMatch(populated, retrieved);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetConfig", dataProvider = "testGetStatusProvider")
    public void testGetStatus(String podid, String div, List<WorkflowJob> fakedWorkflowJobs) {
        when(workflowJobEntityMgr.findByStatusesAndClusterId(any(), any())).thenReturn(fakedWorkflowJobs);
        when(emrCacheService.getClusterId()).thenReturn(EMR_CLUSTER_ID);
        WorkflowThrottlingSystemStatus status = workflowThrottlingService.constructSystemStatus(podid, div);
        Assert.assertNotNull(status);

        assertTotalExistingMatch(status, fakedWorkflowJobs, div, JobStatus.RUNNING);
        assertTotalExistingMatch(status, fakedWorkflowJobs, div, JobStatus.ENQUEUED);

        assertTenantExistingMatch(status.getTenantRunningWorkflow(), fakedWorkflowJobs, JobStatus.RUNNING);
        assertTenantExistingMatch(status.getTenantEnqueuedWorkflow(), fakedWorkflowJobs, JobStatus.ENQUEUED);
    }

    @Test(groups = "functional", dependsOnMethods = "testGetConfig", dataProvider = "testBackPressureProvider")
    public void testQueueLimit(String podid, String div, String customerSpace, String workflowType, List<WorkflowJob> fakedWorkflowJobs, boolean expectedBackPressure) {
        when(workflowJobEntityMgr.findByStatuses(any())).thenReturn(fakedWorkflowJobs);

        Assert.assertEquals(workflowThrottlingService.queueLimitReached(customerSpace, workflowType, podid, div), expectedBackPressure);
    }

    @Test(groups = "functional", dataProvider = "flagProvider")
    public void testGetFlag(String podid, String division, Boolean expectedFlag) {
        Assert.assertEquals(workflowThrottlingService.isWorkflowThrottlingEnabled(podid, division), (boolean) expectedFlag);
    }

    @DataProvider(name = "flagProvider")
    public Object[][] flagProvider() {
        // podId, divId, expectedFlag
        return new Object[][]{
                {TEST_PODID, TEST_DIV1, false},
                {TEST_PODID, TEST_DIV2, true},
                {TEST_PODID, "unknownDivision", false},
                {"unknownPod", TEST_DIV1, false}
        };
    }

    @DataProvider(name = "testGetStatusProvider")
    public Object[][] testGetStatusProvider() {
        // podId, divId, fake wfj
        return new Object[][]{
                {TEST_PODID, TEST_DIV1, FAKE_WORKFLOW_DIV1},
                {TEST_PODID, TEST_DIV2, FAKE_WORKFLOW_DIV2}
        };
    }

    @DataProvider(name = "testBackPressureProvider")
    public Object[][] testBackPressureProvider() {
        // podid, divId, customerSpace workflowConfig, faked wfj, expectBackPressure
        return new Object[][]{
                {TEST_PODID, TEST_DIV1, TEST_TENANT_ID, TEST_WF_TYPE, FAKE_WORKFLOW_FOR_BP, false},
                {TEST_PODID, TEST_DIV2, TEST_TENANT_ID, GENERIC_WF_TYPE, FAKE_WORKFLOW_FOR_BP, true}
        };
    }

    private void populateFakeWorkflows() {
        FAKE_WORKFLOW_DIV1 = new ArrayList<WorkflowJob>() {{
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV1, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV1, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, TEST_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
        }};

        FAKE_WORKFLOW_DIV2 = new ArrayList<WorkflowJob>() {{
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.RUNNING, TEST_DIV2, GENERIC_WF_TYPE, EMR_CLUSTER_ID, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
        }};

        FAKE_WORKFLOW_FOR_BP = new ArrayList<WorkflowJob>() {{
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV1, TEST_WF_TYPE, null, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, TEST_WF_TYPE, null, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, TEST_WF_TYPE, null, tenant));
            add(workflowJobBuilder(JobStatus.ENQUEUED, TEST_DIV2, GENERIC_WF_TYPE, null, tenant));
        }};
    }

    private WorkflowJob workflowJobBuilder(JobStatus jobStatus, String division, String type, String emrClusterId, Tenant tenant) {
        WorkflowJob wfj = new WorkflowJob();
        wfj.setStatus(jobStatus.name());
        wfj.setStack(division);
        wfj.setType(type);
        wfj.setEmrClusterId(emrClusterId);
        wfj.setTenant(tenant);
        return wfj;
    }

    private WorkflowThrottlingConfiguration populateTestConfig() {
        WorkflowThrottlingConfiguration config = new WorkflowThrottlingConfiguration();

        config.setEnvConfig(new HashMap<String, Map<JobStatus, Integer>>() {{
            put(GLOBAL, new HashMap<JobStatus, Integer>() {{
                put(JobStatus.RUNNING, 6);
                put(JobStatus.ENQUEUED, 10);
            }});
            put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {{
                put(JobStatus.RUNNING, 1);
                put(JobStatus.ENQUEUED, 7);
            }});
        }});

        config.setStackConfig(new HashMap<String, Map<String, Map<JobStatus, Integer>>>() {{
            put(TEST_DIV1, new HashMap<String, Map<JobStatus, Integer>>() {{
                put(GLOBAL, new HashMap<JobStatus, Integer>() {{
                    put(JobStatus.RUNNING, 5);
                    put(JobStatus.ENQUEUED, 6);
                }});
                put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {{
                    put(JobStatus.RUNNING, 1);
                    put(JobStatus.ENQUEUED, 3);
                }});
            }});
            put(TEST_DIV2, new HashMap<String, Map<JobStatus, Integer>>() {{
                put(GLOBAL, new HashMap<JobStatus, Integer>() {{
                    put(JobStatus.RUNNING, 2);
                    put(JobStatus.ENQUEUED, 2);
                }});
                put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {{
                    put(JobStatus.RUNNING, 5);
                    put(JobStatus.ENQUEUED, 4);
                }});
            }});
        }});

        config.setTenantConfig(new HashMap<String, Map<String, Map<JobStatus, Integer>>>() {{
            put(GLOBAL, new HashMap<String, Map<JobStatus, Integer>>() {{
                put(GLOBAL, new HashMap<JobStatus, Integer>() {{
                    put(JobStatus.RUNNING, 2);
                    put(JobStatus.ENQUEUED, 1);
                }});
                put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {{
                    put(JobStatus.RUNNING, 1);
                    put(JobStatus.ENQUEUED, 2);
                }});
            }});
            put(TEST_TENANT_ID, new HashMap<String, Map<JobStatus, Integer>>() {{
                put(TEST_WF_TYPE, new HashMap<JobStatus, Integer>() {{
                    put(JobStatus.RUNNING, 10);
                    put(JobStatus.ENQUEUED, 20);
                }});
            }});
        }});
        saveConfigToZK(config);

        return config;
    }

    private void saveConfigToZK(WorkflowThrottlingConfiguration config) {
        Camille c = CamilleEnvironment.getCamille();
        try {
            String envConfigStr = JsonUtils.serialize(config.getEnvConfig());
            c.create(PathBuilder.buildWorkflowThrottlerPodsConfigPath(TEST_PODID), new Document(envConfigStr), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            String divConfigStr = JsonUtils.serialize(config.getStackConfig());
            c.create(PathBuilder.buildWorkflowThrottlerDivisionConfigPath(TEST_PODID), new Document(divConfigStr), ZooDefs.Ids.OPEN_ACL_UNSAFE);

            Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfig = config.getTenantConfig();
            tenantConfig.forEach((tenantId, workflowMap) -> {
                try {
                    if (tenantId.equals(GLOBAL)) {
                        String globalTenantConfigStr = JsonUtils.serialize(tenantConfig.get(GLOBAL));
                        c.create(PathBuilder.buildWorkflowThrottlerGlobalTenantConfigPath(TEST_PODID), new Document(globalTenantConfigStr), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                    } else {
                        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
                        String workflowMapStr = JsonUtils.serialize(tenantConfig.get(tenantId));
                        c.create(PathBuilder.buildWorkflowThrottlerTenantConfigPath(TEST_PODID, customerSpace), new Document(workflowMapStr), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                    }
                } catch (Exception e) {
                    log.error("Error populating tenant config", e);
                }
            });
        } catch (Exception e) {
            log.error("Error populating config.", e);
        }
    }

    private void assertConfigMatch(WorkflowThrottlingConfiguration populated, WorkflowThrottlingConfiguration retrieved) {
        assertWorkflowMapMatch(populated.getEnvConfig(), retrieved.getEnvConfig());
        assertConfigMapMatch(populated.getStackConfig(), retrieved.getStackConfig());
        assertConfigMapMatch(populated.getTenantConfig(), retrieved.getTenantConfig());
    }

    private void assertConfigMapMatch(Map<String, Map<String, Map<JobStatus, Integer>>> c1, Map<String, Map<String, Map<JobStatus, Integer>>> c2) {
        c1.forEach((podId, populatedWorkflowMap) -> {
            assertWorkflowMapMatch(populatedWorkflowMap, c2.getOrDefault(podId, null));
        });
    }

    private void assertWorkflowMapMatch(Map<String, Map<JobStatus, Integer>> m1, Map<String, Map<JobStatus, Integer>> m2) {
        Assert.assertNotNull(m2);
        Assert.assertEquals(m1.keySet(), m2.keySet());
    }

    private void assertTotalExistingMatch(WorkflowThrottlingSystemStatus status, List<WorkflowJob> fakeWorkflows, String division, JobStatus jobStatus) {
        Assert.assertEquals(jobStatus.equals(JobStatus.RUNNING) ? status.getTotalRunningWorkflowInEnv() : status.getTotalEnqueuedWorkflowInEnv(),
                fakeWorkflows.stream().filter(workflowJob -> workflowJob.getStatus().equals(jobStatus.name())).count());
        Assert.assertEquals(jobStatus.equals(JobStatus.RUNNING) ? status.getTotalRunningWorkflowInStack() : status.getTotalEnqueuedWorkflowInStack(),
                fakeWorkflows.stream().filter(workflowJob -> workflowJob.getStatus().equals(jobStatus.name()) && workflowJob.getStack().equals(division)).count());
        Map<String, Integer> map = jobStatus.equals(JobStatus.RUNNING) ? status.getRunningWorkflowInEnv() : status.getEnqueuedWorkflowInEnv();
        Assert.assertEquals((int) map.getOrDefault(TEST_WF_TYPE, 0),
                fakeWorkflows.stream().filter(wf -> wf.getStatus().equals(jobStatus.name()) && wf.getType().equals(TEST_WF_TYPE)).count());
        map = jobStatus.equals(JobStatus.RUNNING) ? status.getRunningWorkflowInStack() : status.getEnqueuedWorkflowInStack();
        Assert.assertEquals((int) map.getOrDefault(TEST_WF_TYPE, 0),
                fakeWorkflows.stream().filter(wf -> wf.getStatus().equals(jobStatus.name()) && wf.getType().equals(TEST_WF_TYPE) && wf.getStack().equals(division)).count());
    }

    private void assertTenantExistingMatch(Map<String, Map<String, Integer>> existingWorkflows, List<WorkflowJob> fakeWorkflows, JobStatus jobStatus) {
        Assert.assertEquals((int) existingWorkflows.get(tenant.getId()).get(GLOBAL),
                fakeWorkflows.stream().filter(wfj -> wfj.getTenant().getId().equals(tenant.getId()) && wfj.getStatus().equals(jobStatus.name())).count());
        Assert.assertEquals((int) existingWorkflows.get(tenant.getId()).getOrDefault(TEST_WF_TYPE, 0),
                fakeWorkflows.stream().filter(wfj -> wfj.getTenant().getId().equals(tenant.getId()) && wfj.getStatus().equals(jobStatus.name()) && wfj.getType().equals(TEST_WF_TYPE)).count());
    }
}
