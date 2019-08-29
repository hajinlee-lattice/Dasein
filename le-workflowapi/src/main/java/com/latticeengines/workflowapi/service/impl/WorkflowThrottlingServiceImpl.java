package com.latticeengines.workflowapi.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlerConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflowapi.service.WorkflowThrottlingService;

@Component("workflowThrottlerService")
public class WorkflowThrottlingServiceImpl implements WorkflowThrottlingService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowThrottlingServiceImpl.class);

    private static final String GLOBAL_KEY = "global";

    private static TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>> configTypeRef = new TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>>() {
    };
    private static TypeReference<Map<String, Map<JobStatus, Integer>>> tenantConfigTypeRef = new TypeReference<Map<String, Map<JobStatus, Integer>>>() {
    };

    private volatile WorkflowThrottlerConfiguration config;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private EMRCacheService emrCacheService;

    @Override
    public WorkflowThrottlerConfiguration getThrottlingConfig(String podid, String division, Set<String> customerSpaces) {
        Camille c = CamilleEnvironment.getCamille();
        config = new WorkflowThrottlerConfiguration();
        try {
            config.setEnvConfig(JsonUtils.deserializeByTypeRef(c.get(PathBuilder.buildWorkflowThrottlerPodsConfigPath(podid)).getData(), configTypeRef));
        } catch (Exception e) {
            log.error("Unable to retrieve throttler environment {} configuration from zk.", podid, e);
            return config = null;
        }
        try {
            config.setStackConfig(JsonUtils.deserializeByTypeRef(c.get(PathBuilder.buildWorkflowThrottlerDivisionConfigPath(podid)).getData(), configTypeRef));
        } catch (Exception e) {
            log.warn("Unable to retrieve throttler division configuration from zk.", e);
            return config = null;
        }
        try {
            Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfigMap = new HashMap<String, Map<String, Map<JobStatus, Integer>>>() {{
                put(GLOBAL_KEY, JsonUtils.deserializeByTypeRef(c.get(PathBuilder.buildWorkflowThrottlerGlobalTenantConfigPath(podid)).getData(), tenantConfigTypeRef));
            }};
            config.setTenantConfig(tenantConfigMap);
            for (String cs : customerSpaces) {
                CustomerSpace customerSpace = CustomerSpace.parse(cs);
                try {
                    tenantConfigMap.put(customerSpace.toString(), JsonUtils.deserializeByTypeRef(c.get(PathBuilder.buildWorkflowThrottlerTenantConfigPath(podid, customerSpace)).getData(), tenantConfigTypeRef));
                } catch (Exception e) {
                    log.warn("Unable to find config for tenant {}. Either config not set, or unable to read from zk.", customerSpace);
                }
            }
        } catch (Exception e) {
            log.warn("Unable to retrieve throttler global tenant configuration from zk.", e);
            return config = null;
        }
        return config;
    }

    @Override
    public boolean isWorkflowThrottlingEnabled(String podid, String division) {
        // In current impl, the flag is per stack
        Camille c = CamilleEnvironment.getCamille();
        try {
            return Boolean.TRUE.equals(JsonUtils.convertValue(c.get(PathBuilder.buildWorkflowThrottlingFlagPath(podid, division)).getData(), Boolean.class));
        } catch (Exception e) {
            log.warn("Unable to retrieve division {} - {} throttling flag from zk.", podid, division, e);
            return false;
        }
    }

    @Override
    public WorkflowThrottlingSystemStatus constructSystemStatus(String podid, String division) {
        Tenant originTenant = MultiTenantContext.getTenant();
        WorkflowThrottlingSystemStatus status = new WorkflowThrottlingSystemStatus();

        try {
            MultiTenantContext.clearTenant();
            List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByStatusesAndClusterId(emrCacheService.getClusterId(), Arrays.asList(JobStatus.RUNNING.name(), JobStatus.ENQUEUED.name(), JobStatus.PENDING.name()));
            addCurrentSystemState(status, workflowJobs, podid, division);
        } finally {
            MultiTenantContext.setTenant(originTenant);
        }

        return status;
    }

    private void addCurrentSystemState(WorkflowThrottlingSystemStatus status, List<WorkflowJob> workflowJobs, String podid, String division) {
        Map<String, Integer> runningWorkflowInEnv = new HashMap<String, Integer>() {{
            put(GLOBAL_KEY, 0);
        }};
        Map<String, Integer> runningWorkflowInStack = new HashMap<String, Integer>() {{
            put(GLOBAL_KEY, 0);
        }};
        Map<String, Integer> enqueuedWorkflowInEnv = new HashMap<String, Integer>() {{
            put(GLOBAL_KEY, 0);
        }};
        Map<String, Integer> enqueuedWorkflowInStack = new HashMap<String, Integer>() {{
            put(GLOBAL_KEY, 0);
        }};
        Map<String, Map<String, Integer>> tenantRunningWorkflow = new HashMap<>();
        Map<String, Map<String, Integer>> tenantEnqueuedWorkflow = new HashMap<>();
        Set<String> tenantIds = new HashSet<>();


        for (WorkflowJob workflow : workflowJobs) {
            String customerSpace = CustomerSpace.parse(workflow.getTenant().getId()).toString();
            tenantIds.add(customerSpace);

            if (workflow.getStatus().equals(JobStatus.RUNNING.name())) {
                tenantRunningWorkflow.putIfAbsent(customerSpace, new HashMap<>());
                addWorkflowToMap(tenantRunningWorkflow.get(customerSpace), workflow);

                addWorkflowToMap(runningWorkflowInEnv, workflow);

                if (division.equals(workflow.getStack())) {
                    addWorkflowToMap(runningWorkflowInStack, workflow);
                }
            } else if (workflow.getStatus().equals(JobStatus.ENQUEUED.name())) {
                tenantEnqueuedWorkflow.putIfAbsent(customerSpace, new HashMap<>());
                addWorkflowToMap(tenantEnqueuedWorkflow.get(customerSpace), workflow);

                addWorkflowToMap(enqueuedWorkflowInEnv, workflow);

                if (division.equals(workflow.getStack())) {
                    addWorkflowToMap(enqueuedWorkflowInStack, workflow);
                }
            }
        }
        status.setRunningWorkflowInEnv(runningWorkflowInEnv);
        status.setRunningWorkflowInStack(runningWorkflowInStack);
        status.setEnqueuedWorkflowInEnv(enqueuedWorkflowInEnv);
        status.setEnqueuedWorkflowInStack(enqueuedWorkflowInStack);
        status.setTenantRunningWorkflow(tenantRunningWorkflow);
        status.setTenantEnqueuedWorkflow(tenantEnqueuedWorkflow);
        status.setConfig(getThrottlingConfig(podid, division, tenantIds));
    }

    private void addWorkflowToMap(Map<String, Integer> map, WorkflowJob workflow) {
        // update record for global and specific type
        String type = workflow.getType();
        map.put(GLOBAL_KEY, map.getOrDefault(GLOBAL_KEY, 0) + 1);
        map.put(type, map.getOrDefault(type, 0) + 1);
    }
}
