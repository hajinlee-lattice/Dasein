package com.latticeengines.workflowapi.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
            addCurrentSystemState(status, workflowJobs, division);
            addCanRunState(status, workflowJobs, podid, division);
        } finally {
            MultiTenantContext.setTenant(originTenant);
        }

        return status;
    }

    private void addCurrentSystemState(WorkflowThrottlingSystemStatus status, List<WorkflowJob> workflowJobs, String division) {
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


        for (WorkflowJob workflow : workflowJobs) {
            String customerSpace = CustomerSpace.parse(workflow.getTenant().getId()).toString();

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
    }

    private void addCanRunState(WorkflowThrottlingSystemStatus status, List<WorkflowJob> workflowJobs, String podid, String division) {
        Set<String> tenantIds = workflowJobs.stream().map(wfj -> wfj.getTenant().getId()).collect(Collectors.toSet());
        WorkflowThrottlerConfiguration config = getThrottlingConfig(podid, division, tenantIds);
        if (config == null) {
            throw new IllegalArgumentException("Failed to get throttling configuration from ZK.");
        }


        status.setCanRunWorkflowInEnv(getAllowedWorkflowCount(config.getEnvConfig(), JobStatus.RUNNING, status.getRunningWorkflowInEnv(), GLOBAL_KEY));
        status.setCanEnqueueWorkflowInEnv(getAllowedWorkflowCount(config.getEnvConfig(), JobStatus.ENQUEUED, status.getEnqueuedWorkflowInEnv(), GLOBAL_KEY));

        status.setCanRunWorkflowInStack(getAllowedWorkflowCount(config.getStackConfig(), JobStatus.RUNNING, status.getRunningWorkflowInStack(), GLOBAL_KEY));
        status.setCanRunWorkflowInStackSpecific(getAllowedWorkflowCount(config.getStackConfig(), JobStatus.RUNNING, status.getRunningWorkflowInStack(), division));
        status.setCanEnqueueWorkflowInStack(getAllowedWorkflowCount(config.getStackConfig(), JobStatus.ENQUEUED, status.getEnqueuedWorkflowInStack(), GLOBAL_KEY));
        status.setCanEnqueueWorkflowInStackSpecific(getAllowedWorkflowCount(config.getStackConfig(), JobStatus.ENQUEUED, status.getEnqueuedWorkflowInStack(), division));

        status.setTenantCanRunWorkflow(getTenantAllowedWorkflowCount(status.getTenantRunningWorkflow(), config.getTenantConfig(), JobStatus.RUNNING));
        status.setTenantCanEnqueueWorkflow(getTenantAllowedWorkflowCount(status.getTenantEnqueuedWorkflow(), config.getTenantConfig(), JobStatus.ENQUEUED));
    }

    private Map<String, Integer> getAllowedWorkflowCount(Map<String, Map<String, Map<JobStatus, Integer>>> config, JobStatus quotaType, Map<String, Integer> existingWorkflows, String configId) {
        Map<String, Integer> allowedWorkflowCount = new HashMap<>();
        Map<String, Map<JobStatus, Integer>> workflowMap = config.get(configId);

        if (workflowMap != null) {
            workflowMap.forEach((workflowType, jobStatusMap) -> {
                int count = existingWorkflows.getOrDefault(workflowType, 0);
                Integer quota = jobStatusMap.get(quotaType);
                if (quota != null) {
                    allowedWorkflowCount.put(workflowType, quota - count);
                }
            });
        }
        return allowedWorkflowCount;
    }

    private Map<String, Map<String, Integer>> getTenantAllowedWorkflowCount(Map<String, Map<String, Integer>> existingWorkflows, Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfig, JobStatus quotaType) {
        Map<String, Map<String, Integer>> allowedWorkflowCount = new HashMap<>();
        tenantConfig.forEach((tenantId, workflowConfigMap) -> {
            if (!tenantId.equals(GLOBAL_KEY)) {
                allowedWorkflowCount.put(tenantId, new HashMap<>());
                workflowConfigMap.forEach((workflowType, jobStatusConfigMap) -> {
                    int count = 0;
                    if (existingWorkflows.get(tenantId) != null) {
                        count = existingWorkflows.get(tenantId).getOrDefault(workflowType, 0);
                    }
                    Integer quota = jobStatusConfigMap.get(quotaType);
                    if (quota != null) {
                        allowedWorkflowCount.get(tenantId).put(workflowType, quota - count);
                    }
                });
            }
        });
        return allowedWorkflowCount;
    }

    private void addWorkflowToMap(Map<String, Integer> map, WorkflowJob workflow) {
        // update record for global and specific type
        String type = workflow.getType();
        map.put(GLOBAL_KEY, map.getOrDefault(GLOBAL_KEY, 0) + 1);
        map.put(type, map.getOrDefault(type, 0) + 1);
    }
}
