package com.latticeengines.workflowapi.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import com.latticeengines.domain.exposed.cdl.workflowThrottling.GreedyWorkflowScheduler;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.ThrottlingResult;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowJobSchedulingObject;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowScheduler;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.NotExceedingEnvQuota;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.NotExceedingStackQuota;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.NotExceedingTenantQuota;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.WorkflowThrottlingConstraint;
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

    private static final String GLOBAL = "global";

    private static TypeReference<Map<String, Map<JobStatus, Integer>>> envConfigTypeRef = new TypeReference<Map<String, Map<JobStatus, Integer>>>() {
    };
    private static TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>> stackConfigTypeRef = new TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>>() {
    };
    private static TypeReference<Map<String, Map<JobStatus, Integer>>> tenantConfigTypeRef = new TypeReference<Map<String, Map<JobStatus, Integer>>>() {
    };

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private EMRCacheService emrCacheService;

    @Override
    public WorkflowThrottlingConfiguration getThrottlingConfig(String podid, String division,
            Set<String> customerSpaces) {
        Camille c = CamilleEnvironment.getCamille();
        WorkflowThrottlingConfiguration config = new WorkflowThrottlingConfiguration();
        try {
            config.setEnvConfig(JsonUtils.deserializeByTypeRef(
                    c.get(PathBuilder.buildWorkflowThrottlerPodsConfigPath(podid)).getData(), envConfigTypeRef));
        } catch (Exception e) {
            log.error("Unable to retrieve throttler environment {} configuration from zk.", podid, e);
            return null;
        }
        try {
            config.setStackConfig(JsonUtils.deserializeByTypeRef(
                    c.get(PathBuilder.buildWorkflowThrottlerDivisionConfigPath(podid)).getData(), stackConfigTypeRef));
        } catch (Exception e) {
            log.warn("Unable to retrieve throttler division configuration from zk.", e);
            return null;
        }
        try {
            Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfigMap = new HashMap<String, Map<String, Map<JobStatus, Integer>>>() {
                {
                    put(GLOBAL,
                            JsonUtils.deserializeByTypeRef(
                                    c.get(PathBuilder.buildWorkflowThrottlerGlobalTenantConfigPath(podid)).getData(),
                                    tenantConfigTypeRef));
                }
            };
            config.setTenantConfig(tenantConfigMap);
            for (String cs : customerSpaces) {
                CustomerSpace customerSpace = CustomerSpace.parse(cs);
                try {
                    tenantConfigMap.put(customerSpace.toString(), JsonUtils.deserializeByTypeRef(
                            c.get(PathBuilder.buildWorkflowThrottlerTenantConfigPath(podid, customerSpace)).getData(),
                            tenantConfigTypeRef));
                } catch (Exception e) {
                    log.warn("Unable to find config for tenant {}. Either config not set, or unable to read from zk.",
                            customerSpace);
                }
            }
        } catch (Exception e) {
            log.warn("Unable to retrieve throttler global tenant configuration from zk.", e);
            return null;
        }
        return config;
    }

    @Override
    public boolean isWorkflowThrottlingEnabled(String podid, String division) {
        // In current impl, the flag is per stack
        Camille c = CamilleEnvironment.getCamille();
        try {
            return Boolean.TRUE.equals(JsonUtils.convertValue(
                    c.get(PathBuilder.buildWorkflowThrottlingFlagPath(podid, division)).getData(), Boolean.class));
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
            List<WorkflowJob> workflowJobs = new ArrayList<>(workflowJobEntityMgr.findByStatusesAndClusterId(
                    emrCacheService.getClusterId(), Collections.singletonList(JobStatus.RUNNING.name())));
            workflowJobs.addAll(workflowJobEntityMgr
                    .findByStatuses(Arrays.asList(JobStatus.ENQUEUED.name(), JobStatus.PENDING.name())));
            addCurrentSystemState(status, workflowJobs, podid, division);
        } finally {
            MultiTenantContext.setTenant(originTenant);
        }

        return status;
    }

    @Override
    public ThrottlingResult getThrottlingResult(String podid, String division) {
        // drain workflowJobs enqueued in db
        try {
            WorkflowThrottlingSystemStatus status = constructSystemStatus(podid, division);
            List<WorkflowThrottlingConstraint> constraintList = Arrays.asList(new NotExceedingEnvQuota(),
                    new NotExceedingStackQuota(), new NotExceedingTenantQuota());
            List<WorkflowJobSchedulingObject> enqueuedWorkflowSchedulingObjects = status.getEnqueuedWorkflowJobs()
                    .stream().map(o -> new WorkflowJobSchedulingObject(o, constraintList)).collect(Collectors.toList());
            WorkflowScheduler scheduler = new GreedyWorkflowScheduler();
            return scheduler.schedule(status, enqueuedWorkflowSchedulingObjects, podid, division);
        } catch (Exception e) {
            log.error("Failed to get workflow throttling result.", e);
            throw e;
        }
    }

    @Override
    public boolean queueLimitReached(String customerSpace, String workflowType, String podid, String division) {
        WorkflowThrottlingConfiguration config = getThrottlingConfig(podid, division,
                Collections.singleton(customerSpace));
        Tenant originTenant = MultiTenantContext.getTenant();
        try {
            MultiTenantContext.clearTenant();
            if (config == null) {
                throw new IllegalArgumentException(String.format(
                        "Unable to retrieve throttling configuration while checking back pressure for tenant %s in %s",
                        customerSpace, division));
            }
            List<WorkflowJob> enqueuedWorkflowJobs = workflowJobEntityMgr
                    .findByStatuses(Collections.singletonList(JobStatus.ENQUEUED.name()));
            List<WorkflowJob> divisionEnqueuedWorkflowJobs = enqueuedWorkflowJobs.stream()
                    .filter(o -> division.equals(o.getStack())).collect(Collectors.toList());
            List<WorkflowJob> tenantEnqueuedWorkflowJobs = enqueuedWorkflowJobs.stream()
                    .filter(o -> customerSpace.equals(o.getTenant().getId())).collect(Collectors.toList());

            if (clusterQueueLimitReached(config.getEnvConfig(), enqueuedWorkflowJobs, workflowType)) {
                log.error("Back pressure detected. From: {}, Id: {}, workflowType: {}", "Environment", GLOBAL,
                        workflowType);
                return true;
            } else if (clusterQueueLimitReached(config.getStackConfig().get(division), divisionEnqueuedWorkflowJobs,
                    workflowType)) {
                log.error("Back pressure detected. From: {}, Id: {}, workflowType: {}", "Stack", division,
                        workflowType);
                return true;
            } else if (tenantQueueLimitReached(config.getTenantConfig(), tenantEnqueuedWorkflowJobs, workflowType,
                    customerSpace)) {
                log.error("Back pressure detected. From: {}, Id: {}, workflowType: {}", "Tenant", customerSpace,
                        workflowType);
                return true;
            }
            return false;
        } finally {
            MultiTenantContext.setTenant(originTenant);
        }
    }

    private boolean tenantQueueLimitReached(Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfig,
            List<WorkflowJob> enqueuedWorkflowJobs, String workflowType, String customerSpace) {
        // construct tenant specific config
        Map<String, Map<JobStatus, Integer>> workflowMap = new HashMap<>(tenantConfig.get(GLOBAL));
        if (tenantConfig.get(customerSpace) != null) {
            workflowMap.putAll(tenantConfig.get(customerSpace));
        }

        // if tenant overwrites quota for this workflow type, skip checking global quota
        Integer quota = workflowMap.get(workflowType) == null ? null
                : workflowMap.get(workflowType).get(JobStatus.ENQUEUED);
        if (quota != null
                && enqueuedWorkflowJobs.stream().filter(o -> workflowType.equals(o.getType())).count() >= quota) {
            return true;
        }

        quota = workflowMap.get(GLOBAL).get(JobStatus.ENQUEUED); // global workflow entry for tenant must exist in
        // constructed map
        return enqueuedWorkflowJobs.size() >= quota;
    }

    private boolean clusterQueueLimitReached(Map<String, Map<JobStatus, Integer>> workflowMap,
            List<WorkflowJob> enqueuedWorkflowJobs, String workflowType) {
        Integer quota;

        if (workflowMap == null) {
            return false;
        }

        quota = workflowMap.get(GLOBAL).get(JobStatus.ENQUEUED);
        if (enqueuedWorkflowJobs.size() >= quota) {
            return true;
        }

        quota = workflowMap.get(workflowType) == null ? null : workflowMap.get(workflowType).get(JobStatus.ENQUEUED);
        if (quota != null
                && enqueuedWorkflowJobs.stream().filter(o -> workflowType.equals(o.getType())).count() >= quota) {
            return true;
        }

        return false;
    }

    private void addCurrentSystemState(WorkflowThrottlingSystemStatus status, List<WorkflowJob> workflowJobs,
            String podid, String division) {
        Map<String, Integer> runningWorkflowInEnv = new HashMap<String, Integer>() {
            {
                put(GLOBAL, 0);
            }
        };
        Map<String, Integer> runningWorkflowInStack = new HashMap<String, Integer>() {
            {
                put(GLOBAL, 0);
            }
        };
        Map<String, Integer> enqueuedWorkflowInEnv = new HashMap<String, Integer>() {
            {
                put(GLOBAL, 0);
            }
        };
        Map<String, Integer> enqueuedWorkflowInStack = new HashMap<String, Integer>() {
            {
                put(GLOBAL, 0);
            }
        };
        Map<String, Map<String, Integer>> tenantRunningWorkflow = new HashMap<>();
        Map<String, Map<String, Integer>> tenantEnqueuedWorkflow = new HashMap<>();
        Set<String> tenantIds = new HashSet<>();
        List<WorkflowJob> enqueuedWorkflowJobs = new ArrayList<>();

        for (WorkflowJob workflow : workflowJobs) {
            String customerSpace = CustomerSpace.parse(workflow.getTenant().getId()).toString();
            tenantIds.add(customerSpace);

            if (workflow.getStatus().equals(JobStatus.RUNNING.name())
                    || workflow.getStatus().equals(JobStatus.PENDING.name())) {
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
                    enqueuedWorkflowJobs.add(workflow);
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
        status.setEnqueuedWorkflowJobs(enqueuedWorkflowJobs);
    }

    private void addWorkflowToMap(Map<String, Integer> map, WorkflowJob workflow) {
        // update record for global and specific type
        String type = workflow.getType();
        map.put(GLOBAL, map.getOrDefault(GLOBAL, 0) + 1);
        map.put(type, map.getOrDefault(type, 0) + 1);
    }
}
