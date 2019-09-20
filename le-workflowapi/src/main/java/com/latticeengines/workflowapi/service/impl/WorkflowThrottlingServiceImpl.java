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

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.GreedyWorkflowScheduler;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.ThrottlingResult;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowJobSchedulingObject;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowScheduler;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.IsForCurrentStack;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.NotExceedingEnvQuota;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.NotExceedingTenantQuota;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.WorkflowThrottlingConstraint;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflowapi.service.WorkflowThrottlingService;

@Component("workflowThrottlerService")
public class WorkflowThrottlingServiceImpl implements WorkflowThrottlingService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowThrottlingServiceImpl.class);

    private static final String GLOBAL = WorkflowThrottlingUtils.GLOBAL;
    private static final String DEFAULT = WorkflowThrottlingUtils.DEFAULT;
    private static final String GLOBALCONFIG = WorkflowThrottlingUtils.GLOBALCONFIG;
    private static final String TENANTCONFIG = WorkflowThrottlingUtils.TENANTCONFIG;

    private static TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>> clusterPropertyFileRef = new TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>>() {
    };
    private static TypeReference<Map<String, Map<JobStatus, Integer>>> tenantPropertyFileRef = new TypeReference<Map<String, Map<JobStatus, Integer>>>() {
    };

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${workflowapi.throttling.masterconfig}")
    private String masterConfigStr;

    @Override
    public WorkflowThrottlingConfiguration getThrottlingConfig(String podid, String division,
            Set<String> customerSpaces) {
        Camille camille = CamilleEnvironment.getCamille();
        WorkflowThrottlingConfiguration config = new WorkflowThrottlingConfiguration();
        try {
            Map<String, Map<String, Map<JobStatus, Integer>>> props = JsonUtils.deserializeByTypeRef(masterConfigStr,
                    clusterPropertyFileRef);
            config.setGlobalLimit(props.get(GLOBALCONFIG));
            Map<String, Map<String, Map<JobStatus, Integer>>> tenantLimit = new HashMap<>();
            tenantLimit.put(DEFAULT, props.get(TENANTCONFIG));
            config.setTenantLimit(tenantLimit);
        } catch (Exception e) {
            log.error("Unable to retrieve master property file.", e);
            return null;
        }
        try { // check pod overwrite
            Path path = PathBuilder.buildWorkflowThrottlingPodsConfigPath(podid);
            if (camille.exists(path)) {
                Map<String, Map<String, Map<JobStatus, Integer>>> props = JsonUtils
                        .deserializeByTypeRef(camille.get(path).getData(), clusterPropertyFileRef);
                WorkflowThrottlingUtils.overwriteConfig(config, props, "pod");
            }
        } catch (Exception e) {
            log.warn("Unable to retrieve pod {} property file. {}", podid, e);
        }
        try { // check division overwrite
            Path path = PathBuilder.buildWorkflowThrottlingDivisionConfigPath(podid, division);
            if (camille.exists(path)) {
                Map<String, Map<String, Map<JobStatus, Integer>>> props = JsonUtils
                        .deserializeByTypeRef(camille.get(path).getData(), clusterPropertyFileRef);
                WorkflowThrottlingUtils.overwriteConfig(config, props, "stack");
            }
        } catch (Exception e) {
            log.warn("Unable to retrieve division {} property file. {}", podid, e);
        }
        // add tenant specific map
        for (String customerSpace : customerSpaces) {
            try {
                Path path = PathBuilder.buildWorkflowThrottlingTenantConfigPath(podid,
                        CustomerSpace.parse(customerSpace));
                if (camille.exists(path)) {
                    String tenantConfigStr = camille.get(path).getData();
                    if (StringUtils.isEmpty(tenantConfigStr)) {
                        continue;
                    }
                    Map<String, Map<JobStatus, Integer>> tenantWorkflowMap = JsonUtils
                            .deserializeByTypeRef(tenantConfigStr, tenantPropertyFileRef);
                    if (MapUtils.isNotEmpty(tenantWorkflowMap)) {
                        config.getTenantLimit().put(customerSpace, tenantWorkflowMap);
                    }
                }
            } catch (Exception e) {
                log.warn("Unable to retrieve tenant {} property file. {}", customerSpace, e);
            }
        }
        return config;
    }

    @Override
    public boolean isWorkflowThrottlingEnabled(String podid, String division) {
        // In current impl, the flag is per stack
        Camille camille = CamilleEnvironment.getCamille();
        try {
            Path flagPath = PathBuilder.buildWorkflowThrottlingFlagPath(podid, division);
            if (camille.exists(flagPath)) {
                return Boolean.TRUE.equals(JsonUtils.convertValue(camille.get(flagPath).getData(), Boolean.class));
            }
            return false;
        } catch (Exception e) {
            log.warn("Unable to retrieve division {}-{} throttling flag from zk.", podid, division, e);
            return false;
        }
    }

    @Override
    public boolean isWorkflowThrottlingRolledOut(String podid, String division, String workflowType) {
        Camille camille = CamilleEnvironment.getCamille();
        try {
            Path flagPath = PathBuilder.buildSingleWorkflowThrottlingFlagPath(podid, division, workflowType);
            if (camille.exists(flagPath)) {
                return Boolean.TRUE.equals(JsonUtils.convertValue(camille.get(flagPath).getData(), Boolean.class));
            }
            return false;
        } catch (Exception e) {
            log.warn("Workflow {} is not enabled for throttling on division {}-{}.", workflowType, podid, division);
            return false;
        }
    }

    @Override
    public boolean shouldEnqueueWorkflow(String podid, String division, String workflowType) {
        return isWorkflowThrottlingEnabled(podid, division)
                && isWorkflowThrottlingRolledOut(podid, division, workflowType);
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
            logGlobalStatus(podid, status.getEnqueuedWorkflowInEnv(),
                    String.format("WorkflowThrottling Global Queue Status on environment=%s: ", podid));
            logGlobalStatus(podid, status.getRunningWorkflowInEnv(),
                    String.format("WorkflowThrottling Global Running Status on environment=%s: ", podid));
            List<WorkflowThrottlingConstraint> constraintList = Arrays.asList(new NotExceedingEnvQuota(),
                    new NotExceedingTenantQuota(), new IsForCurrentStack());
            List<WorkflowJobSchedulingObject> enqueuedWorkflowSchedulingObjects = status.getEnqueuedWorkflowJobs()
                    .stream().map(o -> new WorkflowJobSchedulingObject(o, constraintList)).collect(Collectors.toList());
            WorkflowScheduler scheduler = new GreedyWorkflowScheduler();
            return scheduler.schedule(status, enqueuedWorkflowSchedulingObjects, podid, division);
        } catch (Exception e) {
            log.error("Failed to get workflow throttling result.", e);
            throw e;
        }
    }

    private void logGlobalStatus(String podid, Map<String, Integer> workflowMap, String label) {
        StringBuilder str = new StringBuilder(label);
        log.info(str.append(JsonUtils.serialize(workflowMap)).toString());
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
            List<WorkflowJob> tenantEnqueuedWorkflowJobs = enqueuedWorkflowJobs.stream()
                    .filter(o -> customerSpace.equals(o.getTenant().getId())).collect(Collectors.toList());
            Map<JobStatus, Integer> globalLimitMap = config.getGlobalLimit().getOrDefault(workflowType,
                    config.getGlobalLimit().get(DEFAULT));
            Map<JobStatus, Integer> tenantLimitMap = WorkflowThrottlingUtils.getTenantMap(config.getTenantLimit(),
                    customerSpace, workflowType);
            if (enqueuedWorkflowJobs.size() >= globalLimitMap.get(JobStatus.ENQUEUED)) {
                log.error("Global queue limit reached. Environment: {} WorkflowType: {}, tenant: {}. Limit is {}",
                        podid, workflowType, customerSpace, globalLimitMap.get(JobStatus.ENQUEUED));
                return true;
            }
            if (tenantEnqueuedWorkflowJobs.size() >= tenantLimitMap.get(JobStatus.ENQUEUED)) {
                log.error("Tenant queue limit reached. Environment: {} WorkflowType: {}, tenant: {}. Limit is {}",
                        podid, workflowType, customerSpace, tenantLimitMap.get(JobStatus.ENQUEUED));
                return true;
            }
            return false;
        } finally {
            MultiTenantContext.setTenant(originTenant);
        }
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

                enqueuedWorkflowJobs.add(workflow);
                if (division.equals(workflow.getStack())) {
                    addWorkflowToMap(enqueuedWorkflowInStack, workflow);
                }
            }
        }
        status.setRunningWorkflowInEnv(runningWorkflowInEnv);
        status.setEnqueuedWorkflowInEnv(enqueuedWorkflowInEnv);
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
