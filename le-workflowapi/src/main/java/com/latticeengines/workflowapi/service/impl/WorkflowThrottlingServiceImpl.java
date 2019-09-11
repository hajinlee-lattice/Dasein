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
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.IsForCurrentStack;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.NotExceedingEnvQuota;
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
    private static final String DEFAULT = "default";
    private static final String GLOBALCONFIG = "globalConfig";
    private static final String TENANTCONFIG = "tenantConfig";

    private static TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>> clusterPropertyFileRef = new TypeReference<Map<String, Map<String, Map<JobStatus, Integer>>>>() {
    };
    private static TypeReference<Map<String, Map<JobStatus, Integer>>> tenantPropertyFileRef = new TypeReference<Map<String, Map<JobStatus, Integer>>>() {
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
            Map<String, Map<String, Map<JobStatus, Integer>>> props = JsonUtils.deserializeByTypeRef(
                    c.get(PathBuilder.buildWorkflowThrottlingMasterConfigPath()).getData(), clusterPropertyFileRef);
            config.setGlobalLimit(props.get(GLOBALCONFIG));
            Map<String, Map<String, Map<JobStatus, Integer>>> tenantLimit = new HashMap<>();
            tenantLimit.put(GLOBAL, props.get(TENANTCONFIG));
            config.setTenantLimit(tenantLimit);
        } catch (Exception e) {
            log.error("Unable to retrieve master property file.", e);
            return null;
        }
        try { // check pod overwrite
            Map<String, Map<String, Map<JobStatus, Integer>>> props = JsonUtils.deserializeByTypeRef(
                    c.get(PathBuilder.buildWorkflowThrottlingPodsConfigPath(podid)).getData(), clusterPropertyFileRef);
            overwriteConfig(config, props);
        } catch (Exception e) {
            log.warn("Unable to retrieve pod {} property file. {}", podid, e);
        }
        try { // check division overwrite
            Map<String, Map<String, Map<JobStatus, Integer>>> props = JsonUtils.deserializeByTypeRef(
                    c.get(PathBuilder.buildWorkflowThrottlingDivisionConfigPath(podid, division)).getData(),
                    clusterPropertyFileRef);
            overwriteConfig(config, props);
        } catch (Exception e) {
            log.warn("Unable to retrieve division {} property file. {}", podid, e);
        }
        // add tenant specific map
        for (String customerSpace : customerSpaces) {
            try {
                Map<String, Map<JobStatus, Integer>> tenantWorkflowMap = JsonUtils.deserializeByTypeRef(c.get(
                        PathBuilder.buildWorkflowThrottlingTenantConfigPath(podid, CustomerSpace.parse(customerSpace)))
                        .getData(), tenantPropertyFileRef);
                if (tenantWorkflowMap.keySet().size() > 0) {
                    config.getTenantLimit().put(customerSpace, tenantWorkflowMap);
                }
            } catch (Exception e) {
                log.warn("Unable to retrieve tenant {} property file. {}", customerSpace, e);
            }
        }
        return config;
    }

    private void overwriteConfig(WorkflowThrottlingConfiguration config,
            Map<String, Map<String, Map<JobStatus, Integer>>> props) {
        Map<String, Map<JobStatus, Integer>> globalLimit = props.get(GLOBALCONFIG);
        Map<String, Map<JobStatus, Integer>> tenantLimit = props.get(TENANTCONFIG);

        if (globalLimit != null) {
            config.getGlobalLimit().putAll(globalLimit);
        }
        if (tenantLimit != null) {
            config.getTenantLimit().get(GLOBAL).putAll(tenantLimit);
        }
    }

    @Override
    public boolean isWorkflowThrottlingEnabled(String podid, String division) {
        // In current impl, the flag is per stack
        Camille c = CamilleEnvironment.getCamille();
        try {
            return Boolean.TRUE.equals(JsonUtils.convertValue(
                    c.get(PathBuilder.buildWorkflowThrottlingFlagPath(podid, division)).getData(), Boolean.class));
        } catch (Exception e) {
            log.warn("Unable to retrieve division {}-{} throttling flag from zk.", podid, division, e);
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
            Map<JobStatus, Integer> tenantLimitMap = getTenantMap(config.getTenantLimit(), customerSpace, workflowType);
            if (enqueuedWorkflowJobs.size() >= globalLimitMap.get(JobStatus.ENQUEUED)) {
                log.error("Global queue limit reached. WorkflowType: {}, tenant: {}", workflowType, customerSpace);
                return true;
            }
            if (tenantEnqueuedWorkflowJobs.size() >= tenantLimitMap.get(JobStatus.ENQUEUED)) {
                log.error("Tenant queue limit reached. WorkflowType: {}, tenant: {}", workflowType, customerSpace);
                return true;
            }
            return false;
        } finally {
            MultiTenantContext.setTenant(originTenant);
        }
    }

    private Map<JobStatus, Integer> getTenantMap(Map<String, Map<String, Map<JobStatus, Integer>>> tenantLimit,
            String customerSpace, String workflowType) {
        Map<String, Map<JobStatus, Integer>> tenantMap = tenantLimit.get(customerSpace);
        if (tenantMap == null) {
            return tenantLimit.get(GLOBAL).getOrDefault(workflowType, tenantLimit.get(GLOBAL).get(DEFAULT));
        }
        return tenantMap.getOrDefault(workflowType,
                tenantLimit.get(GLOBAL).getOrDefault(workflowType, tenantLimit.get(GLOBAL).get(DEFAULT)));
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
