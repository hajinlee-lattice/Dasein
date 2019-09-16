package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.JobStatus;

public class WorkflowThrottlingUtils {

    public static final String GLOBAL = "global";
    public static final String DEFAULT = "default";
    public static final String GLOBALCONFIG = "globalConfig"; // global config key in property file json
    public static final String TENANTCONFIG = "tenantConfig"; // tenant config key in property file json

    private static final Logger log = LoggerFactory.getLogger(WorkflowThrottlingUtils.class);

    // get workflow quota map of a workflow for a tenant
    public static Map<JobStatus, Integer> getTenantMap(Map<String, Map<String, Map<JobStatus, Integer>>> tenantLimit,
                                                 String customerSpace, String workflowType) {
        Map<String, Map<JobStatus, Integer>> tenantMap = tenantLimit.get(customerSpace);
        if (tenantMap == null) {
            return tenantLimit.get(DEFAULT).getOrDefault(workflowType, tenantLimit.get(DEFAULT).get(DEFAULT));
        }
        Map<JobStatus, Integer> tenantWorkflowMap = tenantMap.getOrDefault(workflowType, tenantMap.get(DEFAULT));
        if (tenantWorkflowMap == null) {
            return tenantLimit.get(DEFAULT).getOrDefault(workflowType, tenantLimit.get(DEFAULT).get(DEFAULT));
        }
        return tenantWorkflowMap;
    }

    // overwrite config with props read from property file
    public static void overwriteConfig(WorkflowThrottlingConfiguration config, Map<String, Map<String, Map<JobStatus, Integer>>> props, String overwrittenBy) {
        Map<String, Map<JobStatus, Integer>> globalLimit = props.get(GLOBALCONFIG);
        Map<String, Map<JobStatus, Integer>> tenantLimit = props.get(TENANTCONFIG);

        if (globalLimit != null) {
            config.getGlobalLimit().putAll(globalLimit);
            log.info("Workflow throttling global config overwritten by {}", overwrittenBy);
        }
        if (tenantLimit != null) {
            config.getTenantLimit().get(DEFAULT).putAll(tenantLimit);
            log.info("Workflow throttling tenant config overwritten by {}", overwrittenBy);
        }
    }
}
