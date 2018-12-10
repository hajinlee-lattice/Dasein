package com.latticeengines.workflow.exposed.util;

import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class WorkflowUtils {

    private static final Logger log = LoggerFactory.getLogger(WorkflowUtils.class);

    public static Map<String, String> getFlattenedConfig(WorkflowConfiguration workflowConfig) {
        Map<String, String> flattenedConfig = getFlattenedConfig(workflowConfig,
                new StringBuilder(workflowConfig.getWorkflowName()));
        if (log.isDebugEnabled()) {
            log.debug(flattenedConfig.toString());
        }
        return flattenedConfig;
    }

    private static Map<String, String> getFlattenedConfig(WorkflowConfiguration workflowConfig, StringBuilder parent) {
        Map<String, String> flattenedConfig = workflowConfig.getStepConfigRegistry().entrySet().stream() //
                .collect(Collectors.toMap(e -> new StringBuilder(parent).append('.').append(e.getKey()).toString(),
                        e -> e.getValue()));
        int len = parent.length();
        workflowConfig.getSubWorkflowConfigRegistry().entrySet() //
                .forEach(e -> {
                    parent.append('.').append(e.getKey());
                    flattenedConfig.putAll(getFlattenedConfig(e.getValue(), parent));
                    parent.setLength(len);
                });
        return flattenedConfig;
    }
}
