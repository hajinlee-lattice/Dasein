package com.latticeengines.domain.exposed.util;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public final class WorkflowConfigurationUtils {

    protected WorkflowConfigurationUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(WorkflowConfigurationUtils.class);

    public static WorkflowConfiguration getDefaultWorkflowConfiguration(
            Class<? extends WorkflowConfiguration> workflowConfigClass) {
        Class<?> builderClass = Arrays.stream(workflowConfigClass.getDeclaredClasses())
                .filter(c -> c.getSimpleName().equals("Builder")).distinct().findFirst()
                .orElse(null);
        try {
            Object builder = builderClass.newInstance();
            Method build = builderClass.getMethod("build", new Class<?>[] {});
            return (WorkflowConfiguration) build.invoke(builder);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
