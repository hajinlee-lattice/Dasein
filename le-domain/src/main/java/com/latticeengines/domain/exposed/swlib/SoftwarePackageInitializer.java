package com.latticeengines.domain.exposed.swlib;

import org.springframework.context.ApplicationContext;

public interface SoftwarePackageInitializer {

    ApplicationContext initialize(ApplicationContext applicationContext, String module);

    default String getClassifier(String module) {
        if ("workflowapi".equals(module)) {
            return "workflow";
        }
        return "dataflow";

    }
}
