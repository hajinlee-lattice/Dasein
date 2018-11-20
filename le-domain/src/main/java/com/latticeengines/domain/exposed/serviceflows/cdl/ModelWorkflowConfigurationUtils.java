package com.latticeengines.domain.exposed.serviceflows.cdl;

public class ModelWorkflowConfigurationUtils {

    public static boolean skipUseConfiguredModelingAttributesStep(Integer modelIteration) {
        return modelIteration != null && modelIteration.intValue() == 1;
    }
}
