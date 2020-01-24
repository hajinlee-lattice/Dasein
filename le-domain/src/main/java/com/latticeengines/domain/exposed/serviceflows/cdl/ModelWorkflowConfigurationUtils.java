package com.latticeengines.domain.exposed.serviceflows.cdl;

public final class ModelWorkflowConfigurationUtils {

    protected ModelWorkflowConfigurationUtils() {
        throw new UnsupportedOperationException();
    }

    public static boolean skipUseConfiguredModelingAttributesStep(Integer modelIteration) {
        return modelIteration != null && modelIteration.intValue() == 1;
    }
}
