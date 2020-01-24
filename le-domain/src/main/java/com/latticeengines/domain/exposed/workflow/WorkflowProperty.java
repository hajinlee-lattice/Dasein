package com.latticeengines.domain.exposed.workflow;

import com.latticeengines.domain.exposed.BaseProperty;

public final class WorkflowProperty extends BaseProperty {

    protected WorkflowProperty() {
        throw new UnsupportedOperationException();
    }

    public static final String WORKFLOWCONFIG = "workflowapiConfig";
    public static final String WORKFLOWCONFIGCLASS = "workflowapiConfigClass";

    public static final String STEPFLOWCONFIG = "StepflowConfig";
    public static final String STEPFLOWCONFIGCLASS = "StepflowConfigClass";
}
