package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class AddStandardAttributesConfiguration extends DataFlowStepConfiguration {
    public AddStandardAttributesConfiguration() {
        setPurgeSources(true);
        setName("addStandardAttributes");
        setBeanName("addStandardAttributes");
    }
}
