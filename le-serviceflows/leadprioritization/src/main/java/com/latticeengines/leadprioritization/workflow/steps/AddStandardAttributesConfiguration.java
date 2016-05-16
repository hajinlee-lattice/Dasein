package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class AddStandardAttributesConfiguration extends DataFlowStepConfiguration {

    private TransformationGroup transformGroup;

    public AddStandardAttributesConfiguration() {
        setPurgeSources(true);
        setTargetTableName("addStandardAttributesViaJavaFunction");
        setBeanName("addStandardAttributesViaJavaFunction");
    }

    public TransformationGroup getTransformationGroup() {
        return transformGroup;
    }

    public void setTransformationGroup(TransformationGroup transformGroup) {
        this.transformGroup = transformGroup;
    }
}
