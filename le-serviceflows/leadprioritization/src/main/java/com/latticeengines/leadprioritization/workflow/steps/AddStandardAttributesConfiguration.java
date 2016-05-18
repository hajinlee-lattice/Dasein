package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class AddStandardAttributesConfiguration extends DataFlowStepConfiguration {

    private TransformationGroup transformationGroup;

    public AddStandardAttributesConfiguration() {
        setPurgeSources(true);
        setTargetTableName("addStandardAttributesViaJavaFunction");
        setBeanName("addStandardAttributesViaJavaFunction");
    }

    public TransformationGroup getTransformationGroup() {
        return transformationGroup;
    }

    public void setTransformationGroup(TransformationGroup transformationGroup) {
        this.transformationGroup = transformationGroup;
    }
}
