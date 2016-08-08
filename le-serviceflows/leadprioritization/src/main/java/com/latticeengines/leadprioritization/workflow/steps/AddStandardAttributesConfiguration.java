package com.latticeengines.leadprioritization.workflow.steps;

import java.util.Map;

import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class AddStandardAttributesConfiguration extends DataFlowStepConfiguration {

    private TransformationGroup transformationGroup;
    private Map<String, String> runTimeParams;

    public AddStandardAttributesConfiguration() {
        setTargetTableName("addStandardAttributes");
        setBeanName("addStandardAttributes");
    }

    public TransformationGroup getTransformationGroup() {
        return transformationGroup;
    }

    public void setTransformationGroup(TransformationGroup transformationGroup) {
        this.transformationGroup = transformationGroup;
    }
    
    public Map<String, String> getRuntimeParams() {
        return runTimeParams;
    }
    
    public void setRuntimeParams(Map<String, String> runTimeParams) {
        this.runTimeParams = runTimeParams;
    }
}
