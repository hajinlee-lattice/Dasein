package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class AddStandardAttributesConfiguration extends DataFlowStepConfiguration {

    private TransformationGroup transformationGroup;
    private Map<String, String> runTimeParams;
    private List<TransformDefinition> transforms;
    private String sourceSchemaInterpretation;

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

    public List<TransformDefinition> getTransforms() {
        return this.transforms;
    }

    public void setTransforms(List<TransformDefinition> transforms) {
        this.transforms = transforms;
    }

    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }
}
