package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.BaseLPDataFlowStepConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class AddStandardAttributesConfiguration extends BaseLPDataFlowStepConfiguration {

    @JsonProperty
    private TransformationGroup transformationGroup;
    @JsonProperty
    private Map<String, String> runTimeParams;
    @JsonProperty
    private List<TransformDefinition> transforms;
    @JsonProperty
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
