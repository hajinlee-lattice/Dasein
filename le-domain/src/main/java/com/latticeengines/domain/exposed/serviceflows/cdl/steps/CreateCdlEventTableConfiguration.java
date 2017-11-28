package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

public class CreateCdlEventTableConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String sourceSchemaInterpretation;
    private String outputTableName;

    public CreateCdlEventTableConfiguration() {
        setBeanName("createCdlEventTableFlow");
    }

    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    public void setOutputTableName(String outputTableName) {
        this.outputTableName = outputTableName;
    }

    public String getOutputTableName() {
        return this.outputTableName;
    }
}
