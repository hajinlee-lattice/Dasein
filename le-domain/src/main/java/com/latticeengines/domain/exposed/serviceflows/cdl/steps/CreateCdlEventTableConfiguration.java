package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

public class CreateCdlEventTableConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String sourceSchemaInterpretation;
    private String eventColumn;

    public CreateCdlEventTableConfiguration() {
        setBeanName("createCdlEventTableFlow");
    }

    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

    public String getEventColumn() {
        return eventColumn;
    }
}
