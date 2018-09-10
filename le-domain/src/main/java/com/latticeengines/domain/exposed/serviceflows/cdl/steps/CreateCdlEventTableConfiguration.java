package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.domain.exposed.metadata.DataCollection;

public class CreateCdlEventTableConfiguration extends BaseCDLDataFlowStepConfiguration {

    @JsonProperty
    private String sourceSchemaInterpretation;

    @JsonProperty
    private String eventColumn;

    @JsonProperty
    private DataCollection.Version dataCollectionVersion;

    @JsonProperty
    private boolean crossSell;

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

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }

    public boolean isCrossSell() {
        return crossSell;
    }

    public void setCrossSell(boolean crossSell) {
        this.crossSell = crossSell;
    }

}
