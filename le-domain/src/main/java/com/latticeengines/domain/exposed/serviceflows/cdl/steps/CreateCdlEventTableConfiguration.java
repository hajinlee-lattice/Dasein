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

    @JsonProperty
    private boolean useAccountFeature;

    @JsonProperty
    private boolean exportKeyColumnsOnly;

    public CreateCdlEventTableConfiguration() {
        setBeanName("createCdlEventTableFlow");
    }

    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    public String getEventColumn() {
        return eventColumn;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
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

    public boolean isUseAccountFeature() {
        return useAccountFeature;
    }

    public void setUseAccountFeature(boolean useAccountFeature) {
        this.useAccountFeature = useAccountFeature;
    }

    public boolean isExportKeyColumnsOnly() {
        return exportKeyColumnsOnly;
    }

    public void setExportKeyColumnsOnly(boolean exportKeyColumnsOnly) {
        this.exportKeyColumnsOnly = exportKeyColumnsOnly;
    }

}
