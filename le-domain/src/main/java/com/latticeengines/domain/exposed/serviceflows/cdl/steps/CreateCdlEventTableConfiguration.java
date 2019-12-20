package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class CreateCdlEventTableConfiguration extends SparkJobStepConfiguration {

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

    @JsonProperty
    private String targetTableName;

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

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

}
