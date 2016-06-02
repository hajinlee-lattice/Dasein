package com.latticeengines.serviceflows.workflow.modeling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ModelStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String modelingServiceHdfsBaseDir;

    @NotEmptyString
    @NotNull
    private String modelName;

    private String displayName;

    private String eventTableName;

    private String productType;

    private String sourceSchemaInterpretation;

    private String trainingTableName;

    private String transformationGroupName;

    private ModelSummary sourceModelSummary;
    
    private boolean excludePropDataColumns;

    @JsonProperty("modelingServiceHdfsBaseDir")
    public String getModelingServiceHdfsBaseDir() {
        return modelingServiceHdfsBaseDir;
    }

    @JsonProperty("modelingServiceHdfsBaseDir")
    public void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }

    @JsonProperty
    public String getModelName() {
        return modelName;
    }

    @JsonProperty
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    @JsonProperty
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getEventTableName() {
        return eventTableName;
    }

    public void setEventTableName(String eventTableName) {
        this.eventTableName = eventTableName;
    }

    public String getProductType() {
        return productType;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    public String getTrainingTableName() {
        return trainingTableName;
    }

    public void setTrainingTableName(String trainingTableName) {
        this.trainingTableName = trainingTableName;
    }

    public String getTransformationGroupName() {
        return transformationGroupName;
    }

    public void setTransformationGroupName(String transformationGroupName) {
        this.transformationGroupName = transformationGroupName;
    }

    public ModelSummary getSourceModelSummary() {
        return sourceModelSummary;
    }

    public void setSourceModelSummary(ModelSummary sourceModelSummary) {
        this.sourceModelSummary = sourceModelSummary;
    }
    
    @JsonProperty
    public boolean excludePropDataColumns() {
        return excludePropDataColumns;
    }

    @JsonProperty
    public void setExcludePropDataColumns(boolean excludePropDataColumns) {
        this.excludePropDataColumns = excludePropDataColumns;
    }
}
