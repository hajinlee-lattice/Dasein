package com.latticeengines.serviceflows.workflow.modeling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ModelStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String modelingServiceHdfsBaseDir;

    @NotEmptyString
    @NotNull
    private String modelName;

    private String eventTableName;

    private String productType;

    private String sourceSchemaInterpretation;

    private String sourceFileTableName;

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

    public String getSourceFileTableName() {
        return sourceFileTableName;
    }

    public void setSourceFileTableName(String sourceFileTableName) {
        this.sourceFileTableName = sourceFileTableName;
    }
}
