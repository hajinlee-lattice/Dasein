package com.latticeengines.domain.exposed.pls.cdl.rating.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.modeling.ModelingType;

@JsonIgnoreProperties
public class CustomEventModelingConfig implements AdvancedModelingConfig {

    private ModelingType modelingType;

    private String fieldMappingMetadataTableId;

    private String trainingFilePath;

    private List<DataStore> dataStores;

    public ModelingType getModelingType() {
        return modelingType;
    }

    public void setModelingType(ModelingType modelingType) {
        this.modelingType = modelingType;
    }

    public String getFieldMappingMetadataTableId() {
        return fieldMappingMetadataTableId;
    }

    public void setFieldMappingMetadataTableId(String fieldMappingMetadataTableId) {
        this.fieldMappingMetadataTableId = fieldMappingMetadataTableId;
    }

    public String getTrainingFilePath() {
        return trainingFilePath;
    }

    public void setTrainingFilePath(String trainingFilePath) {
        this.trainingFilePath = trainingFilePath;
    }

    public List<DataStore> getDataStores() {
        return dataStores;
    }

    public void setDataStores(List<DataStore> dataStores) {
        this.dataStores = dataStores;
    }

    public static enum DataStore {
        DataCloud, //
        CDL, //
        CustomAttributes;
    }

    @Override
    public void copyConfig(AdvancedModelingConfig config) {
        CustomEventModelingConfig advancedConfInRetrievedAIModel = this;
        CustomEventModelingConfig advancedConfInAIModel = (CustomEventModelingConfig) config;
        advancedConfInRetrievedAIModel.setModelingType(advancedConfInAIModel.getModelingType());
        advancedConfInRetrievedAIModel
                .setFieldMappingMetadataTableId(advancedConfInAIModel.getFieldMappingMetadataTableId());
        advancedConfInRetrievedAIModel.setTrainingFilePath(advancedConfInAIModel.getTrainingFilePath());
        advancedConfInRetrievedAIModel.setDataStores(advancedConfInAIModel.getDataStores());

    }
}
