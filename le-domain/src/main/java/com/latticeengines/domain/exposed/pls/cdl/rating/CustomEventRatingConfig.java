package com.latticeengines.domain.exposed.pls.cdl.rating;

import java.util.List;

import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;

public class CustomEventRatingConfig implements AdvancedRatingConfig {

    private CustomEventModelingType customEventModelingType;

    private String sourceFileName;

    private List<CustomEventModelingConfig.DataStore> dataStores;

    public CustomEventModelingType getCustomEventModelingType() {
        return customEventModelingType;
    }

    public void setCustomEventModelingType(CustomEventModelingType customEventModelingType) {
        this.customEventModelingType = customEventModelingType;
    }

    public String getSourceFileName() {
        return sourceFileName;
    }

    public void setSourceFileName(String sourceFileName) {
        this.sourceFileName = sourceFileName;
    }

    public List<CustomEventModelingConfig.DataStore> getDataStores() {
        return dataStores;
    }

    public void setDataStores(List<CustomEventModelingConfig.DataStore> dataStores) {
        this.dataStores = dataStores;
    }

    @Override
    public void copyConfig(AdvancedRatingConfig config) {
        CustomEventRatingConfig advancedConfInRetrievedAIModel = this;
        CustomEventRatingConfig advancedConfInAIModel = (CustomEventRatingConfig) config;
        advancedConfInRetrievedAIModel.setCustomEventModelingType(advancedConfInAIModel.getCustomEventModelingType());
        advancedConfInRetrievedAIModel.setSourceFileName(advancedConfInAIModel.getSourceFileName());
        advancedConfInRetrievedAIModel.setDataStores(advancedConfInAIModel.getDataStores());
    }
}
