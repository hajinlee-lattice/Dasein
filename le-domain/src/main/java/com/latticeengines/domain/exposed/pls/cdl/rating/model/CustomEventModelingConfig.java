package com.latticeengines.domain.exposed.pls.cdl.rating.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;

@JsonIgnoreProperties
public class CustomEventModelingConfig implements AdvancedModelingConfig {

    private CustomEventModelingType customEventModelingType;

    private String sourceFileName;

    private List<DataStore> dataStores;

    private DedupType deduplicationType = DedupType.ONELEADPERDOMAIN;

    private boolean excludePublicDomains;

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

    public List<DataStore> getDataStores() {
        return dataStores;
    }

    public void setDataStores(List<DataStore> dataStores) {
        this.dataStores = dataStores;
    }

    public DedupType getDeduplicationType() {
        return deduplicationType;
    }

    public void setDeduplicationType(DedupType deduplicationType) {
        this.deduplicationType = deduplicationType;
    }

    public boolean isExcludePublicDomains() {
        return excludePublicDomains;
    }

    public void setExcludePublicDomains(boolean excludePublicDomains) {
        this.excludePublicDomains = excludePublicDomains;
    }

    public enum DataStore {
        DataCloud, //
        CDL, //
        CustomFileAttributes
    }

    @Override
    public void copyConfig(AdvancedModelingConfig config) {
        CustomEventModelingConfig advancedConfInRetrievedAIModel = this;
        CustomEventModelingConfig advancedConfInAIModel = (CustomEventModelingConfig) config;
        advancedConfInRetrievedAIModel.setCustomEventModelingType(advancedConfInAIModel.getCustomEventModelingType());
        advancedConfInRetrievedAIModel.setSourceFileName(advancedConfInAIModel.getSourceFileName());
        advancedConfInRetrievedAIModel.setDataStores(advancedConfInAIModel.getDataStores());
        advancedConfInRetrievedAIModel.setDeduplicationType(advancedConfInAIModel.getDeduplicationType());
        advancedConfInRetrievedAIModel.setExcludePublicDomains(advancedConfInAIModel.isExcludePublicDomains());
    }
}
