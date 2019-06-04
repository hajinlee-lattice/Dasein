package com.latticeengines.domain.exposed.pls.cdl.rating.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomEventModelingConfig implements AdvancedModelingConfig {

    private CustomEventModelingType customEventModelingType;

    private String sourceFileName;

    private String sourceFileDisplayName;

    private List<DataStore> dataStores;

    private DedupType deduplicationType = DedupType.ONELEADPERDOMAIN;

    private boolean excludePublicDomains;

    private String transformationGroup = TransformationGroup.NONE.getName();

    private String dataCloudVersion;

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

    public String getSourceFileDisplayName() {
        return sourceFileDisplayName;
    }

    public void setSourceFileDisplayName(String sourceFileDisplayName) {
        this.sourceFileDisplayName = sourceFileDisplayName;
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

    @JsonIgnore
    public TransformationGroup getConvertedTransformationGroup() {
        try {
            return TransformationGroup.fromName(transformationGroup);
        } catch (Exception e) {
            return TransformationGroup.NONE;
        }
    }

    public String getTransformationGroup() {
        return transformationGroup;
    }

    public void setTransformationGroup(String transformationGroup) {
        this.transformationGroup = transformationGroup;
    }

    @Override
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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
        advancedConfInRetrievedAIModel.setSourceFileDisplayName(advancedConfInAIModel.getSourceFileDisplayName());
        advancedConfInRetrievedAIModel.setDataStores(advancedConfInAIModel.getDataStores());
        advancedConfInRetrievedAIModel.setDeduplicationType(advancedConfInAIModel.getDeduplicationType());
        advancedConfInRetrievedAIModel.setExcludePublicDomains(advancedConfInAIModel.isExcludePublicDomains());
        advancedConfInRetrievedAIModel.setTransformationGroup(advancedConfInAIModel.getTransformationGroup());
        advancedConfInRetrievedAIModel.setDataCloudVersion(advancedConfInAIModel.getDataCloudVersion());
    }

    public static CustomEventModelingConfig getAdvancedModelingConfig(AIModel aiModel) {
        if (aiModel.getAdvancedModelingConfig() == null) {
            aiModel.setAdvancedModelingConfig(new CustomEventModelingConfig());
        }
        return (CustomEventModelingConfig) aiModel.getAdvancedModelingConfig();
    }
}
