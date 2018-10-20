package com.latticeengines.domain.exposed.pls.cdl.rating.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CrossSellModelingConfig implements AdvancedModelingConfig {

    @JsonProperty("targetProducts")
    private List<String> targetProducts;

    @JsonProperty("trainingProducts")
    private List<String> trainingProducts;

    @JsonProperty("modelingStrategy")
    private ModelingStrategy modelingStrategy;

    @JsonProperty("dataCloudVersion")
    private String dataCloudVersion;

    @JsonProperty("filters")
    private Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters;

    public static CrossSellModelingConfig getAdvancedModelingConfig(AIModel aiModel) {
        if (aiModel.getAdvancedModelingConfig() == null) {
            aiModel.setAdvancedModelingConfig(new CrossSellModelingConfig());
        }
        return (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();
    }

    public List<String> getTargetProducts() {
        return targetProducts;
    }

    public void setTargetProducts(List<String> targetProducts) {
        this.targetProducts = targetProducts;
    }

    public List<String> getTrainingProducts() {
        return trainingProducts;
    }

    public void setTrainingProducts(List<String> trainingProducts) {
        this.trainingProducts = trainingProducts;
    }

    public ModelingStrategy getModelingStrategy() {
        if (modelingStrategy == null) {
            modelingStrategy = ModelingStrategy.CROSS_SELL_FIRST_PURCHASE;
        }
        return modelingStrategy;
    }

    public void setModelingStrategy(ModelingStrategy modelingStrategy) {
        this.modelingStrategy = modelingStrategy;
    }

    public Map<CrossSellModelingConfigKeys, ModelingConfigFilter> getFilters() {
        return filters;
    }

    public void setFilters(Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters) {
        this.filters = filters;
    }

    @Override
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    @Override
    public void copyConfig(AdvancedModelingConfig config) {
        CrossSellModelingConfig advancedConfInRetrievedAIModel = this;
        CrossSellModelingConfig advancedConfInAIModel = (CrossSellModelingConfig) config;
        advancedConfInRetrievedAIModel
                .setModelingStrategy(advancedConfInAIModel.getModelingStrategy());
        advancedConfInRetrievedAIModel.setTargetProducts(advancedConfInAIModel.getTargetProducts());
        advancedConfInRetrievedAIModel
                .setTrainingProducts(advancedConfInAIModel.getTrainingProducts());
        advancedConfInRetrievedAIModel.setFilters(advancedConfInAIModel.getFilters());
        advancedConfInRetrievedAIModel
                .setDataCloudVersion(advancedConfInAIModel.getDataCloudVersion());
    }
}
