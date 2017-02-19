package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransformationStepConfig {

    @JsonProperty("Transformer")
    private String transformer = "TransformerBase";

    @JsonProperty("InputSteps")
    private List<Integer> inputSteps;

    @JsonProperty("BaseSources")
    private List<String> baseSources;

    @JsonProperty("BaseVersions")
    private List<String> baseVersions;

    @JsonProperty("BaseTemplates")
    private List<String> baseTemplates;

    @JsonProperty("TargetSource")
    private String targetSource;

    @JsonProperty("TargetVersion")
    private String targetVersion;

    @JsonProperty("TargetTemplate")
    private String targetTemplate;

    @JsonIgnore
    private String configuration;

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }

    @JsonIgnore
    public String getConfiguration() {
        return configuration;
    }

    @JsonIgnore
    public void setConfiguration(String configuration) {
        this.configuration = configuration;
    }

    @JsonProperty("Configuration")
    private JsonNode getConfigurationAsJson() {
        return JsonUtils.deserialize(configuration, JsonNode.class);
    }

    @JsonProperty("Configuration")
    private void setConfigurationViaJson(JsonNode configJson) {
        this.configuration = JsonUtils.serialize(configJson);
    }

    public List<String> getBaseSources() {
        return baseSources;
    }

    public void setBaseSources(List<String> baseSources) {
        this.baseSources = baseSources;
    }

    public List<String> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<String> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public void setBaseTemplates(List<String> baseTemplates) {
        this.baseTemplates = baseTemplates;
    }

    public List<String> getBaseTemplates() {
        return baseTemplates;
    }

    public String getTargetSource() {
        return targetSource;
    }

    public void setTargetSource(String targetSource) {
        this.targetSource = targetSource;
    }

    public String getTargetTemplate() {
        return targetTemplate;
    }

    public void setTargetTemplate(String targetTemplate) {
        this.targetTemplate = targetTemplate;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public List<Integer> getInputSteps() {
       return inputSteps;
    }

    public void setInputSteps(List<Integer> inputSteps) {
       this.inputSteps = inputSteps;
    }

}
