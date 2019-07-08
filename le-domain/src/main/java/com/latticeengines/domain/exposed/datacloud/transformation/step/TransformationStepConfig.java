package com.latticeengines.domain.exposed.datacloud.transformation.step;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransformationStepConfig {

    public static final String SIMPLE = "Simple";
    public static final String ITERATIVE = "Iterative";

    @JsonProperty("Transformer")
    private String transformer;

    @JsonProperty("InputSteps")
    private List<Integer> inputSteps;

    @JsonProperty("BaseSources")
    private List<String> baseSources;

    // SourceName -> SourceTable: If a source is a TableSource, add to this map
    // Generic source location:
    // /Pods/{{POD_ID}}/Services/PropData/Sources/{{SOURCE_NAME}}
    // Table source location:
    // /Pods/{{POD_ID}}/Contracts/{{TENANT_ID}}/Tenants/{{TENANT_ID}}/Spaces/Production/Data/Tables/{{TABLE_NAME}}
    @JsonProperty("BaseTables")
    private Map<String, SourceTable> baseTables;

    // SourceName -> SourceIngestion: If a source is a IngestionSource, add to
    // this map
    // Generic source location:
    // /Pods/{{POD_ID}}/Services/PropData/Sources/{{SOURCE_NAME}}
    // Ingestion source location:
    // /Pods/{{POD_ID}}/Services/PropData/Ingestion/{{SOURCE_NAME}}
    @JsonProperty("BaseIngestions")
    private Map<String, SourceIngestion> baseIngestions;

    @JsonProperty("BaseVersions")
    private List<String> baseVersions;

    @JsonProperty("BaseTemplates")
    private List<String> baseTemplates;

    @JsonProperty("TargetSource")
    private String targetSource;

    @JsonProperty("TargetTable")
    private TargetTable targetTable;

    @JsonProperty("TargetVersion")
    private String targetVersion;

    @JsonProperty("TargetTemplate")
    private String targetTemplate;

    @JsonProperty("StepType")
    private String stepType = SIMPLE;

    @JsonProperty("NoInput")
    private Boolean noInput = null;

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

    public Map<String, SourceTable> getBaseTables() {
        return baseTables;
    }

    public void setBaseTables(Map<String, SourceTable> baseTables) {
        this.baseTables = baseTables;
    }

    public List<String> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<String> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public List<String> getBaseTemplates() {
        return baseTemplates;
    }

    public void setBaseTemplates(List<String> baseTemplates) {
        this.baseTemplates = baseTemplates;
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

    public String getStepType() {
        return stepType;
    }

    public void setStepType(String stepType) {
        this.stepType = stepType;
    }

    public TargetTable getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(TargetTable targetTable) {
        this.targetTable = targetTable;
    }

    public boolean getNoInput() {
        //consider null as false
        return Boolean.TRUE.equals(noInput);
    }

    public void setNoInput(boolean noInput) {
        this.noInput = noInput;
    }

    public Map<String, SourceIngestion> getBaseIngestions() {
        return baseIngestions;
    }

    public void setBaseIngestions(Map<String, SourceIngestion> baseIngestions) {
        this.baseIngestions = baseIngestions;
    }
}
