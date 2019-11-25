package com.latticeengines.domain.exposed.datacloud.transformation.step;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Transformation
 * up to date if there is any new change
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransformationStepConfig {

    public static final String SIMPLE = "Simple";
    public static final String ITERATIVE = "Iterative";

    // Transformer name for the pipeline step which identifies which job to run
    @JsonProperty("Transformer")
    private String transformer;

    // A pipeline step has 2 types of input source:
    // -- Output of preceding step within same pipeline by providing step
    // sequence number
    // -- Source with specified name
    // "InputSteps" is a list of step sequence numbers which is the 1st type of
    // input source
    // When transformation dataflow takes input list, 1st type of input sources
    // are listed before 2nd type of input sources
    @JsonProperty("InputSteps")
    private List<Integer> inputSteps;

    // "BaseSources" is a list of source names is the 2nd type of input source
    // (See comments for "InputSteps" for the other type of input source)
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

    // If provided, read base sources with specified base versions
    // If not provided, read base sources with current version
    @JsonProperty("BaseVersions")
    private List<String> baseVersions;

    @JsonProperty("BaseTemplates")
    private List<String> baseTemplates;

    // If output of the step needs to be persisted, provide target source name
    // for output
    // If not provided, output of the step could be used as input for subsequent
    // steps within same pipeline, and will be deleted when the pipeline job is
    // finished if "KeepTemp" is false
    @JsonProperty("TargetSource")
    private String targetSource;

    // If output of the step is a table source, provide TargetTable instead of
    // TargetSource
    @JsonProperty("TargetTable")
    private TargetTable targetTable;

    // partition keys used in target table source
    @JsonProperty("TargetPartitionKeys")
    private List<String> targetPartitionKeys;

    // If provided, target source is generated with specified version
    // If not provided (general use case), target source is generated with
    // pipeline version
    @JsonProperty("TargetVersion")
    private String targetVersion;

    @JsonProperty("TargetTemplate")
    private String targetTemplate;

    // Simple: The pipeline step runs once
    // Iterative: The pipeline step runs repeatedly until the output count is no
    // longer changed
    @JsonProperty("StepType")
    private String stepType = SIMPLE;

    // This transformer doesn't need input. "BaseSources", "BaseTables",
    // "BaseIngestions" and "BaseVersions" are all ignored
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

    public List<String> getTargetPartitionKeys() {
        return targetPartitionKeys;
    }

    public void setTargetPartitionKeys(List<String> targetPartitionKeys) {
        this.targetPartitionKeys = targetPartitionKeys;
    }
}
