package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransformationFlowParameters extends DataFlowParameters {

    public static final String ENGINE_CONFIG = "EngineConfig";

    @JsonProperty("ConfJsonPath")
    private String confJsonPath;

    @JsonProperty("ConfJson")
    private String confJson;

    @JsonProperty("TimestampField")
    private String timestampField;

    @JsonProperty("PrimaryKeys")
    private List<String> primaryKeys;

    @JsonProperty("BaseTables")
    private List<String> baseTables;

    @JsonProperty("Columns")
    private List<SourceColumn> columns;

    @JsonProperty("BaseSourceColumns")
    private List<List<SourceColumn>> baseSourceColumns;

    @JsonProperty("Timestamp")
    private Date timestamp;

    @JsonProperty("TemplateSourceMap")
    private Map<String, String> templateSourceMap;

    @JsonProperty(ENGINE_CONFIG)
    private EngineConfiguration engineConfiguration;

    public String getConfJsonPath() {
        return confJsonPath;
    }

    public void setConfJsonPath(String confJsonPath) {
        this.confJsonPath = confJsonPath;
    }

    public String getConfJson() {
        return confJson;
    }

    public void setConfJson(String confJson) {
        this.confJson = confJson;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<String> getBaseTables() {
        return baseTables;
    }

    public void setBaseTables(List<String> baseTables) {
        this.baseTables = baseTables;
    }

    public List<SourceColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<SourceColumn> columns) {
        this.columns = columns;
    }

    public Map<String, String> getTemplateSourceMap() {
        return templateSourceMap;
    }

    public void setTemplateSourceMap(Map<String, String> templateSourceMap) {
        this.templateSourceMap = templateSourceMap;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public List<List<SourceColumn>> getBaseSourceColumns() {
        return baseSourceColumns;
    }

    public void setBaseSourceColumns(List<List<SourceColumn>> baseSourceColumns) {
        this.baseSourceColumns = baseSourceColumns;
    }

    public EngineConfiguration getEngineConfiguration() {
        return engineConfiguration;
    }

    public void setEngineConfiguration(EngineConfiguration engineConfiguration) {
        this.engineConfiguration = engineConfiguration;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class EngineConfiguration {

        @JsonProperty("Engine")
        private String engine;

        @JsonProperty("JobProps")
        private Map<String, String> jobProperties;

        @JsonProperty("Partitions")
        private Integer partitions;

        public String getEngine() {
            return engine;
        }

        public void setEngine(String engine) {
            this.engine = engine;
        }

        public Map<String, String> getJobProperties() {
            return jobProperties;
        }

        public void setJobProperties(Map<String, String> jobProperties) {
            this.jobProperties = jobProperties;
        }

        public Integer getPartitions() {
            return partitions;
        }

        public void setPartitions(Integer partitions) {
            this.partitions = partitions;
        }
    }
}
