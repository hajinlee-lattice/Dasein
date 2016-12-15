package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class TransformationFlowParameters extends DataFlowParameters {

    @JsonProperty("ConfJsonPath")
    private String confJsonPath;

    @JsonProperty("ConfJson")
    private String confJson;

    @JsonProperty("TimestampField")
    private String timestampField;

    @JsonProperty("PrimaryKeys")
    private List<String> primaryKeys;

    @JsonProperty("FakedCurrentTime")
    private List<String> baseTables;

    @JsonProperty("Columns")
    private List<SourceColumn> columns;

    @JsonProperty("BaseSourceColumns")
    private List<List<SourceColumn>> baseSourceColumns;

    @JsonProperty("Timestamp")
    private Date timestamp;

    @JsonProperty("TemplateSourceMap")
    private Map<String, String> templateSourceMap;

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
}
