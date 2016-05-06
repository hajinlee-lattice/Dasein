package com.latticeengines.domain.exposed.propdata.match;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchInput implements Fact, Dimension {

    private Tenant tenant;

    private List<String> fields;
    private List<List<Object>> data;
    private int numRows;

    private Boolean returnUnmatched = true;

    private InputBuffer inputBuffer;
    private IOBufferType outputBufferType;

    // optional, but better to provide. if not, will be resolved from the fields
    private Map<MatchKey, List<String>> keyMap;

    // only one of these is needed, custom selection has higher priority
    private ColumnSelection.Predefined predefinedSelection;
    private ColumnSelection customSelection;

    // if not provided, pick latest
    private String predefinedVersion;

    private String matchEngine;
    private Integer numSelectedColumns;

    // only applicable to bulk match
    private String yarnQueue;

    private UUID uuid;

    @JsonProperty("ReturnUnmatched")
    public Boolean getReturnUnmatched() {
        return returnUnmatched;
    }

    @JsonProperty("ReturnUnmatched")
    public void setReturnUnmatched(Boolean returnUnmatched) {
        this.returnUnmatched = returnUnmatched;
    }

    @JsonProperty("KeyMap")
    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

    @JsonProperty("KeyMap")
    public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
        this.keyMap = keyMap;
    }

    @JsonProperty("Fields")
    public List<String> getFields() {
        return fields;
    }

    @JsonProperty("Fields")
    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @JsonIgnore
    @MetricField(name = "InputFields", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumInputFields() {
        return getFields().size();
    }

    @JsonProperty("Data")
    public List<List<Object>> getData() {
        return data;
    }

    @JsonProperty("Data")
    public void setData(List<List<Object>> data) {
        this.data = data;
        setNumRows(data.size());
    }

    @JsonProperty("InputBuffer")
    public InputBuffer getInputBuffer() {
        return inputBuffer;
    }

    @JsonProperty("InputBuffer")
    public void setInputBuffer(InputBuffer buffer) {
        this.inputBuffer = buffer;
    }

    @JsonProperty("OutputBufferType")
    public IOBufferType getOutputBufferType() {
        return outputBufferType;
    }

    @JsonProperty("OutputBufferType")
    public void setOutputBufferType(IOBufferType outputBufferType) {
        this.outputBufferType = outputBufferType;
    }

    @JsonIgnore
    @MetricField(name = "InputRows", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumRows() {
        return numRows;
    }

    @JsonIgnore
    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    @MetricTagGroup
    @JsonProperty("Tenant")
    public Tenant getTenant() {
        return tenant;
    }

    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @MetricTagGroup
    @JsonProperty("PredefinedSelection")
    public ColumnSelection.Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    @JsonProperty("PredefinedSelection")
    public void setPredefinedSelection(ColumnSelection.Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
    }

    @JsonProperty("PredefinedVersion")
    public String getPredefinedVersion() {
        return predefinedVersion;
    }

    @JsonProperty("PredefinedVersion")
    public void setPredefinedVersion(String predefinedVersion) {
        this.predefinedVersion = predefinedVersion;
    }

    @JsonProperty("CustomSelection")
    public ColumnSelection getCustomSelection() {
        return customSelection;
    }

    @JsonProperty("CustomSelection")
    public void setCustomSelection(ColumnSelection customSelection) {
        this.customSelection = customSelection;
    }

    @MetricTag(tag = "MatchEngine")
    @JsonIgnore
    public String getMatchEngine() {
        return matchEngine;
    }

    @JsonIgnore
    public void setMatchEngine(String matchEngine) {
        this.matchEngine = matchEngine;
    }

    @JsonIgnore
    @MetricField(name = "SelectedColumns", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumSelectedColumns() {
        return numSelectedColumns;
    }

    @JsonIgnore
    public void setNumSelectedColumns(Integer numSelectedColumns) {
        this.numSelectedColumns = numSelectedColumns;
    }

    @JsonIgnore
    public UUID getUuid() {
        return uuid;
    }

    @JsonIgnore
    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @JsonProperty("YarnQueue")
    public String getYarnQueue() {
        return yarnQueue;
    }

    @JsonProperty("YarnQueue")
    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
