package com.latticeengines.domain.exposed.propdata.match;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchInput implements Fact, Dimension {

    private Tenant tenant;

    private List<String> fields;
    private List<List<Object>> data;

    // optional, but better to provide. if not, will be resolved from the fields
    private Map<MatchKey, String> keyMap;

    // only one of these is needed, custom selection has higher priority
    private ColumnSelection.Predefined predefinedSelection;
    private ColumnSelection customSelection;

    // if not provided, pick latest
    private String predefinedVersion;

    private String matchEngine;
    private Integer numSelectedColumns;

    @JsonProperty("KeyMap")
    public Map<MatchKey, String> getKeyMap() {
        return keyMap;
    }

    @JsonProperty("KeyMap")
    public void setKeyMap(Map<MatchKey, String> keyMap) {
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
    }

    @JsonIgnore
    @MetricField(name = "InputRows", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumRows() {
        return getData().size();
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
}
