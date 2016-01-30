package com.latticeengines.domain.exposed.propdata.manage;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchInput {

    private List<MatchKey> keys;
    private List<List<Object>> data;
    private Tenant tenant;

    // only one of these is needed, custom selection has higher priority
    private ColumnSelection.Predefined predefinedSelection;
    private ColumnSelection customSelection;

    // if not provided, pick latest
    private String predefinedVersion;

    @JsonProperty("Keys")
    public List<MatchKey> getKeys() {
        return keys;
    }

    @JsonProperty("Keys")
    public void setKeys(List<MatchKey> keys) {
        this.keys = keys;
    }

    @JsonProperty("Data")
    public List<List<Object>> getData() {
        return data;
    }

    @JsonProperty("Data")
    public void setData(List<List<Object>> data) {
        this.data = data;
    }

    @JsonProperty("Tenant")
    public Tenant getTenant() {
        return tenant;
    }

    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

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

}
