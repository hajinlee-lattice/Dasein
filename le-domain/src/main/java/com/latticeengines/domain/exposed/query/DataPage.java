package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPage {
    @JsonProperty("metadata")
    private Map<String, ColumnMetadata> metadata;

    @JsonProperty("data")
    private List<Map<String, Object>> data;

    public DataPage(List<Map<String, Object>> data) {
        this.data = data;
    }

    public DataPage() {
        this.data = new ArrayList<>();
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public Map<String, ColumnMetadata> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, ColumnMetadata> metadata) {
        this.metadata = metadata;
    }
}
